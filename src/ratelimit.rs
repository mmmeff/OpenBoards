use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sqlx::Row;

use crate::db::{Database, Dialect};
use crate::error::{BoardError, BoardResult, ErrorCode};

#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Clone)]
pub struct InMemoryLimiter {
    rps: f64,
    burst: f64,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

/// Per-principal token-bucket rate limiter.
///
/// All profiles of a principal share one bucket (matching Board's per-login/deployment limit).
/// - SQLite (`InMemory`): process-local mutex map. ponytail: ceiling is unbounded entries across
///   millions of unique principals (upgrade path: periodic idle-bucket eviction).
/// - Postgres (`PostgresSql`): single-statement atomic upsert into `UNLOGGED TABLE rate_limit_buckets`
///   using integer milli-tokens and Postgres `clock_timestamp()` (zero app-node clock skew).
///   ponytail: single-row contention ceiling ~5,000 req/s for a single principal (upgrade path:
///   local token-batch reservation or Redis).
#[derive(Clone)]
pub enum RateLimiter {
    InMemory(InMemoryLimiter),
    PostgresSql {
        db: Database,
        rps: i64,
        burst_milli: i64,
    },
}

const PG_RATE_LIMIT_SQL: &str = r#"
WITH now_cte AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
)
INSERT INTO rate_limit_buckets (principal_id, tokens_milli, last_refill_ms, last_allowed)
SELECT $1, ($3 - 1000), now_ms, 1 FROM now_cte
ON CONFLICT (principal_id) DO UPDATE SET
    tokens_milli = CASE
        WHEN LEAST(
            $3,
            rate_limit_buckets.tokens_milli
                + LEAST(
                    GREATEST(0::BIGINT, (SELECT now_ms FROM now_cte) - rate_limit_buckets.last_refill_ms),
                    ($3 / $2) + 1
                  ) * $2
        ) >= 1000
        THEN LEAST(
            $3,
            rate_limit_buckets.tokens_milli
                + LEAST(
                    GREATEST(0::BIGINT, (SELECT now_ms FROM now_cte) - rate_limit_buckets.last_refill_ms),
                    ($3 / $2) + 1
                  ) * $2
        ) - 1000
        ELSE LEAST(
            $3,
            rate_limit_buckets.tokens_milli
                + LEAST(
                    GREATEST(0::BIGINT, (SELECT now_ms FROM now_cte) - rate_limit_buckets.last_refill_ms),
                    ($3 / $2) + 1
                  ) * $2
        )
    END,
    last_refill_ms = GREATEST(rate_limit_buckets.last_refill_ms, (SELECT now_ms FROM now_cte)),
    last_allowed = CASE
        WHEN LEAST(
            $3,
            rate_limit_buckets.tokens_milli
                + LEAST(
                    GREATEST(0::BIGINT, (SELECT now_ms FROM now_cte) - rate_limit_buckets.last_refill_ms),
                    ($3 / $2) + 1
                  ) * $2
        ) >= 1000
        THEN 1
        ELSE 0
    END
RETURNING last_allowed, tokens_milli
"#;

impl RateLimiter {
    pub fn new(rps: u32, burst: u32) -> Self {
        Self::InMemory(InMemoryLimiter {
            rps: f64::from(rps),
            burst: f64::from(burst.max(1)),
            buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn for_database(db: &Database, rps: u32, burst: u32) -> Self {
        match db.dialect() {
            Dialect::Sqlite => Self::new(rps, burst),
            Dialect::Postgres => Self::PostgresSql {
                db: db.clone(),
                rps: i64::from(rps),
                burst_milli: i64::from(burst.max(1)) * 1000,
            },
        }
    }

    pub fn is_enabled(&self) -> bool {
        match self {
            Self::InMemory(mem) => mem.rps > 0.0,
            Self::PostgresSql { rps, .. } => *rps > 0,
        }
    }

    pub async fn check(&self, principal_id: &str) -> BoardResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        match self {
            Self::InMemory(mem) => {
                let now = Instant::now();
                let mut map = mem
                    .buckets
                    .lock()
                    .map_err(|_| BoardError::internal("rate limiter mutex poisoned"))?;
                let bucket = map.entry(principal_id.to_string()).or_insert(Bucket {
                    tokens: mem.burst,
                    last_refill: now,
                });

                let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
                bucket.tokens = (bucket.tokens + elapsed * mem.rps).min(mem.burst);
                bucket.last_refill = now;

                if bucket.tokens >= 1.0 {
                    bucket.tokens -= 1.0;
                    Ok(())
                } else {
                    let deficit = 1.0 - bucket.tokens;
                    let retry_after = (deficit / mem.rps).ceil().max(1.0) as u64;
                    Err(BoardError::new(
                        ErrorCode::RateLimited,
                        "rate limit exceeded for principal",
                    )
                    .with_retry_after(retry_after)
                    .with_hint(format!("Retry after {retry_after} second(s).")))
                }
            }
            Self::PostgresSql {
                db,
                rps,
                burst_milli,
            } => {
                let pool = db
                    .pg_pool()
                    .ok_or_else(|| BoardError::internal("postgres pool missing for rate limiter"))?;
                let row = sqlx::query(PG_RATE_LIMIT_SQL)
                    .bind(principal_id)
                    .bind(*rps)
                    .bind(*burst_milli)
                    .fetch_one(pool)
                    .await?;
                let allowed: i64 = row.try_get(0)?;
                let tokens_milli: i64 = row.try_get(1)?;
                if allowed != 0 {
                    Ok(())
                } else {
                    let deficit_milli = (1000 - tokens_milli).max(1);
                    let denom = (*rps).saturating_mul(1000).max(1);
                    let retry_after = ((deficit_milli + denom - 1) / denom).max(1) as u64;
                    Err(BoardError::new(
                        ErrorCode::RateLimited,
                        "rate limit exceeded for principal",
                    )
                    .with_retry_after(retry_after)
                    .with_hint(format!("Retry after {retry_after} second(s).")))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enforces_burst_and_reports_retry_after() {
        let rl = RateLimiter::new(1, 2);
        assert!(rl.check("user:u1").await.is_ok());
        assert!(rl.check("user:u1").await.is_ok());
        let err = rl.check("user:u1").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::RateLimited);
        assert!(err.retry_after_secs.unwrap() >= 1);
        // Independent principal still has full burst
        assert!(rl.check("user:u2").await.is_ok());
    }

    #[tokio::test]
    async fn zero_rps_disables_limiting() {
        let rl = RateLimiter::new(0, 1);
        for _ in 0..50 {
            assert!(rl.check("user:u1").await.is_ok());
        }
    }
}
