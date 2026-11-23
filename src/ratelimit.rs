use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::error::{BoardError, BoardResult, ErrorCode};

#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Per-principal token-bucket rate limiter.
///
/// All profiles of a principal share one bucket (matching Board's per-login/deployment limit).
/// ponytail: in-memory HashMap keyed by principal_id; ceiling is unbounded entries across
/// millions of unique principals (upgrade path: periodic idle-bucket eviction).
#[derive(Clone)]
pub struct RateLimiter {
    rps: f64,
    burst: f64,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

impl RateLimiter {
    pub fn new(rps: u32, burst: u32) -> Self {
        Self {
            rps: f64::from(rps),
            burst: f64::from(burst.max(1)),
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.rps > 0.0
    }

    pub fn check(&self, principal_id: &str) -> BoardResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        let now = Instant::now();
        let mut map = self
            .buckets
            .lock()
            .map_err(|_| BoardError::internal("rate limiter mutex poisoned"))?;
        let bucket = map.entry(principal_id.to_string()).or_insert(Bucket {
            tokens: self.burst,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rps).min(self.burst);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let deficit = 1.0 - bucket.tokens;
            let retry_after = (deficit / self.rps).ceil().max(1.0) as u64;
            Err(BoardError::new(
                ErrorCode::RateLimited,
                "rate limit exceeded for principal",
            )
            .with_retry_after(retry_after)
            .with_hint(format!("Retry after {retry_after} second(s).")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_burst_and_reports_retry_after() {
        let rl = RateLimiter::new(1, 2);
        assert!(rl.check("user:u1").is_ok());
        assert!(rl.check("user:u1").is_ok());
        let err = rl.check("user:u1").unwrap_err();
        assert_eq!(err.code, ErrorCode::RateLimited);
        assert!(err.retry_after_secs.unwrap() >= 1);
        // Independent principal still has full burst
        assert!(rl.check("user:u2").is_ok());
    }

    #[test]
    fn zero_rps_disables_limiting() {
        let rl = RateLimiter::new(0, 1);
        for _ in 0..50 {
            assert!(rl.check("user:u1").is_ok());
        }
    }
}
