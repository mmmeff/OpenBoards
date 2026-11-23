use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::error::{BoardError, BoardResult};

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "001_init",
        sql: r#"
            CREATE TABLE IF NOT EXISTS server_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        "#,
    },
    Migration {
        version: 2,
        name: "002_auth_actors",
        sql: r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                username_lower TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                email_lower TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                avatar TEXT,
                revoked_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                name_lower TEXT NOT NULL UNIQUE,
                revoked_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS apps (
                id TEXT PRIMARY KEY,
                slug TEXT NOT NULL,
                slug_lower TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                scopes TEXT NOT NULL,
                revoked_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tokens (
                id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL UNIQUE,
                principal_kind TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                environment TEXT,
                label TEXT,
                revoked_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tokens_principal
                ON tokens (principal_kind, principal_id);

            CREATE TABLE IF NOT EXISTS profiles (
                principal_id TEXT NOT NULL,
                name_lower TEXT NOT NULL,
                name TEXT NOT NULL,
                avatar TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (principal_id, name_lower)
            );

            CREATE TABLE IF NOT EXISTS actor_activity (
                board_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (board_id, channel, actor_id)
            );
            CREATE INDEX IF NOT EXISTS idx_actor_activity_board
                ON actor_activity (board_id);
        "#,
    },
    Migration {
        version: 3,
        name: "003_boards_channels_grants",
        sql: r#"
            CREATE TABLE IF NOT EXISTS boards (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                name_lower TEXT NOT NULL UNIQUE,
                next_grant_seq INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (board_id, path)
            );
            CREATE INDEX IF NOT EXISTS idx_channels_board
                ON channels (board_id);

            CREATE TABLE IF NOT EXISTS grants (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                id TEXT NOT NULL,
                principals_json TEXT NOT NULL,
                can_json TEXT NOT NULL,
                created_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (board_id, seq),
                UNIQUE (board_id, id)
            );
            CREATE INDEX IF NOT EXISTS idx_grants_board
                ON grants (board_id);
        "#,
    },
    Migration {
        version: 4,
        name: "004_threads_messages",
        sql: r#"
            ALTER TABLE boards ADD COLUMN next_msg_seq INTEGER NOT NULL DEFAULT 1;

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                channel TEXT NOT NULL,
                re TEXT,
                reply_to TEXT,
                text TEXT NOT NULL,
                by_actor TEXT NOT NULL,
                by_principal TEXT NOT NULL,
                by_profile TEXT,
                from_source TEXT,
                no_sync INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 1,
                deleted_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (board_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_messages_board_channel_seq
                ON messages (board_id, channel, seq);
            CREATE INDEX IF NOT EXISTS idx_messages_board_re_seq
                ON messages (board_id, re, seq);

            CREATE TABLE IF NOT EXISTS message_reactions (
                board_id TEXT NOT NULL,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                emoji TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (message_id, actor_id, emoji)
            );
            CREATE INDEX IF NOT EXISTS idx_message_reactions_board
                ON message_reactions (board_id, message_id);

            CREATE TABLE IF NOT EXISTS message_mentions (
                board_id TEXT NOT NULL,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                actor_id TEXT NOT NULL,
                PRIMARY KEY (message_id, actor_id)
            );
            CREATE INDEX IF NOT EXISTS idx_message_mentions_board_actor
                ON message_mentions (board_id, actor_id);

            CREATE TABLE IF NOT EXISTS idempotency_keys (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                caller_actor TEXT NOT NULL,
                key TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                channel TEXT NOT NULL,
                thread_root TEXT,
                message_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (board_id, caller_actor, key)
            );
        "#,
    },
    Migration {
        version: 5,
        name: "005_labels_and_schemas",
        sql: r#"
            CREATE TABLE IF NOT EXISTS message_labels (
                board_id TEXT NOT NULL,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (message_id, key)
            );
            CREATE INDEX IF NOT EXISTS idx_message_labels_board_kv
                ON message_labels (board_id, key, value);

            CREATE TABLE IF NOT EXISTS schemas (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version INTEGER NOT NULL,
                description TEXT,
                schema_json TEXT NOT NULL,
                created_by TEXT,
                created_at TEXT NOT NULL,
                UNIQUE (name, version)
            );

            CREATE TABLE IF NOT EXISTS message_blocks (
                board_id TEXT NOT NULL,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                idx INTEGER NOT NULL,
                schema_name TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                PRIMARY KEY (message_id, idx)
            );
            CREATE INDEX IF NOT EXISTS idx_message_blocks_board
                ON message_blocks (board_id, message_id);
        "#,
    },
    Migration {
        version: 6,
        name: "006_attachments",
        sql: r#"
            CREATE TABLE IF NOT EXISTS attachments (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                content_type TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                sha256 TEXT NOT NULL DEFAULT '',
                uploader_actor TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_attachments_board
                ON attachments (board_id);

            CREATE TABLE IF NOT EXISTS message_attachments (
                board_id TEXT NOT NULL,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                attachment_id TEXT NOT NULL REFERENCES attachments(id),
                idx INTEGER NOT NULL,
                PRIMARY KEY (message_id, attachment_id)
            );
            CREATE INDEX IF NOT EXISTS idx_message_attachments_att
                ON message_attachments (board_id, attachment_id);
        "#,
    },
    Migration {
        version: 7,
        name: "007_events",
        sql: r#"
            ALTER TABLE boards ADD COLUMN next_event_seq INTEGER NOT NULL DEFAULT 1;

            CREATE TABLE IF NOT EXISTS events (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                cursor TEXT NOT NULL,
                event_type TEXT NOT NULL,
                channel TEXT,
                message_id TEXT,
                actor_id TEXT,
                bookkeeping INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                before_snapshot_json TEXT,
                after_snapshot_json TEXT,
                after_message_json TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (board_id, seq),
                UNIQUE (board_id, cursor)
            );
            CREATE INDEX IF NOT EXISTS idx_events_board_msg
                ON events (board_id, message_id);
        "#,
    },
    Migration {
        version: 8,
        name: "008_tasks",
        sql: r#"
            CREATE TABLE IF NOT EXISTS tasks (
                message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                seq INTEGER NOT NULL,
                state TEXT NOT NULL,
                owner_actor TEXT NOT NULL,
                assigned_for TEXT,
                claimed_by TEXT,
                claim_epoch INTEGER NOT NULL DEFAULT 0,
                claim_token TEXT,
                claim_until TEXT,
                block_reason TEXT,
                block_kind TEXT,
                result TEXT,
                fail_reason TEXT,
                cancel_reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_board_state_seq
                ON tasks (board_id, state, seq);
            CREATE INDEX IF NOT EXISTS idx_tasks_board_channel_state
                ON tasks (board_id, channel, state, seq);
            CREATE INDEX IF NOT EXISTS idx_tasks_claimed_until
                ON tasks (state, claim_until);

            CREATE TABLE IF NOT EXISTS task_needs (
                board_id TEXT NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(message_id) ON DELETE CASCADE,
                needs_task_id TEXT NOT NULL REFERENCES tasks(message_id) ON DELETE CASCADE,
                PRIMARY KEY (task_id, needs_task_id)
            );
            CREATE INDEX IF NOT EXISTS idx_task_needs_reverse
                ON task_needs (board_id, needs_task_id);

            CREATE TABLE IF NOT EXISTS task_batches (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                caller_actor TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                channel TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                task_ids_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (board_id, caller_actor, idempotency_key)
            );
        "#,
    },
    Migration {
        version: 9,
        name: "009_schedules",
        sql: r#"
            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                re TEXT,
                text TEXT NOT NULL,
                labels_json TEXT NOT NULL,
                attachments_json TEXT NOT NULL,
                schema_id TEXT,
                no_sync INTEGER NOT NULL DEFAULT 0,
                creator_actor TEXT NOT NULL,
                creator_principal TEXT NOT NULL,
                creator_profile TEXT,
                creator_avatar TEXT,
                every_secs INTEGER,
                max_times INTEGER,
                until_at TEXT,
                next_run_at TEXT NOT NULL,
                runs_completed INTEGER NOT NULL DEFAULT 0,
                last_run_at TEXT,
                last_message_id TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_schedules_due
                ON schedules (status, next_run_at);
            CREATE INDEX IF NOT EXISTS idx_schedules_board_thread
                ON schedules (board_id, re, status, next_run_at);
            CREATE INDEX IF NOT EXISTS idx_schedules_board_channel
                ON schedules (board_id, channel, status, next_run_at);
        "#,
    },
];

/// SQLite database wrapper in WAL mode with foreign keys enabled.
///
/// ponytail: single Mutex<Connection> keeps SQLite access simple with zero pool crate overhead.
/// Ceiling: concurrent read throughput under heavy read load; upgrade path is a separate
/// read-only connection pool (e.g. r2d2/deadpool) while keeping serialized writes.
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn open(path: &Path) -> BoardResult<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    BoardError::internal(format!(
                        "failed to create data directory '{}': {e}",
                        parent.display()
                    ))
                })?;
            }
        }
        let conn = Connection::open(path)?;
        Self::init(conn, true)
    }

    pub fn open_in_memory() -> BoardResult<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn, false)
    }

    fn init(mut conn: Connection, enable_wal: bool) -> BoardResult<Self> {
        if enable_wal {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
        }
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        run_migrations(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn with_conn<T, F>(&self, f: F) -> BoardResult<T>
    where
        F: FnOnce(&Connection) -> BoardResult<T>,
    {
        let guard = self
            .conn
            .lock()
            .map_err(|_| BoardError::internal("database mutex poisoned"))?;
        f(&guard)
    }

    pub fn with_tx<T, F>(&self, f: F) -> BoardResult<T>
    where
        F: FnOnce(&Transaction<'_>) -> BoardResult<T>,
    {
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| BoardError::internal("database mutex poisoned"))?;
        let tx = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn schema_version(&self) -> BoardResult<i64> {
        self.with_conn(|conn| {
            let version: i64 = conn.query_row(
                "SELECT COALESCE(MAX(version), 0) FROM _migrations",
                [],
                |row| row.get(0),
            )?;
            Ok(version)
        })
    }
}

fn run_migrations(conn: &mut Connection) -> BoardResult<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS _migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );
        "#,
    )?;

    for migration in MIGRATIONS {
        let applied: bool = conn
            .query_row(
                "SELECT 1 FROM _migrations WHERE version = ?1",
                rusqlite::params![migration.version],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if applied {
            continue;
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(migration.sql)?;
        tx.execute(
            "INSERT INTO _migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![migration.version, migration.name, Utc::now().to_rfc3339()],
        )?;
        tx.commit()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_file_db_with_wal_and_foreign_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("openboards.db");
        let db = Database::open(&db_path).unwrap();
        let expected_version = MIGRATIONS.iter().map(|m| m.version).max().unwrap_or(0);

        assert_eq!(db.schema_version().unwrap(), expected_version);

        db.with_conn(|conn| {
            let journal_mode: String =
                conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            let foreign_keys: i64 =
                conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
            assert_eq!(journal_mode.to_lowercase(), "wal");
            assert_eq!(foreign_keys, 1);
            Ok(())
        })
        .unwrap();

        // Reopening is idempotent
        let db2 = Database::open(&db_path).unwrap();
        assert_eq!(db2.schema_version().unwrap(), expected_version);
    }

    #[test]
    fn initializes_in_memory_db_with_foreign_keys() {
        let db = Database::open_in_memory().unwrap();
        let expected_version = MIGRATIONS.iter().map(|m| m.version).max().unwrap_or(0);
        assert_eq!(db.schema_version().unwrap(), expected_version);
        db.with_conn(|conn| {
            let foreign_keys: i64 =
                conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
            assert_eq!(foreign_keys, 1);
            Ok(())
        })
        .unwrap();
    }
}
