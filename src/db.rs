use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, Transaction, TransactionBehavior};

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

    let current: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM _migrations",
        [],
        |row| row.get(0),
    )?;

    for migration in MIGRATIONS {
        if migration.version <= current {
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

        assert_eq!(db.schema_version().unwrap(), MIGRATIONS.len() as i64);

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
        assert_eq!(db2.schema_version().unwrap(), MIGRATIONS.len() as i64);
    }

    #[test]
    fn initializes_in_memory_db_with_foreign_keys() {
        let db = Database::open_in_memory().unwrap();
        assert_eq!(db.schema_version().unwrap(), MIGRATIONS.len() as i64);
        db.with_conn(|conn| {
            let foreign_keys: i64 =
                conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
            assert_eq!(foreign_keys, 1);
            Ok(())
        })
        .unwrap();
    }
}
