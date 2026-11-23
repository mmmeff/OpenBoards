use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior};

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
    Migration {
        version: 10,
        name: "010_watchers",
        sql: r#"
            CREATE TABLE IF NOT EXISTS watchers (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                owner_principal TEXT NOT NULL,
                id TEXT NOT NULL,
                tail_expr TEXT NOT NULL,
                filter_json TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                secret TEXT,
                last_cursor INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (board_id, owner_principal, id)
            );
            CREATE INDEX IF NOT EXISTS idx_watchers_owner
                ON watchers (owner_principal);
            CREATE INDEX IF NOT EXISTS idx_watchers_board
                ON watchers (board_id);

            CREATE TABLE IF NOT EXISTS watcher_deliveries (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                owner_principal TEXT NOT NULL,
                watcher_id TEXT NOT NULL,
                event_seq INTEGER NOT NULL,
                event_cursor TEXT NOT NULL,
                delivery_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_http_status INTEGER,
                last_error TEXT,
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (board_id, owner_principal, watcher_id, event_seq),
                FOREIGN KEY (board_id, owner_principal, watcher_id)
                    REFERENCES watchers (board_id, owner_principal, id)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_watcher_deliveries_watcher
                ON watcher_deliveries (board_id, owner_principal, watcher_id, event_seq);
            CREATE INDEX IF NOT EXISTS idx_watcher_deliveries_status
                ON watcher_deliveries (status, next_attempt_at);
        "#,
    },
    Migration {
        version: 11,
        name: "011_sync",
        sql: r#"
            CREATE TABLE IF NOT EXISTS sync_sources (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                key TEXT NOT NULL,
                project_id TEXT NOT NULL,
                project_name TEXT NOT NULL,
                environment TEXT NOT NULL,
                owner_principal TEXT NOT NULL,
                platform TEXT NOT NULL,
                direction TEXT NOT NULL,
                mappings_json TEXT NOT NULL,
                webhook_secret TEXT,
                outbound_url TEXT,
                outbound_token TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (board_id, project_id, key)
            );
            CREATE INDEX IF NOT EXISTS idx_sync_sources_board
                ON sync_sources (board_id);
            CREATE INDEX IF NOT EXISTS idx_sync_sources_key
                ON sync_sources (key);

            CREATE TABLE IF NOT EXISTS thread_sync_links (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                thread_root_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                source_id TEXT NOT NULL REFERENCES sync_sources(id) ON DELETE CASCADE,
                platform TEXT NOT NULL,
                platform_channel TEXT NOT NULL,
                platform_thread_id TEXT NOT NULL,
                platform_url TEXT,
                direction TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (board_id, source_id, platform_channel, platform_thread_id)
            );
            CREATE INDEX IF NOT EXISTS idx_thread_sync_links_root
                ON thread_sync_links (board_id, thread_root_id);
            CREATE INDEX IF NOT EXISTS idx_thread_sync_links_url
                ON thread_sync_links (board_id, platform_url);

            CREATE TABLE IF NOT EXISTS message_sync_map (
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                source_id TEXT NOT NULL REFERENCES sync_sources(id) ON DELETE CASCADE,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                platform_message_id TEXT NOT NULL,
                platform_url TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (board_id, source_id, message_id),
                UNIQUE (board_id, source_id, platform_message_id)
            );

            CREATE TABLE IF NOT EXISTS sync_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                source_id TEXT NOT NULL REFERENCES sync_sources(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                headers_json TEXT NOT NULL,
                body_json TEXT NOT NULL,
                received_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sync_inbox_source
                ON sync_inbox (board_id, source_id, id DESC);

            CREATE TABLE IF NOT EXISTS sync_deliveries (
                id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                source_id TEXT NOT NULL REFERENCES sync_sources(id) ON DELETE CASCADE,
                link_id TEXT NOT NULL REFERENCES thread_sync_links(id) ON DELETE CASCADE,
                message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_http_status INTEGER,
                last_error TEXT,
                next_attempt_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sync_deliveries_pending
                ON sync_deliveries (status, next_attempt_at);
        "#,
    },
    Migration {
        version: 12,
        name: "012_thread_summaries",
        sql: r#"
            CREATE TABLE IF NOT EXISTS threads (
                root_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
                board_id TEXT NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
                channel TEXT NOT NULL,
                root_seq INTEGER NOT NULL,
                last_seq INTEGER NOT NULL,
                last_id TEXT NOT NULL,
                last_reply_id TEXT,
                reply_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_threads_board_last_seq
                ON threads (board_id, last_seq DESC);
            CREATE INDEX IF NOT EXISTS idx_threads_board_channel_last_seq
                ON threads (board_id, channel, last_seq DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_board_channel_re_seq
                ON messages (board_id, channel, re, seq);

            INSERT OR IGNORE INTO threads (
                root_id, board_id, channel, root_seq, last_seq, last_id, last_reply_id, reply_count
            )
            SELECT
                r.id,
                r.board_id,
                r.channel,
                r.seq,
                COALESCE(
                    (SELECT MAX(m.seq) FROM messages m WHERE m.board_id = r.board_id AND m.re = r.id),
                    r.seq
                ),
                COALESCE(
                    (SELECT m.id FROM messages m WHERE m.board_id = r.board_id AND m.re = r.id ORDER BY m.seq DESC LIMIT 1),
                    r.id
                ),
                (SELECT m.id FROM messages m WHERE m.board_id = r.board_id AND m.re = r.id ORDER BY m.seq DESC LIMIT 1),
                (SELECT COUNT(*) FROM messages m WHERE m.board_id = r.board_id AND m.re = r.id)
            FROM messages r
            WHERE r.re IS NULL;
        "#,
    },
];

/// SQLite database wrapper in WAL mode with foreign keys enabled.
///
/// File-backed databases use a serialized write connection paired with a small pool of
/// read-only WAL connections so concurrent reads scale across CPU cores without blocking writes.
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
    read_conns: Option<Arc<Vec<Mutex<Connection>>>>,
    next_reader: Arc<AtomicUsize>,
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
        let db = Self::init(conn, true)?;

        let reader_count = std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, 8))
            .unwrap_or(4);
        let mut readers = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let rc = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            rc.pragma_update(None, "query_only", "ON")?;
            rc.pragma_update(None, "foreign_keys", "ON")?;
            rc.pragma_update(None, "busy_timeout", 5000)?;
            readers.push(Mutex::new(rc));
        }

        Ok(Self {
            conn: db.conn,
            read_conns: Some(Arc::new(readers)),
            next_reader: db.next_reader,
        })
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
            read_conns: None,
            next_reader: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn with_conn<T, F>(&self, f: F) -> BoardResult<T>
    where
        F: FnOnce(&Connection) -> BoardResult<T>,
    {
        if let Some(readers) = &self.read_conns {
            let start = self.next_reader.fetch_add(1, Ordering::Relaxed) % readers.len();
            for offset in 0..readers.len() {
                let idx = (start + offset) % readers.len();
                if let Ok(guard) = readers[idx].try_lock() {
                    return f(&guard);
                }
            }
            let guard = readers[start]
                .lock()
                .map_err(|_| BoardError::internal("read database mutex poisoned"))?;
            return f(&guard);
        }
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

/// Builds a comma-separated placeholder list (`?,?,...`) for `IN (...)` batch queries.
pub fn sql_placeholders(count: usize) -> String {
    if count == 0 {
        return String::new();
    }
    let mut s = String::with_capacity(count * 2 - 1);
    for i in 0..count {
        if i > 0 {
            s.push(',');
        }
        s.push('?');
    }
    s
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

        // Read-only WAL pool enforces query_only = ON
        let write_err = db2.with_conn(|conn| {
            conn.execute(
                "INSERT INTO server_meta (key, value) VALUES ('ro_test', '1')",
                [],
            )?;
            Ok(())
        });
        assert!(write_err.is_err());
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
