use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Postgres, Row, Sqlite, Transaction};

use crate::config::{DbTarget, DEFAULT_DB_MAX_CONNECTIONS};
use crate::error::{BoardError, BoardResult};

const STARTUP_LOCK_KEY: i64 = 67_108_864_198_201;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlArg<'a> {
    Text(Cow<'a, str>),
    OptText(Option<Cow<'a, str>>),
    Int(i64),
    OptInt(Option<i64>),
    Bytes(Cow<'a, [u8]>),
}

impl<'a> From<&'a str> for SqlArg<'a> {
    fn from(v: &'a str) -> Self {
        Self::Text(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a String> for SqlArg<'a> {
    fn from(v: &'a String) -> Self {
        Self::Text(Cow::Borrowed(v.as_str()))
    }
}

impl From<String> for SqlArg<'_> {
    fn from(v: String) -> Self {
        Self::Text(Cow::Owned(v))
    }
}

impl<'a> From<Option<&'a str>> for SqlArg<'a> {
    fn from(v: Option<&'a str>) -> Self {
        Self::OptText(v.map(Cow::Borrowed))
    }
}

impl<'a> From<&'a Option<String>> for SqlArg<'a> {
    fn from(v: &'a Option<String>) -> Self {
        Self::OptText(v.as_deref().map(Cow::Borrowed))
    }
}

impl From<Option<String>> for SqlArg<'_> {
    fn from(v: Option<String>) -> Self {
        Self::OptText(v.map(Cow::Owned))
    }
}

impl From<i64> for SqlArg<'_> {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<Option<i64>> for SqlArg<'_> {
    fn from(v: Option<i64>) -> Self {
        Self::OptInt(v)
    }
}

impl<'a> From<&'a [u8]> for SqlArg<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self::Bytes(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a Vec<u8>> for SqlArg<'a> {
    fn from(v: &'a Vec<u8>) -> Self {
        Self::Bytes(Cow::Borrowed(v.as_slice()))
    }
}

impl From<Vec<u8>> for SqlArg<'_> {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(Cow::Owned(v))
    }
}

#[macro_export]
macro_rules! sql_args {
    () => {
        &[] as &[$crate::db::SqlArg<'_>]
    };
    ($($arg:expr),+ $(,)?) => {
        &[$($crate::db::SqlArg::from($arg)),+] as &[$crate::db::SqlArg<'_>]
    };
}

pub enum DbRow {
    Sqlite(SqliteRow),
    Postgres(PgRow),
}

impl DbRow {
    pub fn get_str(&self, idx: usize) -> BoardResult<String> {
        match self {
            Self::Sqlite(r) => r.try_get(idx).map_err(BoardError::from),
            Self::Postgres(r) => r.try_get(idx).map_err(BoardError::from),
        }
    }

    pub fn get_opt_str(&self, idx: usize) -> BoardResult<Option<String>> {
        match self {
            Self::Sqlite(r) => r.try_get(idx).map_err(BoardError::from),
            Self::Postgres(r) => r.try_get(idx).map_err(BoardError::from),
        }
    }

    pub fn get_i64(&self, idx: usize) -> BoardResult<i64> {
        match self {
            Self::Sqlite(r) => r.try_get(idx).map_err(BoardError::from),
            Self::Postgres(r) => r.try_get(idx).map_err(BoardError::from),
        }
    }

    pub fn get_opt_i64(&self, idx: usize) -> BoardResult<Option<i64>> {
        match self {
            Self::Sqlite(r) => r.try_get(idx).map_err(BoardError::from),
            Self::Postgres(r) => r.try_get(idx).map_err(BoardError::from),
        }
    }

    pub fn get_bytes(&self, idx: usize) -> BoardResult<Vec<u8>> {
        match self {
            Self::Sqlite(r) => r.try_get(idx).map_err(BoardError::from),
            Self::Postgres(r) => r.try_get(idx).map_err(BoardError::from),
        }
    }
}

/// Rewrites SQLite-style `?N` (or bare `?`) placeholders to Postgres `$N` placeholders
/// while skipping single-quoted string literals, double-quoted identifiers, `--` line
/// comments, and `/* ... */` block comments.
pub fn rewrite_placeholders_for_pg(sql: &str) -> Cow<'_, str> {
    if !sql.contains('?') {
        return Cow::Borrowed(sql);
    }

    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 8);
    let mut i = 0;
    let mut auto_idx: usize = 0;

    while i < len {
        let b = bytes[i];

        // Single-quoted SQL string literal (handles '' escapes)
        if b == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Double-quoted SQL identifier (handles "" escapes)
        if b == b'"' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    i += 1;
                    if i < len && bytes[i] == b'"' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Line comment: -- ...\n
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Block comment: /* ... */
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(len);
            out.push_str(&sql[start..i]);
            continue;
        }

        if b == b'?' {
            let mut j = i + 1;
            while j < len && bytes[j].is_ascii_digit() {
                j += 1;
            }
            out.push('$');
            if j > i + 1 {
                out.push_str(&sql[i + 1..j]);
                i = j;
            } else {
                auto_idx += 1;
                use std::fmt::Write;
                let _ = write!(&mut out, "{auto_idx}");
                i += 1;
            }
            continue;
        }

        // Copy full UTF-8 character safely
        let ch = sql[i..].chars().next().expect("valid utf-8 boundary");
        out.push(ch);
        i += ch.len_utf8();
    }

    Cow::Owned(out)
}

/// Escapes `\`, `%`, and `_` for portable `LIKE ?N ESCAPE '\'` prefix matching.
pub fn escape_like_prefix(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Builds a comma-separated numbered placeholder list (`?1,?2,...` or starting at `start`)
/// for portable `IN (...)` batch queries across SQLite and PostgreSQL.
pub fn sql_placeholders_from(start: usize, count: usize) -> String {
    if count == 0 {
        return String::new();
    }
    let mut s = String::with_capacity(count * 4);
    for offset in 0..count {
        if offset > 0 {
            s.push(',');
        }
        use std::fmt::Write;
        let _ = write!(&mut s, "?{}", start + offset);
    }
    s
}

/// Builds a comma-separated numbered placeholder list starting at `?1`.
pub fn sql_placeholders(count: usize) -> String {
    sql_placeholders_from(1, count)
}

fn bind_sqlite_args<'q>(
    mut q: sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    args: &'q [SqlArg<'q>],
) -> sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for arg in args {
        q = match arg {
            SqlArg::Text(s) => q.bind(s.as_ref()),
            SqlArg::OptText(opt) => q.bind(opt.as_deref()),
            SqlArg::Int(v) => q.bind(*v),
            SqlArg::OptInt(opt) => q.bind(*opt),
            SqlArg::Bytes(b) => q.bind(b.as_ref()),
        };
    }
    q
}

fn bind_pg_args<'q>(
    mut q: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    args: &'q [SqlArg<'q>],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for arg in args {
        q = match arg {
            SqlArg::Text(s) => q.bind(s.as_ref()),
            SqlArg::OptText(opt) => q.bind(opt.as_deref()),
            SqlArg::Int(v) => q.bind(*v),
            SqlArg::OptInt(opt) => q.bind(*opt),
            SqlArg::Bytes(b) => q.bind(b.as_ref()),
        };
    }
    q
}

struct Migration {
    version: i64,
    name: &'static str,
    sqlite_sql: &'static str,
    postgres_sql: Option<&'static str>,
}

impl Migration {
    fn ddl_for(&self, dialect: Dialect) -> Cow<'static, str> {
        match dialect {
            Dialect::Sqlite => Cow::Borrowed(self.sqlite_sql),
            Dialect::Postgres => match self.postgres_sql {
                Some(pg) => Cow::Borrowed(pg),
                None => Cow::Owned(sqlite_ddl_to_postgres(self.sqlite_sql)),
            },
        }
    }
}

fn sqlite_ddl_to_postgres(sqlite_sql: &str) -> String {
    sqlite_sql
        .replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        .replace("INTEGER", "BIGINT")
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "001_init",
        sqlite_sql: r#"
            CREATE TABLE IF NOT EXISTS server_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
        "#,
        postgres_sql: None,
    },
    Migration {
        version: 2,
        name: "002_auth_actors",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 3,
        name: "003_boards_channels_grants",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 4,
        name: "004_threads_messages",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 5,
        name: "005_labels_and_schemas",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 6,
        name: "006_attachments",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 7,
        name: "007_events",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 8,
        name: "008_tasks",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 9,
        name: "009_schedules",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 10,
        name: "010_watchers",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 11,
        name: "011_sync",
        sqlite_sql: r#"
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
        postgres_sql: None,
    },
    Migration {
        version: 12,
        name: "012_thread_summaries",
        sqlite_sql: r#"
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

            INSERT INTO threads (
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
            WHERE r.re IS NULL
            ON CONFLICT (root_id) DO NOTHING;
        "#,
        postgres_sql: None,
    },
    Migration {
        version: 13,
        name: "013_horizontal_scaling",
        sqlite_sql: r#"
            ALTER TABLE watchers ADD COLUMN dispatch_locked_until TEXT;
            ALTER TABLE watchers ADD COLUMN dispatch_owner TEXT;
            ALTER TABLE watchers ADD COLUMN next_retry_at TEXT;
            CREATE INDEX IF NOT EXISTS idx_watchers_dispatch
                ON watchers (dispatch_locked_until, next_retry_at);

            ALTER TABLE sync_deliveries ADD COLUMN locked_until TEXT;
            ALTER TABLE sync_deliveries ADD COLUMN locked_by TEXT;
            CREATE INDEX IF NOT EXISTS idx_sync_deliveries_claim
                ON sync_deliveries (status, next_attempt_at, locked_until);
        "#,
        postgres_sql: Some(
            r#"
            ALTER TABLE watchers ADD COLUMN IF NOT EXISTS dispatch_locked_until TEXT;
            ALTER TABLE watchers ADD COLUMN IF NOT EXISTS dispatch_owner TEXT;
            ALTER TABLE watchers ADD COLUMN IF NOT EXISTS next_retry_at TEXT;
            CREATE INDEX IF NOT EXISTS idx_watchers_dispatch
                ON watchers (dispatch_locked_until, next_retry_at);

            ALTER TABLE sync_deliveries ADD COLUMN IF NOT EXISTS locked_until TEXT;
            ALTER TABLE sync_deliveries ADD COLUMN IF NOT EXISTS locked_by TEXT;
            CREATE INDEX IF NOT EXISTS idx_sync_deliveries_claim
                ON sync_deliveries (status, next_attempt_at, locked_until);

            CREATE TABLE IF NOT EXISTS blob_objects (
                sha256 TEXT PRIMARY KEY,
                size BIGINT NOT NULL,
                chunk_count BIGINT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blob_chunks (
                sha256 TEXT NOT NULL REFERENCES blob_objects(sha256) ON DELETE CASCADE,
                chunk_idx BIGINT NOT NULL,
                data BYTEA NOT NULL,
                PRIMARY KEY (sha256, chunk_idx)
            );

            CREATE TABLE IF NOT EXISTS blob_staging_chunks (
                attachment_id TEXT NOT NULL REFERENCES attachments(id) ON DELETE CASCADE,
                chunk_idx BIGINT NOT NULL,
                data BYTEA NOT NULL,
                PRIMARY KEY (attachment_id, chunk_idx)
            );
            CREATE INDEX IF NOT EXISTS idx_blob_staging_attachment
                ON blob_staging_chunks (attachment_id);

            CREATE UNLOGGED TABLE IF NOT EXISTS rate_limit_buckets (
                principal_id TEXT PRIMARY KEY,
                tokens_milli BIGINT NOT NULL,
                last_refill_ms BIGINT NOT NULL,
                last_allowed BIGINT NOT NULL
            );
        "#,
        ),
    },
];

#[derive(Clone)]
enum DbBackend {
    Sqlite {
        writer: SqlitePool,
        readers: Option<SqlitePool>,
    },
    Postgres(PgPool),
}

/// Multi-SQL persistence handle supporting embedded SQLite (WAL + concurrent reader pool)
/// and PostgreSQL (`PgPool`).
#[derive(Clone)]
pub struct Database {
    backend: DbBackend,
}

/// Lightweight read-only handle over `Database` that routes SQLite reads to the
/// concurrent `query_only = ON` WAL reader pool (when file-backed) and Postgres reads to `PgPool`.
#[derive(Clone, Copy)]
pub struct DbRead<'a>(&'a Database);

enum TxBackend {
    Sqlite(Transaction<'static, Sqlite>),
    Postgres(Transaction<'static, Postgres>),
}

#[derive(Default)]
struct PendingPgNotifies {
    board_events: HashMap<String, i64>,
    task_events: HashMap<String, i64>,
    notify_watcher: bool,
    notify_sync: bool,
}

/// Active database transaction (`BEGIN IMMEDIATE` on SQLite, `BEGIN` on PostgreSQL).
/// Dropping without calling `commit().await` rolls back automatically.
pub struct DbTx {
    backend: TxBackend,
    pending_notifies: PendingPgNotifies,
}

/// Unified query execution interface implemented for both `DbRead<'_>` (pool reads)
/// and `DbTx` (active transactions), allowing domain helpers to run in either context.
pub trait Queryable: Send {
    fn dialect(&self) -> Dialect;

    fn execute<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<u64>> + Send + 'a;

    fn fetch_all<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<Vec<DbRow>>> + Send + 'a;

    fn fetch_optional<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<Option<DbRow>>> + Send + 'a;

    fn fetch_one<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<DbRow>> + Send + 'a;
}

impl Queryable for DbRead<'_> {
    fn dialect(&self) -> Dialect {
        self.0.dialect()
    }

    fn execute<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<u64>> + Send + 'a {
        let db = self.0;
        async move {
            match &db.backend {
                DbBackend::Sqlite { writer, readers } => {
                    let pool = readers.as_ref().unwrap_or(writer);
                    let res = bind_sqlite_args(sqlx::query(sql), args)
                        .execute(pool)
                        .await?;
                    Ok(res.rows_affected())
                }
                DbBackend::Postgres(pool) => {
                    let pg_sql = rewrite_placeholders_for_pg(sql);
                    let res = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                        .execute(pool)
                        .await?;
                    Ok(res.rows_affected())
                }
            }
        }
    }

    fn fetch_all<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<Vec<DbRow>>> + Send + 'a {
        let db = self.0;
        async move {
            match &db.backend {
                DbBackend::Sqlite { writer, readers } => {
                    let pool = readers.as_ref().unwrap_or(writer);
                    let rows = bind_sqlite_args(sqlx::query(sql), args)
                        .fetch_all(pool)
                        .await?;
                    Ok(rows.into_iter().map(DbRow::Sqlite).collect())
                }
                DbBackend::Postgres(pool) => {
                    let pg_sql = rewrite_placeholders_for_pg(sql);
                    let rows = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                        .fetch_all(pool)
                        .await?;
                    Ok(rows.into_iter().map(DbRow::Postgres).collect())
                }
            }
        }
    }

    fn fetch_optional<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<Option<DbRow>>> + Send + 'a {
        let db = self.0;
        async move {
            match &db.backend {
                DbBackend::Sqlite { writer, readers } => {
                    let pool = readers.as_ref().unwrap_or(writer);
                    let row = bind_sqlite_args(sqlx::query(sql), args)
                        .fetch_optional(pool)
                        .await?;
                    Ok(row.map(DbRow::Sqlite))
                }
                DbBackend::Postgres(pool) => {
                    let pg_sql = rewrite_placeholders_for_pg(sql);
                    let row = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                        .fetch_optional(pool)
                        .await?;
                    Ok(row.map(DbRow::Postgres))
                }
            }
        }
    }

    fn fetch_one<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> impl Future<Output = BoardResult<DbRow>> + Send + 'a {
        let db = self.0;
        async move {
            match &db.backend {
                DbBackend::Sqlite { writer, readers } => {
                    let pool = readers.as_ref().unwrap_or(writer);
                    let row = bind_sqlite_args(sqlx::query(sql), args)
                        .fetch_one(pool)
                        .await?;
                    Ok(DbRow::Sqlite(row))
                }
                DbBackend::Postgres(pool) => {
                    let pg_sql = rewrite_placeholders_for_pg(sql);
                    let row = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                        .fetch_one(pool)
                        .await?;
                    Ok(DbRow::Postgres(row))
                }
            }
        }
    }
}

impl Queryable for DbTx {
    fn dialect(&self) -> Dialect {
        match &self.backend {
            TxBackend::Sqlite(_) => Dialect::Sqlite,
            TxBackend::Postgres(_) => Dialect::Postgres,
        }
    }

    async fn execute<'a>(&'a mut self, sql: &'a str, args: &'a [SqlArg<'a>]) -> BoardResult<u64> {
        match &mut self.backend {
            TxBackend::Sqlite(tx) => {
                let res = bind_sqlite_args(sqlx::query(sql), args)
                    .execute(&mut **tx)
                    .await?;
                Ok(res.rows_affected())
            }
            TxBackend::Postgres(tx) => {
                let pg_sql = rewrite_placeholders_for_pg(sql);
                let res = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                    .execute(&mut **tx)
                    .await?;
                Ok(res.rows_affected())
            }
        }
    }

    async fn fetch_all<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> BoardResult<Vec<DbRow>> {
        match &mut self.backend {
            TxBackend::Sqlite(tx) => {
                let rows = bind_sqlite_args(sqlx::query(sql), args)
                    .fetch_all(&mut **tx)
                    .await?;
                Ok(rows.into_iter().map(DbRow::Sqlite).collect())
            }
            TxBackend::Postgres(tx) => {
                let pg_sql = rewrite_placeholders_for_pg(sql);
                let rows = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                    .fetch_all(&mut **tx)
                    .await?;
                Ok(rows.into_iter().map(DbRow::Postgres).collect())
            }
        }
    }

    async fn fetch_optional<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> BoardResult<Option<DbRow>> {
        match &mut self.backend {
            TxBackend::Sqlite(tx) => {
                let row = bind_sqlite_args(sqlx::query(sql), args)
                    .fetch_optional(&mut **tx)
                    .await?;
                Ok(row.map(DbRow::Sqlite))
            }
            TxBackend::Postgres(tx) => {
                let pg_sql = rewrite_placeholders_for_pg(sql);
                let row = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                    .fetch_optional(&mut **tx)
                    .await?;
                Ok(row.map(DbRow::Postgres))
            }
        }
    }

    async fn fetch_one<'a>(
        &'a mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> BoardResult<DbRow> {
        match &mut self.backend {
            TxBackend::Sqlite(tx) => {
                let row = bind_sqlite_args(sqlx::query(sql), args)
                    .fetch_one(&mut **tx)
                    .await?;
                Ok(DbRow::Sqlite(row))
            }
            TxBackend::Postgres(tx) => {
                let pg_sql = rewrite_placeholders_for_pg(sql);
                let row = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                    .fetch_one(&mut **tx)
                    .await?;
                Ok(DbRow::Postgres(row))
            }
        }
    }
}

impl DbTx {
    /// Returns `" FOR UPDATE"` on PostgreSQL and `""` on SQLite (which already serializes
    /// writers via `BEGIN IMMEDIATE`).
    pub fn lock_row_suffix(&self) -> &'static str {
        match self.dialect() {
            Dialect::Sqlite => "",
            Dialect::Postgres => " FOR UPDATE",
        }
    }

    /// Acquires a transaction-scoped advisory lock on PostgreSQL (automatically released on
    /// commit/rollback). No-op on SQLite because `BEGIN IMMEDIATE` already serializes writers.
    pub async fn advisory_xact_lock(&mut self, namespace: &str, key: &str) -> BoardResult<()> {
        match &mut self.backend {
            TxBackend::Sqlite(_) => Ok(()),
            TxBackend::Postgres(tx) => {
                let lock_id = format!("{namespace}:{key}");
                sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                    .bind(&lock_id)
                    .execute(&mut **tx)
                    .await?;
                Ok(())
            }
        }
    }

    /// Executes a statement returning the raw `sqlx::Error` so callers can inspect constraint
    /// violation codes directly (e.g., race-tolerant unique inserts).
    pub async fn execute_raw_err<'a>(
        &mut self,
        sql: &'a str,
        args: &'a [SqlArg<'a>],
    ) -> Result<u64, sqlx::Error> {
        match &mut self.backend {
            TxBackend::Sqlite(tx) => {
                let res = bind_sqlite_args(sqlx::query(sql), args)
                    .execute(&mut **tx)
                    .await?;
                Ok(res.rows_affected())
            }
            TxBackend::Postgres(tx) => {
                let pg_sql = rewrite_placeholders_for_pg(sql);
                let res = bind_pg_args(sqlx::query(pg_sql.as_ref()), args)
                    .execute(&mut **tx)
                    .await?;
                Ok(res.rows_affected())
            }
        }
    }

    /// Queues a PostgreSQL `pg_notify('ob_events', '<board_id>:<seq>')` emitted inside this
    /// transaction right before commit (no-op on SQLite and discarded on rollback).
    pub fn queue_event_notify(&mut self, board_id: &str, seq: i64) {
        if self.dialect() == Dialect::Postgres {
            let entry = self
                .pending_notifies
                .board_events
                .entry(board_id.to_string())
                .or_insert(seq);
            if seq > *entry {
                *entry = seq;
            }
        }
    }

    /// Queues a PostgreSQL `pg_notify('ob_tasks', '<board_id>:<seq>')` emitted inside this
    /// transaction right before commit (no-op on SQLite and discarded on rollback).
    pub fn queue_task_notify(&mut self, board_id: &str, seq: i64) {
        if self.dialect() == Dialect::Postgres {
            let entry = self
                .pending_notifies
                .task_events
                .entry(board_id.to_string())
                .or_insert(seq);
            if seq > *entry {
                *entry = seq;
            }
        }
    }

    /// Queues a PostgreSQL watcher wakeup notification emitted inside this transaction on commit.
    pub fn queue_watcher_notify(&mut self) {
        if self.dialect() == Dialect::Postgres {
            self.pending_notifies.notify_watcher = true;
        }
    }

    /// Queues a PostgreSQL `pg_notify('ob_sync', 'sync')` emitted inside this transaction on commit.
    pub fn queue_sync_notify(&mut self) {
        if self.dialect() == Dialect::Postgres {
            self.pending_notifies.notify_sync = true;
        }
    }

    pub async fn commit(mut self) -> BoardResult<()> {
        if let TxBackend::Postgres(ref mut tx) = self.backend {
            for (board_id, seq) in &self.pending_notifies.board_events {
                let payload = format!("{board_id}:{seq}");
                sqlx::query("SELECT pg_notify('ob_events', $1)")
                    .bind(&payload)
                    .execute(&mut **tx)
                    .await?;
            }
            for (board_id, seq) in &self.pending_notifies.task_events {
                let payload = format!("{board_id}:{seq}");
                sqlx::query("SELECT pg_notify('ob_tasks', $1)")
                    .bind(&payload)
                    .execute(&mut **tx)
                    .await?;
            }
            if self.pending_notifies.notify_watcher {
                sqlx::query("SELECT pg_notify('ob_events', '__watcher__')")
                    .execute(&mut **tx)
                    .await?;
            }
            if self.pending_notifies.notify_sync {
                sqlx::query("SELECT pg_notify('ob_sync', 'sync')")
                    .execute(&mut **tx)
                    .await?;
            }
        }
        match self.backend {
            TxBackend::Sqlite(tx) => tx.commit().await?,
            TxBackend::Postgres(tx) => tx.commit().await?,
        }
        Ok(())
    }

    pub async fn rollback(self) -> BoardResult<()> {
        match self.backend {
            TxBackend::Sqlite(tx) => tx.rollback().await?,
            TxBackend::Postgres(tx) => tx.rollback().await?,
        }
        Ok(())
    }
}

impl Database {
    /// Opens a file-backed SQLite database in WAL mode with a concurrent read-only WAL pool
    /// and runs all pending migrations.
    pub async fn open(path: &Path) -> BoardResult<Self> {
        let target = DbTarget::SqlitePath(path.to_path_buf());
        Self::connect(&target, DEFAULT_DB_MAX_CONNECTIONS).await
    }

    /// Opens an in-memory SQLite database (single pinned connection) and runs all migrations.
    pub async fn open_in_memory() -> BoardResult<Self> {
        let target = DbTarget::SqliteUrl("sqlite::memory:".to_string());
        Self::connect(&target, 1).await
    }

    /// Connects to `target` and runs all pending migrations under `with_startup_lock`.
    pub async fn connect(target: &DbTarget, max_connections: u32) -> BoardResult<Self> {
        let db = Self::connect_raw(target, max_connections).await?;
        db.with_startup_lock(|| db.run_migrations()).await?;
        Ok(db)
    }

    /// Connects to `target` without running migrations yet (so startup can hold one
    /// `with_startup_lock` across migrations + initial seeding).
    pub async fn connect_raw(target: &DbTarget, max_connections: u32) -> BoardResult<Self> {
        match target {
            DbTarget::SqlitePath(path) => {
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
                let writer_opts = SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .foreign_keys(true)
                    .busy_timeout(Duration::from_secs(5));
                let reader_opts = SqliteConnectOptions::new()
                    .filename(path)
                    .read_only(true)
                    .foreign_keys(true)
                    .busy_timeout(Duration::from_secs(5))
                    .pragma("query_only", "ON");
                Self::connect_sqlite_file(writer_opts, reader_opts).await
            }
            DbTarget::SqliteUrl(url) => {
                let is_memory = url.contains(":memory:") || url.contains("mode=memory");
                let base_opts = SqliteConnectOptions::from_str(url).map_err(|_| {
                    BoardError::bad_request("invalid SQLite DATABASE_URL format")
                })?;
                if is_memory {
                    let opts = base_opts
                        .foreign_keys(true)
                        .busy_timeout(Duration::from_secs(5));
                    let writer = SqlitePoolOptions::new()
                        .max_connections(1)
                        .idle_timeout(None)
                        .max_lifetime(None)
                        .connect_with(opts)
                        .await?;
                    Ok(Self {
                        backend: DbBackend::Sqlite {
                            writer,
                            readers: None,
                        },
                    })
                } else {
                    let writer_opts = base_opts
                        .clone()
                        .create_if_missing(true)
                        .journal_mode(SqliteJournalMode::Wal)
                        .synchronous(SqliteSynchronous::Normal)
                        .foreign_keys(true)
                        .busy_timeout(Duration::from_secs(5));
                    let reader_opts = base_opts
                        .read_only(true)
                        .foreign_keys(true)
                        .busy_timeout(Duration::from_secs(5))
                        .pragma("query_only", "ON");
                    Self::connect_sqlite_file(writer_opts, reader_opts).await
                }
            }
            DbTarget::PostgresUrl(url) => {
                let opts = PgConnectOptions::from_str(url)
                    .map_err(|_| {
                        BoardError::bad_request(
                            "invalid PostgreSQL DATABASE_URL format (credentials redacted)",
                        )
                    })?
                    .application_name("openboards");
                let pool = PgPoolOptions::new()
                    .max_connections(max_connections.max(1))
                    .acquire_timeout(Duration::from_secs(10))
                    .connect_with(opts)
                    .await
                    .map_err(|e| {
                        BoardError::internal(format!(
                            "failed to connect to {}: {e}",
                            target.redacted_display()
                        ))
                    })?;
                Ok(Self {
                    backend: DbBackend::Postgres(pool),
                })
            }
        }
    }

    async fn connect_sqlite_file(
        writer_opts: SqliteConnectOptions,
        reader_opts: SqliteConnectOptions,
    ) -> BoardResult<Self> {
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(writer_opts)
            .await?;
        // Ensure the database file and WAL journal are initialized before opening read-only pool.
        let conn = writer.acquire().await?;
        drop(conn);

        let reader_count = std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, 8) as u32)
            .unwrap_or(4);
        let readers = SqlitePoolOptions::new()
            .max_connections(reader_count)
            .connect_with(reader_opts)
            .await?;

        Ok(Self {
            backend: DbBackend::Sqlite {
                writer,
                readers: Some(readers),
            },
        })
    }

    pub fn dialect(&self) -> Dialect {
        match &self.backend {
            DbBackend::Sqlite { .. } => Dialect::Sqlite,
            DbBackend::Postgres(_) => Dialect::Postgres,
        }
    }

    /// Returns a read-only handle implementing `Queryable`.
    pub fn read_conn(&self) -> DbRead<'_> {
        DbRead(self)
    }

    /// Begins a write transaction (`BEGIN IMMEDIATE` on SQLite, `BEGIN` on Postgres).
    pub async fn begin_tx(&self) -> BoardResult<DbTx> {
        match &self.backend {
            DbBackend::Sqlite { writer, .. } => {
                let tx = writer.begin_with("BEGIN IMMEDIATE").await?;
                Ok(DbTx {
                    backend: TxBackend::Sqlite(tx),
                    pending_notifies: PendingPgNotifies::default(),
                })
            }
            DbBackend::Postgres(pool) => {
                let tx = pool.begin().await?;
                Ok(DbTx {
                    backend: TxBackend::Postgres(tx),
                    pending_notifies: PendingPgNotifies::default(),
                })
            }
        }
    }

    /// Serializes startup DDL migrations and first-boot seeding across replicas via a
    /// PostgreSQL session advisory lock. No-op wrapper on SQLite.
    pub async fn with_startup_lock<T, F, Fut>(&self, f: F) -> BoardResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BoardResult<T>>,
    {
        match &self.backend {
            DbBackend::Sqlite { .. } => f().await,
            DbBackend::Postgres(pool) => {
                let mut lock_conn = pool.acquire().await?;
                sqlx::query("SELECT pg_advisory_lock($1)")
                    .bind(STARTUP_LOCK_KEY)
                    .execute(&mut *lock_conn)
                    .await?;
                let result = f().await;
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(STARTUP_LOCK_KEY)
                    .execute(&mut *lock_conn)
                    .await;
                result
            }
        }
    }

    /// Applies all unapplied schema migrations (`001`–`013`).
    pub async fn run_migrations(&self) -> BoardResult<()> {
        self.run_migrations_up_to(i64::MAX).await
    }

    /// Applies unapplied schema migrations up to and including `max_version`.
    pub async fn run_migrations_up_to(&self, max_version: i64) -> BoardResult<()> {
        let dialect = self.dialect();
        let meta_ddl = match dialect {
            Dialect::Sqlite => {
                "CREATE TABLE IF NOT EXISTS _migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );"
            }
            Dialect::Postgres => {
                "CREATE TABLE IF NOT EXISTS _migrations (
                    version BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );"
            }
        };

        {
            let mut tx = self.begin_tx().await?;
            execute_raw_ddl(&mut tx, meta_ddl).await?;
            tx.commit().await?;
        }

        for migration in MIGRATIONS {
            if migration.version > max_version {
                break;
            }
            let applied = self
                .read_conn()
                .fetch_optional(
                    "SELECT 1 FROM _migrations WHERE version = ?1",
                    sql_args![migration.version],
                )
                .await?
                .is_some();
            if applied {
                continue;
            }

            let ddl = migration.ddl_for(dialect);
            let mut tx = self.begin_tx().await?;
            // Re-check inside tx in case another startup race completed before us
            let already = tx
                .fetch_optional(
                    "SELECT 1 FROM _migrations WHERE version = ?1",
                    sql_args![migration.version],
                )
                .await?
                .is_some();
            if already {
                tx.commit().await?;
                continue;
            }
            execute_raw_ddl(&mut tx, ddl.as_ref()).await?;
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "INSERT INTO _migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
                sql_args![migration.version, migration.name, &now],
            )
            .await?;
            tx.commit().await?;
        }

        Ok(())
    }

    /// Verifies live database connectivity with a 2-second timeout (used by `GET /readyz`).
    pub async fn ping(&self) -> BoardResult<()> {
        let fut = async {
            let mut conn = self.read_conn();
            let _ = conn.fetch_one("SELECT 1", sql_args![]).await?;
            Ok::<(), BoardError>(())
        };
        tokio::time::timeout(Duration::from_secs(2), fut)
            .await
            .map_err(|_| BoardError::internal("database readiness check timed out"))?
    }

    pub async fn schema_version(&self) -> BoardResult<i64> {
        let mut conn = self.read_conn();
        let row = conn
            .fetch_one("SELECT COALESCE(MAX(version), 0) FROM _migrations", sql_args![])
            .await?;
        row.get_i64(0)
    }

    /// Exposes the underlying `PgPool` when running on PostgreSQL (used by Phase B `PgListener`).
    pub fn pg_pool(&self) -> Option<&PgPool> {
        match &self.backend {
            DbBackend::Postgres(pool) => Some(pool),
            DbBackend::Sqlite { .. } => None,
        }
    }
}

async fn execute_raw_ddl(tx: &mut DbTx, ddl: &str) -> BoardResult<()> {
    match &mut tx.backend {
        TxBackend::Sqlite(inner) => {
            sqlx::raw_sql(ddl).execute(&mut **inner).await?;
        }
        TxBackend::Postgres(inner) => {
            sqlx::raw_sql(ddl).execute(&mut **inner).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_placeholders_safely_for_postgres() {
        assert_eq!(rewrite_placeholders_for_pg("SELECT 1"), "SELECT 1");
        assert_eq!(
            rewrite_placeholders_for_pg("SELECT * FROM t WHERE a = ?1 AND b = ?2 AND c = ?1"),
            "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1"
        );
        // Non-sequential ?5 before ?4
        assert_eq!(
            rewrite_placeholders_for_pg("WHERE x LIKE ?5 OR y = ?4"),
            "WHERE x LIKE $5 OR y = $4"
        );
        // Bare ? rewritten sequentially
        assert_eq!(
            rewrite_placeholders_for_pg("WHERE a = ? AND id IN (?,?,?)"),
            "WHERE a = $1 AND id IN ($2,$3,$4)"
        );
        // Single-quoted literals, escaped quotes, comments, and double-quoted identifiers untouched
        assert_eq!(
            rewrite_placeholders_for_pg(
                "SELECT 'what''s ?1?', \"col?2\" FROM t -- comment ?3\n/* block ?4 */ WHERE a = ?1"
            ),
            "SELECT 'what''s ?1?', \"col?2\" FROM t -- comment ?3\n/* block ?4 */ WHERE a = $1"
        );
    }

    #[test]
    fn escapes_like_prefixes_and_builds_numbered_placeholders() {
        assert_eq!(escape_like_prefix("v2_beta"), r"v2\_beta");
        assert_eq!(escape_like_prefix(r"a%b\c_d"), r"a\%b\\c\_d");
        assert_eq!(sql_placeholders(0), "");
        assert_eq!(sql_placeholders(3), "?1,?2,?3");
        assert_eq!(sql_placeholders_from(2, 3), "?2,?3,?4");
    }

    #[test]
    fn generates_clean_postgres_ddl_for_all_migrations() {
        assert_eq!(MIGRATIONS.len(), 13);
        for m in MIGRATIONS {
            let pg = m.ddl_for(Dialect::Postgres);
            assert!(
                !pg.contains("INTEGER"),
                "migration {} still contains INTEGER in PG DDL",
                m.name
            );
            assert!(
                !pg.contains("AUTOINCREMENT"),
                "migration {} still contains AUTOINCREMENT in PG DDL",
                m.name
            );
            assert!(
                !pg.contains("INSERT OR IGNORE"),
                "migration {} still contains INSERT OR IGNORE in PG DDL",
                m.name
            );
        }
    }

    #[tokio::test]
    async fn initializes_file_db_with_wal_and_foreign_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("openboards.db");
        let db = Database::open(&db_path).await.unwrap();
        let expected_version = MIGRATIONS.iter().map(|m| m.version).max().unwrap_or(0);

        assert_eq!(db.dialect(), Dialect::Sqlite);
        assert_eq!(db.schema_version().await.unwrap(), expected_version);
        db.ping().await.unwrap();

        let mut conn = db.read_conn();
        let jm = conn.fetch_one("PRAGMA journal_mode", sql_args![]).await.unwrap();
        assert_eq!(jm.get_str(0).unwrap().to_lowercase(), "wal");
        let fk = conn.fetch_one("PRAGMA foreign_keys", sql_args![]).await.unwrap();
        assert_eq!(fk.get_i64(0).unwrap(), 1);

        // Reopening is idempotent
        let db2 = Database::open(&db_path).await.unwrap();
        assert_eq!(db2.schema_version().await.unwrap(), expected_version);

        // Read-only WAL pool enforces query_only = ON
        let mut ro = db2.read_conn();
        let write_err = ro
            .execute(
                "INSERT INTO server_meta (key, value) VALUES ('ro_test', '1')",
                sql_args![],
            )
            .await;
        assert!(write_err.is_err());
    }

    #[tokio::test]
    async fn initializes_in_memory_db_with_foreign_keys() {
        let db = Database::open_in_memory().await.unwrap();
        let expected_version = MIGRATIONS.iter().map(|m| m.version).max().unwrap_or(0);
        assert_eq!(db.schema_version().await.unwrap(), expected_version);
        db.ping().await.unwrap();
        let mut conn = db.read_conn();
        let fk = conn.fetch_one("PRAGMA foreign_keys", sql_args![]).await.unwrap();
        assert_eq!(fk.get_i64(0).unwrap(), 1);
    }
}
