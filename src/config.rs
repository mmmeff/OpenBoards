use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Args;
use serde::Deserialize;

pub const DEFAULT_RATE_LIMIT_RPS: u32 = 100;
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 200;
pub const DEFAULT_DB_MAX_CONNECTIONS: u32 = 20;
pub const DEFAULT_DATA_DIR: &str = "./.openboards";
pub const LOCAL_DATA_DIR: &str = "./.board";
pub const CLI_CONFIG_FILENAME: &str = ".openboard.json";

/// Global CLI configuration loaded from `.openboard.json` (or `--config <path>`).
///
/// Unknown fields are rejected so typos in config keys fail fast with a clear error.
/// `DATABASE_URL` is intentionally NOT supported here (strictly env-var-only).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliFileConfig {
    #[serde(rename = "$schema")]
    pub schema: Option<String>,
    pub url: Option<String>,
    #[serde(alias = "name")]
    pub board: Option<String>,
    pub token: Option<String>,
    #[serde(alias = "as")]
    pub profile: Option<String>,
    pub avatar: Option<String>,
    #[serde(alias = "scope")]
    pub team: Option<String>,
    pub ensure: Option<bool>,
    pub json: Option<bool>,
}

/// Searches `start_dir` and its parent directories upward for `.openboard.json`,
/// returning the closest ancestor match (or `None` if none exists up to filesystem root).
pub fn find_cli_config(start_dir: &Path) -> Option<PathBuf> {
    for dir in start_dir.ancestors() {
        let candidate = dir.join(CLI_CONFIG_FILENAME);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Reads and parses a `.openboard.json` file from an exact path.
pub fn load_cli_config_from_path(path: &Path) -> Result<CliFileConfig, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file '{}': {e}", path.display()))?;
    serde_json::from_str::<CliFileConfig>(&raw)
        .map_err(|e| format!("failed to parse config file '{}': {e}", path.display()))
}

/// Loads CLI configuration from either an explicit `--config` / `BOARD_CONFIG` path
/// or by walking upward from `start_dir` for `.openboard.json`.
///
/// - Missing/unreadable explicit config returns an error naming the path.
/// - Invalid JSON or unknown fields in any loaded config returns an error naming the path.
/// - Absence during upward search returns `Ok(None)`.
pub fn load_cli_config(
    explicit_path: Option<&Path>,
    start_dir: &Path,
) -> Result<Option<(PathBuf, CliFileConfig)>, String> {
    if let Some(path) = explicit_path {
        let cfg = load_cli_config_from_path(path)?;
        return Ok(Some((path.to_path_buf(), cfg)));
    }
    match find_cli_config(start_dir) {
        Some(path) => {
            let cfg = load_cli_config_from_path(&path)?;
            Ok(Some((path, cfg)))
        }
        None => Ok(None),
    }
}

/// Database connection target resolved strictly from `DATABASE_URL` or default SQLite path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbTarget {
    SqlitePath(PathBuf),
    SqliteUrl(String),
    PostgresUrl(String),
}

impl DbTarget {
    pub fn is_postgres(&self) -> bool {
        matches!(self, Self::PostgresUrl(_))
    }

    /// Returns a credential-redacted summary safe for startup logs and error messages.
    pub fn redacted_display(&self) -> String {
        match self {
            Self::SqlitePath(path) => format!("sqlite ({})", path.display()),
            Self::SqliteUrl(url) => {
                if url.contains(":memory:") || url.contains("mode=memory") {
                    "sqlite (:memory:)".to_string()
                } else {
                    let without_query = url.split(['?', '#']).next().unwrap_or(url);
                    let path = without_query
                        .strip_prefix("sqlite://")
                        .or_else(|| without_query.strip_prefix("sqlite:"))
                        .unwrap_or(without_query);
                    format!("sqlite ({path})")
                }
            }
            Self::PostgresUrl(url) => {
                let without_scheme = url
                    .strip_prefix("postgresql://")
                    .or_else(|| url.strip_prefix("postgres://"))
                    .unwrap_or("");
                let without_query = without_scheme.split(['?', '#']).next().unwrap_or("");
                let host_and_db = match without_query.rsplit_once('@') {
                    Some((_, after_at)) => after_at,
                    None => without_query,
                };
                if host_and_db.is_empty() {
                    "postgres (<redacted>)".to_string()
                } else {
                    format!("postgres ({host_and_db})")
                }
            }
        }
    }
}

/// Resolves a `DbTarget` strictly from an optional `DATABASE_URL` value.
/// Never echoes the raw connection string in error messages so credentials cannot leak.
pub fn resolve_db_target(
    database_url: Option<&str>,
    default_db_path: PathBuf,
    local_mode: bool,
) -> Result<DbTarget, String> {
    let Some(raw) = database_url.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(DbTarget::SqlitePath(default_db_path));
    };

    if raw.starts_with("sqlite://") || raw.starts_with("sqlite:") {
        return Ok(DbTarget::SqliteUrl(raw.to_string()));
    }
    if raw.starts_with("postgres://") || raw.starts_with("postgresql://") {
        if local_mode {
            return Err(
                "--local cannot be combined with a Postgres DATABASE_URL; unset DATABASE_URL or run without --local"
                    .to_string(),
            );
        }
        return Ok(DbTarget::PostgresUrl(raw.to_string()));
    }

    Err(
        "unsupported DATABASE_URL scheme; supported schemes are sqlite://, postgres://, and postgresql://"
            .to_string(),
    )
}

fn parse_max_connections(raw: Option<&str>) -> Result<u32, String> {
    let Some(val) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(DEFAULT_DB_MAX_CONNECTIONS);
    };
    let parsed: u32 = val.parse().map_err(|_| {
        format!("invalid OPENBOARDS_DB_MAX_CONNECTIONS value '{val}'; expected a positive integer")
    })?;
    if parsed == 0 {
        return Err("OPENBOARDS_DB_MAX_CONNECTIONS must be at least 1".to_string());
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Address to bind the HTTP server to
    #[arg(long, env = "OPENBOARDS_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    /// Directory for SQLite database and local blob storage
    #[arg(long, env = "OPENBOARDS_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Team identifier/slug served by this instance
    #[arg(long, env = "OPENBOARDS_TEAM", default_value = "default")]
    pub team: String,

    /// Optional bootstrap admin bearer token (generated on first boot if omitted)
    #[arg(long, env = "OPENBOARDS_ADMIN_TOKEN")]
    pub admin_token: Option<String>,

    /// Per-principal sustained rate limit (requests per second; 0 disables)
    #[arg(long, env = "OPENBOARDS_RATE_LIMIT_RPS", default_value_t = DEFAULT_RATE_LIMIT_RPS)]
    pub rate_limit_rps: u32,

    /// Per-principal burst capacity for rate limiting
    #[arg(long, env = "OPENBOARDS_RATE_LIMIT_BURST", default_value_t = DEFAULT_RATE_LIMIT_BURST)]
    pub rate_limit_burst: u32,

    /// Local development mode (defaults data dir to ./.board, persists token to .board/token, prints env exports)
    #[arg(long, default_value_t = false)]
    pub local: bool,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub team: String,
    pub admin_token: Option<String>,
    pub rate_limit_rps: u32,
    pub rate_limit_burst: u32,
    pub db_target: DbTarget,
    pub db_max_connections: u32,
}

impl From<ServeArgs> for ServerConfig {
    fn from(args: ServeArgs) -> Self {
        let data_dir = args.data_dir.unwrap_or_else(|| {
            if args.local {
                PathBuf::from(LOCAL_DATA_DIR)
            } else {
                PathBuf::from(DEFAULT_DATA_DIR)
            }
        });
        let db_path = data_dir.join("openboards.db");
        Self {
            listen: args.listen,
            data_dir,
            team: args.team,
            admin_token: args.admin_token,
            rate_limit_rps: args.rate_limit_rps,
            rate_limit_burst: args.rate_limit_burst,
            db_target: DbTarget::SqlitePath(db_path),
            db_max_connections: DEFAULT_DB_MAX_CONNECTIONS,
        }
    }
}

impl ServerConfig {
    /// Builds `ServerConfig` from `ServeArgs`, reading `DATABASE_URL` and
    /// `OPENBOARDS_DB_MAX_CONNECTIONS` strictly from process environment variables.
    pub fn from_serve_args(args: ServeArgs) -> Result<Self, String> {
        let database_url = std::env::var("DATABASE_URL").ok();
        let max_conns = std::env::var("OPENBOARDS_DB_MAX_CONNECTIONS").ok();
        Self::from_serve_args_with_env(args, database_url.as_deref(), max_conns.as_deref())
    }

    pub fn from_serve_args_with_env(
        args: ServeArgs,
        database_url: Option<&str>,
        max_connections_env: Option<&str>,
    ) -> Result<Self, String> {
        let local_mode = args.local;
        let mut cfg = Self::from(args);
        cfg.db_target = resolve_db_target(database_url, cfg.db_path(), local_mode)?;
        cfg.db_max_connections = parse_max_connections(max_connections_env)?;
        Ok(cfg)
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("openboards.db")
    }

    pub fn local_token_path(&self) -> PathBuf {
        self.data_dir.join("token")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        serve: ServeArgs,
    }

    #[test]
    fn parses_serve_cli_flags_and_defaults() {
        let w = Wrapper::try_parse_from(["serve"]).unwrap();
        assert!(!w.serve.local);
        let cfg = ServerConfig::from(w.serve);
        assert_eq!(cfg.listen.to_string(), "127.0.0.1:8080");
        assert_eq!(cfg.data_dir, PathBuf::from("./.openboards"));
        assert_eq!(cfg.team, "default");
        assert!(cfg.admin_token.is_none());
        assert_eq!(cfg.rate_limit_rps, DEFAULT_RATE_LIMIT_RPS);
        assert_eq!(cfg.rate_limit_burst, DEFAULT_RATE_LIMIT_BURST);
        assert_eq!(cfg.db_max_connections, DEFAULT_DB_MAX_CONNECTIONS);
        assert_eq!(
            cfg.db_target,
            DbTarget::SqlitePath(PathBuf::from("./.openboards/openboards.db"))
        );

        let w = Wrapper::try_parse_from(["serve", "--local"]).unwrap();
        assert!(w.serve.local);
        let cfg = ServerConfig::from(w.serve);
        assert_eq!(cfg.data_dir, PathBuf::from("./.board"));

        let w = Wrapper::try_parse_from([
            "serve",
            "--listen",
            "0.0.0.0:9000",
            "--data-dir",
            "/tmp/ob",
            "--team",
            "acme",
            "--admin-token",
            "secret-tok",
            "--rate-limit-rps",
            "25",
            "--rate-limit-burst",
            "50",
        ])
        .unwrap();
        let cfg = ServerConfig::from(w.serve);
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:9000");
        assert_eq!(cfg.db_path(), PathBuf::from("/tmp/ob/openboards.db"));
        assert_eq!(cfg.team, "acme");
        assert_eq!(cfg.admin_token.as_deref(), Some("secret-tok"));
        assert_eq!(cfg.rate_limit_rps, 25);
        assert_eq!(cfg.rate_limit_burst, 50);

        // --database-url CLI flag must NOT exist (strictly env-var-only)
        let rejected = Wrapper::try_parse_from([
            "serve",
            "--database-url",
            "postgres://user:pass@localhost/db",
        ]);
        assert!(rejected.is_err());
    }

    #[test]
    fn resolves_env_only_database_url_and_redacts_credentials() {
        let w = Wrapper::try_parse_from(["serve"]).unwrap();
        let cfg = ServerConfig::from_serve_args_with_env(
            w.serve,
            Some("postgres://admin:supersecret@db.internal:5432/openboards?sslmode=require"),
            Some("32"),
        )
        .unwrap();
        assert!(cfg.db_target.is_postgres());
        assert_eq!(cfg.db_max_connections, 32);
        let display = cfg.db_target.redacted_display();
        assert_eq!(display, "postgres (db.internal:5432/openboards)");
        assert!(!display.contains("supersecret"));
        assert!(!display.contains("admin"));
        assert!(!display.contains("sslmode"));

        // --local + Postgres DATABASE_URL is rejected
        let w_local = Wrapper::try_parse_from(["serve", "--local"]).unwrap();
        let err = ServerConfig::from_serve_args_with_env(
            w_local.serve,
            Some("postgresql://u:secret@localhost:5432/ob"),
            None,
        )
        .unwrap_err();
        assert!(err.contains("--local"), "err={err}");
        assert!(!err.contains("secret"), "must not leak password in error");

        // Unsupported scheme fails without leaking raw URL/password
        let w_bad = Wrapper::try_parse_from(["serve"]).unwrap();
        let err = ServerConfig::from_serve_args_with_env(
            w_bad.serve,
            Some("mysql://root:my_secret_pw@localhost/ob"),
            None,
        )
        .unwrap_err();
        assert!(err.contains("unsupported DATABASE_URL scheme"), "err={err}");
        assert!(!err.contains("my_secret_pw"), "must not leak password");

        // sqlite:// URL works even with --local
        let w_sq = Wrapper::try_parse_from(["serve", "--local"]).unwrap();
        let cfg_sq = ServerConfig::from_serve_args_with_env(
            w_sq.serve,
            Some("sqlite::memory:"),
            None,
        )
        .unwrap();
        assert!(!cfg_sq.db_target.is_postgres());
        assert_eq!(cfg_sq.db_target.redacted_display(), "sqlite (:memory:)");

        // Zero max connections is rejected
        let w_zero = Wrapper::try_parse_from(["serve"]).unwrap();
        assert!(ServerConfig::from_serve_args_with_env(w_zero.serve, None, Some("0")).is_err());
    }

    #[test]
    fn parses_cli_file_config_and_aliases() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(CLI_CONFIG_FILENAME);
        std::fs::write(
            &path,
            r#"{
                "$schema": "https://example.com/openboard.schema.json",
                "url": "http://127.0.0.1:8080",
                "name": "eng",
                "token": "ob_tok",
                "as": "fixer",
                "avatar": "https://example.com/a.png",
                "scope": "acme",
                "ensure": true,
                "json": true
            }"#,
        )
        .unwrap();

        let cfg = load_cli_config_from_path(&path).unwrap();
        assert_eq!(cfg.url.as_deref(), Some("http://127.0.0.1:8080"));
        assert_eq!(cfg.board.as_deref(), Some("eng"));
        assert_eq!(cfg.token.as_deref(), Some("ob_tok"));
        assert_eq!(cfg.profile.as_deref(), Some("fixer"));
        assert_eq!(cfg.avatar.as_deref(), Some("https://example.com/a.png"));
        assert_eq!(cfg.team.as_deref(), Some("acme"));
        assert_eq!(cfg.ensure, Some(true));
        assert_eq!(cfg.json, Some(true));

        let canonical_path = tmp.path().join("canonical.json");
        std::fs::write(
            &canonical_path,
            r#"{
                "board": "ops",
                "profile": "worker-1",
                "team": "core",
                "ensure": false,
                "json": false
            }"#,
        )
        .unwrap();
        let cfg2 = load_cli_config_from_path(&canonical_path).unwrap();
        assert_eq!(cfg2.board.as_deref(), Some("ops"));
        assert_eq!(cfg2.profile.as_deref(), Some("worker-1"));
        assert_eq!(cfg2.team.as_deref(), Some("core"));
        assert_eq!(cfg2.ensure, Some(false));
        assert_eq!(cfg2.json, Some(false));
    }

    #[test]
    fn cli_file_config_rejects_unknown_fields_and_invalid_json_with_path() {
        let tmp = tempfile::tempdir().unwrap();

        let bad_field = tmp.path().join("unknown.json");
        std::fs::write(
            &bad_field,
            r#"{"url":"http://127.0.0.1:8080","database_url":"postgres://x"}"#,
        )
        .unwrap();
        let err = load_cli_config_from_path(&bad_field).unwrap_err();
        assert!(err.contains(&bad_field.display().to_string()), "err={err}");
        assert!(err.contains("database_url"), "err={err}");

        let bad_json = tmp.path().join("broken.json");
        std::fs::write(&bad_json, "{ not valid json").unwrap();
        let err = load_cli_config_from_path(&bad_json).unwrap_err();
        assert!(err.contains(&bad_json.display().to_string()), "err={err}");

        let missing = tmp.path().join("does-not-exist.json");
        let err = load_cli_config(Some(&missing), tmp.path()).unwrap_err();
        assert!(err.contains(&missing.display().to_string()), "err={err}");
    }

    #[test]
    fn finds_nearest_ancestor_openboard_json() {
        let tmp = tempfile::tempdir().unwrap();
        let root_cfg = tmp.path().join(CLI_CONFIG_FILENAME);
        let mid = tmp.path().join("packages");
        let leaf = mid.join("service/src");
        std::fs::create_dir_all(&leaf).unwrap();

        let isolated_search = find_cli_config(&leaf);
        assert!(
            isolated_search
                .as_ref()
                .map(|p| !p.starts_with(tmp.path()))
                .unwrap_or(true)
        );

        std::fs::write(&root_cfg, r#"{"board":"root-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, root_cfg);
        assert_eq!(found_cfg.board.as_deref(), Some("root-board"));

        let mid_cfg = mid.join(CLI_CONFIG_FILENAME);
        std::fs::write(&mid_cfg, r#"{"board":"mid-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, mid_cfg);
        assert_eq!(found_cfg.board.as_deref(), Some("mid-board"));

        let leaf_dir_trap = leaf.join(CLI_CONFIG_FILENAME);
        std::fs::create_dir(&leaf_dir_trap).unwrap();
        let (found_path, _) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, mid_cfg);

        let custom = tmp.path().join("custom.json");
        std::fs::write(&custom, r#"{"board":"explicit-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(Some(&custom), &leaf).unwrap().unwrap();
        assert_eq!(found_path, custom);
        assert_eq!(found_cfg.board.as_deref(), Some("explicit-board"));
    }
}
