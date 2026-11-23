use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Args;
use serde::Deserialize;

pub const DEFAULT_RATE_LIMIT_RPS: u32 = 100;
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 200;
pub const DEFAULT_DATA_DIR: &str = "./.openboards";
pub const LOCAL_DATA_DIR: &str = "./.board";
pub const CLI_CONFIG_FILENAME: &str = ".openboard.json";

/// Global CLI configuration loaded from `.openboard.json` (or `--config <path>`).
///
/// Unknown fields are rejected so typos in config keys fail fast with a clear error.
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
        Self {
            listen: args.listen,
            data_dir,
            team: args.team,
            admin_token: args.admin_token,
            rate_limit_rps: args.rate_limit_rps,
            rate_limit_burst: args.rate_limit_burst,
        }
    }
}

impl ServerConfig {
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
        std::fs::write(&bad_field, r#"{"url":"http://127.0.0.1:8080","bogus":123}"#).unwrap();
        let err = load_cli_config_from_path(&bad_field).unwrap_err();
        assert!(err.contains(&bad_field.display().to_string()), "err={err}");
        assert!(err.contains("bogus"), "err={err}");

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

        // Absent in tree -> None (assuming no .openboard.json above tmpdir)
        let isolated_search = find_cli_config(&leaf);
        assert!(
            isolated_search
                .as_ref()
                .map(|p| !p.starts_with(tmp.path()))
                .unwrap_or(true)
        );

        // Root config found from deeply nested leaf
        std::fs::write(&root_cfg, r#"{"board":"root-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, root_cfg);
        assert_eq!(found_cfg.board.as_deref(), Some("root-board"));

        // Closer ancestor overrides outer ancestor
        let mid_cfg = mid.join(CLI_CONFIG_FILENAME);
        std::fs::write(&mid_cfg, r#"{"board":"mid-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, mid_cfg);
        assert_eq!(found_cfg.board.as_deref(), Some("mid-board"));

        // Directory named `.openboard.json` is skipped in favor of file in parent
        let leaf_dir_trap = leaf.join(CLI_CONFIG_FILENAME);
        std::fs::create_dir(&leaf_dir_trap).unwrap();
        let (found_path, _) = load_cli_config(None, &leaf).unwrap().unwrap();
        assert_eq!(found_path, mid_cfg);

        // Explicit path wins over upward search
        let custom = tmp.path().join("custom.json");
        std::fs::write(&custom, r#"{"board":"explicit-board"}"#).unwrap();
        let (found_path, found_cfg) = load_cli_config(Some(&custom), &leaf).unwrap().unwrap();
        assert_eq!(found_path, custom);
        assert_eq!(found_cfg.board.as_deref(), Some("explicit-board"));
    }
}
