use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

pub const DEFAULT_RATE_LIMIT_RPS: u32 = 100;
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 200;

#[derive(Debug, Parser)]
#[command(name = "openboards", version, about = "Self-hostable message board for teams and coding agents")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the OpenBoards HTTP server
    Serve(ServeArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Address to bind the HTTP server to
    #[arg(long, env = "OPENBOARDS_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    /// Directory for SQLite database and local blob storage
    #[arg(long, env = "OPENBOARDS_DATA_DIR", default_value = "./.openboards")]
    pub data_dir: PathBuf,

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
        Self {
            listen: args.listen,
            data_dir: args.data_dir,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_serve_cli_flags_and_defaults() {
        let cli = Cli::try_parse_from(["openboards", "serve"]).unwrap();
        let Command::Serve(args) = cli.command;
        assert_eq!(args.listen.to_string(), "127.0.0.1:8080");
        assert_eq!(args.data_dir, PathBuf::from("./.openboards"));
        assert_eq!(args.team, "default");
        assert!(args.admin_token.is_none());
        assert_eq!(args.rate_limit_rps, DEFAULT_RATE_LIMIT_RPS);
        assert_eq!(args.rate_limit_burst, DEFAULT_RATE_LIMIT_BURST);

        let cli = Cli::try_parse_from([
            "openboards",
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
        let Command::Serve(args) = cli.command;
        let cfg = ServerConfig::from(args);
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:9000");
        assert_eq!(cfg.db_path(), PathBuf::from("/tmp/ob/openboards.db"));
        assert_eq!(cfg.team, "acme");
        assert_eq!(cfg.admin_token.as_deref(), Some("secret-tok"));
        assert_eq!(cfg.rate_limit_rps, 25);
        assert_eq!(cfg.rate_limit_burst, 50);
    }
}
