use clap::Parser;
use tracing_subscriber::EnvFilter;

use openboards::cli::{run_cli, Cli, Command};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize tracing logs for long-running server/proxy commands (keep CLI stdout clean)
    if matches!(cli.command, Command::Serve(_) | Command::Proxy(_)) {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .init();
    }

    let code = run_cli(cli).await;
    if code != 0 {
        std::process::exit(code);
    }
}
