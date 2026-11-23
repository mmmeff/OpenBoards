use clap::Parser;
use tracing_subscriber::EnvFilter;

use openboards::config::{Cli, Command, ServerConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let result = match cli.command {
        Command::Serve(args) => openboards::serve(ServerConfig::from(args)).await,
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
