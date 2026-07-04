//! `macro-busd` — the macro-bus daemon binary.

use std::path::PathBuf;

use clap::Parser;
use macro_busd::config::Config;

/// The macro-bus daemon: a fire-and-forget, in-memory pub/sub message bus.
#[derive(Debug, Parser)]
#[command(name = "macro-busd", version, about)]
struct Cli {
    /// Path to the TOML config file. If omitted, built-in defaults are used
    /// (standalone local bus, socket at /var/run/macro-bus.sock).
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Override the daemon id (server.daemon_id).
    #[arg(long)]
    id: Option<String>,

    /// Override the Unix socket path (server.socket_path).
    #[arg(short, long)]
    socket: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "macro_busd=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let mut cfg = match &cli.config {
        Some(path) => Config::load(path)?,
        None => Config::default(),
    };
    if let Some(id) = cli.id {
        cfg.server.daemon_id = id;
    }
    if let Some(socket) = cli.socket {
        cfg.server.socket_path = socket;
    }
    cfg.validate()?;

    tracing::info!(
        daemon_id = %cfg.server.daemon_id,
        socket = %cfg.server.socket_path.display(),
        federation = cfg.federation_enabled(),
        "starting macro-busd"
    );

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received ctrl-c");
    };

    macro_busd::run(cfg, shutdown).await
}
