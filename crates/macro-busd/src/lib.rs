//! # macro-busd
//!
//! The macro-bus daemon library. The `macro-busd` binary is a thin wrapper
//! around [`run`]. Exposed as a library so integration tests can drive a daemon
//! in-process.

#![forbid(unsafe_code)]

pub mod config;
pub mod conn;
pub mod federation;
pub mod registry;
pub mod server;
pub mod tls;

use std::sync::Arc;

use config::Config;
use conn::Forwarder;
use registry::Registry;
use server::LocalServer;

/// Build the shared registry for a config.
pub fn build_registry(cfg: &Config) -> Arc<Registry> {
    Arc::new(Registry::new(
        cfg.server.daemon_id.clone(),
        cfg.limits.seen_capacity,
    ))
}

/// Run a daemon to completion: bind the local socket, start federation (if
/// configured), and serve until `shutdown` resolves.
pub async fn run(cfg: Config, shutdown: impl std::future::Future<Output = ()>) -> anyhow::Result<()> {
    cfg.validate()?;
    let registry = build_registry(&cfg);

    // Start federation first so the forwarder exists before local clients can
    // publish. In standalone mode this is `None`.
    let forwarder: Option<Arc<dyn Forwarder>> = if cfg.federation_enabled() {
        let cluster = federation::Cluster::start(cfg.clone(), registry.clone()).await?;
        Some(cluster as Arc<dyn Forwarder>)
    } else {
        None
    };

    let server = LocalServer::bind(
        &cfg.server.socket_path,
        registry.clone(),
        forwarder,
        cfg.limits.clone(),
    )?;
    server.serve(shutdown).await;
    Ok(())
}
