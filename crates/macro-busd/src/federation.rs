//! Cluster federation (server-to-server). Implemented in Phase 4.

use std::sync::Arc;

use macro_bus_proto::Message;

use crate::config::Config;
use crate::conn::Forwarder;
use crate::registry::{Registry, TypeReg};

/// The cluster: peer links plus the fan-in/fan-out federation logic.
pub struct Cluster;

impl Cluster {
    /// Start federation. Placeholder until Phase 4.
    pub async fn start(_cfg: Config, _registry: Arc<Registry>) -> anyhow::Result<Arc<Cluster>> {
        anyhow::bail!("federation not yet implemented")
    }
}

impl Forwarder for Cluster {
    fn forward_local(&self, _msg: Arc<Message>) {}
    fn propagate_registration(&self, _type_name: String, _reg: TypeReg) {}
}
