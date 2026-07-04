//! Daemon configuration (TOML), loaded with `serde`.
//!
//! Everything has a sensible default so a bare `macro-busd` (no config file)
//! runs as a standalone local bus. A config file overrides fields; the CLI
//! overrides the config file for a few common knobs.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

/// Default Unix domain socket path used on both Linux and FreeBSD.
pub use macro_bus_proto::DEFAULT_SOCKET_PATH;

/// Top-level daemon configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Local socket server settings.
    #[serde(default)]
    pub server: ServerConfig,
    /// Bounds on messages, lines and queues.
    #[serde(default)]
    pub limits: Limits,
    /// Optional federation / cluster settings. Absent => standalone.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// Optional TLS material for federation links.
    pub tls: Option<TlsConfig>,
}

/// Local socket server settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// This daemon's cluster-unique identifier. MUST be unique across a
    /// cluster (it tags message ids and breaks registration ties).
    #[serde(default = "default_daemon_id")]
    pub daemon_id: String,
    /// Path to the Unix domain socket local apps connect to.
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

/// Bounds enforced by the daemon.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Maximum total body size of a single message, in octets.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Maximum length of a command line, in octets (excluding CRLF).
    #[serde(default = "default_max_command_line_bytes")]
    pub max_command_line_bytes: usize,
    /// Maximum length of a single DATA body line, in octets (excluding CRLF).
    #[serde(default = "default_max_body_line_bytes")]
    pub max_body_line_bytes: usize,
    /// Depth of each subscriber's bounded outbound queue (tail-drop beyond this).
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,
    /// Capacity of the per-daemon seen-message set used for loop prevention.
    #[serde(default = "default_seen_capacity")]
    pub seen_capacity: usize,
}

/// Federation / cluster settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Address to listen on for inbound peer (federation) connections. When
    /// `None`, this daemon does not accept peer links.
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    /// Static list of peers to dial. Empty => standalone.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// Base reconnect backoff in milliseconds (doubles up to `reconnect_max_ms`).
    #[serde(default = "default_reconnect_base_ms")]
    pub reconnect_base_ms: u64,
    /// Maximum reconnect backoff in milliseconds.
    #[serde(default = "default_reconnect_max_ms")]
    pub reconnect_max_ms: u64,
}

/// A single configured peer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// The peer's expected daemon id (used for logging and its cert name).
    pub id: String,
    /// `host:port` to dial.
    pub addr: String,
}

/// TLS material for federation. Mutual TLS is used: every peer presents a
/// client/server certificate signed by a CA in `ca`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to this daemon's certificate chain (PEM).
    pub cert: PathBuf,
    /// Path to this daemon's private key (PEM).
    pub key: PathBuf,
    /// Path to the CA bundle used to verify peers (PEM).
    pub ca: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            daemon_id: default_daemon_id(),
            socket_path: default_socket_path(),
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_message_bytes: default_max_message_bytes(),
            max_command_line_bytes: default_max_command_line_bytes(),
            max_body_line_bytes: default_max_body_line_bytes(),
            queue_depth: default_queue_depth(),
            seen_capacity: default_seen_capacity(),
        }
    }
}

impl Config {
    /// Load a config from a TOML file.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing config {}: {e}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate cross-field invariants.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.server.daemon_id.is_empty() {
            anyhow::bail!("server.daemon_id must not be empty");
        }
        if (!self.cluster.peers.is_empty() || self.cluster.listen.is_some())
            && self.tls.is_none()
        {
            anyhow::bail!("cluster federation requires a [tls] section (cert/key/ca)");
        }
        if self.limits.queue_depth == 0 {
            anyhow::bail!("limits.queue_depth must be >= 1");
        }
        Ok(())
    }

    /// True iff any federation (listener or peers) is configured.
    pub fn federation_enabled(&self) -> bool {
        self.cluster.listen.is_some() || !self.cluster.peers.is_empty()
    }
}

fn default_daemon_id() -> String {
    "macro-busd".to_string()
}
fn default_socket_path() -> PathBuf {
    PathBuf::from(DEFAULT_SOCKET_PATH)
}
fn default_max_message_bytes() -> usize {
    1 << 20 // 1 MiB
}
fn default_max_command_line_bytes() -> usize {
    4096
}
fn default_max_body_line_bytes() -> usize {
    65536
}
fn default_queue_depth() -> usize {
    1024
}
fn default_seen_capacity() -> usize {
    65536
}
fn default_reconnect_base_ms() -> u64 {
    500
}
fn default_reconnect_max_ms() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_standalone() {
        let cfg = Config::default();
        assert!(!cfg.federation_enabled());
        assert_eq!(cfg.server.socket_path, PathBuf::from(DEFAULT_SOCKET_PATH));
        assert_eq!(cfg.limits.queue_depth, 1024);
        cfg.validate().unwrap();
    }

    #[test]
    fn parses_minimal_toml() {
        let cfg: Config = toml::from_str(
            r#"
            [server]
            daemon_id = "d1"
            socket_path = "/tmp/mb.sock"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.server.daemon_id, "d1");
        assert_eq!(cfg.limits.max_message_bytes, 1 << 20);
    }

    #[test]
    fn parses_cluster_toml() {
        let cfg: Config = toml::from_str(
            r#"
            [server]
            daemon_id = "d1"

            [cluster]
            listen = "0.0.0.0:9440"
            [[cluster.peers]]
            id = "d2"
            addr = "127.0.0.1:9441"

            [tls]
            cert = "/etc/mb/cert.pem"
            key = "/etc/mb/key.pem"
            ca = "/etc/mb/ca.pem"
        "#,
        )
        .unwrap();
        assert!(cfg.federation_enabled());
        assert_eq!(cfg.cluster.peers.len(), 1);
        assert_eq!(cfg.cluster.peers[0].id, "d2");
        cfg.validate().unwrap();
    }

    #[test]
    fn federation_without_tls_is_rejected() {
        let cfg: Config = toml::from_str(
            r#"
            [cluster]
            listen = "0.0.0.0:9440"
        "#,
        )
        .unwrap();
        assert!(cfg.validate().is_err());
    }
}
