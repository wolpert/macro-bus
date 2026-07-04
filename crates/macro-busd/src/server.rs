//! The local Unix-domain-socket server: bind, accept, and spawn a
//! [`Conn`](crate::conn::Conn) task per client.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::UnixListener;

use crate::conn::{Conn, Forwarder};
use crate::config::Limits;
use crate::registry::Registry;

/// A bound local server, ready to accept clients.
pub struct LocalServer {
    listener: UnixListener,
    socket_path: PathBuf,
    registry: Arc<Registry>,
    forwarder: Option<Arc<dyn Forwarder>>,
    limits: Limits,
}

impl LocalServer {
    /// Bind the Unix socket at `socket_path`, replacing any stale socket file,
    /// and restrict its permissions to the owner (mode 0600).
    pub fn bind(
        socket_path: &Path,
        registry: Arc<Registry>,
        forwarder: Option<Arc<dyn Forwarder>>,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        // Remove a stale socket left by a previous run (in-memory only; a
        // restart is a clean slate, so an old socket file is never meaningful).
        if let Ok(meta) = std::fs::symlink_metadata(socket_path) {
            use std::os::unix::fs::FileTypeExt;
            if meta.file_type().is_socket() {
                let _ = std::fs::remove_file(socket_path);
            } else {
                anyhow::bail!(
                    "refusing to bind: {} exists and is not a socket",
                    socket_path.display()
                );
            }
        }
        if let Some(parent) = socket_path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                anyhow::bail!("socket directory {} does not exist", parent.display());
            }
        }

        let listener = UnixListener::bind(socket_path)
            .map_err(|e| anyhow::anyhow!("binding {}: {e}", socket_path.display()))?;

        // Restrict the socket to its owner. Portable across Linux & FreeBSD.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(socket_path, perms)
                .map_err(|e| anyhow::anyhow!("chmod {}: {e}", socket_path.display()))?;
        }

        Ok(LocalServer {
            listener,
            socket_path: socket_path.to_path_buf(),
            registry,
            forwarder,
            limits,
        })
    }

    /// Accept connections until `shutdown` resolves.
    pub async fn serve(self, shutdown: impl std::future::Future<Output = ()>) {
        tracing::info!(socket = %self.socket_path.display(), "local server listening");
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("local server shutting down");
                    break;
                }
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            let conn = Conn::new(
                                stream,
                                self.registry.clone(),
                                self.forwarder.clone(),
                                self.limits.clone(),
                            );
                            tokio::spawn(conn.run());
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "accept failed");
                        }
                    }
                }
            }
        }
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        // Best-effort cleanup of the socket file.
        let _ = std::fs::remove_file(&self.socket_path);
    }
}
