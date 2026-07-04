//! TLS configuration for federation links (mutual TLS via rustls).
//!
//! Both directions of a peer link authenticate with certificates: when we dial
//! a peer we verify its certificate against the configured CA bundle and
//! present our own; when we accept a peer we require and verify its client
//! certificate against the same bundle. Peer identity is bound to the
//! certificate's subject-alternative-name, which by convention is the peer's
//! daemon id (that is the `ServerName` we verify against on dial).
//!
//! `ring` is used as the crypto provider for portability across Linux and
//! FreeBSD (no C toolchain / assembler requirements at build time).

use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::config::TlsConfig;

/// Install the process-wide `ring` crypto provider (idempotent).
pub fn ensure_crypto_provider() {
    // Errors only if a provider is already installed; that is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn load_certs(path: &std::path::Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let data = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("reading cert {}: {e}", path.display()))?;
    let mut reader = BufReader::new(&data[..]);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs.map_err(|e| anyhow::anyhow!("parsing certs {}: {e}", path.display()))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in {}", path.display());
    }
    Ok(certs)
}

fn load_key(path: &std::path::Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let data = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("reading key {}: {e}", path.display()))?;
    let mut reader = BufReader::new(&data[..]);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| anyhow::anyhow!("parsing key {}: {e}", path.display()))?;
    key.ok_or_else(|| anyhow::anyhow!("no private key found in {}", path.display()))
}

fn load_roots(path: &std::path::Path) -> anyhow::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| anyhow::anyhow!("adding CA cert from {}: {e}", path.display()))?;
    }
    Ok(roots)
}

/// Build the rustls server config for accepting peer links (requires and
/// verifies client certificates — mutual TLS).
pub fn server_config(tls: &TlsConfig) -> anyhow::Result<Arc<ServerConfig>> {
    ensure_crypto_provider();
    let certs = load_certs(&tls.cert)?;
    let key = load_key(&tls.key)?;
    let roots = Arc::new(load_roots(&tls.ca)?);

    let verifier = rustls::server::WebPkiClientVerifier::builder(roots)
        .build()
        .map_err(|e| anyhow::anyhow!("building client verifier: {e}"))?;

    let cfg = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("building server tls config: {e}"))?;
    Ok(Arc::new(cfg))
}

/// Build the rustls client config for dialing peers (verifies the peer's
/// server certificate against the CA and presents our own — mutual TLS).
pub fn client_config(tls: &TlsConfig) -> anyhow::Result<Arc<ClientConfig>> {
    ensure_crypto_provider();
    let certs = load_certs(&tls.cert)?;
    let key = load_key(&tls.key)?;
    let roots = load_roots(&tls.ca)?;

    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("building client tls config: {e}"))?;
    Ok(Arc::new(cfg))
}
