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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmp(name: &str) -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("mb-tls-{}-{}-{}", std::process::id(), n, name))
    }

    /// Write a self-signed cert/key for `id` and return (cert_path, key_path,
    /// cert_pem).
    fn write_node(id: &str) -> (std::path::PathBuf, std::path::PathBuf, String) {
        let ck = rcgen::generate_simple_self_signed(vec![id.to_string()]).unwrap();
        let cert_pem = ck.cert.pem();
        let key_pem = ck.key_pair.serialize_pem();
        let cert_path = tmp(&format!("{id}.crt"));
        let key_path = tmp(&format!("{id}.key"));
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();
        (cert_path, key_path, cert_pem)
    }

    fn tls_config_for(id: &str, ca_certs: &str) -> TlsConfig {
        let (cert, key, _) = write_node(id);
        let ca = tmp(&format!("{id}-ca.pem"));
        std::fs::write(&ca, ca_certs).unwrap();
        TlsConfig { cert, key, ca }
    }

    #[test]
    fn builds_server_and_client_configs() {
        let (cert, key, cert_pem) = write_node("d1");
        let ca = tmp("ca.pem");
        std::fs::write(&ca, &cert_pem).unwrap();
        let tls = TlsConfig { cert, key, ca };
        assert!(server_config(&tls).is_ok());
        assert!(client_config(&tls).is_ok());
    }

    #[test]
    fn loads_certs_key_and_roots() {
        let (cert, key, cert_pem) = write_node("d2");
        assert_eq!(load_certs(&cert).unwrap().len(), 1);
        load_key(&key).unwrap();
        let ca = tmp("roots.pem");
        // A CA bundle with two certs.
        std::fs::write(&ca, format!("{cert_pem}{cert_pem}")).unwrap();
        let roots = load_roots(&ca).unwrap();
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn missing_files_error() {
        let tls = TlsConfig {
            cert: tmp("nope.crt"),
            key: tmp("nope.key"),
            ca: tmp("nope.ca"),
        };
        assert!(server_config(&tls).is_err());
        assert!(client_config(&tls).is_err());
    }

    #[test]
    fn empty_cert_file_errors() {
        let empty = tmp("empty.pem");
        std::fs::write(&empty, "").unwrap();
        assert!(load_certs(&empty).is_err());
    }

    #[test]
    fn key_file_without_key_errors() {
        // A file that parses as PEM certs but contains no private key.
        let (_, _, cert_pem) = write_node("d3");
        let f = tmp("certonly.pem");
        std::fs::write(&f, cert_pem).unwrap();
        assert!(load_key(&f).is_err());
    }

    #[test]
    fn shared_ca_bundle_builds_for_two_nodes() {
        // Emulate the two-node cluster: shared CA bundle = both certs.
        let ck1 = rcgen::generate_simple_self_signed(vec!["n1".to_string()]).unwrap();
        let ck2 = rcgen::generate_simple_self_signed(vec!["n2".to_string()]).unwrap();
        let bundle = format!("{}{}", ck1.cert.pem(), ck2.cert.pem());
        let t1 = tls_config_for("n1", &bundle);
        let t2 = tls_config_for("n2", &bundle);
        assert!(server_config(&t1).is_ok());
        assert!(client_config(&t2).is_ok());
    }
}
