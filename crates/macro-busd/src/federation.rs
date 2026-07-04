//! Cluster federation (server-to-server), MBP §9.
//!
//! Federation carries (a) forwarded messages and (b) type-registration
//! ownership between daemons over mutual-TLS TCP links. It reuses the MBP line
//! style and adds two peering verbs:
//!
//! ```text
//! FEED <type> <msg-id> <origin-daemon-id>   ; + DATA body, dot-terminated
//! RREG <type> <key> <origin-daemon-id> <ts> ; registration propagation
//! ```
//!
//! ## Link topology
//!
//! Every daemon **dials** each configured peer and **accepts** inbound peer
//! links. Application frames are *sent* only over dialed links and *received*
//! only over accepted links, so each ordered pair of peers has exactly one
//! send path and one receive path — no wasted duplicate flooding.
//!
//! ## Loop prevention (MBP §9.3)
//!
//! Every message carries a cluster-unique `<msg-id>` and `<origin>`. A daemon
//! drops any `<msg-id>` already in its bounded seen-set, never returns a
//! message to the peer it arrived from, and never delivers a duplicate id to a
//! local subscriber (all enforced by [`Registry::ingest_remote`]). Forwarding
//! is fire-and-forget: there is **no** store-and-forward for down peers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use macro_bus_proto::status;
use macro_bus_proto::{frame, peer_greeting, Message};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::{Config, PeerConfig};
use crate::conn::Forwarder;
use crate::registry::{Registry, TypeReg};
use crate::tls;

/// Depth of each peer link's outbound frame queue.
const PEER_QUEUE_DEPTH: usize = 4096;

/// A frame queued toward a peer over a dialed link.
#[derive(Clone)]
enum OutFrame {
    /// Forward a message to the peer.
    Feed(Arc<Message>),
    /// Propagate a registration to the peer.
    Rreg { type_name: String, reg: TypeReg },
}

impl OutFrame {
    fn serialize(&self) -> String {
        match self {
            OutFrame::Feed(msg) => {
                let mut s = msg.feed_header();
                s.push_str(frame::CRLF);
                s.push_str(&frame::encode_body(&msg.body));
                s
            }
            OutFrame::Rreg { type_name, reg } => {
                format!(
                    "RREG {} {} {} {}{}",
                    type_name, reg.key, reg.origin_daemon, reg.ts, frame::CRLF
                )
            }
        }
    }
}

/// The cluster: TLS material, peer send-links, and the federation logic.
pub struct Cluster {
    registry: Arc<Registry>,
    cfg: Config,
    connector: Option<TlsConnector>,
    /// Dialed peers currently up: peer id -> outbound frame sender.
    peers: Mutex<HashMap<String, mpsc::Sender<OutFrame>>>,
    max_message_bytes: usize,
}

impl Cluster {
    /// Start federation: build TLS, spawn the listener (if configured) and one
    /// reconnecting dialer per configured peer. Returns the shared handle used
    /// as the daemon's [`Forwarder`].
    pub async fn start(cfg: Config, registry: Arc<Registry>) -> anyhow::Result<Arc<Cluster>> {
        tls::ensure_crypto_provider();
        let tls_cfg = cfg
            .tls
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("federation requires [tls]"))?;

        let connector = if cfg.cluster.peers.is_empty() {
            None
        } else {
            Some(TlsConnector::from(tls::client_config(tls_cfg)?))
        };

        let cluster = Arc::new(Cluster {
            registry: registry.clone(),
            cfg: cfg.clone(),
            connector,
            peers: Mutex::new(HashMap::new()),
            max_message_bytes: cfg.limits.max_message_bytes,
        });

        // Listener for inbound peer links.
        if let Some(listen) = cfg.cluster.listen {
            let acceptor = TlsAcceptor::from(tls::server_config(tls_cfg)?);
            let listener = TcpListener::bind(listen).await.map_err(|e| {
                anyhow::anyhow!("binding federation listener on {listen}: {e}")
            })?;
            tracing::info!(%listen, "federation listener bound");
            let me = cluster.clone();
            tokio::spawn(me.run_listener(listener, acceptor));
        }

        // One reconnecting dialer per configured peer.
        for peer in cfg.cluster.peers.clone() {
            let me = cluster.clone();
            tokio::spawn(me.run_dialer(peer));
        }

        Ok(cluster)
    }

    fn daemon_id(&self) -> &str {
        self.registry.daemon_id()
    }

    /// Send `frame` to all connected dialed peers except `exclude`.
    fn broadcast(&self, frame: OutFrame, exclude: Option<&str>) {
        let peers = self.peers.lock().unwrap();
        for (id, tx) in peers.iter() {
            if Some(id.as_str()) == exclude {
                continue;
            }
            match tx.try_send(frame.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(peer = %id, "federation queue full; dropping frame");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => { /* dialer will clean up */ }
            }
        }
    }

    // --- outbound dialing ---------------------------------------------------

    async fn run_dialer(self: Arc<Cluster>, peer: PeerConfig) {
        let base = Duration::from_millis(self.cfg.cluster.reconnect_base_ms.max(1));
        let max = Duration::from_millis(self.cfg.cluster.reconnect_max_ms.max(1));
        let mut backoff = base;
        loop {
            match self.dial_once(&peer).await {
                Ok(()) => {
                    tracing::info!(peer = %peer.id, "peer link closed; will reconnect");
                    backoff = base;
                }
                Err(e) => {
                    tracing::debug!(peer = %peer.id, error = %e, "peer dial failed");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max);
        }
    }

    async fn dial_once(&self, peer: &PeerConfig) -> anyhow::Result<()> {
        let connector = self
            .connector
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no TLS connector"))?;
        let server_name = rustls::pki_types::ServerName::try_from(peer.id.clone())
            .map_err(|_| anyhow::anyhow!("peer id {} is not a valid TLS server name", peer.id))?;

        let tcp = TcpStream::connect(&peer.addr).await?;
        tcp.set_nodelay(true).ok();
        let tls = connector.connect(server_name, tcp).await?;
        let (rd, mut wr) = tokio::io::split(tls);

        // Greeting exchange: write ours, read theirs.
        wr.write_all(format!("{}\r\n", peer_greeting(self.daemon_id())).as_bytes()).await?;
        wr.flush().await?;
        let mut br = BufReader::new(rd);
        let greeting = read_line(&mut br).await?.ok_or_else(|| anyhow::anyhow!("peer closed before greeting"))?;
        let remote_id = parse_greeting_id(&greeting).unwrap_or_else(|| peer.id.clone());
        tracing::info!(peer = %peer.id, remote = %remote_id, "dialed peer link up (mTLS)");

        // Register the send side.
        let (tx, rx) = mpsc::channel(PEER_QUEUE_DEPTH);
        self.peers.lock().unwrap().insert(peer.id.clone(), tx);

        // Reconcile: push our full registration table to the peer.
        for (type_name, reg) in self.registry.all_registrations() {
            let _ = self
                .peers
                .lock()
                .unwrap()
                .get(&peer.id)
                .map(|t| t.try_send(OutFrame::Rreg { type_name, reg }));
        }

        // Run the read and write loops until either ends.
        let result = tokio::select! {
            r = self.read_loop(br, remote_id.clone()) => r,
            w = write_loop(wr, rx) => w,
        };

        // Deregister the send side.
        self.peers.lock().unwrap().remove(&peer.id);
        result
    }

    // --- inbound accepting --------------------------------------------------

    async fn run_listener(self: Arc<Cluster>, listener: TcpListener, acceptor: TlsAcceptor) {
        loop {
            match listener.accept().await {
                Ok((tcp, addr)) => {
                    tcp.set_nodelay(true).ok();
                    let acceptor = acceptor.clone();
                    let me = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = me.handle_inbound(tcp, acceptor).await {
                            tracing::debug!(%addr, error = %e, "inbound peer link ended");
                        }
                    });
                }
                Err(e) => tracing::warn!(error = %e, "federation accept failed"),
            }
        }
    }

    async fn handle_inbound(&self, tcp: TcpStream, acceptor: TlsAcceptor) -> anyhow::Result<()> {
        let tls = acceptor.accept(tcp).await?; // completes mTLS, verifies client cert
        let (rd, mut wr) = tokio::io::split(tls);

        // Greeting exchange.
        wr.write_all(format!("{}\r\n", peer_greeting(self.daemon_id())).as_bytes()).await?;
        wr.flush().await?;
        let mut br = BufReader::new(rd);
        let greeting = read_line(&mut br).await?.ok_or_else(|| anyhow::anyhow!("peer closed before greeting"))?;
        let remote_id = parse_greeting_id(&greeting)
            .ok_or_else(|| anyhow::anyhow!("malformed peer greeting: {greeting}"))?;
        tracing::info!(remote = %remote_id, "accepted peer link up (mTLS)");

        // Inbound links are receive-only (we send over our dialed link).
        self.read_loop(br, remote_id).await
    }

    // --- inbound frame processing -------------------------------------------

    /// Read and apply federation frames from a peer until the link closes.
    async fn read_loop<R>(&self, mut br: BufReader<R>, from_peer: String) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        while let Some(line) = read_line(&mut br).await? {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, ' ');
            let verb = parts.next().unwrap_or("");
            let rest = parts.next().unwrap_or("");
            match verb {
                "FEED" => self.handle_feed(rest, &mut br, &from_peer).await?,
                "RREG" => self.handle_rreg(rest, &from_peer),
                // Greetings / unknown informational lines are ignored.
                _ if verb.chars().next().is_some_and(|c| c.is_ascii_digit()) => {}
                _ => tracing::debug!(peer = %from_peer, %verb, "ignoring unknown federation verb"),
            }
        }
        Ok(())
    }

    async fn handle_feed<R>(
        &self,
        rest: &str,
        br: &mut BufReader<R>,
        from_peer: &str,
    ) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        // rest = "<type> <msg-id> <origin>"
        let mut it = rest.split(' ');
        let type_name = it.next().unwrap_or("").to_string();
        let msg_id = it.next().unwrap_or("").to_string();
        let origin = it.next().unwrap_or("").to_string();

        // Always read the body to keep the stream framed, even if we will drop.
        let body = read_body(br, self.max_message_bytes).await?;

        if type_name.is_empty() || msg_id.is_empty() || origin.is_empty() {
            tracing::warn!(peer = %from_peer, "malformed FEED header; dropping");
            return Ok(());
        }
        let Some(body) = body else {
            return Ok(()); // oversize; already drained and dropped
        };

        let msg = Arc::new(Message::new(type_name, msg_id, origin, body));
        // Deliver locally + dedup. If new, re-forward to peers except the source.
        if self.registry.ingest_remote(msg.clone()) {
            self.broadcast(OutFrame::Feed(msg), Some(from_peer));
        }
        Ok(())
    }

    fn handle_rreg(&self, rest: &str, from_peer: &str) {
        // rest = "<type> <key> <origin> <ts>"
        let mut it = rest.split(' ');
        let type_name = it.next().unwrap_or("").to_string();
        let key = it.next().unwrap_or("").to_string();
        let origin = it.next().unwrap_or("").to_string();
        let ts: u64 = match it.next().and_then(|s| s.parse().ok()) {
            Some(t) => t,
            None => {
                tracing::warn!(peer = %from_peer, "malformed RREG; dropping");
                return;
            }
        };
        if type_name.is_empty() || key.is_empty() || origin.is_empty() {
            tracing::warn!(peer = %from_peer, "incomplete RREG; dropping");
            return;
        }
        let incoming = TypeReg { key, origin_daemon: origin, ts };
        if let Some(effective) = self.registry.apply_remote_registration(&type_name, incoming) {
            // The table changed; propagate the now-effective record onward.
            self.broadcast(OutFrame::Rreg { type_name, reg: effective }, Some(from_peer));
        }
    }
}

impl Forwarder for Cluster {
    fn forward_local(&self, msg: Arc<Message>) {
        self.broadcast(OutFrame::Feed(msg), None);
    }

    fn propagate_registration(&self, type_name: String, reg: TypeReg) {
        self.broadcast(OutFrame::Rreg { type_name, reg }, None);
    }
}

/// Drain `rx`, serializing each frame to `wr`, until the channel closes or a
/// write fails.
async fn write_loop<W>(mut wr: W, mut rx: mpsc::Receiver<OutFrame>) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = rx.recv().await {
        wr.write_all(frame.serialize().as_bytes()).await?;
        wr.flush().await?;
    }
    Ok(())
}

/// Read one CRLF/LF-terminated line, stripping the terminator. `Ok(None)` on EOF.
async fn read_line<R>(br: &mut BufReader<R>) -> std::io::Result<Option<String>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let n = br.read_until(b'\n', &mut buf).await?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

/// Read a dot-terminated DATA body, un-stuffing lines. Enforces `max_bytes`
/// (oversize bodies are drained to the terminator then reported empty-dropped).
async fn read_body<R>(br: &mut BufReader<R>, max_bytes: usize) -> std::io::Result<Option<Vec<String>>>
where
    R: AsyncRead + Unpin,
{
    let mut lines = Vec::new();
    let mut total = 0usize;
    let mut too_large = false;
    loop {
        let line = match read_line(br).await? {
            Some(l) => l,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof during federated body",
                ))
            }
        };
        if line == frame::TERMINATOR {
            break;
        }
        let unstuffed = frame::unstuff_line(&line).to_string();
        total += unstuffed.len() + 1;
        if total > max_bytes {
            too_large = true;
        }
        if !too_large {
            lines.push(unstuffed);
        }
    }
    if too_large {
        tracing::warn!("federated message exceeded max size; dropped");
        Ok(None)
    } else {
        Ok(Some(lines))
    }
}

/// Parse the daemon id out of a `200 <id> ...` greeting line.
fn parse_greeting_id(line: &str) -> Option<String> {
    let mut it = line.split(' ');
    let code = it.next()?;
    if code != status::SERVICE_READY.to_string() {
        return None;
    }
    let id = it.next()?;
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_frame_wire_format() {
        let msg = Arc::new(Message::new("sensors.temp", "d1-a", "d1", vec!["21.4C".into(), ".dot".into()]));
        let wire = OutFrame::Feed(msg).serialize();
        assert_eq!(wire, "FEED sensors.temp d1-a d1\r\n21.4C\r\n..dot\r\n.\r\n");
    }

    #[test]
    fn rreg_frame_wire_format() {
        let f = OutFrame::Rreg {
            type_name: "t".into(),
            reg: TypeReg { key: "k".into(), origin_daemon: "d1".into(), ts: 42 },
        };
        assert_eq!(f.serialize(), "RREG t k d1 42\r\n");
    }

    #[test]
    fn greeting_id_parsing() {
        assert_eq!(parse_greeting_id("200 d1 macro-bus-peer MBP/1.0 ready").as_deref(), Some("d1"));
        assert_eq!(parse_greeting_id("400 nope"), None);
        assert_eq!(parse_greeting_id("garbage"), None);
    }

    #[tokio::test]
    async fn read_line_handles_crlf_lf_and_eof() {
        let data: &[u8] = b"first\r\nsecond\nthird";
        let mut br = BufReader::new(data);
        assert_eq!(read_line(&mut br).await.unwrap().as_deref(), Some("first"));
        assert_eq!(read_line(&mut br).await.unwrap().as_deref(), Some("second"));
        // No trailing newline: the last chunk is still returned...
        assert_eq!(read_line(&mut br).await.unwrap().as_deref(), Some("third"));
        // ...then EOF.
        assert_eq!(read_line(&mut br).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_body_unstuffs_and_terminates() {
        let data: &[u8] = b"line1\r\n..dotted\r\nplain\r\n.\r\ntrailing";
        let mut br = BufReader::new(data);
        let body = read_body(&mut br, 1_000_000).await.unwrap();
        assert_eq!(
            body,
            Some(vec!["line1".to_string(), ".dotted".to_string(), "plain".to_string()])
        );
        // The reader stops at the terminator; trailing bytes remain.
        assert_eq!(read_line(&mut br).await.unwrap().as_deref(), Some("trailing"));
    }

    #[tokio::test]
    async fn read_body_oversize_is_dropped() {
        // Two 10-byte lines but a 15-byte cap -> None (drained to terminator).
        let data: &[u8] = b"aaaaaaaaaa\r\nbbbbbbbbbb\r\n.\r\n";
        let mut br = BufReader::new(data);
        assert_eq!(read_body(&mut br, 15).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_body_eof_mid_body_errors() {
        let data: &[u8] = b"line-without-terminator\r\n";
        let mut br = BufReader::new(data);
        assert!(read_body(&mut br, 1_000_000).await.is_err());
    }
}
