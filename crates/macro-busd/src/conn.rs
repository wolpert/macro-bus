//! Per-connection handling for the local client protocol.
//!
//! ## Concurrency model & the 1xx ordering guarantee
//!
//! Each connection is a single task that owns its stream. The task multiplexes
//! three things with `tokio::select!`:
//!
//! * reading bytes from the client,
//! * draining its bounded outbound queue (delivering `101 MSG` pushes), and
//! * emitting `102 DROP` notices when the queue overflowed.
//!
//! Crucially, once a *complete* command line has been read, the task processes
//! it to completion — including reading a DATA body and writing the final
//! response — **without** touching the outbound queue. Asynchronous `1xx`
//! pushes are therefore only ever written *between* command/response exchanges,
//! never in the middle of one, satisfying MBP §4.4. Byte reads use
//! [`AsyncReadExt::read`], which is cancel-safe, so multiplexing never loses
//! buffered input.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use macro_bus_proto::command::Command;
use macro_bus_proto::status::{self, Code};
use macro_bus_proto::{command, frame, greeting, Message, PROTOCOL_ID};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};

use crate::config::Limits;
use crate::registry::{ConnHandle, Outbound, Registry, TypeReg};

/// The daemon's link to the cluster. In a standalone daemon this is absent.
pub trait Forwarder: Send + Sync {
    /// Forward a message that was just published on this daemon to all peers.
    fn forward_local(&self, msg: Arc<Message>);
    /// Propagate a newly-created or changed type registration to all peers.
    fn propagate_registration(&self, type_name: String, reg: TypeReg);
}

/// Monotonic source of connection ids.
static CONN_SEQ: AtomicU64 = AtomicU64::new(1);

/// Allocate a fresh connection id.
pub fn next_conn_id() -> u64 {
    CONN_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A live client connection.
pub struct Conn<S> {
    stream: S,
    rbuf: Vec<u8>,
    id: u64,
    registry: Arc<Registry>,
    forwarder: Option<Arc<dyn Forwarder>>,
    limits: Limits,

    // Outbound path (also handed to the registry as a `ConnHandle`).
    out_tx: mpsc::Sender<Outbound>,
    out_rx: mpsc::Receiver<Outbound>,
    drops: Arc<Mutex<HashMap<String, u64>>>,
    drop_notify: Arc<Notify>,

    my_subs: HashSet<String>,
}

/// Result of trying to extract a line from the read buffer.
enum LineTake {
    /// A complete line (CRLF/LF stripped).
    Line(Vec<u8>),
    /// No complete line buffered yet.
    Need,
    /// The line exceeded the allowed length.
    TooLong,
}

impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Create a connection wrapper around an accepted stream.
    pub fn new(
        stream: S,
        registry: Arc<Registry>,
        forwarder: Option<Arc<dyn Forwarder>>,
        limits: Limits,
    ) -> Self {
        let (out_tx, out_rx) = mpsc::channel(limits.queue_depth);
        Conn {
            stream,
            rbuf: Vec::with_capacity(4096),
            id: next_conn_id(),
            registry,
            forwarder,
            limits,
            out_tx,
            out_rx,
            drops: Arc::new(Mutex::new(HashMap::new())),
            drop_notify: Arc::new(Notify::new()),
            my_subs: HashSet::new(),
        }
    }

    fn conn_handle(&self) -> ConnHandle {
        ConnHandle {
            id: self.id,
            tx: self.out_tx.clone(),
            drops: self.drops.clone(),
            drop_notify: self.drop_notify.clone(),
        }
    }

    /// Run the connection to completion (returns when the client disconnects,
    /// QUITs, or a fatal IO error occurs). Cleans up subscriptions on exit.
    pub async fn run(mut self) {
        if let Err(e) = self.serve().await {
            tracing::debug!(conn = self.id, error = %e, "connection closed");
        }
        let subs = std::mem::take(&mut self.my_subs);
        self.registry.remove_conn(self.id, &subs);
    }

    async fn serve(&mut self) -> std::io::Result<()> {
        let banner = greeting(self.registry.daemon_id());
        self.write_raw(&format!("{banner}\r\n")).await?;

        let mut tmp = [0u8; 8192];
        loop {
            self.flush_drops().await?;

            // Process any already-buffered command line without reading more.
            match self.take_line(self.limits.max_command_line_bytes) {
                LineTake::Line(line) => {
                    if !self.handle_command(line).await? {
                        return Ok(());
                    }
                    continue;
                }
                LineTake::TooLong => {
                    self.write_line(status::SYNTAX, "command line too long")
                        .await?;
                    return Ok(());
                }
                LineTake::Need => {}
            }

            tokio::select! {
                res = self.stream.read(&mut tmp) => {
                    let n = res?;
                    if n == 0 {
                        return Ok(()); // EOF
                    }
                    self.rbuf.extend_from_slice(&tmp[..n]);
                }
                Some(out) = self.out_rx.recv() => {
                    self.write_outbound(out).await?;
                }
                _ = self.drop_notify.notified() => {
                    // loop around -> flush_drops emits 102 DROP lines.
                }
            }
        }
    }

    /// Handle one command line. Returns `Ok(false)` to close the connection.
    async fn handle_command(&mut self, line: Vec<u8>) -> std::io::Result<bool> {
        let text = String::from_utf8_lossy(&line);
        let cmd = match command::parse(&text) {
            Ok(c) => c,
            Err(e) => {
                self.write_line(e.code, &e.reason).await?;
                return Ok(true);
            }
        };

        match cmd {
            Command::Quit => {
                self.write_line(status::CLOSING, "closing connection")
                    .await?;
                return Ok(false);
            }
            Command::Capabilities => self.write_capabilities().await?,
            Command::Help => self.write_help().await?,
            Command::ListTypes => self.write_type_list().await?,
            Command::Register { type_name, key } => {
                match self.registry.register_local(&type_name, &key, now_ms()) {
                    Ok(reg) => {
                        if reg.changed {
                            if let Some(f) = &self.forwarder {
                                f.propagate_registration(type_name.clone(), reg.reg);
                            }
                        }
                        self.write_line(status::REGISTERED, &format!("{type_name} registered"))
                            .await?;
                    }
                    Err(e) => self.write_line(e.code, &e.reason).await?,
                }
            }
            Command::Subscribe { type_name } => {
                self.registry.subscribe(&type_name, self.conn_handle());
                self.my_subs.insert(type_name.clone());
                self.write_line(status::SUBSCRIBED, &format!("subscribed {type_name}"))
                    .await?;
            }
            Command::Unsubscribe { type_name } => {
                self.registry.unsubscribe(&type_name, self.id);
                self.my_subs.remove(&type_name);
                self.write_line(status::UNSUBSCRIBED, &format!("unsubscribed {type_name}"))
                    .await?;
            }
            Command::Publish { type_name, key } => {
                self.handle_publish(type_name, key).await?;
            }
        }
        Ok(true)
    }

    async fn handle_publish(&mut self, type_name: String, key: String) -> std::io::Result<bool> {
        // Pre-flight authorization BEFORE inviting the body, so we can reject
        // (430/441) instead of sending 354 and reading a body we will discard.
        if let Some(e) = self.registry.publish_precheck(&type_name, &key) {
            self.write_line(e.code, &e.reason).await?;
            return Ok(true);
        }

        self.write_line(
            status::START_BODY,
            "enter message body; end with <CRLF>.<CRLF>",
        )
        .await?;

        let body = match self.read_body().await? {
            Some(b) => b,
            None => {
                // Body exceeded limits; connection drained to the terminator.
                self.write_line(status::TOO_LARGE, "message too large")
                    .await?;
                return Ok(true);
            }
        };

        match self.registry.publish_local(&type_name, &key, body) {
            Ok(msg) => {
                if let Some(f) = &self.forwarder {
                    f.forward_local(msg);
                }
                self.write_line(status::ACCEPTED, "message accepted")
                    .await?;
            }
            Err(e) => self.write_line(e.code, &e.reason).await?,
        }
        Ok(true)
    }

    /// Read a DATA body: lines until a lone `.`; dot-unstuff; enforce size
    /// limits. Returns `Ok(None)` if the body was too large (still drained to
    /// the terminator so the stream stays framed).
    async fn read_body(&mut self) -> std::io::Result<Option<Vec<String>>> {
        let mut lines: Vec<String> = Vec::new();
        let mut total: usize = 0;
        let mut too_large = false;

        loop {
            let raw = match self
                .read_line_blocking(self.limits.max_body_line_bytes)
                .await?
            {
                Some(l) => l,
                None => {
                    // Oversize single line: treat whole body as too large but we
                    // cannot resync mid-line, so close by returning None here.
                    return Ok(None);
                }
            };
            let s = String::from_utf8_lossy(&raw);
            if s == frame::TERMINATOR {
                break;
            }
            let unstuffed = frame::unstuff_line(&s).to_string();
            total += unstuffed.len() + 1;
            if total > self.limits.max_message_bytes {
                too_large = true;
            }
            if !too_large {
                lines.push(unstuffed);
            }
        }

        if too_large {
            Ok(None)
        } else {
            Ok(Some(lines))
        }
    }

    // --- line IO ------------------------------------------------------------

    /// Extract a complete line from the read buffer, if one is present.
    fn take_line(&mut self, max: usize) -> LineTake {
        if let Some(pos) = self.rbuf.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = self.rbuf.drain(..=pos).collect();
            line.pop(); // '\n'
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.len() > max {
                return LineTake::TooLong;
            }
            LineTake::Line(line)
        } else if self.rbuf.len() > max {
            LineTake::TooLong
        } else {
            LineTake::Need
        }
    }

    /// Read a single line, blocking on the socket as needed (NOT multiplexed
    /// with the outbound queue — used inside a DATA exchange). Returns
    /// `Ok(None)` on an over-long line, and errors on EOF mid-body.
    async fn read_line_blocking(&mut self, max: usize) -> std::io::Result<Option<Vec<u8>>> {
        let mut tmp = [0u8; 8192];
        loop {
            match self.take_line(max) {
                LineTake::Line(l) => return Ok(Some(l)),
                LineTake::TooLong => return Ok(None),
                LineTake::Need => {}
            }
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof during DATA body",
                ));
            }
            self.rbuf.extend_from_slice(&tmp[..n]);
        }
    }

    // --- writers ------------------------------------------------------------

    async fn write_line(&mut self, code: Code, text: &str) -> std::io::Result<()> {
        let line = format!("{code} {text}\r\n");
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.flush().await
    }

    async fn write_raw(&mut self, s: &str) -> std::io::Result<()> {
        self.stream.write_all(s.as_bytes()).await?;
        self.stream.flush().await
    }

    async fn write_outbound(&mut self, out: Outbound) -> std::io::Result<()> {
        match out {
            Outbound::Deliver(msg) => self.write_raw(&msg.encode_delivery()).await,
            Outbound::Note(text) => self.write_line(status::NOTE, &text).await,
        }
    }

    /// Emit any pending `102 DROP` notices for this connection.
    async fn flush_drops(&mut self) -> std::io::Result<()> {
        let pending: Vec<(String, u64)> = {
            let mut d = self.drops.lock().unwrap();
            if d.is_empty() {
                return Ok(());
            }
            d.drain().collect()
        };
        for (type_name, count) in pending {
            self.write_line(status::DROP, &format!("DROP {type_name} {count}"))
                .await?;
        }
        Ok(())
    }

    async fn write_capabilities(&mut self) -> std::io::Result<()> {
        self.write_line(status::INFO_FOLLOWS, "capabilities follow")
            .await?;
        let caps = [
            format!("VERSION {PROTOCOL_ID}"),
            format!("MAXMSG {}", self.limits.max_message_bytes),
            format!("QUEUE {}", self.limits.queue_depth),
            "DROP-POLICY tail-drop".to_string(),
            "PAYLOAD text".to_string(),
            "TLS federation".to_string(),
        ];
        let mut block = String::new();
        for c in caps {
            block.push_str(&frame::stuff_line(&c));
            block.push_str(frame::CRLF);
        }
        block.push_str(frame::TERMINATOR);
        block.push_str(frame::CRLF);
        self.write_raw(&block).await
    }

    async fn write_help(&mut self) -> std::io::Result<()> {
        self.write_line(status::INFO_FOLLOWS, "help follows")
            .await?;
        let help = [
            "REGISTER <type> <key>    claim a type (first-registrant wins)",
            "SUBSCRIBE <type>         start receiving a type (no key)",
            "UNSUBSCRIBE <type>       stop receiving a type",
            "PUBLISH <type> <key>     publish; then send body, end with '.'",
            "LIST TYPES               list known types",
            "CAPABILITIES             list capabilities",
            "HELP                     this help",
            "QUIT                     close the connection",
        ];
        let mut block = String::new();
        for l in help {
            block.push_str(&frame::stuff_line(l));
            block.push_str(frame::CRLF);
        }
        block.push_str(frame::TERMINATOR);
        block.push_str(frame::CRLF);
        self.write_raw(&block).await
    }

    async fn write_type_list(&mut self) -> std::io::Result<()> {
        self.write_line(status::TYPE_LIST, "type list follows")
            .await?;
        let types = self.registry.list_types();
        let mut block = String::new();
        for t in types {
            block.push_str(&frame::stuff_line(&t));
            block.push_str(frame::CRLF);
        }
        block.push_str(frame::TERMINATOR);
        block.push_str(frame::CRLF);
        self.write_raw(&block).await
    }
}
