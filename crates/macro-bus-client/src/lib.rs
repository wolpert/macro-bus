//! # macro-bus-client
//!
//! A small async client library for the Macro-Bus Protocol (MBP/1.0).
//!
//! A [`Client`] wraps one connection. It exposes request/response commands
//! ([`register`](Client::register), [`publish`](Client::publish),
//! [`subscribe`](Client::subscribe), …) and an async
//! [`next_event`](Client::next_event) for receiving server-initiated `1xx`
//! pushes (delivered messages, drop notices, notes).
//!
//! ## Threading model
//!
//! A `Client` is driven from a single task (`&mut self`): commands and
//! `next_event` share the one connection. Any `1xx` push seen while awaiting a
//! command's reply is buffered and later returned by `next_event`, exactly as
//! MBP §4.4 requires (pushes only ever arrive *between* exchanges). For
//! concurrent publish-and-subscribe, open two clients — one connection each,
//! mirroring the daemon's one-task-per-connection model.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::VecDeque;
use std::path::Path;

use macro_bus_proto::status::{self, Code};
use macro_bus_proto::{frame, Message};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Default cap on a single received line (matches the daemon's DATA line cap).
const DEFAULT_MAX_LINE: usize = 128 * 1024;

/// An asynchronous, server-initiated event (a `1xx` push).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A delivered message (`101 MSG`).
    Message(Message),
    /// A slow-consumer tail-drop notice (`102 DROP`).
    Drop {
        /// The message type messages were dropped for.
        type_name: String,
        /// How many were dropped since the previous notice for this type.
        count: u64,
    },
    /// A free-form informational note (`190 NOTE`).
    Note(String),
}

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Underlying IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The server closed the connection unexpectedly.
    #[error("connection closed by server")]
    Closed,
    /// The server returned a `4xx`/`5xx` error for a command.
    #[error("server error {code}: {text}")]
    Server {
        /// The MBP status code.
        code: Code,
        /// The reason text.
        text: String,
    },
    /// The server sent a response that violates the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// A raw command response: a final status line plus, for list-style replies, a
/// dot-terminated block.
#[derive(Debug, Clone)]
struct Response {
    code: Code,
    text: String,
    block: Option<Vec<String>>,
}

/// An MBP client over an arbitrary byte stream. Use [`Client::connect`] for the
/// common Unix-socket case.
pub struct Client<S> {
    io: BufReader<S>,
    pending: VecDeque<Event>,
    max_line: usize,
    /// The daemon id parsed from the greeting.
    daemon_id: String,
}

impl Client<UnixStream> {
    /// Connect to a daemon's Unix socket and read the greeting.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(socket_path.as_ref()).await?;
        Client::from_stream(stream).await
    }
}

impl<S> Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Wrap an already-connected stream, consuming the greeting line.
    pub async fn from_stream(stream: S) -> Result<Self, ClientError> {
        let mut client = Client {
            io: BufReader::new(stream),
            pending: VecDeque::new(),
            max_line: DEFAULT_MAX_LINE,
            daemon_id: String::new(),
        };
        let greeting = client.read_line().await?;
        let (code, text) = split_status(&greeting)?;
        if code != status::SERVICE_READY {
            return Err(ClientError::Server { code, text });
        }
        // "200 <daemon-id> macro-bus MBP/1.0 ready"
        client.daemon_id = text.split(' ').next().unwrap_or("").to_string();
        Ok(client)
    }

    /// The peer daemon's id, as reported in its greeting.
    pub fn daemon_id(&self) -> &str {
        &self.daemon_id
    }

    // --- commands -----------------------------------------------------------

    /// `REGISTER <type> <key>` — claim ownership of a type. Succeeds on `210`
    /// (including the idempotent same-key case); a conflicting key yields
    /// [`ClientError::Server`] with code `433`.
    pub async fn register(&mut self, type_name: &str, key: &str) -> Result<(), ClientError> {
        self.send(&format!("REGISTER {type_name} {key}")).await?;
        self.expect(status::REGISTERED).await.map(|_| ())
    }

    /// `SUBSCRIBE <type>` — begin receiving a type on this connection.
    pub async fn subscribe(&mut self, type_name: &str) -> Result<(), ClientError> {
        self.send(&format!("SUBSCRIBE {type_name}")).await?;
        self.expect(status::SUBSCRIBED).await.map(|_| ())
    }

    /// `UNSUBSCRIBE <type>` — stop receiving a type.
    pub async fn unsubscribe(&mut self, type_name: &str) -> Result<(), ClientError> {
        self.send(&format!("UNSUBSCRIBE {type_name}")).await?;
        self.expect(status::UNSUBSCRIBED).await.map(|_| ())
    }

    /// `PUBLISH <type> <key>` then a DATA body. `body` is the sequence of body
    /// lines (no CRLFs); they are dot-stuffed automatically. Succeeds on `250`.
    pub async fn publish(
        &mut self,
        type_name: &str,
        key: &str,
        body: &[impl AsRef<str>],
    ) -> Result<(), ClientError> {
        self.send(&format!("PUBLISH {type_name} {key}")).await?;
        // Expect the 354 body invitation (errors like 430/441 come here instead).
        let resp = self.read_response().await?;
        if resp.code != status::START_BODY {
            return Err(ClientError::Server {
                code: resp.code,
                text: resp.text,
            });
        }
        let lines: Vec<String> = body.iter().map(|l| l.as_ref().to_string()).collect();
        let block = frame::encode_body(&lines);
        self.io.write_all(block.as_bytes()).await?;
        self.io.flush().await?;
        self.expect(status::ACCEPTED).await.map(|_| ())
    }

    /// `LIST TYPES` — enumerate known type names (keys are never disclosed).
    pub async fn list_types(&mut self) -> Result<Vec<String>, ClientError> {
        self.send("LIST TYPES").await?;
        let resp = self.expect(status::TYPE_LIST).await?;
        Ok(resp.block.unwrap_or_default())
    }

    /// `CAPABILITIES` — the server's advertised capabilities.
    pub async fn capabilities(&mut self) -> Result<Vec<String>, ClientError> {
        self.send("CAPABILITIES").await?;
        let resp = self.expect(status::INFO_FOLLOWS).await?;
        Ok(resp.block.unwrap_or_default())
    }

    /// `HELP` — human-readable help lines.
    pub async fn help(&mut self) -> Result<Vec<String>, ClientError> {
        self.send("HELP").await?;
        let resp = self.expect(status::INFO_FOLLOWS).await?;
        Ok(resp.block.unwrap_or_default())
    }

    /// `QUIT` — ask the server to close the connection.
    pub async fn quit(&mut self) -> Result<(), ClientError> {
        self.send("QUIT").await?;
        self.expect(status::CLOSING).await.map(|_| ())
    }

    // --- async receive ------------------------------------------------------

    /// Await the next asynchronous server push (a delivered [`Event::Message`],
    /// [`Event::Drop`] or [`Event::Note`]). Buffered pushes seen during earlier
    /// commands are returned first.
    ///
    /// Returns [`ClientError::Closed`] if the connection ends. Returns
    /// [`ClientError::Protocol`] if a non-`1xx` line arrives unsolicited (which
    /// must not happen between exchanges per MBP §4.4).
    pub async fn next_event(&mut self) -> Result<Event, ClientError> {
        if let Some(ev) = self.pending.pop_front() {
            return Ok(ev);
        }
        let line = self.read_line().await?;
        let (code, rest) = split_status(&line)?;
        if status::is_async(code) {
            self.read_push(code, &rest).await
        } else {
            Err(ClientError::Protocol(format!(
                "unexpected non-1xx line while awaiting a push: {line}"
            )))
        }
    }

    // --- internals ----------------------------------------------------------

    async fn send(&mut self, line: &str) -> Result<(), ClientError> {
        self.io.write_all(line.as_bytes()).await?;
        self.io.write_all(b"\r\n").await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Read the reply to a just-sent command, buffering any `1xx` pushes that
    /// arrive before it.
    async fn read_response(&mut self) -> Result<Response, ClientError> {
        loop {
            let line = self.read_line().await?;
            let (code, rest) = split_status(&line)?;
            if status::is_async(code) {
                let ev = self.read_push(code, &rest).await?;
                self.pending.push_back(ev);
                continue;
            }
            // A dot-terminated block follows these codes.
            let block = if code == status::TYPE_LIST || code == status::INFO_FOLLOWS {
                Some(self.read_block().await?)
            } else {
                None
            };
            return Ok(Response {
                code,
                text: rest,
                block,
            });
        }
    }

    /// Like [`read_response`], but treat any code other than `expected` as a
    /// [`ClientError::Server`].
    async fn expect(&mut self, expected: Code) -> Result<Response, ClientError> {
        let resp = self.read_response().await?;
        if resp.code == expected {
            Ok(resp)
        } else {
            Err(ClientError::Server {
                code: resp.code,
                text: resp.text,
            })
        }
    }

    /// Parse a `1xx` push whose header (after the code) is `rest`, reading a
    /// body block for `101 MSG`.
    async fn read_push(&mut self, code: Code, rest: &str) -> Result<Event, ClientError> {
        match code {
            status::MSG => {
                // "MSG <type> <msg-id> <origin>"
                let mut it = rest.split(' ');
                let kw = it.next().unwrap_or("");
                if kw != "MSG" {
                    return Err(ClientError::Protocol(format!("bad 101 header: {rest}")));
                }
                let type_name = it.next().unwrap_or("").to_string();
                let msg_id = it.next().unwrap_or("").to_string();
                let origin = it.next().unwrap_or("").to_string();
                if type_name.is_empty() || msg_id.is_empty() || origin.is_empty() {
                    return Err(ClientError::Protocol(format!(
                        "incomplete 101 header: {rest}"
                    )));
                }
                let body = self.read_block().await?;
                Ok(Event::Message(Message::new(
                    type_name, msg_id, origin, body,
                )))
            }
            status::DROP => {
                // "DROP <type> <count>"
                let mut it = rest.split(' ');
                let kw = it.next().unwrap_or("");
                if kw != "DROP" {
                    return Err(ClientError::Protocol(format!("bad 102 header: {rest}")));
                }
                let type_name = it.next().unwrap_or("").to_string();
                let count = it
                    .next()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ClientError::Protocol(format!("bad 102 count: {rest}")))?;
                Ok(Event::Drop { type_name, count })
            }
            status::NOTE => {
                // "NOTE <text>" — strip the NOTE keyword if present.
                let text = rest.strip_prefix("NOTE ").unwrap_or(rest).to_string();
                Ok(Event::Note(text))
            }
            other => Err(ClientError::Protocol(format!("unknown 1xx code {other}"))),
        }
    }

    /// Read a dot-terminated block, un-stuffing each line.
    async fn read_block(&mut self) -> Result<Vec<String>, ClientError> {
        let mut out = Vec::new();
        loop {
            let line = self.read_line().await?;
            if line == frame::TERMINATOR {
                return Ok(out);
            }
            out.push(frame::unstuff_line(&line).to_string());
        }
    }

    /// Read one CRLF/LF-terminated line, stripping the terminator.
    async fn read_line(&mut self) -> Result<String, ClientError> {
        let mut buf = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            let n = tokio::io::AsyncReadExt::read(&mut self.io, &mut byte).await?;
            if n == 0 {
                return Err(ClientError::Closed);
            }
            if byte[0] == b'\n' {
                break;
            }
            buf.push(byte[0]);
            if buf.len() > self.max_line {
                return Err(ClientError::Protocol("line too long".into()));
            }
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

/// Split `"<code> <text>"` into `(code, text)`.
fn split_status(line: &str) -> Result<(Code, String), ClientError> {
    if line.len() < 3 {
        return Err(ClientError::Protocol(format!(
            "short response line: {line:?}"
        )));
    }
    let code: Code = line[..3]
        .parse()
        .map_err(|_| ClientError::Protocol(format!("non-numeric status: {line:?}")))?;
    let text = line.get(4..).unwrap_or("").to_string();
    Ok((code, text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, DuplexStream};

    #[test]
    fn split_status_parsing() {
        assert_eq!(split_status("210 ok").unwrap(), (210, "ok".to_string()));
        assert_eq!(split_status("221").unwrap(), (221, String::new()));
        assert!(matches!(
            split_status("ab").unwrap_err(),
            ClientError::Protocol(_)
        ));
        assert!(matches!(
            split_status("xyz foo").unwrap_err(),
            ClientError::Protocol(_)
        ));
    }

    /// Spawn a scripted "server" that writes `script` (starting with a greeting)
    /// and then holds the connection until `hold` is done, returning a connected
    /// client. The server ignores anything the client sends.
    async fn scripted(script: &'static [u8], keep_open: bool) -> Client<DuplexStream> {
        let (client_io, mut server_io) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            server_io.write_all(script).await.unwrap();
            server_io.flush().await.unwrap();
            if keep_open {
                // Keep the write half alive so the client doesn't see EOF.
                std::future::pending::<()>().await;
            }
            // else: drop server_io -> client sees Closed.
        });
        Client::from_stream(client_io).await.unwrap()
    }

    #[tokio::test]
    async fn greeting_records_daemon_id() {
        let c = scripted(b"200 dz macro-bus MBP/1.0 ready\r\n", true).await;
        assert_eq!(c.daemon_id(), "dz");
    }

    #[tokio::test]
    async fn greeting_error_is_surfaced() {
        let (client_io, mut server_io) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            server_io
                .write_all(b"400 service not available\r\n")
                .await
                .unwrap();
            server_io.flush().await.unwrap();
            std::future::pending::<()>().await;
        });
        match Client::from_stream(client_io).await {
            Err(ClientError::Server { code, .. }) => assert_eq!(code, 400),
            Err(e) => panic!("expected 400, got error {e:?}"),
            Ok(_) => panic!("expected 400, got a connected client"),
        }
    }

    #[tokio::test]
    async fn message_push_is_parsed_and_unstuffed() {
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              101 MSG t abc d\r\nline1\r\n..dot\r\n.\r\n",
            true,
        )
        .await;
        match c.next_event().await.unwrap() {
            Event::Message(m) => {
                assert_eq!(m.type_name, "t");
                assert_eq!(m.msg_id, "abc");
                assert_eq!(m.origin, "d");
                assert_eq!(m.body, vec!["line1".to_string(), ".dot".to_string()]);
            }
            other => panic!("expected message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drop_and_note_pushes() {
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              102 DROP sensors.temp 37\r\n\
              190 NOTE peer d2 up\r\n",
            true,
        )
        .await;
        assert_eq!(
            c.next_event().await.unwrap(),
            Event::Drop {
                type_name: "sensors.temp".into(),
                count: 37
            }
        );
        assert_eq!(
            c.next_event().await.unwrap(),
            Event::Note("peer d2 up".into())
        );
    }

    #[tokio::test]
    async fn non_1xx_while_awaiting_push_is_protocol_error() {
        let mut c = scripted(b"200 d macro-bus MBP/1.0 ready\r\n250 unexpected\r\n", true).await;
        assert!(matches!(
            c.next_event().await,
            Err(ClientError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn closed_connection_reports_closed() {
        let mut c = scripted(b"200 d macro-bus MBP/1.0 ready\r\n", false).await;
        assert!(matches!(c.next_event().await, Err(ClientError::Closed)));
    }

    #[tokio::test]
    async fn command_error_mapped_to_server_error() {
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n433 t already registered\r\n",
            true,
        )
        .await;
        match c.register("t", "k").await {
            Err(ClientError::Server { code, text }) => {
                assert_eq!(code, 433);
                assert!(text.contains("already registered"));
            }
            other => panic!("expected server error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn simple_command_acks() {
        // subscribe / unsubscribe / quit happy paths.
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              211 subscribed t\r\n\
              212 unsubscribed t\r\n\
              221 closing connection\r\n",
            true,
        )
        .await;
        c.subscribe("t").await.unwrap();
        c.unsubscribe("t").await.unwrap();
        c.quit().await.unwrap();
    }

    #[tokio::test]
    async fn list_and_help_and_capabilities_blocks() {
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              215 type list follows\r\na.t\r\nb.t\r\n.\r\n\
              231 help follows\r\nHELP line\r\n.\r\n\
              231 capabilities follow\r\nVERSION MBP/1.0\r\n.\r\n",
            true,
        )
        .await;
        assert_eq!(
            c.list_types().await.unwrap(),
            vec!["a.t".to_string(), "b.t".to_string()]
        );
        assert_eq!(c.help().await.unwrap(), vec!["HELP line".to_string()]);
        assert_eq!(
            c.capabilities().await.unwrap(),
            vec!["VERSION MBP/1.0".to_string()]
        );
    }

    #[tokio::test]
    async fn publish_body_then_ack() {
        // 354 invitation, then (after body) 250.
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              354 enter body\r\n250 message accepted\r\n",
            true,
        )
        .await;
        c.publish("t", "k", &[".dotted", "plain"]).await.unwrap();
    }

    #[tokio::test]
    async fn publish_precheck_error_instead_of_354() {
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n441 authorization key mismatch\r\n",
            true,
        )
        .await;
        match c.publish("t", "wrong", &["x"]).await {
            Err(ClientError::Server { code, .. }) => assert_eq!(code, 441),
            other => panic!("expected 441, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn push_buffered_during_command_is_returned_by_next_event() {
        // A 101 push arrives before the command reply; the reply must still be
        // matched, and the push returned afterwards.
        let mut c = scripted(
            b"200 d macro-bus MBP/1.0 ready\r\n\
              101 MSG t id1 d\r\nhi\r\n.\r\n\
              211 subscribed t\r\n",
            true,
        )
        .await;
        c.subscribe("t").await.unwrap();
        match c.next_event().await.unwrap() {
            Event::Message(m) => assert_eq!(m.body, vec!["hi".to_string()]),
            other => panic!("expected buffered message, got {other:?}"),
        }
    }
}
