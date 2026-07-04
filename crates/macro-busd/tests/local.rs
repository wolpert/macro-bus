//! Phase 2 integration tests: a standalone daemon driven over its Unix socket
//! with a minimal in-test raw client (no dependency on the client library, to
//! keep this phase self-contained).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use macro_busd::config::{Config, Limits, ServerConfig};
use macro_busd::server::LocalServer;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::oneshot;

static SOCK_SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_socket() -> PathBuf {
    let n = SOCK_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("mb-test-{}-{}.sock", std::process::id(), n))
}

/// A running standalone daemon for one test.
struct Harness {
    socket: PathBuf,
    _shutdown: oneshot::Sender<()>,
}

impl Harness {
    fn start(daemon_id: &str) -> Harness {
        Self::start_with(daemon_id, |_| {})
    }

    fn start_with(daemon_id: &str, tweak: impl FnOnce(&mut Limits)) -> Harness {
        let socket = unique_socket();
        let mut cfg = Config {
            server: ServerConfig {
                daemon_id: daemon_id.to_string(),
                socket_path: socket.clone(),
            },
            ..Config::default()
        };
        tweak(&mut cfg.limits);

        let registry = macro_busd::build_registry(&cfg);
        let server =
            LocalServer::bind(&socket, registry.clone(), None, cfg.limits.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(server.serve(async move {
            let _ = rx.await;
        }));

        Harness { socket, _shutdown: tx }
    }

    async fn connect(&self) -> RawClient {
        // The listener is bound synchronously before serve() spawns, so connect
        // succeeds immediately; retry a couple of times just in case.
        let mut last = None;
        for _ in 0..50 {
            match UnixStream::connect(&self.socket).await {
                Ok(s) => {
                    let mut c = RawClient { io: BufReader::new(s) };
                    // Consume the greeting.
                    let banner = c.read_line().await;
                    assert!(banner.starts_with("200 "), "greeting was {banner:?}");
                    return c;
                }
                Err(e) => {
                    last = Some(e);
                    tokio::task::yield_now().await;
                }
            }
        }
        panic!("could not connect: {last:?}");
    }
}

/// Minimal line-oriented client.
struct RawClient {
    io: BufReader<UnixStream>,
}

impl RawClient {
    async fn send(&mut self, line: &str) {
        self.io.write_all(line.as_bytes()).await.unwrap();
        self.io.write_all(b"\r\n").await.unwrap();
        self.io.flush().await.unwrap();
    }

    async fn read_line(&mut self) -> String {
        let mut s = String::new();
        let n = self.io.read_line(&mut s).await.unwrap();
        assert!(n > 0, "unexpected EOF");
        while s.ends_with('\n') || s.ends_with('\r') {
            s.pop();
        }
        s
    }

    /// Read the numeric status code of the next line and the whole line.
    async fn read_status(&mut self) -> (u16, String) {
        let line = self.read_line().await;
        let code = line[..3].parse().unwrap_or_else(|_| panic!("no code in {line:?}"));
        (code, line)
    }

    /// Read a dot-terminated block, returning its (un-stuffed) lines.
    async fn read_block(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let line = self.read_line().await;
            if line == "." {
                break;
            }
            let unstuffed = if let Some(rest) = line.strip_prefix('.') {
                rest.to_string()
            } else {
                line
            };
            out.push(unstuffed);
        }
        out
    }
}

#[tokio::test]
async fn publish_subscribe_delivery() {
    let h = Harness::start("d-test");
    let mut pubc = h.connect().await;
    let mut subc = h.connect().await;

    subc.send("SUBSCRIBE sensors.temp").await;
    assert_eq!(subc.read_status().await.0, 211);

    pubc.send("REGISTER sensors.temp s3cr3t").await;
    assert_eq!(pubc.read_status().await.0, 210);

    pubc.send("PUBLISH sensors.temp s3cr3t").await;
    assert_eq!(pubc.read_status().await.0, 354);
    pubc.send("21.4C").await;
    pubc.send(".").await;
    assert_eq!(pubc.read_status().await.0, 250);

    // Subscriber receives the async push.
    let (code, header) = subc.read_status().await;
    assert_eq!(code, 101);
    assert!(header.starts_with("101 MSG sensors.temp "), "header: {header}");
    assert!(header.ends_with(" d-test"), "origin daemon in header: {header}");
    let body = subc.read_block().await;
    assert_eq!(body, vec!["21.4C".to_string()]);
}

#[tokio::test]
async fn dot_stuffed_multiline_body_roundtrips() {
    let h = Harness::start("d1");
    let mut pubc = h.connect().await;
    let mut subc = h.connect().await;

    subc.send("SUBSCRIBE t").await;
    subc.read_status().await;
    pubc.send("REGISTER t k").await;
    pubc.read_status().await;

    pubc.send("PUBLISH t k").await;
    assert_eq!(pubc.read_status().await.0, 354);
    pubc.send("line one").await;
    pubc.send("..dotted").await; // wire-level: represents a body line ".dotted"
    pubc.send("plain").await;
    pubc.send(".").await;
    assert_eq!(pubc.read_status().await.0, 250);

    assert_eq!(subc.read_status().await.0, 101);
    let body = subc.read_block().await;
    assert_eq!(body, vec!["line one".to_string(), ".dotted".to_string(), "plain".to_string()]);
}

#[tokio::test]
async fn auth_key_rejection_and_unknown_type() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;

    // Publishing an unregistered type -> 430, no 354.
    c.send("PUBLISH nope k").await;
    assert_eq!(c.read_status().await.0, 430);

    c.send("REGISTER orders.created owner-key").await;
    assert_eq!(c.read_status().await.0, 210);

    // Wrong key -> 441, no 354, no body read.
    c.send("PUBLISH orders.created wrong").await;
    assert_eq!(c.read_status().await.0, 441);

    // Correct key -> 354.
    c.send("PUBLISH orders.created owner-key").await;
    assert_eq!(c.read_status().await.0, 354);
    c.send(".").await;
    assert_eq!(c.read_status().await.0, 250);
}

#[tokio::test]
async fn registration_idempotent_and_conflict() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;

    c.send("REGISTER t key1").await;
    assert_eq!(c.read_status().await.0, 210);
    // Same key -> idempotent success.
    c.send("REGISTER t key1").await;
    assert_eq!(c.read_status().await.0, 210);
    // Different key -> 433.
    c.send("REGISTER t key2").await;
    assert_eq!(c.read_status().await.0, 433);
}

#[tokio::test]
async fn list_types_never_leaks_keys() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;
    c.send("REGISTER b.type secret-b").await;
    c.read_status().await;
    c.send("REGISTER a.type secret-a").await;
    c.read_status().await;

    c.send("LIST TYPES").await;
    assert_eq!(c.read_status().await.0, 215);
    let types = c.read_block().await;
    assert_eq!(types, vec!["a.type".to_string(), "b.type".to_string()]);
    // No key material anywhere.
    assert!(types.iter().all(|t| !t.contains("secret")));
}

#[tokio::test]
async fn capabilities_and_help_blocks() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;
    c.send("CAPABILITIES").await;
    assert_eq!(c.read_status().await.0, 231);
    let caps = c.read_block().await;
    assert!(caps.iter().any(|l| l.starts_with("VERSION MBP/1.0")));
    assert!(caps.iter().any(|l| l == "DROP-POLICY tail-drop"));

    c.send("HELP").await;
    assert_eq!(c.read_status().await.0, 231);
    let help = c.read_block().await;
    assert!(help.iter().any(|l| l.contains("PUBLISH")));
}

#[tokio::test]
async fn syntax_errors() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;
    c.send("FROBNICATE x").await;
    assert_eq!(c.read_status().await.0, 500);
    c.send("SUBSCRIBE").await;
    assert_eq!(c.read_status().await.0, 501);
    c.send("SUBSCRIBE bad!name").await;
    assert_eq!(c.read_status().await.0, 521);
}

#[tokio::test]
async fn slow_consumer_tail_drop_notice() {
    // Depth-1 outbound queue. To force real backpressure we publish many LARGE
    // messages while the subscriber never reads: once the kernel socket buffer
    // and the depth-1 queue are both full, the connection writer blocks and the
    // registry tail-drops the rest. Total volume (200 * ~64 KiB ≈ 12 MiB) far
    // exceeds any socket send buffer, so drops are guaranteed on any OS.
    let h = Harness::start_with("d1", |l| {
        l.queue_depth = 1;
    });
    let mut pubc = h.connect().await;
    let mut subc = h.connect().await;

    subc.send("SUBSCRIBE t").await;
    subc.read_status().await;
    pubc.send("REGISTER t k").await;
    pubc.read_status().await;

    let big = "x".repeat(65_000);
    for _ in 0..200 {
        pubc.send("PUBLISH t k").await;
        assert_eq!(pubc.read_status().await.0, 354);
        pubc.send(&big).await;
        pubc.send(".").await;
        assert_eq!(pubc.read_status().await.0, 250);
    }

    // Now the subscriber drains. It sees some delivered 101 MSGs, then a
    // 102 DROP notice accounting for the messages it missed.
    let mut saw_msg = false;
    let mut saw_drop = false;
    for _ in 0..250 {
        let (code, line) = subc.read_status().await;
        match code {
            101 => {
                saw_msg = true;
                let _ = subc.read_block().await;
            }
            102 => {
                saw_drop = true;
                assert!(line.starts_with("102 DROP t "), "drop line: {line}");
                let count: u64 = line.rsplit(' ').next().unwrap().parse().unwrap();
                assert!(count >= 1);
                break;
            }
            other => panic!("unexpected code {other}: {line}"),
        }
    }
    assert!(saw_msg, "expected at least one delivered message");
    assert!(saw_drop, "expected a 102 DROP notice");
}

#[tokio::test]
async fn quit_closes_connection() {
    let h = Harness::start("d1");
    let mut c = h.connect().await;
    c.send("QUIT").await;
    assert_eq!(c.read_status().await.0, 221);
    // Server closed: next read hits EOF.
    let mut s = String::new();
    let n = c.io.read_line(&mut s).await.unwrap();
    assert_eq!(n, 0, "expected EOF after QUIT");
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let h = Harness::start("d1");
    let mut pubc = h.connect().await;
    let mut subc = h.connect().await;
    pubc.send("REGISTER t k").await;
    pubc.read_status().await;
    subc.send("SUBSCRIBE t").await;
    subc.read_status().await;
    subc.send("UNSUBSCRIBE t").await;
    assert_eq!(subc.read_status().await.0, 212);

    // Publish after unsubscribe; the subscriber must NOT receive it.
    pubc.send("PUBLISH t k").await;
    pubc.read_status().await;
    pubc.send("gone").await;
    pubc.send(".").await;
    assert_eq!(pubc.read_status().await.0, 250);

    // A read on the (now-unsubscribed) connection must time out — nothing is
    // delivered. 250 ms is ample given delivery is otherwise immediate.
    let got = tokio::time::timeout(std::time::Duration::from_millis(250), subc.read_line()).await;
    assert!(got.is_err(), "unsubscribed connection unexpectedly received: {got:?}");
}
