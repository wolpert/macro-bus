//! Phase 3 integration tests: the `macro-bus-client` library against a real
//! daemon, including the async-push / command-response demultiplexing.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use macro_bus_client::{Client, ClientError, Event};
use macro_busd::config::{Config, ServerConfig};
use macro_busd::server::LocalServer;
use tokio::sync::oneshot;

static SOCK_SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_socket() -> PathBuf {
    let n = SOCK_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("mb-clib-{}-{}.sock", std::process::id(), n))
}

struct Harness {
    socket: PathBuf,
    _shutdown: oneshot::Sender<()>,
}

impl Harness {
    fn start(daemon_id: &str) -> Harness {
        let socket = unique_socket();
        let cfg = Config {
            server: ServerConfig {
                daemon_id: daemon_id.to_string(),
                socket_path: socket.clone(),
            },
            ..Config::default()
        };
        let registry = macro_busd::build_registry(&cfg);
        let server = LocalServer::bind(&socket, registry, None, cfg.limits.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(server.serve(async move {
            let _ = rx.await;
        }));
        Harness {
            socket,
            _shutdown: tx,
        }
    }

    async fn client(&self) -> Client<tokio::net::UnixStream> {
        for _ in 0..50 {
            if let Ok(c) = Client::connect(&self.socket).await {
                return c;
            }
            tokio::task::yield_now().await;
        }
        panic!("could not connect");
    }
}

#[tokio::test]
async fn end_to_end_via_client_library() {
    let h = Harness::start("d-lib");
    let mut publisher = h.client().await;
    let mut subscriber = h.client().await;

    assert_eq!(subscriber.daemon_id(), "d-lib");

    subscriber.subscribe("sensors.temp").await.unwrap();
    publisher.register("sensors.temp", "s3cr3t").await.unwrap();
    publisher
        .publish("sensors.temp", "s3cr3t", &["21.4C"])
        .await
        .unwrap();

    match subscriber.next_event().await.unwrap() {
        Event::Message(m) => {
            assert_eq!(m.type_name, "sensors.temp");
            assert_eq!(m.origin, "d-lib");
            assert_eq!(m.body, vec!["21.4C".to_string()]);
        }
        other => panic!("expected message, got {other:?}"),
    }
}

#[tokio::test]
async fn register_conflict_surfaces_as_server_error() {
    let h = Harness::start("d1");
    let mut a = h.client().await;
    let mut b = h.client().await;
    a.register("t", "key1").await.unwrap();
    // Same key is idempotent.
    b.register("t", "key1").await.unwrap();
    // Different key -> 433.
    match b.register("t", "key2").await {
        Err(ClientError::Server { code, .. }) => assert_eq!(code, 433),
        other => panic!("expected 433, got {other:?}"),
    }
}

#[tokio::test]
async fn publish_auth_errors() {
    let h = Harness::start("d1");
    let mut c = h.client().await;
    match c.publish("unknown", "k", &["x"]).await {
        Err(ClientError::Server { code, .. }) => assert_eq!(code, 430),
        other => panic!("expected 430, got {other:?}"),
    }
    c.register("t", "right").await.unwrap();
    match c.publish("t", "wrong", &["x"]).await {
        Err(ClientError::Server { code, .. }) => assert_eq!(code, 441),
        other => panic!("expected 441, got {other:?}"),
    }
}

#[tokio::test]
async fn multiline_body_roundtrip_with_dot_lines() {
    let h = Harness::start("d1");
    let mut p = h.client().await;
    let mut s = h.client().await;
    s.subscribe("t").await.unwrap();
    p.register("t", "k").await.unwrap();
    // Body includes a line that starts with '.' — the library dot-stuffs it.
    p.publish("t", "k", &["hello", ".world", "..dots", "bye"])
        .await
        .unwrap();

    match s.next_event().await.unwrap() {
        Event::Message(m) => {
            assert_eq!(
                m.body,
                vec![
                    "hello".to_string(),
                    ".world".to_string(),
                    "..dots".to_string(),
                    "bye".to_string()
                ]
            );
        }
        other => panic!("expected message, got {other:?}"),
    }
}

#[tokio::test]
async fn command_buffers_pending_push() {
    // The demux guarantee: a command issued while a push is pending must still
    // return its own reply; the push is buffered for next_event().
    let h = Harness::start("d1");
    let mut p = h.client().await;
    let mut s = h.client().await;

    s.subscribe("t").await.unwrap();
    p.register("t", "k").await.unwrap();
    p.publish("t", "k", &["payload"]).await.unwrap();

    // Give delivery a moment to land in the subscriber's socket buffer.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Now issue a command on the subscriber connection. Its reply (a type list)
    // must come back correctly even though a 101 MSG is waiting.
    let types = s.list_types().await.unwrap();
    assert_eq!(types, vec!["t".to_string()]);

    // And the buffered push is still delivered.
    match s.next_event().await.unwrap() {
        Event::Message(m) => assert_eq!(m.body, vec!["payload".to_string()]),
        other => panic!("expected buffered message, got {other:?}"),
    }
}

#[tokio::test]
async fn capabilities_and_list() {
    let h = Harness::start("d1");
    let mut c = h.client().await;
    let caps = c.capabilities().await.unwrap();
    assert!(caps.iter().any(|l| l.starts_with("VERSION MBP/1.0")));
    c.register("z.type", "k").await.unwrap();
    c.register("a.type", "k").await.unwrap();
    let types = c.list_types().await.unwrap();
    assert_eq!(types, vec!["a.type".to_string(), "z.type".to_string()]);
}
