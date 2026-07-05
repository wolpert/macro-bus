//! Phase 4 integration test: a two-daemon cluster over real mutual TLS.
//!
//! Proves that a message published on daemon A reaches a subscriber on daemon
//! B (and vice-versa), that there is no duplicate delivery (loop prevention),
//! and that type-registration ownership propagates so B enforces A's auth key.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use macro_bus_client::{Client, ClientError, Event};
use macro_busd::config::{ClusterConfig, Config, Limits, PeerConfig, ServerConfig, TlsConfig};
use tokio::net::UnixStream;
use tokio::sync::oneshot;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp(name: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("mb-cluster-{}-{}-{}", std::process::id(), n, name))
}

/// Grab an ephemeral localhost port by binding and immediately releasing it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A self-signed cert+key whose SAN is the daemon id, written to disk.
struct Node {
    id: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_pem: String,
}

fn gen_node(id: &str) -> Node {
    let ck = rcgen::generate_simple_self_signed(vec![id.to_string()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.signing_key.serialize_pem();
    let cert_path = tmp(&format!("{id}.crt"));
    let key_path = tmp(&format!("{id}.key"));
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();
    Node {
        id: id.to_string(),
        cert_path,
        key_path,
        cert_pem,
    }
}

/// A running daemon in the cluster test.
struct Daemon {
    socket: PathBuf,
    _shutdown: oneshot::Sender<()>,
}

impl Daemon {
    async fn connect(&self) -> Client<UnixStream> {
        for _ in 0..100 {
            if let Ok(c) = Client::connect(&self.socket).await {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("could not connect to {}", self.socket.display());
    }
}

fn start_daemon(
    node: &Node,
    listen_port: u16,
    peer_id: &str,
    peer_port: u16,
    ca_path: &std::path::Path,
) -> Daemon {
    let socket = tmp(&format!("{}.sock", node.id));
    let cfg = Config {
        server: ServerConfig {
            daemon_id: node.id.clone(),
            socket_path: socket.clone(),
        },
        limits: Limits::default(),
        cluster: ClusterConfig {
            listen: Some(format!("127.0.0.1:{listen_port}").parse().unwrap()),
            peers: vec![PeerConfig {
                id: peer_id.to_string(),
                addr: format!("127.0.0.1:{peer_port}"),
            }],
            reconnect_base_ms: 50,
            reconnect_max_ms: 200,
        },
        tls: Some(TlsConfig {
            cert: node.cert_path.clone(),
            key: node.key_path.clone(),
            ca: ca_path.to_path_buf(),
        }),
    };
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = macro_busd::run(cfg, async move {
            let _ = rx.await;
        })
        .await;
    });
    Daemon {
        socket,
        _shutdown: tx,
    }
}

/// Start a daemon that dials several peers (full-mesh member).
fn start_daemon_multi(
    node: &Node,
    listen_port: u16,
    peers: &[(&str, u16)],
    ca_path: &std::path::Path,
) -> Daemon {
    let socket = tmp(&format!("{}.sock", node.id));
    let cfg = Config {
        server: ServerConfig {
            daemon_id: node.id.clone(),
            socket_path: socket.clone(),
        },
        limits: Limits::default(),
        cluster: ClusterConfig {
            listen: Some(format!("127.0.0.1:{listen_port}").parse().unwrap()),
            peers: peers
                .iter()
                .map(|(id, port)| PeerConfig {
                    id: id.to_string(),
                    addr: format!("127.0.0.1:{port}"),
                })
                .collect(),
            reconnect_base_ms: 50,
            reconnect_max_ms: 200,
        },
        tls: Some(TlsConfig {
            cert: node.cert_path.clone(),
            key: node.key_path.clone(),
            ca: ca_path.to_path_buf(),
        }),
    };
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = macro_busd::run(cfg, async move {
            let _ = rx.await;
        })
        .await;
    });
    Daemon {
        socket,
        _shutdown: tx,
    }
}

/// Read the next message event within a timeout.
async fn next_msg(
    c: &mut Client<UnixStream>,
    timeout: Duration,
) -> Option<macro_bus_proto::Message> {
    loop {
        match tokio::time::timeout(timeout, c.next_event()).await {
            Ok(Ok(Event::Message(m))) => return Some(m),
            Ok(Ok(_)) => continue, // skip drop/note
            Ok(Err(_)) => return None,
            Err(_) => return None, // timed out
        }
    }
}

#[tokio::test]
async fn two_daemon_cluster_forwards_with_loop_prevention() {
    // Two nodes; shared CA bundle = both self-signed certs.
    let a = gen_node("d1");
    let b = gen_node("d2");
    let ca_path = tmp("ca.pem");
    std::fs::write(&ca_path, format!("{}{}", a.cert_pem, b.cert_pem)).unwrap();

    let port_a = free_port();
    let port_b = free_port();

    let da = start_daemon(&a, port_a, "d2", port_b, &ca_path);
    let db = start_daemon(&b, port_b, "d1", port_a, &ca_path);

    // Register the type on A and subscribe on B.
    let mut pub_a = da.connect().await;
    let mut sub_b = db.connect().await;
    pub_a.register("sensors.temp", "s3cr3t").await.unwrap();
    sub_b.subscribe("sensors.temp").await.unwrap();

    // --- Establish the link: publish probes on A until one reaches B. The TLS
    // peer link and registration propagation take a moment to come up. ---
    let mut established = false;
    for _ in 0..100 {
        pub_a
            .publish("sensors.temp", "s3cr3t", &["probe"])
            .await
            .unwrap();
        if next_msg(&mut sub_b, Duration::from_millis(100))
            .await
            .is_some()
        {
            established = true;
            break;
        }
    }
    assert!(established, "message from A never reached B's subscriber");

    // Drain any remaining probes so the queue is empty.
    while next_msg(&mut sub_b, Duration::from_millis(150))
        .await
        .is_some()
    {}

    // --- Cross-daemon delivery with a unique payload, exactly once. ---
    pub_a
        .publish("sensors.temp", "s3cr3t", &["UNIQUE-A-PAYLOAD"])
        .await
        .unwrap();
    let got = next_msg(&mut sub_b, Duration::from_millis(1000))
        .await
        .expect("delivery to B");
    assert_eq!(got.body, vec!["UNIQUE-A-PAYLOAD".to_string()]);
    assert_eq!(
        got.origin, "d1",
        "origin daemon preserved across federation"
    );

    // Loop prevention: the same message must NOT be delivered twice.
    let dup = next_msg(&mut sub_b, Duration::from_millis(300)).await;
    assert!(dup.is_none(), "duplicate delivery detected: {dup:?}");

    // --- Registration propagated: B now knows d1 owns the type + its key. ---
    let mut pub_b = db.connect().await;
    // Wrong key on B is rejected using the propagated ownership.
    match pub_b.publish("sensors.temp", "WRONG", &["x"]).await {
        Err(ClientError::Server { code, .. }) => assert_eq!(code, 441),
        other => panic!("expected 441 on B for wrong key, got {other:?}"),
    }

    // --- Reverse direction: publish on B reaches a subscriber on A. ---
    let mut sub_a = da.connect().await;
    sub_a.subscribe("sensors.temp").await.unwrap();
    // Small settle for the SUBSCRIBE to register before we publish.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut reverse_ok = false;
    for _ in 0..50 {
        pub_b
            .publish("sensors.temp", "s3cr3t", &["FROM-B"])
            .await
            .unwrap();
        if let Some(m) = next_msg(&mut sub_a, Duration::from_millis(100)).await {
            assert_eq!(m.body, vec!["FROM-B".to_string()]);
            assert_eq!(m.origin, "d2");
            reverse_ok = true;
            break;
        }
    }
    assert!(reverse_ok, "message from B never reached A's subscriber");
}

#[tokio::test]
async fn three_node_cluster_reforwards_without_duplicates() {
    // Full mesh d1-d2-d3. A message published on d1 must reach subscribers on
    // BOTH d2 and d3 exactly once, even though d3 can receive it directly from
    // d1 AND re-forwarded from d2 (dedup + re-forward path).
    let n1 = gen_node("d1");
    let n2 = gen_node("d2");
    let n3 = gen_node("d3");
    let ca = tmp("ca3.pem");
    std::fs::write(
        &ca,
        format!("{}{}{}", n1.cert_pem, n2.cert_pem, n3.cert_pem),
    )
    .unwrap();

    let p1 = free_port();
    let p2 = free_port();
    let p3 = free_port();

    let d1 = start_daemon_multi(&n1, p1, &[("d2", p2), ("d3", p3)], &ca);
    let d2 = start_daemon_multi(&n2, p2, &[("d1", p1), ("d3", p3)], &ca);
    let d3 = start_daemon_multi(&n3, p3, &[("d1", p1), ("d2", p2)], &ca);

    let mut pub1 = d1.connect().await;
    let mut sub2 = d2.connect().await;
    let mut sub3 = d3.connect().await;
    pub1.register("evt", "k").await.unwrap();
    sub2.subscribe("evt").await.unwrap();
    sub3.subscribe("evt").await.unwrap();

    // Establish the mesh: publish probes until both d2 and d3 receive one.
    let mut up2 = false;
    let mut up3 = false;
    for _ in 0..200 {
        pub1.publish("evt", "k", &["probe"]).await.unwrap();
        if !up2
            && next_msg(&mut sub2, Duration::from_millis(50))
                .await
                .is_some()
        {
            up2 = true;
        }
        if !up3
            && next_msg(&mut sub3, Duration::from_millis(50))
                .await
                .is_some()
        {
            up3 = true;
        }
        if up2 && up3 {
            break;
        }
    }
    assert!(
        up2 && up3,
        "mesh never fully established (d2={up2}, d3={up3})"
    );

    // Drain any leftover probes on both subscribers.
    while next_msg(&mut sub2, Duration::from_millis(150))
        .await
        .is_some()
    {}
    while next_msg(&mut sub3, Duration::from_millis(150))
        .await
        .is_some()
    {}

    // One unique message from d1 -> exactly once on d2 and d3.
    pub1.publish("evt", "k", &["ONCE"]).await.unwrap();

    let g2 = next_msg(&mut sub2, Duration::from_millis(1000))
        .await
        .expect("d2 delivery");
    assert_eq!(g2.body, vec!["ONCE".to_string()]);
    assert!(
        next_msg(&mut sub2, Duration::from_millis(300))
            .await
            .is_none(),
        "d2 got a duplicate"
    );

    let g3 = next_msg(&mut sub3, Duration::from_millis(1000))
        .await
        .expect("d3 delivery");
    assert_eq!(g3.body, vec!["ONCE".to_string()]);
    assert_eq!(g3.origin, "d1");
    assert!(
        next_msg(&mut sub3, Duration::from_millis(300))
            .await
            .is_none(),
        "d3 got a duplicate"
    );
}
