//! True end-to-end tests: spawn the **actual compiled binaries** (`macro-busd`
//! and the `macro-bus` CLI) as separate OS processes and drive the whole system
//! over real sockets — no in-process shortcuts. This is the "prove it works"
//! test: it exercises argument parsing, the daemon binary, the CLI binary, the
//! Unix socket transport, and (for the cluster test) real mutual TLS between two
//! daemon processes.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp(name: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("mb-e2e-{}-{}-{}", std::process::id(), n, name))
}

/// Path to the compiled `macro-bus` CLI (guaranteed built for this crate).
fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_macro-bus"))
}

/// Path to the compiled `macro-busd` daemon. It lives next to the CLI in the
/// target dir. Under `cargo test --workspace` it is already built; under
/// `cargo test -p macro-bus-cli` we build it on demand so the test is
/// self-contained.
fn daemon_bin() -> PathBuf {
    let dir = cli_bin().parent().unwrap().to_path_buf();
    let path = dir.join("macro-busd");
    if !path.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "-p", "macro-busd", "--bin", "macro-busd"])
            .status()
            .expect("spawn cargo build for macro-busd");
        assert!(status.success(), "failed to build macro-busd");
    }
    path
}

/// A daemon child process that is killed on drop.
struct DaemonProc(std::process::Child);
impl Drop for DaemonProc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_socket(path: &std::path::Path, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("socket {} never appeared", path.display());
}

/// Run the CLI to completion, asserting success, and return its stdout.
fn cli(args: &[&str]) -> String {
    let out = Command::new(cli_bin())
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn macro-bus CLI");
    assert!(
        out.status.success(),
        "CLI {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn end_to_end_standalone_real_binaries() {
    let sock = tmp("d.sock");

    // 1. Start the real daemon binary.
    let daemon = DaemonProc(
        Command::new(daemon_bin())
            .args(["--id", "e2e", "--socket", sock.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn macro-busd"),
    );
    wait_for_socket(&sock, Duration::from_secs(5));
    let s = sock.to_str().unwrap();

    // 2. Register a type via the CLI binary.
    let out = cli(&["--socket", s, "register", "chat.msg", "sekret"]);
    assert!(out.contains("registered chat.msg"), "register said: {out}");

    // 3. Start a subscriber CLI process, capturing its stdout.
    let sub = Command::new(cli_bin())
        .args(["--socket", s, "subscribe", "chat.msg"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn subscriber");
    // Give it time to connect and SUBSCRIBE.
    std::thread::sleep(Duration::from_millis(600));

    // 4. Publish two messages via the CLI: one from --message, one from stdin.
    cli(&[
        "--socket",
        s,
        "publish",
        "chat.msg",
        "sekret",
        "--message",
        "hello from e2e",
    ]);

    let mut pubc = Command::new(cli_bin())
        .args(["--socket", s, "publish", "chat.msg", "sekret"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn publisher");
    pubc.stdin
        .take()
        .unwrap()
        .write_all(b"line A\nline B\n")
        .unwrap();
    assert!(pubc.wait().unwrap().success());

    // 5. LIST TYPES via CLI shows the registered type (no keys).
    let list = cli(&["--socket", s, "list"]);
    assert!(list.contains("chat.msg"), "list said: {list}");
    assert!(
        !list.contains("sekret"),
        "list must never leak keys: {list}"
    );

    // Let deliveries land, then stop the daemon so the subscriber exits cleanly.
    std::thread::sleep(Duration::from_millis(500));
    drop(daemon);

    let out = sub.wait_with_output().expect("collect subscriber output");
    let stdout = String::from_utf8_lossy(&out.stdout);

    // 6. The subscriber process received both publishes end to end.
    assert!(
        stdout.contains("hello from e2e"),
        "subscriber missed msg 1:\n{stdout}"
    );
    assert!(
        stdout.contains("line A") && stdout.contains("line B"),
        "subscriber missed msg 2:\n{stdout}"
    );
    assert!(
        stdout.contains("from e2e"),
        "delivery header should name origin daemon:\n{stdout}"
    );
}

// --- two-node cluster over real mutual TLS, real binaries -------------------

fn gen_cert(id: &str) -> (PathBuf, PathBuf, String) {
    let ck = rcgen::generate_simple_self_signed(vec![id.to_string()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.key_pair.serialize_pem();
    let cert = tmp(&format!("{id}.crt"));
    let key = tmp(&format!("{id}.key"));
    std::fs::write(&cert, &cert_pem).unwrap();
    std::fs::write(&key, &key_pem).unwrap();
    (cert, key, cert_pem)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn end_to_end_cluster_real_binaries() {
    // Two real daemon processes federated over mutual TLS; publish on d1 via the
    // CLI, receive on d2 via the CLI.
    let (c1, k1, p1) = gen_cert("d1");
    let (c2, k2, p2) = gen_cert("d2");
    let ca = tmp("ca.pem");
    std::fs::write(&ca, format!("{p1}{p2}")).unwrap();

    let sock1 = tmp("d1.sock");
    let sock2 = tmp("d2.sock");
    let port1 = free_port();
    let port2 = free_port();

    let write_cfg = |id: &str,
                     sock: &std::path::Path,
                     listen: u16,
                     peer_id: &str,
                     peer_port: u16,
                     cert: &std::path::Path,
                     key: &std::path::Path| {
        let path = tmp(&format!("{id}.toml"));
        let body = format!(
            "[server]\ndaemon_id = \"{id}\"\nsocket_path = \"{}\"\n\n\
             [cluster]\nlisten = \"127.0.0.1:{listen}\"\nreconnect_base_ms = 100\nreconnect_max_ms = 1000\n\
             [[cluster.peers]]\nid = \"{peer_id}\"\naddr = \"127.0.0.1:{peer_port}\"\n\n\
             [tls]\ncert = \"{}\"\nkey = \"{}\"\nca = \"{}\"\n",
            sock.display(), cert.display(), key.display(), ca.display(),
        );
        std::fs::write(&path, body).unwrap();
        path
    };

    let cfg1 = write_cfg("d1", &sock1, port1, "d2", port2, &c1, &k1);
    let cfg2 = write_cfg("d2", &sock2, port2, "d1", port1, &c2, &k2);

    let _d1 = DaemonProc(
        Command::new(daemon_bin())
            .args(["--config", cfg1.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn d1"),
    );
    let _d2 = DaemonProc(
        Command::new(daemon_bin())
            .args(["--config", cfg2.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn d2"),
    );
    wait_for_socket(&sock1, Duration::from_secs(5));
    wait_for_socket(&sock2, Duration::from_secs(5));
    let (s1, s2) = (sock1.to_str().unwrap(), sock2.to_str().unwrap());

    // Register on d1, subscribe on d2 (separate daemon).
    cli(&["--socket", s1, "register", "weather.temp", "k"]);
    let sub = Command::new(cli_bin())
        .args(["--socket", s2, "subscribe", "weather.temp"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn cluster subscriber");

    // Publish repeatedly on d1 while the TLS peer link comes up (~1s). At least
    // one message lands after the link is established.
    for _ in 0..15 {
        cli(&[
            "--socket",
            s1,
            "publish",
            "weather.temp",
            "k",
            "--message",
            "CROSS-NODE-18C",
        ]);
        std::thread::sleep(Duration::from_millis(300));
    }

    // Stop the daemons so the subscriber exits cleanly and flushes its stdout.
    drop(_d1);
    drop(_d2);
    let out = sub
        .wait_with_output()
        .expect("collect d2 subscriber output");
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        stdout.contains("CROSS-NODE-18C"),
        "message published on d1 never reached the d2 subscriber:\n{stdout}"
    );
    // The delivery header names the ORIGIN daemon (d1), proving it crossed.
    assert!(
        stdout.contains("from d1"),
        "delivery should be attributed to origin d1:\n{stdout}"
    );
}
