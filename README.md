# macro-bus

A small, fast, **fire-and-forget** publish/subscribe message bus for local
applications, written in Rust. It runs equally well on **Linux and FreeBSD**.

- Local apps talk to a per-host daemon (`macro-busd`) over a **Unix domain
  socket** using a **human-readable, line-oriented protocol** in the spirit of
  NNTP and SMTP — you can drive the whole bus by hand with `nc`/`socat`.
- Daemons optionally form a **cluster** over **mutual-TLS** links so a message
  published on one box is delivered to matching listeners on *every* box.
- **Everything is in memory.** Nothing is ever persisted. A restart is a clean
  slate — by design.

The wire protocol is specified normatively in [`PROTOCOL.md`](PROTOCOL.md)
(MBP/1.0); the implementation conforms to it.

## The mental model (read this first)

1. **Types.** Messages are typed. A *type* is an opaque string like
   `sensors.temperature` or `orders.created`. Apps publish and subscribe at the
   type level (exact match; no wildcards in v1).
2. **First-registrant owns a type.** The first app to `REGISTER` a type sets its
   **authorization key**. Anyone can *subscribe* to a type without a key, but to
   *publish* you must present the owner's key.
3. **Fire-and-forget.** When you publish, the daemon hands the message to every
   *current* listener and drops it. No acknowledgements, no buffering for absent
   or slow consumers, no replay. If nobody is listening, the message evaporates.
4. **One logical bus across a cluster.** Daemons forward published messages to
   their configured peers, which deliver to their own local listeners. Loop
   prevention guarantees exactly-once delivery to each listener.
5. **Slow consumers can't stall the bus.** Each subscriber has a bounded queue;
   if it overflows, the daemon **tail-drops** for that subscriber only and tells
   it (`102 DROP`). Publishers are never back-pressured.

## Workspace layout

| Crate | What it is |
|---|---|
| `macro-bus-proto` | Protocol types, parser/serializer, status codes, DATA framing. Shared, IO-free. |
| `macro-busd` | The daemon: socket server, in-memory registry, TLS federation. |
| `macro-bus-client` | Async Rust client library. |
| `macro-bus-cli` | The `macro-bus` command-line tool / reference client. |

## Quickstart (standalone, one box)

Build:

```sh
cargo build --release
```

Start the daemon on a socket you can write to (the default is
`/var/run/macro-bus.sock`, which usually needs root; use a local path for
testing):

```sh
./target/release/macro-busd --id d1 --socket /tmp/macro-bus.sock
```

In a second terminal, subscribe:

```sh
./target/release/macro-bus --socket /tmp/macro-bus.sock subscribe sensors.temp
```

In a third terminal, claim the type and publish:

```sh
./target/release/macro-bus --socket /tmp/macro-bus.sock register sensors.temp s3cr3t
echo 21.4C | ./target/release/macro-bus --socket /tmp/macro-bus.sock publish sensors.temp s3cr3t
```

The subscriber prints:

```
--- sensors.temp [d1-1] from d1
21.4C
```

### Driving it by hand

Because the protocol is plain text, you can use `nc`:

```sh
$ nc -U /tmp/macro-bus.sock
200 d1 macro-bus MBP/1.0 ready
REGISTER sensors.temp s3cr3t
210 sensors.temp registered
PUBLISH sensors.temp s3cr3t
354 enter message body; end with <CRLF>.<CRLF>
21.4C
.
250 message accepted
QUIT
221 closing connection
```

There is a scripted end-to-end demo in
[`examples/demo.sh`](examples/demo.sh) and a Rust client example in
[`crates/macro-bus-client/examples/ping_pong.rs`](crates/macro-bus-client/examples/ping_pong.rs):

```sh
./examples/demo.sh
cargo run -p macro-bus-client --example ping_pong -- /tmp/macro-bus.sock
```

## Quickstart (a two-node cluster on one machine)

The [`examples/two-node-cluster.sh`](examples/two-node-cluster.sh) script
generates self-signed mTLS certs, starts two federated daemons, subscribes on
node B, publishes on node A, and shows the message crossing the cluster:

```sh
./examples/two-node-cluster.sh
```

To wire a cluster yourself, give each daemon a config file (see below) with a
`[cluster]` section listing its peers and a `[tls]` section. Certs can be made
with [`scripts/gen-certs.sh`](scripts/gen-certs.sh).

## The CLI

```
macro-bus [--socket PATH] <command>

  register <type> <key>       Claim a type (first-registrant wins).
  publish  <type> <key>       Publish; body from --message or stdin.
           [--message TEXT]
  subscribe <type>...         Subscribe and print deliveries until Ctrl-C.
  list                        List known types (keys are never shown).
  capabilities                Show the daemon's capabilities.
  remote-help                 Show the daemon's protocol HELP.
```

## Configuration

`macro-busd` reads an optional TOML config (`--config path.toml`). Every field
has a sensible default, so a bare `macro-busd` runs as a standalone local bus.
CLI flags `--id` and `--socket` override the corresponding fields.

```toml
[server]
# Cluster-unique daemon id. MUST be unique across a cluster: it tags message
# ids and breaks registration ties. In a cluster it must match this daemon's
# TLS certificate subjectAltName.
daemon_id = "d1"
# Unix socket local apps connect to.
socket_path = "/var/run/macro-bus.sock"

[limits]
max_message_bytes      = 1048576   # 1 MiB max total body
max_command_line_bytes = 4096      # command line cap
max_body_line_bytes    = 65536     # single DATA line cap
queue_depth            = 1024      # per-subscriber outbound queue (tail-drop beyond)
seen_capacity          = 65536     # loop-prevention dedup horizon

# --- Federation (optional). Omit the whole cluster/tls sections for a
# --- standalone local bus.
[cluster]
listen           = "0.0.0.0:9440"  # accept inbound peer links here
reconnect_base_ms = 500            # dial backoff base
reconnect_max_ms  = 30000          # dial backoff ceiling

[[cluster.peers]]
id   = "d2"                        # peer daemon id (== its cert SAN)
addr = "10.0.0.2:9440"             # host:port to dial

[tls]
cert = "/etc/macro-bus/d1.crt"     # this daemon's cert chain (PEM)
key  = "/etc/macro-bus/d1.key"     # this daemon's private key (PEM)
ca   = "/etc/macro-bus/ca.pem"     # CA bundle used to verify peers (PEM)
```

A sample is provided at [`config.example.toml`](config.example.toml).

If any `[cluster]` field is present, a `[tls]` section is **required** — cluster
links are always encrypted.

## Design trade-offs and their consequences

macro-bus makes a few opinionated choices. Each buys simplicity at a real cost:

- **In-memory only, no persistence.** A daemon restart loses *all* state — type
  ownership, keys, and subscriptions. Clients re-register and re-subscribe on
  reconnect. Consequence: never treat the bus as a system of record; there is no
  durability and no replay. This keeps the mental model tiny and the daemon
  stateless-on-disk.
- **Fire-and-forget delivery.** Publishing tells you the message was *fanned
  out*, not *processed*. There is no delivery receipt and no retry. Consequence:
  use it for live signals (telemetry, cache invalidations, notifications), not
  for work that must not be lost. If you need at-least-once, put a durable queue
  in front of your consumers.
- **First-registrant auth.** Ownership is claimed, not administered. The first
  app to register a type owns its key cluster-wide. Consequence: it's a
  coordination convention among cooperating apps and a guard against typo-level
  cross-talk — not a security boundary. In a cluster, concurrent first
  registrations converge deterministically by `(timestamp, daemon-id)`, so a
  local registration can later be overridden by an earlier one learned from a
  peer.
- **Tail-drop for slow consumers.** A slow subscriber loses *its own* messages
  (with a `102 DROP` notice) rather than stalling publishers or other
  subscribers. Consequence: bounded, predictable memory and latency; a consumer
  that can't keep up must widen its queue (`queue_depth`) or read faster.
- **Cleartext local auth key.** The per-type key crosses the *local* socket in
  cleartext; it is only as strong as the socket's filesystem permissions (0600
  by default). It is not a substitute for OS access control. Federation links,
  by contrast, are always mutually TLS-authenticated.
- **No store-and-forward between daemons.** If a peer is down, messages for it
  are dropped and forwarding resumes on reconnect. Consequence: the local bus
  never blocks on a slow/absent peer, at the cost of gaps in cross-node delivery
  during outages.

## Portability

The code targets Linux and FreeBSD and avoids Linux-only syscalls. It uses
portable POSIX + `tokio`; the socket is a standard Unix domain socket; TLS uses
rustls with the `ring` provider (no C toolchain or assembler needed at build
time). There are no `epoll`-specific assumptions and no `SO_PEERCRED` usage. The
only platform-specific code is guarded socket-permission handling behind
`cfg(unix)`.

## Future extensions (explicitly out of scope for v1)

- Hierarchical / wildcard subscriptions (`sensors.*`).
- Binary / base64 payloads (announced via a `PAYLOAD` capability).
- Dynamic peer discovery (v1 uses a static peer list).
- `drop-oldest` as an alternative to `tail-drop`.
- OS-level peer-credential checks for local clients.

## License

Licensed under either of MIT or Apache-2.0 at your option.
