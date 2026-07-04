# Build Prompt: `macro-bus` — a lightweight local + cluster message bus

---

## Mission

Build **`macro-bus`**, a small, fast communications bus written in **Rust** that runs equally well on
**FreeBSD and Linux**. It provides fire-and-forget publish/subscribe messaging for local applications
via **Unix domain (file) sockets**, and optionally forms a **cluster** of daemons that forward
messages to each other so the bus behaves as one logical bus across many machines.

Design values, in priority order:
1. **Correctness and simplicity** — the mental model must be tiny and obvious.
2. **Portability** — identical behavior on FreeBSD and Linux; no OS-specific syscalls that only exist
   on one of them. Prefer portable POSIX + `tokio`. Guard any platform-specific code behind `cfg`.
3. **Human-readable, debuggable protocol** — a person can `nc`/`socat` into the socket and drive the
   bus by hand, exactly like the classic **NNTP / SMTP (SENDMAIL)** line protocols.
4. **In-memory only** — nothing is ever persisted. No disk queues, no WAL, no replay.

## Core concepts and required semantics

- **One daemon per box** (`macro-busd`). It owns a Unix domain socket that local apps connect to.
- **Messages are typed.** A *message type* is a string identifier (e.g. `sensors.temperature`,
  `orders.created`). Apps publish and subscribe **at the message-type level**.
- **Fire-and-forget delivery.** When a message is published, the daemon hands it to **every current
  listener** of that type and then **drops it**. No acknowledgement of processing, no buffering for
  slow/absent consumers, no persistence, no replay. If no one is listening, the message evaporates.
- **Distributed bus (required).** A message published on box A must be delivered to matching listeners
  on **every box in the cluster**, not just box A. Daemons forward published messages to their peers,
  which then deliver to their own local listeners. (See "Cluster" below for loop-prevention rules.)
- **Message-type authorization — first-registrant owns it.**
  - Publishing to a type requires presenting an **authorization key** for that type.
  - The **first app in the cluster to register a type sets its key.** That registration must be
    propagated to peer daemons so the whole cluster agrees on the owner key for that type.
  - Any later publisher to that type **must present the matching key** or be rejected.
  - **Listening/subscribing is open** — no key required to subscribe.
  - When a type is not yet known cluster-wide, the daemons coordinate so that the registration
    resolves consistently (define and document the conflict rule — e.g. deterministic
    first-writer-wins with tie-breaking by timestamp then daemon id; reject conflicting concurrent
    registrations with a clear protocol error).
- **Everything in memory.** Type registry, auth keys, and subscriptions live in RAM and are rebuilt as
  daemons and clients (re)connect. A daemon restart loses all state, which is acceptable by design —
  document this clearly.

## The client protocol (local apps ↔ daemon)

Design a **text, line-oriented, request/response protocol** in the spirit of NNTP and SMTP:

- ASCII/UTF-8, CRLF-terminated command lines.
- **3-digit numeric status codes** with a short human-readable reason on responses (e.g. `200 OK`,
  `281 type registered`, `480 authorization required`, `483 key mismatch`, `501 syntax error`).
- Multi-line payloads use **SMTP `DATA`-style framing**: a body sent line by line and terminated by a
  line containing a single `.`; dot-stuff leading dots. Keep binary out of scope for v1 (payloads are
  text); document how a future binary/base64 mode would slot in.
- Provide a **greeting banner** on connect and a **`CAPABILITIES`/`HELP`** command, again NNTP-style.

Define and document the full command set. At minimum support:

| Command (illustrative) | Purpose |
|---|---|
| `REGISTER <type> <key>` | Claim ownership of a message type with an auth key (first-registrant wins). |
| `PUBLISH <type> <key>` then `DATA`…`.` | Publish a message; daemon validates the key, fans out, drops. |
| `SUBSCRIBE <type>` | Start receiving messages of a type on this connection (no key needed). |
| `UNSUBSCRIBE <type>` | Stop receiving a type. |
| `LIST TYPES` | Enumerate known types (not keys). |
| `CAPABILITIES` / `HELP` / `QUIT` | Session/introspection/teardown. |

**Pinned decision — asynchronous message push (server-initiated).** Delivered messages and other
unsolicited server notifications use a **reserved `1xx` status-code range**. A conforming client MUST
be able to tell a pushed notification from the response to its own command **purely by the leading
digit `1`**: command responses are always `2xx`/`3xx`/`4xx`/`5xx`, async server pushes are always
`1xx`. Pushes are emitted only **between complete command/response exchanges** (never in the middle of
one), so a client's read loop dispatches every `1xx` line to its subscription handlers and treats the
next non-`1xx` line as the response to any pending command. Message delivery is:

```
101 MSG <type> <msg-id> <origin-daemon-id>
<dot-stuffed body lines>
.
```

**Pinned decision — slow-consumer policy.** Publishing NEVER blocks on a slow subscriber. Each
subscriber connection has a **bounded outbound queue** (depth configurable, sensible default e.g.
1024). Fan-out is a non-blocking try-send; if a subscriber's queue is full, the daemon **tail-drops**
(the arriving message is dropped for that subscriber only — no reordering, no back-pressure onto the
publisher or other subscribers). The daemon increments a per-subscriber dropped counter, logs it
rate-limited, and emits an async notice to the affected connection so the app knows it fell behind:

```
102 DROP <type> <count-since-last-notice>
```

`drop-oldest` MAY be offered as a configurable alternative, but tail-drop is the default.

Full wire grammar, the complete status-code registry, framing rules, and worked transcripts are
specified normatively in the **embedded RFC (Appendix A)** below — implement to that document.

## Cluster: server-to-server (required, secure)

- Peers are configured via a **static peer list in the daemon config** (host\:port each). Dynamic
  discovery is explicitly out of scope for v1 — note it as a future extension.
- The **inter-daemon protocol is also text** and line-oriented (same spirit as the client protocol;
  it may share a core parser but has its own command set for federation).
- **Secure transport:** wrap inter-daemon links in **TLS via `rustls`**. Support mutual TLS
  (peers authenticate each other with certs) and make the cluster links **optional** — a daemon with
  no peers configured runs as a perfectly good standalone local bus.
- The cluster must handle:
  - **Message forwarding** with **loop prevention** (e.g. origin tagging / hop tracking / seen-message
    dedup) so a message never ping-pongs or gets delivered twice to the same listener.
  - **Type-registration propagation** so auth-key ownership is consistent cluster-wide, including the
    concurrent-registration conflict rule described above.
  - **Peer connect/reconnect/backoff** and clean behavior when a peer is down (the local bus keeps
    working; forwarding to that peer resumes on reconnect — no store-and-forward, dropped is fine).

## Rust implementation guidance

- **Async runtime: `tokio`.** Pragmatic, well-known crates are welcome: `serde` where it genuinely
  helps (config), `rustls`/`tokio-rustls` for TLS, `clap` for CLI, `tracing` for logging, `anyhow`/
  `thiserror` for errors. Don't over-engineer; keep the dependency tree reasonable.
- **Workspace layout** (adjust names as sensible):
  - `macro-bus-proto` — protocol types, parser/serializer, status codes (shared by daemon + client).
  - `macro-busd` — the daemon binary (socket server, subscription registry, type/auth registry,
    cluster/federation module, config, TLS).
  - `macro-bus-client` — a small Rust client library so apps embed easily, plus a
  - `macro-bus` CLI — a thin command-line tool (publish/subscribe/register/list) useful for manual
    testing and scripting, doubling as the reference client.
- **Config file** (TOML): socket path (default e.g. `/var/run/macro-bus.sock` on both OSes, overridable),
  peer list, TLS cert/key/CA paths, limits (max message size, max subscriptions, etc.).
- **Concurrency model:** one task per client connection; a shared in-memory registry (types → owner
  key; type → set of local subscriber senders) behind appropriate async-safe primitives. Fan-out via
  per-subscriber bounded channels using the tail-drop slow-consumer policy pinned above (dropped
  counter, rate-limited log, and `102 DROP` async notice).
- **Portability:** verify it builds and the test suite passes on **both Linux and FreeBSD**. Avoid
  Linux-only APIs (e.g. don't rely on `epoll`-specific behavior, `SO_PEERCRED` Linux quirks, etc.);
  if peer-credential checks are used for local auth, provide a portable abstraction with per-OS impls.

## Deliverables

1. A working `cargo` **workspace** that builds cleanly on Linux and FreeBSD (`cargo build`, `cargo clippy`).
2. `macro-busd` daemon, `macro-bus` CLI, and `macro-bus-client` library as described.
3. **Protocol specification** document (`PROTOCOL.md`): publish the **RFC in Appendix A** as the
   normative spec, filling in any TODOs (finalize the cluster/federation section, add worked
   transcripts). The implementation MUST conform to it. Keep the RFC and the code in sync.
4. **Tests:**
   - Unit tests for the protocol parser/serializer and the auth-registry conflict logic.
   - Integration tests: local publish→subscribe delivery; auth-key rejection; two-daemon cluster test
     proving a message published on daemon A reaches a subscriber on daemon B, with loop prevention
     and no duplicate delivery.
5. **`README.md`**: what it is, the mental model, quickstart (start daemon, subscribe with the CLI in
   one terminal, publish in another), config reference, and a section on the design trade-offs
   (in-memory only, fire-and-forget, first-registrant auth) and their consequences.
6. Example: a tiny demo showing two apps talking over the bus, and a two-node cluster docker-compose
   or shell script for local testing.

## Assumptions baked in (override any of these if you disagree — call them out if you do)

- Socket path, message-size limits, and peer list are configurable; sensible defaults provided.
- Message types are opaque strings; **no wildcard/hierarchical subscriptions in v1** (exact-match
  only). Note hierarchical matching (e.g. `sensors.*`) as a documented future extension.
- Payloads are **text** in v1; binary is a future extension via an announced capability.
- No authentication of *local* clients beyond the auth-key-per-type model (optionally mention OS-level
  socket file permissions / peer-cred checks as a hardening note).
- No persistence, ever. A restart is a clean slate.

## Suggested build order

1. `macro-bus-proto`: define commands, status codes, framing; parser/serializer with unit tests.
2. `macro-busd` single-box: socket server, registry, register/publish/subscribe, fan-out, local
   integration tests. Ship a genuinely working standalone bus first.
3. `macro-bus` CLI + `macro-bus-client` library against the running daemon.
4. Cluster layer: config peer list, rustls links, message forwarding with loop prevention, type-reg
   propagation + conflict rule, two-daemon integration test.
5. Docs (`PROTOCOL.md`, `README.md`), examples, and a final portability pass (Linux + FreeBSD).

Start by proposing the concrete protocol grammar and workspace layout, then implement in the order
above. Keep commits scoped per phase.

---

# Appendix A — RFC: The Macro-Bus Protocol (MBP/1.0)

> Ship this as `PROTOCOL.md`. It is the normative reference; the implementation must conform to it.
> Sections marked *TODO* are for the implementer to finalize during the cluster phase.

```
Macro-Bus Protocol Working Note                                    MBP/1.0
Category: Experimental                                            July 2026

                     The Macro-Bus Protocol (MBP)

Status of This Memo

   This document specifies MBP/1.0, a text, line-oriented protocol for a
   fire-and-forget, in-memory, publish/subscribe message bus for local
   applications, with optional secure federation between per-host daemons.
   Distribution of this memo is unlimited.

Table of Contents

   1.  Introduction
   2.  Requirements Language
   3.  Model
   4.  Connection and Session Model
   5.  Message Framing (DATA blocks and dot-stuffing)
   6.  Command Reference (client protocol)
   7.  Asynchronous Server Notifications (1xx)
   8.  Response Status Code Registry
   9.  Federation (server-to-server)                              [TODO]
   10. Security Considerations
   11. Collected ABNF Grammar
   12. Example Sessions

1.  Introduction

   MBP is deliberately reminiscent of NNTP [RFC3977] and SMTP [RFC5321]:
   ASCII/UTF-8 command lines terminated by CRLF, three-digit numeric
   response codes, and SMTP-style DATA blocks terminated by a line
   containing a single ".". A human operator MUST be able to drive the bus
   by hand with a tool such as nc(1) or socat(1).

   Messages are typed, delivered to every current listener, and then
   dropped. Nothing is persisted. A message published on any daemon in a
   cluster is delivered to matching listeners on every daemon in that
   cluster.

2.  Requirements Language

   The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
   "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
   document are to be interpreted as described in RFC 2119.

3.  Model

   3.1.  Message Types
      A message type is an opaque, case-sensitive token (see ABNF, Sec 11).
      In v1 subscriptions are exact-match; hierarchical/wildcard matching
      is reserved for a future revision.

   3.2.  Authorization Keys (first-registrant owns)
      A type MAY be claimed by REGISTER, which binds an authorization key
      to that type. The FIRST successful registration in the cluster owns
      the key. To PUBLISH to a type, a client MUST present the matching
      key. Re-registering a type with the SAME key is idempotent and
      succeeds; registering with a DIFFERENT key MUST be rejected (433).
      SUBSCRIBE requires no key.

      Concurrent cluster registrations for the same type are resolved
      deterministically: the winner is the registration with the lowest
      (timestamp, daemon-id) tuple; losers receive 433. Daemons MUST
      propagate registrations to peers (Sec 9).

      Publishing to a type that has never been registered MUST be rejected
      with 430 (unknown message type); a type must be REGISTERed before it
      can be PUBLISHed to, but MAY be SUBSCRIBEd to beforehand.

   3.3.  Delivery Semantics
      Delivery is fire-and-forget. On PUBLISH, the daemon fans the message
      out to every current local subscriber and forwards it to peers; it is
      then dropped. There is no acknowledgement of processing, no buffering
      for absent consumers, no replay. Publishing MUST NOT block on slow
      subscribers; see Sec 7.2 for the tail-drop policy.

4.  Connection and Session Model

   4.1.  Local transport is a Unix domain (file) socket. Federation
         transport is TCP wrapped in TLS (Sec 9, Sec 10).

   4.2.  On connect the server MUST send a one-line greeting:
            200 <daemon-id> macro-bus MBP/1.0 ready
         A server refusing service SHOULD send 400 and close.

   4.3.  Commands are case-insensitive tokens; arguments are
         case-sensitive. Lines are CRLF-terminated and SHOULD NOT exceed an
         implementation-defined limit (default 4096 octets for command
         lines; DATA body line limits are separate and larger).

   4.4.  Response discipline: every command yields exactly one final
         response line (optionally preceded by a multi-line block for
         list-style replies, dot-terminated). Asynchronous 1xx
         notifications (Sec 7) MAY be interleaved BETWEEN a client's
         command/response exchanges but MUST NOT appear between a command
         and its own response. A client distinguishes the two solely by the
         leading digit: 1 == asynchronous push, 2/3/4/5 == command reply.

5.  Message Framing (DATA blocks)

   Message bodies use SMTP-style framing. After the server invites the body
   with 354, the client sends zero or more body lines and then a terminator
   line containing exactly ".". Any body line beginning with "." MUST be
   dot-stuffed by prefixing an extra "." on send; the receiver MUST strip
   one leading "." from any body line that begins with "..". Bodies are
   UTF-8 text in v1. The 101 MSG push (Sec 7.1) uses the identical framing.

6.  Command Reference

   6.1.  CAPABILITIES
      Lists supported capabilities as a dot-terminated block after 101...
      no: after "231 capabilities follow". Example capabilities: VERSION
      MBP/1.0, MAXMSG <octets>, QUEUE <depth>, TLS (federation), DROP-POLICY
      tail-drop.

   6.2.  HELP
      Returns human-readable help as a "231"-style dot-terminated block.

   6.3.  REGISTER <type> <key>
      Claims <type> with authorization <key>. Responses:
         210 <type> registered
         433 <type> already registered   (different key, or lost a race)
         501 syntax error in parameters
         521 invalid type name

   6.4.  SUBSCRIBE <type>
      Adds this connection as a listener for <type>. No key required.
         211 subscribed <type>
      SUBSCRIBE to an already-subscribed type is idempotent (211).

   6.5.  UNSUBSCRIBE <type>
         212 unsubscribed <type>

   6.6.  PUBLISH <type> <key>
      Two-step, SMTP-style:
         C: PUBLISH sensors.temp s3cr3t
         S: 354 enter message body; end with <CRLF>.<CRLF>
         C: 21.4C
         C: .
         S: 250 message accepted
      Error replies are sent INSTEAD of 354 (no body is then read):
         430 unknown message type       (never REGISTERed)
         440 authorization required      (no key given)
         441 authorization key mismatch
         452 message too large           (may also be sent after body)
      "250 message accepted" means the message was fanned out to current
      listeners and queued for federation; it does NOT imply any listener
      processed it (fire-and-forget).

   6.7.  LIST TYPES
         215 type list follows
         <type> per line, dot-terminated
         .
      Keys are NEVER disclosed.

   6.8.  QUIT
         221 closing connection    (server then closes)

7.  Asynchronous Server Notifications (1xx)

   7.1.  101 MSG — message delivery to a subscriber
         101 MSG <type> <msg-id> <origin-daemon-id>
         <dot-stuffed body>
         .
      <msg-id> is a cluster-unique identifier used for dedup/loop
      prevention (Sec 9). <origin-daemon-id> names the daemon where the
      message was first published. A daemon MUST NOT deliver the same
      <msg-id> to the same connection more than once.

   7.2.  102 DROP — slow-consumer notice
         102 DROP <type> <count>
      Emitted when the connection's bounded outbound queue overflowed and
      one or more messages were tail-dropped for it. <count> is the number
      dropped since the previous 102 for that type. Delivery to other
      subscribers and other types is unaffected; the publisher is never
      back-pressured.

   7.3.  190 NOTE — informational (OPTIONAL)
      Free-form operational notices (e.g. peer up/down). Clients MAY ignore.

8.  Response Status Code Registry

   1xx  Asynchronous, server-initiated (unsolicited) notifications
        101  message delivery (MSG; body follows)
        102  slow-consumer drop notice (DROP)
        190  informational note (NOTE)               [OPTIONAL]
   2xx  Positive completion
        200  service ready (greeting)
        210  type registered
        211  subscribed
        212  unsubscribed
        215  type list follows (dot-terminated block)
        221  closing connection
        231  capabilities / help follow (dot-terminated block)
        250  message accepted
   3xx  Intermediate; further input required
        354  start message body; end with <CRLF>.<CRLF>
   4xx  Transient / operational failure
        400  service not available (closing)
        430  unknown message type (not registered)
        433  type already registered (ownership conflict)
        440  authorization required
        441  authorization key mismatch
        452  message too large / capacity exceeded
   5xx  Permanent / syntax error
        500  syntax error, command unrecognized
        501  syntax error in parameters or arguments
        502  command not implemented
        503  bad sequence of commands
        521  invalid message type name

9.  Federation (server-to-server)                                   [TODO]

   Federation carries (a) forwarded messages and (b) registration/ownership
   propagation between daemons. It reuses the MBP line style over a
   TLS-protected TCP connection and adds peering verbs. Normative rules the
   implementer MUST finalize here:

   9.1.  Peering handshake and mutual authentication (mTLS, Sec 10).
   9.2.  A FEED/forward verb carrying <type> <msg-id> <origin-daemon-id> +
         DATA body, mirroring 101 MSG.
   9.3.  Loop prevention: every message carries a cluster-unique <msg-id>
         and <origin-daemon-id>. A daemon MUST drop any message whose
         <msg-id> it has already seen (bounded seen-set / TTL), MUST NOT
         return a message to the peer it arrived from, and MUST NOT deliver
         a duplicate <msg-id> to any local subscriber. A hop counter MAY be
         included as a secondary safeguard.
   9.4.  Registration propagation and the deterministic (timestamp,
         daemon-id) conflict-resolution rule from Sec 3.2, including how a
         newly joined/reconnected daemon reconciles its type table.
   9.5.  Peer down/reconnect: NO store-and-forward. Messages destined for an
         unreachable peer are dropped; forwarding resumes on reconnect.

10. Security Considerations

   Local access is bounded by filesystem permissions on the socket;
   deployments SHOULD restrict the socket's mode/ownership. The per-type
   authorization key gates publishing but is transmitted in cleartext over
   the LOCAL socket and is therefore only as strong as local socket
   permissions; it is NOT a substitute for OS access control. Federation
   links MUST use TLS and SHOULD use mutual TLS so daemons authenticate one
   another. Auth keys MUST NEVER be disclosed by LIST or any other command.
   Because the bus is fire-and-forget and in-memory, it offers no
   durability or delivery guarantees and MUST NOT be relied on for
   at-least-once semantics.

11. Collected ABNF Grammar (RFC 5234)

   CRLF        = %d13 %d10
   SP          = %d32
   token       = 1*tchar
   tchar       = ALPHA / DIGIT / "-" / "." / "_" / "/"
   type        = token                 ; opaque, case-sensitive
   key         = 1*VCHAR               ; no SP; case-sensitive
   msg-id      = token
   daemon-id   = token

   command     = ( capabilities / help / register / subscribe /
                   unsubscribe / publish / list / quit ) CRLF
   register    = %s"REGISTER"    SP type SP key
   subscribe   = %s"SUBSCRIBE"   SP type
   unsubscribe = %s"UNSUBSCRIBE" SP type
   publish     = %s"PUBLISH"     SP type SP key
   list        = %s"LIST" SP %s"TYPES"
   capabilities= %s"CAPABILITIES"
   help        = %s"HELP"
   quit        = %s"QUIT"
                 ; command tokens are matched case-insensitively

   resp-line   = 3DIGIT SP *VCHAR CRLF
   async-push  = msg-push / drop-note / note
   msg-push    = "101 MSG" SP type SP msg-id SP daemon-id CRLF body
   drop-note   = "102 DROP" SP type SP 1*DIGIT CRLF
   note        = "190 NOTE" SP *VCHAR CRLF
   body        = *body-line "." CRLF
   body-line   = [ "." ] *VCHAR CRLF   ; leading "." is dot-stuffed

12. Example Sessions

   12.1.  Register, publish, deliver (two connections, one daemon)

      Connection P (publisher):
         S: 200 d1 macro-bus MBP/1.0 ready
         C: REGISTER sensors.temp s3cr3t
         S: 210 sensors.temp registered
         C: PUBLISH sensors.temp s3cr3t
         S: 354 enter message body; end with <CRLF>.<CRLF>
         C: 21.4C
         C: .
         S: 250 message accepted

      Connection L (listener), already subscribed:
         S: 200 d1 macro-bus MBP/1.0 ready
         C: SUBSCRIBE sensors.temp
         S: 211 subscribed sensors.temp
         S: 101 MSG sensors.temp 0f3a-1 d1
         S: 21.4C
         S: .

   12.2.  Authorization failure
         C: PUBLISH sensors.temp wrongkey
         S: 441 authorization key mismatch

   12.3.  Slow consumer
         S: 102 DROP sensors.temp 37
```

*Implementer note:* keep this RFC and the code mutually consistent — when behavior changes, update
MBP first, bump the version if wire-incompatible, then implement.
