# The Macro-Bus Protocol (MBP/1.0)

> This document is the **normative** specification for macro-bus. The
> implementation conforms to it. When behavior changes, update this document
> first, bump the version if the change is wire-incompatible, then implement.

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
   9.  Federation (server-to-server)
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
      (timestamp, daemon-id) tuple; losers converge to the winner's key.
      Timestamps are milliseconds since the Unix epoch as measured by the
      registering daemon. Daemons MUST propagate registrations to peers
      (Sec 9). Because ownership is eventually consistent, a client that
      registered locally MAY later find its key overridden by an earlier
      (lower-tuple) registration learned from a peer; see Sec 9.4.

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
         lines; DATA body line limits are separate and larger). Servers
         SHOULD also accept a bare LF as a line terminator to be friendly to
         hand-driving with nc(1).

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

   A future binary/base64 mode would be negotiated via a CAPABILITIES entry
   (e.g. "PAYLOAD base64") and signalled per-message; it is out of scope
   for v1, which advertises "PAYLOAD text".

6.  Command Reference

   6.1.  CAPABILITIES
      Lists supported capabilities as a dot-terminated block after
      "231 capabilities follow". Example capabilities: VERSION MBP/1.0,
      MAXMSG <octets>, QUEUE <depth>, DROP-POLICY tail-drop, PAYLOAD text,
      TLS federation.

   6.2.  HELP
      Returns human-readable help as a "231"-style dot-terminated block.

   6.3.  REGISTER <type> <key>
      Claims <type> with authorization <key>. Responses:
         210 <type> registered           (new, or idempotent same-key)
         433 <type> already registered    (different key, or lost a race)
         501 syntax error in parameters
         521 invalid type name

   6.4.  SUBSCRIBE <type>
      Adds this connection as a listener for <type>. No key required.
         211 subscribed <type>
      SUBSCRIBE to an already-subscribed type is idempotent (211).

   6.5.  UNSUBSCRIBE <type>
         212 unsubscribed <type>
      UNSUBSCRIBE of a type not currently subscribed is idempotent (212).

   6.6.  PUBLISH <type> <key>
      Two-step, SMTP-style:
         C: PUBLISH sensors.temp s3cr3t
         S: 354 enter message body; end with <CRLF>.<CRLF>
         C: 21.4C
         C: .
         S: 250 message accepted
      Error replies are sent INSTEAD of 354 (no body is then read):
         430 unknown message type       (never REGISTERed)
         440 authorization required      (reserved; no key given)
         441 authorization key mismatch
         452 message too large           (may also be sent after body)
      "250 message accepted" means the message was fanned out to current
      listeners and queued for federation; it does NOT imply any listener
      processed it (fire-and-forget). NOTE: the v1 command grammar requires
      a key argument, so 440 is reserved for a future keyless syntax; a
      missing key is a 501 syntax error today.

   6.7.  LIST TYPES
         215 type list follows
         <type> per line, dot-terminated
         .
      Keys are NEVER disclosed. The list is sorted and cluster-wide to the
      extent registrations have propagated.

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
      Free-form operational notices. Clients MAY ignore.

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
        440  authorization required                     [RESERVED]
        441  authorization key mismatch
        452  message too large / capacity exceeded
   5xx  Permanent / syntax error
        500  syntax error, command unrecognized
        501  syntax error in parameters or arguments
        502  command not implemented
        503  bad sequence of commands
        521  invalid message type name

9.  Federation (server-to-server)

   Federation carries (a) forwarded messages and (b) registration/ownership
   propagation between daemons. It reuses the MBP line style over a
   TLS-protected TCP connection and adds two peering verbs.

   9.1.  Peering handshake and mutual authentication
      Peers are configured statically (host:port + expected daemon id).
      Every daemon dials each configured peer AND accepts inbound peer
      links on its configured listen address. Both directions are wrapped
      in TLS and MUST use mutual TLS (Sec 10): the dialer verifies the
      peer's certificate against the configured CA and presents its own;
      the accepter requires and verifies the peer's client certificate.
      Peer identity is bound to the certificate's subjectAltName, which by
      convention equals the peer's daemon id and is the name verified on
      dial.

      Immediately after the TLS handshake each side writes a one-line
      greeting and reads the peer's:
         200 <daemon-id> macro-bus-peer MBP/1.0 ready

      Topology rule (loop-free flooding without duplicate links): a daemon
      SENDS application frames only over links it dialed, and RECEIVES only
      over links it accepted. With symmetric static configuration this
      yields exactly one send path and one receive path per ordered pair.

   9.2.  Verbs
      FEED <type> <msg-id> <origin-daemon-id>
         <dot-stuffed body>
         .
            Forwards a message, mirroring 101 MSG. Framing is identical to
            Sec 5. Sent by a daemon to each dialed peer for every message
            published locally, and re-forwarded on receipt per Sec 9.3.

      RREG <type> <key> <origin-daemon-id> <timestamp>
            Propagates a type registration. <timestamp> is the origin
            daemon's registration time in Unix milliseconds. Sent to each
            dialed peer when a local REGISTER creates or changes ownership,
            re-forwarded on receipt when it changes the local table, and
            sent as a full snapshot to a peer when a dialed link comes up
            (reconciliation).

   9.3.  Loop prevention
      Every message carries a cluster-unique <msg-id> (assigned by the
      origin daemon as "<origin-daemon-id>-<hex-sequence>") and its
      <origin-daemon-id>. On receiving a FEED a daemon:
         (a) consults a bounded seen-set of recent <msg-id>s; if present,
             the message is a duplicate and MUST be dropped;
         (b) otherwise records the <msg-id>, delivers to matching local
             subscribers exactly once, and re-forwards the FEED to every
             dialed peer EXCEPT the one it arrived from.
      A daemon therefore never returns a message to the peer it arrived
      from and never delivers a duplicate <msg-id> to a local subscriber.
      The seen-set is FIFO-bounded (default 65536 ids); this is memory-
      safe and, for a fire-and-forget bus, an acceptable dedup horizon.

   9.4.  Registration propagation and conflict resolution
      On receiving RREG, a daemon compares the incoming record with any
      local record for that type:
         - unknown type      -> adopt incoming; re-forward.
         - identical record  -> no-op.
         - different record  -> keep the record with the LOWEST
                                (timestamp, daemon-id) tuple. If the
                                incoming record wins, replace the local one
                                and re-forward; otherwise ignore.
      Re-forwarding excludes the source peer. Because the rule is a total
      order on (timestamp, daemon-id) and application is idempotent and
      commutative, all daemons converge to the same owner key regardless of
      message ordering or which links delivered which RREGs. A daemon that
      (re)joins reconciles by receiving the full snapshot from each peer it
      dials.

   9.5.  Peer down / reconnect
      There is NO store-and-forward. A message destined for an unreachable
      peer is dropped; the local bus keeps working. Dialers reconnect with
      exponential backoff (configurable base/max). On reconnect the dialer
      re-sends its registration snapshot; in-flight messages lost during
      the outage are not recovered (fire-and-forget).

10. Security Considerations

   Local access is bounded by filesystem permissions on the socket;
   deployments SHOULD restrict the socket's mode/ownership. The daemon
   creates the socket with mode 0600 by default. The per-type authorization
   key gates publishing but is transmitted in cleartext over the LOCAL
   socket and is therefore only as strong as local socket permissions; it
   is NOT a substitute for OS access control. Federation links MUST use TLS
   and SHOULD use mutual TLS so daemons authenticate one another. Auth keys
   MUST NEVER be disclosed by LIST or any other command. Because the bus is
   fire-and-forget and in-memory, it offers no durability or delivery
   guarantees and MUST NOT be relied on for at-least-once semantics.

11. Collected ABNF Grammar (RFC 5234)

   CRLF        = %d13 %d10
   SP          = %d32
   token       = 1*tchar
   tchar       = ALPHA / DIGIT / "-" / "." / "_" / "/"
   type        = token                 ; opaque, case-sensitive
   key         = 1*VCHAR               ; no SP; case-sensitive
   msg-id      = token
   daemon-id   = token
   timestamp   = 1*DIGIT               ; Unix milliseconds

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

   ; Federation (Sec 9)
   peer-greet  = "200" SP daemon-id SP "macro-bus-peer" SP "MBP/1.0"
                 SP "ready" CRLF
   feed        = %s"FEED" SP type SP msg-id SP daemon-id CRLF body
   rreg        = %s"RREG" SP type SP key SP daemon-id SP timestamp CRLF

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
         S: 101 MSG sensors.temp d1-1 d1
         S: 21.4C
         S: .

   12.2.  Authorization failure
         C: PUBLISH sensors.temp wrongkey
         S: 441 authorization key mismatch

   12.3.  Unknown type on publish
         C: PUBLISH never.registered k
         S: 430 never.registered not registered

   12.4.  Dot-stuffing a body line that begins with "."
         C: PUBLISH logs.line k
         S: 354 enter message body; end with <CRLF>.<CRLF>
         C: ..config starts with a dot          (wire form of ".config...")
         C: .
         S: 250 message accepted
      The delivered body line is ".config starts with a dot".

   12.5.  Slow consumer
         S: 102 DROP sensors.temp 37

   12.6.  Capabilities
         C: CAPABILITIES
         S: 231 capabilities follow
         S: VERSION MBP/1.0
         S: MAXMSG 1048576
         S: QUEUE 1024
         S: DROP-POLICY tail-drop
         S: PAYLOAD text
         S: TLS federation
         S: .

   12.7.  Federation: message published on d1 delivered on d2

      On d1, a client publishes (as in 12.1). d1 forwards to its dialed
      peer d2 over the mTLS link:

         d1 -> d2:  FEED sensors.temp d1-1 d1
         d1 -> d2:  21.4C
         d1 -> d2:  .

      d2 has never seen msg-id "d1-1", so it records it, delivers to its
      local subscriber, and does not return it to d1:

         d2 -> L(on d2):  101 MSG sensors.temp d1-1 d1
         d2 -> L(on d2):  21.4C
         d2 -> L(on d2):  .

      A second copy arriving by any path (e.g. via a third daemon) carries
      the same msg-id "d1-1" and is dropped by d2's seen-set: no duplicate
      delivery.

   12.8.  Federation: registration propagation

         d1 -> d2:  RREG sensors.temp s3cr3t d1 1751650000000

      d2 adopts (or conflict-resolves) the ownership so that a PUBLISH on
      d2 with the wrong key is rejected 441, exactly as on d1.
```

*Implementer note:* keep this document and the code mutually consistent — when
behavior changes, update MBP first, bump the version if wire-incompatible, then
implement.
