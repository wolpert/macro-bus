//! # macro-bus-proto
//!
//! Wire-protocol types for the **Macro-Bus Protocol (MBP/1.0)** — a text,
//! line-oriented, fire-and-forget publish/subscribe protocol in the spirit of
//! NNTP and SMTP. This crate is shared by the daemon (`macro-busd`), the client
//! library (`macro-bus-client`) and the CLI.
//!
//! It is deliberately IO-free: it defines the [`Command`] grammar, the
//! [`status`] code registry, DATA-block [`frame`]ing / dot-stuffing, and the
//! in-flight [`Message`] type. The async plumbing lives in the daemon and
//! client crates. The normative specification is `PROTOCOL.md`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod command;
pub mod frame;
pub mod message;
pub mod status;
pub mod validate;

pub use command::{parse, Command, ParseError};
pub use message::Message;
pub use status::Code;

/// Protocol identifier string used in greetings and capabilities.
pub const PROTOCOL_ID: &str = "MBP/1.0";

/// Default Unix domain socket path, used by the daemon and the CLI on both
/// Linux and FreeBSD.
pub const DEFAULT_SOCKET_PATH: &str = "/var/run/macro-bus.sock";

/// The client-facing greeting line (without trailing CRLF) for a given daemon.
pub fn greeting(daemon_id: &str) -> String {
    format!(
        "{} {} macro-bus {} ready",
        status::SERVICE_READY,
        daemon_id,
        PROTOCOL_ID
    )
}

/// The federation-link greeting line (without trailing CRLF) for a given daemon.
pub fn peer_greeting(daemon_id: &str) -> String {
    format!(
        "{} {} macro-bus-peer {} ready",
        status::SERVICE_READY,
        daemon_id,
        PROTOCOL_ID
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_format() {
        assert_eq!(greeting("d1"), "200 d1 macro-bus MBP/1.0 ready");
    }
}
