//! MBP/1.0 response status code registry (RFC Appendix A, Section 8).
//!
//! Status codes are 3-digit numbers. The leading digit classifies the code:
//!
//! * `1xx` — asynchronous, server-initiated notifications (never a command reply)
//! * `2xx` — positive completion
//! * `3xx` — intermediate; further input required
//! * `4xx` — transient / operational failure
//! * `5xx` — permanent / syntax error
//!
//! A conforming client distinguishes an unsolicited server push from the reply
//! to its own command **solely by the leading digit `1`**.

/// A 3-digit MBP status code.
pub type Code = u16;

// --- 1xx: asynchronous server-initiated notifications -----------------------

/// Message delivery to a subscriber (body follows, dot-terminated).
pub const MSG: Code = 101;
/// Slow-consumer tail-drop notice.
pub const DROP: Code = 102;
/// Free-form informational note (OPTIONAL).
pub const NOTE: Code = 190;

// --- 2xx: positive completion ----------------------------------------------

/// Service ready (connection greeting).
pub const SERVICE_READY: Code = 200;
/// Message type registered.
pub const REGISTERED: Code = 210;
/// Subscribed to a type.
pub const SUBSCRIBED: Code = 211;
/// Unsubscribed from a type.
pub const UNSUBSCRIBED: Code = 212;
/// Type list follows (dot-terminated block).
pub const TYPE_LIST: Code = 215;
/// Closing connection.
pub const CLOSING: Code = 221;
/// Capabilities / help follow (dot-terminated block).
pub const INFO_FOLLOWS: Code = 231;
/// Message accepted (fanned out + queued for federation).
pub const ACCEPTED: Code = 250;

// --- 3xx: intermediate ------------------------------------------------------

/// Start message body; end with `<CRLF>.<CRLF>`.
pub const START_BODY: Code = 354;

// --- 4xx: transient / operational failure -----------------------------------

/// Service not available (closing).
pub const NOT_AVAILABLE: Code = 400;
/// Unknown message type (never registered).
pub const UNKNOWN_TYPE: Code = 430;
/// Type already registered (ownership conflict / lost race).
pub const ALREADY_REGISTERED: Code = 433;
/// Authorization required (no key given).
pub const AUTH_REQUIRED: Code = 440;
/// Authorization key mismatch.
pub const KEY_MISMATCH: Code = 441;
/// Message too large / capacity exceeded.
pub const TOO_LARGE: Code = 452;

// --- 5xx: permanent / syntax error ------------------------------------------

/// Syntax error, command unrecognized.
pub const SYNTAX: Code = 500;
/// Syntax error in parameters or arguments.
pub const SYNTAX_PARAMS: Code = 501;
/// Command not implemented.
pub const NOT_IMPLEMENTED: Code = 502;
/// Bad sequence of commands.
pub const BAD_SEQUENCE: Code = 503;
/// Invalid message type name.
pub const INVALID_TYPE: Code = 521;

/// Classification of a status code by its leading digit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// `1xx` — asynchronous server push (not a command reply).
    Async,
    /// `2xx` — positive completion.
    Positive,
    /// `3xx` — intermediate; more input required.
    Intermediate,
    /// `4xx` — transient / operational failure.
    Transient,
    /// `5xx` — permanent / syntax error.
    Permanent,
    /// Any other leading digit (non-conforming).
    Other,
}

/// Classify a status code by its leading digit.
///
/// This is the single most important predicate for a client's read loop: a
/// line whose code [`is_async`] is an unsolicited push and must be routed to
/// subscription handlers; anything else is the reply to a pending command.
pub fn class(code: Code) -> Class {
    match code / 100 {
        1 => Class::Async,
        2 => Class::Positive,
        3 => Class::Intermediate,
        4 => Class::Transient,
        5 => Class::Permanent,
        _ => Class::Other,
    }
}

/// True iff `code` is an asynchronous server-initiated push (`1xx`).
pub fn is_async(code: Code) -> bool {
    class(code) == Class::Async
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_by_leading_digit() {
        assert_eq!(class(101), Class::Async);
        assert_eq!(class(200), Class::Positive);
        assert_eq!(class(354), Class::Intermediate);
        assert_eq!(class(441), Class::Transient);
        assert_eq!(class(500), Class::Permanent);
        assert_eq!(class(999), Class::Other);
    }

    #[test]
    fn only_1xx_is_async() {
        assert!(is_async(MSG));
        assert!(is_async(DROP));
        assert!(is_async(NOTE));
        assert!(!is_async(SERVICE_READY));
        assert!(!is_async(ACCEPTED));
        assert!(!is_async(START_BODY));
        assert!(!is_async(KEY_MISMATCH));
        assert!(!is_async(SYNTAX));
    }
}
