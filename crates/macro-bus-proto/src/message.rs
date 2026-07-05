//! The in-flight message representation shared by the daemon, client library
//! and federation layer.

use crate::frame;

/// A published message as it travels through the bus.
///
/// Bodies are represented as a vector of text lines (no CRLFs, un-stuffed).
/// A single-line publish such as `21.4C` is one element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The message type this was published to.
    pub type_name: String,
    /// Cluster-unique identifier, used for loop-prevention / dedup.
    pub msg_id: String,
    /// The id of the daemon on which the message was first published.
    pub origin: String,
    /// The message body, one entry per line, already un-stuffed.
    pub body: Vec<String>,
}

impl Message {
    /// Construct a message.
    pub fn new(
        type_name: impl Into<String>,
        msg_id: impl Into<String>,
        origin: impl Into<String>,
        body: Vec<String>,
    ) -> Self {
        Message {
            type_name: type_name.into(),
            msg_id: msg_id.into(),
            origin: origin.into(),
            body,
        }
    }

    /// The `101 MSG` header line (without trailing CRLF) that precedes the body
    /// when delivering to a subscriber.
    pub fn msg_header(&self) -> String {
        format!(
            "{} MSG {} {} {}",
            crate::status::MSG,
            self.type_name,
            self.msg_id,
            self.origin
        )
    }

    /// The full `101 MSG` delivery block: header line + dot-stuffed body +
    /// terminator, all CRLF-terminated.
    pub fn encode_delivery(&self) -> String {
        let mut out = self.msg_header();
        out.push_str(frame::CRLF);
        out.push_str(&frame::encode_body(&self.body));
        out
    }

    /// The `FEED` header line (without trailing CRLF) used to forward this
    /// message to a peer daemon over the federation link.
    pub fn feed_header(&self) -> String {
        format!("FEED {} {} {}", self.type_name, self.msg_id, self.origin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_block_format() {
        let m = Message::new("sensors.temp", "d1-1", "d1", vec!["21.4C".into()]);
        assert_eq!(m.msg_header(), "101 MSG sensors.temp d1-1 d1");
        assert_eq!(
            m.encode_delivery(),
            "101 MSG sensors.temp d1-1 d1\r\n21.4C\r\n.\r\n"
        );
    }

    #[test]
    fn dotted_body_line_is_stuffed_in_delivery() {
        let m = Message::new("t", "d1-2", "d1", vec![".leading".into()]);
        assert_eq!(
            m.encode_delivery(),
            "101 MSG t d1-2 d1\r\n..leading\r\n.\r\n"
        );
    }

    #[test]
    fn feed_header_format() {
        let m = Message::new("t", "d1-3", "d1", vec![]);
        assert_eq!(m.feed_header(), "FEED t d1-3 d1");
    }
}
