//! SMTP-style `DATA` block framing and dot-stuffing (RFC Appendix A, Section 5).
//!
//! A message body is a sequence of text lines. On the wire the body is sent
//! line by line and terminated by a line containing exactly `.`. Any body line
//! beginning with `.` is *dot-stuffed* by prefixing an extra `.` on send; the
//! receiver strips one leading `.` from any body line beginning with `..`.
//!
//! This module operates on already-split lines (no CRLFs). The async IO layer
//! is responsible for reading/writing individual CRLF-terminated lines and for
//! recognising the lone `.` terminator.

/// The line that terminates a DATA block / message body.
pub const TERMINATOR: &str = ".";

/// CRLF line terminator used on the wire.
pub const CRLF: &str = "\r\n";

/// Dot-stuff a single outgoing body line: if it begins with `.`, prefix one
/// extra `.`. Returns a borrowed slice when no stuffing is needed.
pub fn stuff_line(line: &str) -> std::borrow::Cow<'_, str> {
    if line.starts_with('.') {
        let mut s = String::with_capacity(line.len() + 1);
        s.push('.');
        s.push_str(line);
        std::borrow::Cow::Owned(s)
    } else {
        std::borrow::Cow::Borrowed(line)
    }
}

/// Reverse [`stuff_line`] for an incoming body line: strip one leading `.` if
/// the line begins with `..`.
///
/// The caller must have already determined that `line` is a body line and not
/// the lone `.` terminator.
pub fn unstuff_line(line: &str) -> &str {
    if let Some(rest) = line.strip_prefix('.') {
        // Only lines beginning with ".." were stuffed; a stuffed "." becomes
        // ".." and unstuffs back to ".". A raw body line can never be a lone
        // "." because that is the terminator, so stripping one dot is correct.
        rest
    } else {
        line
    }
}

/// Serialize a body (as a slice of lines) into a complete DATA block including
/// the terminating `.` line, using CRLF between lines.
pub fn encode_body(lines: &[String]) -> String {
    let mut out = String::new();
    for line in lines {
        out.push_str(&stuff_line(line));
        out.push_str(CRLF);
    }
    out.push_str(TERMINATOR);
    out.push_str(CRLF);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stuffing_roundtrip() {
        for raw in ["hello", ".hidden", "..double", "normal.", "", "21.4C"] {
            let stuffed = stuff_line(raw);
            let back = unstuff_line(&stuffed);
            assert_eq!(back, raw, "roundtrip failed for {raw:?}");
        }
    }

    #[test]
    fn stuff_only_when_leading_dot() {
        assert_eq!(&*stuff_line("foo"), "foo");
        assert_eq!(&*stuff_line(".foo"), "..foo");
        assert_eq!(&*stuff_line("."), "..");
    }

    #[test]
    fn unstuff_strips_one_dot() {
        assert_eq!(unstuff_line("..foo"), ".foo");
        assert_eq!(unstuff_line("..."), "..");
        assert_eq!(unstuff_line("foo"), "foo");
    }

    #[test]
    fn encodes_full_block() {
        let body = vec!["line one".to_string(), ".dotted".to_string()];
        assert_eq!(encode_body(&body), "line one\r\n..dotted\r\n.\r\n");
    }

    #[test]
    fn encodes_empty_body() {
        assert_eq!(encode_body(&[]), ".\r\n");
    }
}
