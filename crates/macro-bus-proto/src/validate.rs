//! Validation of message-type names and authorization keys per the MBP ABNF
//! (RFC Appendix A, Section 11).
//!
//! ```abnf
//! token = 1*tchar
//! tchar = ALPHA / DIGIT / "-" / "." / "_" / "/"
//! type  = token          ; opaque, case-sensitive
//! key   = 1*VCHAR        ; no SP; case-sensitive
//! ```

/// True iff `c` is a `tchar` (a valid character for a message-type token).
pub fn is_tchar(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '/')
}

/// True iff `s` is a valid message-type name: a non-empty `token`.
pub fn is_valid_type(s: &str) -> bool {
    !s.is_empty() && s.chars().all(is_tchar)
}

/// True iff `s` is a valid authorization key: one or more visible ASCII
/// characters (`VCHAR`, %x21-7E) with no spaces.
pub fn is_valid_key(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_graphic())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_types() {
        assert!(is_valid_type("sensors.temperature"));
        assert!(is_valid_type("orders/created"));
        assert!(is_valid_type("a-b_c.d/e"));
        assert!(is_valid_type("X"));
    }

    #[test]
    fn invalid_types() {
        assert!(!is_valid_type(""));
        assert!(!is_valid_type("has space"));
        assert!(!is_valid_type("bang!"));
        assert!(!is_valid_type("tab\there"));
        assert!(!is_valid_type("uni\u{00e9}code"));
    }

    #[test]
    fn keys() {
        assert!(is_valid_key("s3cr3t"));
        assert!(is_valid_key("!@#$%^&*()"));
        assert!(!is_valid_key(""));
        assert!(!is_valid_key("has space"));
        assert!(!is_valid_key("nul\0"));
    }
}
