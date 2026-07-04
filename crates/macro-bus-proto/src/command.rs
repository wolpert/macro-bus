//! Client-protocol command parsing (RFC Appendix A, Section 6).
//!
//! Command tokens are matched case-insensitively; arguments are
//! case-sensitive. A parsed [`Command`] is always structurally valid: type
//! names and keys have been checked against the ABNF.

use crate::status::{self, Code};
use crate::validate;

/// A fully-parsed, structurally-valid client command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `CAPABILITIES` — list supported capabilities.
    Capabilities,
    /// `HELP` — human-readable help.
    Help,
    /// `REGISTER <type> <key>` — claim ownership of a type.
    Register {
        /// The message type being claimed.
        type_name: String,
        /// The authorization key bound to the type.
        key: String,
    },
    /// `SUBSCRIBE <type>` — start listening for a type.
    Subscribe {
        /// The message type to listen for.
        type_name: String,
    },
    /// `UNSUBSCRIBE <type>` — stop listening for a type.
    Unsubscribe {
        /// The message type to stop listening for.
        type_name: String,
    },
    /// `PUBLISH <type> <key>` — begin a publish (server replies `354`, body follows).
    Publish {
        /// The message type to publish to.
        type_name: String,
        /// The authorization key that must match the type's owner key.
        key: String,
    },
    /// `LIST TYPES` — enumerate known types.
    ListTypes,
    /// `QUIT` — close the connection.
    Quit,
}

/// Failure to parse a command line, carrying the MBP status code the server
/// should respond with and a human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// The MBP status code to return (a `5xx`).
    pub code: Code,
    /// Short human-readable reason (goes after the code on the wire).
    pub reason: String,
}

impl ParseError {
    fn new(code: Code, reason: impl Into<String>) -> Self {
        ParseError { code, reason: reason.into() }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.code, self.reason)
    }
}

impl std::error::Error for ParseError {}

/// Parse a single command line (without its CRLF terminator).
///
/// Leading/trailing ASCII whitespace is tolerated. Returns [`ParseError`] with
/// the appropriate `5xx` code when the line is not a valid command.
pub fn parse(line: &str) -> Result<Command, ParseError> {
    let line = line.trim_matches(|c: char| c == ' ' || c == '\t');
    if line.is_empty() {
        return Err(ParseError::new(status::SYNTAX, "empty command"));
    }

    // Split into the verb and the remainder. We split the verb off first so we
    // can dispatch, then parse the specific argument shape per command.
    let mut parts = line.splitn(2, ' ');
    let verb = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim_start_matches(' ');

    match verb.to_ascii_uppercase().as_str() {
        "CAPABILITIES" => expect_no_args(rest, Command::Capabilities),
        "HELP" => expect_no_args(rest, Command::Help),
        "QUIT" => expect_no_args(rest, Command::Quit),
        "REGISTER" => {
            let (type_name, key) = two_args(rest)?;
            check_type(&type_name)?;
            check_key(&key)?;
            Ok(Command::Register { type_name, key })
        }
        "PUBLISH" => {
            let (type_name, key) = two_args(rest)?;
            check_type(&type_name)?;
            check_key(&key)?;
            Ok(Command::Publish { type_name, key })
        }
        "SUBSCRIBE" => {
            let type_name = one_arg(rest)?;
            check_type(&type_name)?;
            Ok(Command::Subscribe { type_name })
        }
        "UNSUBSCRIBE" => {
            let type_name = one_arg(rest)?;
            check_type(&type_name)?;
            Ok(Command::Unsubscribe { type_name })
        }
        "LIST" => {
            // Only `LIST TYPES` is defined.
            if rest.eq_ignore_ascii_case("TYPES") {
                Ok(Command::ListTypes)
            } else if rest.is_empty() {
                Err(ParseError::new(status::SYNTAX_PARAMS, "LIST requires a sub-command (TYPES)"))
            } else {
                Err(ParseError::new(status::NOT_IMPLEMENTED, "only LIST TYPES is supported"))
            }
        }
        _ => Err(ParseError::new(status::SYNTAX, "command unrecognized")),
    }
}

fn expect_no_args(rest: &str, cmd: Command) -> Result<Command, ParseError> {
    if rest.trim().is_empty() {
        Ok(cmd)
    } else {
        Err(ParseError::new(status::SYNTAX_PARAMS, "command takes no arguments"))
    }
}

fn one_arg(rest: &str) -> Result<String, ParseError> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Err(ParseError::new(status::SYNTAX_PARAMS, "missing argument"));
    }
    if rest.contains(' ') {
        return Err(ParseError::new(status::SYNTAX_PARAMS, "too many arguments"));
    }
    Ok(rest.to_string())
}

fn two_args(rest: &str) -> Result<(String, String), ParseError> {
    let rest = rest.trim();
    let mut it = rest.split(' ').filter(|s| !s.is_empty());
    let a = it
        .next()
        .ok_or_else(|| ParseError::new(status::SYNTAX_PARAMS, "missing arguments"))?;
    let b = it
        .next()
        .ok_or_else(|| ParseError::new(status::SYNTAX_PARAMS, "missing second argument"))?;
    if it.next().is_some() {
        return Err(ParseError::new(status::SYNTAX_PARAMS, "too many arguments"));
    }
    Ok((a.to_string(), b.to_string()))
}

fn check_type(t: &str) -> Result<(), ParseError> {
    if validate::is_valid_type(t) {
        Ok(())
    } else {
        Err(ParseError::new(status::INVALID_TYPE, "invalid message type name"))
    }
}

fn check_key(k: &str) -> Result<(), ParseError> {
    if validate::is_valid_key(k) {
        Ok(())
    } else {
        Err(ParseError::new(status::SYNTAX_PARAMS, "invalid authorization key"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_commands() {
        assert_eq!(parse("CAPABILITIES").unwrap(), Command::Capabilities);
        assert_eq!(parse("help").unwrap(), Command::Help);
        assert_eq!(parse("QuIt").unwrap(), Command::Quit);
        assert_eq!(parse("LIST TYPES").unwrap(), Command::ListTypes);
        assert_eq!(parse("list types").unwrap(), Command::ListTypes);
    }

    #[test]
    fn commands_are_case_insensitive_args_are_not() {
        assert_eq!(
            parse("register Sensors.Temp S3cr3t").unwrap(),
            Command::Register { type_name: "Sensors.Temp".into(), key: "S3cr3t".into() }
        );
        // A different-cased type is a distinct type.
        assert_ne!(
            parse("SUBSCRIBE sensors.temp").unwrap(),
            parse("SUBSCRIBE sensors.TEMP").unwrap()
        );
    }

    #[test]
    fn register_and_publish_shapes() {
        assert_eq!(
            parse("PUBLISH a.b key1").unwrap(),
            Command::Publish { type_name: "a.b".into(), key: "key1".into() }
        );
        assert_eq!(
            parse("SUBSCRIBE a.b").unwrap(),
            Command::Subscribe { type_name: "a.b".into() }
        );
        assert_eq!(
            parse("UNSUBSCRIBE a.b").unwrap(),
            Command::Unsubscribe { type_name: "a.b".into() }
        );
    }

    #[test]
    fn tolerates_surrounding_and_internal_whitespace() {
        assert_eq!(parse("  SUBSCRIBE   a.b  ").unwrap(), Command::Subscribe { type_name: "a.b".into() });
        assert_eq!(parse("REGISTER   a.b   k").unwrap(), Command::Register { type_name: "a.b".into(), key: "k".into() });
    }

    #[test]
    fn rejects_bad_arity() {
        assert_eq!(parse("SUBSCRIBE").unwrap_err().code, status::SYNTAX_PARAMS);
        assert_eq!(parse("SUBSCRIBE a b").unwrap_err().code, status::SYNTAX_PARAMS);
        assert_eq!(parse("REGISTER a").unwrap_err().code, status::SYNTAX_PARAMS);
        assert_eq!(parse("REGISTER a b c").unwrap_err().code, status::SYNTAX_PARAMS);
        assert_eq!(parse("CAPABILITIES x").unwrap_err().code, status::SYNTAX_PARAMS);
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(parse("FROBNICATE x").unwrap_err().code, status::SYNTAX);
        assert_eq!(parse("").unwrap_err().code, status::SYNTAX);
    }

    #[test]
    fn rejects_invalid_type_name() {
        assert_eq!(parse("SUBSCRIBE bad name").unwrap_err().code, status::SYNTAX_PARAMS); // too many args
        assert_eq!(parse("SUBSCRIBE bad!name").unwrap_err().code, status::INVALID_TYPE);
        assert_eq!(parse("REGISTER bad!name k").unwrap_err().code, status::INVALID_TYPE);
    }

    #[test]
    fn list_variants() {
        assert_eq!(parse("LIST").unwrap_err().code, status::SYNTAX_PARAMS);
        assert_eq!(parse("LIST FROBS").unwrap_err().code, status::NOT_IMPLEMENTED);
    }
}
