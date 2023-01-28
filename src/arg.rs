//! Command line argument parsing and quoting utilities.
//!
//! Provides lossless OsString conversions to and from String by shell-like escaping and quoting.

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Display, Formatter};
use std::mem;

use itertools::Itertools;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use stfu8::DecodeError;

/// Argument passed to the app
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Arg(OsString);

impl Arg {
    pub fn from_escaped_string(s: &str) -> Result<Self, DecodeError> {
        Ok(Arg(from_stfu8(s)?))
    }

    pub fn to_escaped_string(&self) -> String {
        to_stfu8(self.0.clone())
    }

    pub fn quote(&self) -> String {
        quote(self.0.to_os_string())
    }

    pub fn as_os_str(&self) -> &OsStr {
        self.0.as_ref()
    }
}

impl AsRef<OsStr> for Arg {
    fn as_ref(&self) -> &OsStr {
        self.0.as_os_str()
    }
}

impl From<OsString> for Arg {
    fn from(s: OsString) -> Self {
        Arg(s)
    }
}

impl From<&OsStr> for Arg {
    fn from(s: &OsStr) -> Self {
        Arg(OsString::from(s))
    }
}

impl From<&str> for Arg {
    fn from(s: &str) -> Self {
        Arg(OsString::from(s))
    }
}

struct ArgVisitor;

impl Visitor<'_> for ArgVisitor {
    type Value = Arg;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an STFU encoded string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let arg = Arg::from_escaped_string(v).map_err(|e| E::custom(e.to_string()))?;
        Ok(arg)
    }
}

impl Serialize for Arg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_escaped_string().as_str())
    }
}

impl<'de> Deserialize<'de> for Arg {
    fn deserialize<D>(deserializer: D) -> Result<Arg, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ArgVisitor)
    }
}

/// Returns a lossless string representation in [STFU8 format](https://crates.io/crates/stfu8).
#[cfg(unix)]
pub fn to_stfu8(s: OsString) -> String {
    use std::os::unix::ffi::OsStringExt;
    let raw_path_bytes = s.into_vec();
    stfu8::encode_u8(&raw_path_bytes)
}

/// Returns a lossless string representation in [STFU8 format](https://crates.io/crates/stfu8).
#[cfg(windows)]
pub fn to_stfu8(s: OsString) -> String {
    use std::os::windows::ffi::OsStrExt;
    let raw_path_bytes: Vec<u16> = s.encode_wide().collect();
    stfu8::encode_u16(&raw_path_bytes)
}

/// Decodes the path from the string encoded with [`to_stfu8`](OsString::to_stfu8).
#[cfg(unix)]
pub fn from_stfu8(encoded: &str) -> Result<OsString, DecodeError> {
    use std::os::unix::ffi::OsStringExt;
    let raw_bytes = stfu8::decode_u8(encoded)?;
    Ok(OsString::from_vec(raw_bytes))
}

/// Decodes the path from the string encoded with [`to_stfu8`](OsString::to_stfu8).
#[cfg(windows)]
pub fn from_stfu8(encoded: &str) -> Result<OsString, DecodeError> {
    use std::os::windows::ffi::OsStringExt;
    let raw_bytes = stfu8::decode_u16(encoded)?;
    Ok(OsString::from_wide(&raw_bytes))
}

const SPECIAL_CHARS: [char; 25] = [
    '|', '&', ';', '<', '>', '(', ')', '{', '}', '$', '`', '\\', '\'', '"', ' ', '\t', '*', '?',
    '+', '[', ']', '#', '˜', '=', '%',
];

/// Escapes special characters in a string, so that it will retain its literal meaning when used as
/// a part of command in Unix shell.
///
/// It tries to avoid introducing any unnecessary quotes or escape characters, but specifics
/// regarding quoting style are left unspecified.
pub fn quote(s: OsString) -> String {
    let lossy = s.to_string_lossy();
    if lossy
        .chars()
        .any(|c| c < '\u{20}' || c == '\u{7f}' || c == '\u{fffd}' || c == '\'')
    {
        format!("$'{}'", to_stfu8(s).replace('\'', "\\'"))
    } else if lossy.chars().any(|c| SPECIAL_CHARS.contains(&c)) {
        format!("'{}'", lossy)
    } else {
        lossy.to_string()
    }
}

#[derive(Debug)]
pub struct ParseError {
    pub msg: String,
}

impl ParseError {
    pub fn new(msg: &str) -> ParseError {
        ParseError {
            msg: msg.to_string(),
        }
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for ParseError {}

enum State {
    /// Within a delimiter.
    Delimiter,
    /// After backslash, but before starting word.
    Backslash,
    /// Within an unquoted word.
    Unquoted,
    /// After backslash in an unquoted word.
    UnquotedBackslash,
    /// Within a single quoted word.
    SingleQuoted,
    /// Within a double quoted word.
    DoubleQuoted,
    /// After backslash inside a double quoted word.
    DoubleQuotedBackslash,
    /// After dollar in an unquoted word.
    Dollar,
    /// Within a quoted word preceded by a dollar sign.
    DollarQuoted,
    /// After backslash in a dollar-quoted word.
    DollarQuotedBackslash,
    /// Inside a comment.
    Comment,
}

/// Appends a character to OsString
fn append(s: &mut OsString, c: char) {
    let mut buf = [0; 4];
    let c = c.encode_utf8(&mut buf);
    s.push(c)
}

/// Splits command line into separate arguments, in much the same way Unix shell would, but without
/// many of expansion the shell would perform.
///
/// The split functionality is compatible with behaviour of Unix shell, but with word expansions
/// limited to quote removal, and without special token recognition rules for operators.
///
/// The result is exactly the same as one obtained from Unix shell as long as those unsupported
/// features are not present in input: no operators, no variable assignments, no tilde expansion,
/// no parameter expansion, no command substitution, no arithmetic expansion, no pathname
/// expansion.
///
/// In case those unsupported shell features are present, the syntax that introduce them is
/// interpreted literally.
///
/// # Errors
///
/// When input contains unmatched quote, an error is returned.
///
/// # Compatibility with other implementations
///
/// It should be fully compatible with g_shell_parse_argv from GLib, except that in GLib
/// it is an error not to have any words after tokenization.
///
/// It is also very close to shlex.split available in Python standard library, when used in POSIX
/// mode with support for comments. Though, shlex implementation diverges from POSIX, and from
/// implementation contained herein in three aspects. First, it doesn't support line continuations.
/// Second, inside double quotes, the backslash characters retains its special meaning as an escape
/// character only when followed by \\ or \", whereas POSIX specifies that it should retain its
/// special meaning when followed by: $, \`, \", \\, or a newline. Third, it treats carriage return
/// as one of delimiters.
/// ```
pub fn split(s: &str) -> Result<Vec<Arg>, ParseError> {
    // Based on shell-words crate by Tomasz Miąsko
    // Handling of dollar quotes added by Piotr Kołaczkowski

    use State::*;

    let mut words = Vec::new();
    let mut word = OsString::new();

    let mut pos = 0;
    let mut dollar_quote_start = 0;

    let mut chars = s.chars();
    let mut state = Delimiter;

    loop {
        let c = chars.next();
        state = match state {
            Delimiter => match c {
                None => break,
                Some('\'') => SingleQuoted,
                Some('\"') => DoubleQuoted,
                Some('\\') => Backslash,
                Some('\t') | Some(' ') | Some('\n') => Delimiter,
                Some('$') => Dollar,
                Some('#') => Comment,
                Some(c) => {
                    append(&mut word, c);
                    Unquoted
                }
            },
            Backslash => match c {
                None => {
                    append(&mut word, '\\');
                    words.push(Arg(mem::replace(&mut word, OsString::new())));
                    break;
                }
                Some('\n') => Delimiter,
                Some(c) => {
                    append(&mut word, c);
                    Unquoted
                }
            },
            Unquoted => match c {
                None => {
                    words.push(Arg(mem::replace(&mut word, OsString::new())));
                    break;
                }
                Some('\'') => SingleQuoted,
                Some('\"') => DoubleQuoted,
                Some('\\') => UnquotedBackslash,
                Some('$') => Dollar,
                Some('\t') | Some(' ') | Some('\n') => {
                    words.push(Arg(mem::replace(&mut word, OsString::new())));
                    Delimiter
                }
                Some(c) => {
                    append(&mut word, c);
                    Unquoted
                }
            },
            UnquotedBackslash => match c {
                None => {
                    append(&mut word, '\\');
                    words.push(Arg(mem::replace(&mut word, OsString::new())));
                    break;
                }
                Some('\n') => Unquoted,
                Some(c) => {
                    append(&mut word, c);
                    Unquoted
                }
            },
            SingleQuoted => match c {
                None => return Err(ParseError::new("Unclosed single quote")),
                Some('\'') => Unquoted,
                Some(c) => {
                    append(&mut word, c);
                    SingleQuoted
                }
            },
            DoubleQuoted => match c {
                None => return Err(ParseError::new("Unclosed double quote")),
                Some('\"') => Unquoted,
                Some('\\') => DoubleQuotedBackslash,
                Some(c) => {
                    append(&mut word, c);
                    DoubleQuoted
                }
            },
            DoubleQuotedBackslash => match c {
                None => return Err(ParseError::new("Unexpected end of input")),
                Some('\n') => DoubleQuoted,
                Some(c @ '$') | Some(c @ '`') | Some(c @ '"') | Some(c @ '\\') => {
                    append(&mut word, c);
                    DoubleQuoted
                }
                Some(c) => {
                    append(&mut word, '\\');
                    append(&mut word, c);
                    DoubleQuoted
                }
            },
            Dollar => match c {
                None => return Err(ParseError::new("Unexpected end of input")),
                Some('\'') => {
                    dollar_quote_start = pos + 1;
                    DollarQuoted
                }
                Some(_) => return Err(ParseError::new("Expected single quote")),
            },
            DollarQuoted => match c {
                None => return Err(ParseError::new("Unclosed single quote")),
                Some('\\') => DollarQuotedBackslash,
                Some('\'') => {
                    let quoted_slice = &s[dollar_quote_start..pos].replace("\\'", "'");
                    let decoded = from_stfu8(quoted_slice).map_err(|e| {
                        ParseError::new(format!("Failed to decode STFU-8 chunk: {}", e).as_str())
                    })?;
                    word.push(decoded.as_os_str());
                    Unquoted
                }
                Some(_) => DollarQuoted,
            },
            DollarQuotedBackslash => match c {
                None => return Err(ParseError::new("Unexpected end of input")),
                Some(_) => DollarQuoted,
            },
            Comment => match c {
                None => break,
                Some('\n') => Delimiter,
                Some(_) => Comment,
            },
        };
        pos += 1;
    }

    Ok(words)
}

/// Joins multiple command line args into a single-line escaped representation
pub fn join(args: &[Arg]) -> String {
    args.iter().map(|arg| arg.quote()).join(" ")
}

#[cfg(test)]
mod test {
    use std::ffi::OsString;

    use crate::arg::{quote, split, Arg};

    #[test]
    fn quote_no_special_chars() {
        assert_eq!(quote(OsString::from("abc/def_123.txt")), "abc/def_123.txt");
    }

    #[test]
    fn quote_path_with_control_chars() {
        assert_eq!(quote(OsString::from("a\nb")), "$'a\\nb'");
        assert_eq!(quote(OsString::from("a\tb")), "$'a\\tb'");
    }

    #[test]
    fn quote_path_with_special_chars() {
        assert_eq!(quote(OsString::from("a b")), "'a b'");
        assert_eq!(quote(OsString::from("a*b")), "'a*b'");
        assert_eq!(quote(OsString::from("a?b")), "'a?b'");
        assert_eq!(quote(OsString::from("$ab")), "'$ab'");
        assert_eq!(quote(OsString::from("a(b)")), "'a(b)'");
        assert_eq!(quote(OsString::from("a\\b")), "'a\\b'");
    }

    #[test]
    fn quote_path_with_single_quotes() {
        assert_eq!(quote(OsString::from("a'b")), "$'a\\'b'");
        assert_eq!(quote(OsString::from("a'b'")), "$'a\\'b\\''");
    }

    #[test]
    fn split_unquoted_args() {
        assert_eq!(
            split("arg1 arg2").unwrap(),
            vec![Arg::from("arg1"), Arg::from("arg2")]
        )
    }

    #[test]
    fn split_single_quoted_args() {
        assert_eq!(
            split("'arg1 with spaces' arg2").unwrap(),
            vec![Arg::from("arg1 with spaces"), Arg::from("arg2")]
        )
    }

    #[test]
    fn split_doubly_quoted_args() {
        assert_eq!(
            split("\"arg1 with spaces\" arg2").unwrap(),
            vec![Arg::from("arg1 with spaces"), Arg::from("arg2")]
        )
    }

    #[test]
    fn split_quotes_escaping() {
        assert_eq!(
            split("\"escaped \\\" quotes\"").unwrap(),
            vec![Arg::from("escaped \" quotes")]
        )
    }

    #[test]
    fn split_escaped_single_quote() {
        assert_eq!(
            split("$'single\\'quote'").unwrap(),
            vec![Arg::from("single'quote")]
        );
    }

    #[test]
    fn split_spaces_escaping() {
        assert_eq!(
            split("escaped\\ space").unwrap(),
            vec![Arg::from("escaped space")]
        )
    }

    #[test]
    fn dollar_quoting() {
        assert_eq!(
            split("arg1 $'arg2-\\n\\t\\\\' arg3-$'\\x7f'").unwrap(),
            vec![
                Arg::from("arg1"),
                Arg::from("arg2-\n\t\\"),
                Arg::from("arg3-\x7f")
            ]
        )
    }
}
