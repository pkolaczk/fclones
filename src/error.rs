use std::fmt::{Display, Formatter};
use std::{fmt, io};

/// Error reported by top-level fclones functions
#[derive(Debug)]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new(msg: String) -> Error {
        Error { message: msg }
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::new(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::new(s.to_owned())
    }
}

/// Returns error kind.
/// Maps `libc::EOPNOTSUPP` error to `ErrorKind::Unsupported` on Unix.
pub fn error_kind(error: &io::Error) -> io::ErrorKind {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EOPNOTSUPP) {
        return io::ErrorKind::Unsupported;
    }
    error.kind()
}
