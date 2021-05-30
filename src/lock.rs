use core::fmt;
use std::fmt::{Display, Formatter};
use std::fs::Metadata;
use std::io;

use crate::path::Path;
use std::io::ErrorKind;

/// Semi-portable file locking.
/// Provides safe access to file metadata.
/// On Unix, advisory lock through fnctl is used.
/// On Windows, file is open in read-write mode.
pub struct FileLock {
    pub path: Path,
    pub metadata: Metadata,

    #[cfg(unix)]
    _lock: file_lock::FileLock,

    #[cfg(windows)]
    _lock: File,
}

impl FileLock {
    /// Locks a file and obtains its metadata.
    /// On error, the error message will contain the path.
    #[cfg(unix)]
    pub fn new(path: Path) -> io::Result<FileLock> {
        let path_str = path.to_string_lossy();
        let path_buf = path.to_path_buf();
        if !path_buf.exists() {
            return Err(io::Error::new(ErrorKind::NotFound, format!("File not found: {}", path)));
        }
        let lock = file_lock::FileLock::lock(path_str.as_str(), false, true).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to acquire exclusive lock on {}: {}", path, e),
            )
        })?;
        let metadata = lock.file.metadata().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path, e),
            )
        })?;
        Ok(FileLock {
            _lock: lock,
            path,
            metadata,
        })
    }

    /// Locks a file and obtains its metadata.
    /// On error, the error message will contain the path.
    #[cfg(windows)]
    pub fn new(path: Path) -> io::Result<FileLock> {
        let options = fs::OpenOptions::new().read(true).write(true).create(false);
        let file = options.open(&path.to_path_buf()).map_err(|e| {
            io::Error::new(e.kind(), format!("Failed to open file {}: {}", path, e))
        })?;
        let metadata = file.metadata().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path, e),
            )
        })?;
        Ok(FileLock {
            _lock: file,
            path,
            metadata,
        })
    }

    /// Releases the lock and returns the file path and metadata
    pub fn release(self) -> (Path, Metadata) {
        (self.path, self.metadata)
    }
}

impl Display for FileLock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", self.path).as_str())
    }
}
