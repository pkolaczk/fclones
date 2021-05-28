use crate::path::Path;
use std::io;

/// Semi-portable file locking.
/// Currently supported only on Unix.
/// Does nothing on Windows.
pub struct FileLock {
    #[cfg(unix)]
    _lock: file_lock::FileLock,
}

impl FileLock {
    #[cfg(unix)]
    pub fn new(path: &Path) -> io::Result<FileLock> {
        let path_str = path.to_string_lossy();
        let lock = file_lock::FileLock::lock(path_str.as_str(), false, true);
        lock.map(|l| FileLock { _lock: l })
    }

    #[cfg(windows)]
    pub fn new(path: &Path) -> io::Result<FileLock> {
        Ok(FileLock)
    }
}
