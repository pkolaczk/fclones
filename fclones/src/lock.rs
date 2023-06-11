use crate::error::error_kind;
use std::fs::File;
use std::{fs, io};

use crate::path::Path;

/// Portable file locking.
///
/// On Unix, advisory lock through fnctl is used.
/// On Windows, file is open in read-write mode.
///
/// The file must exist before locking.
pub struct FileLock {
    pub file: File,
}

impl FileLock {
    #[cfg(unix)]
    fn nix_as_io_error<T>(result: nix::Result<T>) -> io::Result<T> {
        match result {
            Ok(x) => Ok(x),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates a libc::flock initialized to zeros.
    /// Should be safe, because flock contains primitive fields only, no references.
    #[cfg(unix)]
    fn new_flock() -> libc::flock {
        unsafe { std::mem::zeroed() }
    }

    #[cfg(unix)]
    fn fcntl_lock(file: &File) -> io::Result<()> {
        use nix::fcntl::*;
        use std::os::unix::io::AsRawFd;
        let mut f = Self::new_flock();
        f.l_type = libc::F_WRLCK as i16;
        f.l_whence = libc::SEEK_SET as i16;
        let result = nix::fcntl::fcntl(file.as_raw_fd(), FcntlArg::F_SETLK(&f));
        Self::nix_as_io_error(result).map(|_| {})
    }

    #[cfg(unix)]
    fn fcntl_unlock(file: &File) -> io::Result<()> {
        use nix::fcntl::*;
        use std::os::unix::io::AsRawFd;
        let mut f = Self::new_flock();
        f.l_type = libc::F_UNLCK as i16;
        f.l_whence = libc::SEEK_SET as i16;
        let result = nix::fcntl::fcntl(file.as_raw_fd(), FcntlArg::F_SETLK(&f));
        Self::nix_as_io_error(result).map(|_| {})
    }

    /// Locks a file and obtains its metadata.
    /// On error, the error message will contain the path.
    pub fn new(path: &Path) -> io::Result<FileLock> {
        let path_buf = path.to_path_buf();
        let file = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create(false)
            .open(path_buf)
            .map_err(|e| {
                io::Error::new(
                    error_kind(&e),
                    format!("Failed to open file {} for write: {}", path.display(), e),
                )
            })?;

        #[cfg(unix)]
        if let Err(e) = Self::fcntl_lock(&file) {
            return Err(io::Error::new(
                error_kind(&e),
                format!("Failed to lock file {}: {}", path.display(), e),
            ));
        };

        Ok(FileLock { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = Self::fcntl_unlock(&self.file);
    }
}
