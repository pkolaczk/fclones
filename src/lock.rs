use core::fmt;
use std::fmt::{Display, Formatter};
use std::fs::{File, Metadata};
use std::{fs, io};

use crate::path::Path;

/// Provides locked file metadata access.
///
/// Locks the file before fetching the file metadata.
/// On Unix, advisory lock through fnctl is used.
/// On Windows, file is open in read-write mode.
///
/// For symbolic links, returns the symbolic link metadata.
pub struct FileLock {
    pub path: Path,
    pub metadata: Metadata,
    pub file: File,
}

impl FileLock {
    #[cfg(unix)]
    fn nix_as_io_error<T>(result: nix::Result<T>) -> io::Result<T> {
        match result {
            Ok(x) => Ok(x),
            Err(nix::Error::Sys(errno)) => Err(io::Error::from_raw_os_error(errno as i32)),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    #[cfg(unix)]
    fn fcntl_lock(file: &File) -> io::Result<()> {
        use nix::fcntl::*;
        use std::os::unix::io::AsRawFd;

        let f = libc::flock {
            l_type: libc::F_WRLCK as i16,
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };
        let result = nix::fcntl::fcntl(file.as_raw_fd(), FcntlArg::F_SETLK(&f));
        Self::nix_as_io_error(result).map(|_| {})
    }

    #[cfg(unix)]
    fn fcntl_unlock(file: &File) -> io::Result<()> {
        use nix::fcntl::*;
        use std::os::unix::io::AsRawFd;

        let f = libc::flock {
            l_type: libc::F_UNLCK as i16,
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };
        let result = nix::fcntl::fcntl(file.as_raw_fd(), FcntlArg::F_SETLK(&f));
        Self::nix_as_io_error(result).map(|_| {})
    }

    /// Locks a file and obtains its metadata.
    /// On error, the error message will contain the path.
    pub fn new(path: Path) -> io::Result<FileLock> {
        let path_buf = path.to_path_buf();
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path_buf)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("Failed to open file {}: {}", path, e))
            })?;

        #[cfg(unix)]
        if let Err(e) = Self::fcntl_lock(&file) {
            return Err(io::Error::new(
                e.kind(),
                format!("Failed to lock file {}: {}", path, e),
            ));
        };

        let metadata = fs::symlink_metadata(&path_buf).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path, e),
            )
        })?;

        Ok(FileLock {
            path,
            metadata,
            file,
        })
    }

    #[cfg(unix)]
    pub fn device_id(&self) -> Option<u64> {
        use std::os::unix::fs::MetadataExt;
        Some(self.metadata.dev())
    }

    #[cfg(windows)]
    pub fn device_id(&self) -> Option<u64> {
        use crate::files::FileId;
        FileId::from_file(&self.file).ok().map(|f| f.device)
    }
}

impl Display for FileLock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", self.path).as_str())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = Self::fcntl_unlock(&self.file);
    }
}
