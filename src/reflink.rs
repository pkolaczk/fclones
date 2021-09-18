use std::fs;
use std::fs::Metadata;
use std::io;

use filetime::FileTime;

use crate::dedupe::{FileMetadata, FsCommand};
use crate::lock::FileLock;
use crate::log::Log;
use crate::Path as FcPath;

pub fn reflink(target: &FileMetadata, link: &FileMetadata, log: &Log) -> io::Result<()> {
    let _ = FileLock::new(&link.path)?; // don't touch a locked file

    if cfg!(any(target_os = "linux", target_os = "android")) && !crate::config::test::crosstest() {
        linux_reflink(target, link, log).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to reflink {} -> {}: {}", link, target, e),
            )
        })
    } else {
        reflink_keep_some_metadata(target, link, log)
    }
}

// Reflink which expects the destination to not exist
fn safe_reflink(target: &FcPath, link: &FcPath) -> io::Result<()> {
    reflink::reflink(&target.to_path_buf(), &link.to_path_buf()).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to reflink {} -> {}: {}", link, target, e),
        )
    })
}

// Dummy function so tests compile
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn linux_reflink(target: &Path, link: &Path, log: &Log) -> io::Result<()> {
    unreachable!()
}

// First reflink (not move) the target file out of the way (this also checks for
// reflink support), then overwrite the existing file to preserve metadata.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_reflink(target: &FileMetadata, link: &FileMetadata, log: &Log) -> io::Result<()> {
    let tmp = FsCommand::temp_file(&link.path);
    let std_tmp = tmp.to_path_buf();

    let fs_target = target.path.to_path_buf();
    let std_link = link.path.to_path_buf();

    // Backup via reflink, if this fails the fs does not support reflinking
    if let Err(e) = reflink_overwrite(&std_link, &std_tmp) {
        if let Err(e) = FsCommand::remove(&tmp) {
            log.warn(format!("Failed to remove temporary {}: {}", &tmp, e))
        }
        return Err(e);
    }

    let result = match reflink_overwrite(&fs_target, &std_link) {
        Err(e) => {
            if let Err(remove_err) = FsCommand::unsafe_rename(&tmp, &link.path) {
                log.warn(format!(
                    "Failed to undo deduplication from {} to {}: {}",
                    &link, &tmp, remove_err
                ))
            } else if let Err(err) = restore_some_metadata(&std_link, &link.metadata) {
                log.warn(format!("Failed keep metadata for {}: {:?}", link, err))
            }
            Err(e)
        }
        ok => ok,
    };

    if let Err(e) = FsCommand::remove(&tmp) {
        log.warn(format!("Failed to remove temporary {}: {}", &tmp, e))
    }

    result
}

/// Reflink `target` to `link` and expect these two files to be equally sized.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn reflink_overwrite(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
    use nix::request_code_write;
    use std::os::unix::prelude::AsRawFd;

    let src = std::fs::File::open(&target)?;

    // This operation does not require `.truncate(true)`
    // because the files are already of the same size.
    let dest = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&link)?;

    // from /usr/include/linux/fs.h:
    // #define FICLONE		_IOW(0x94, 9, int)
    const FICLONE_TYPE: u8 = 0x94;
    const FICLONE_NR: u8 = 9;
    const FICLONE_SIZE: usize = std::mem::size_of::<libc::c_int>();

    let ret = unsafe {
        libc::ioctl(
            dest.as_raw_fd(),
            request_code_write!(FICLONE_TYPE, FICLONE_NR, FICLONE_SIZE),
            src.as_raw_fd(),
        )
    };

    #[allow(clippy::if_same_then_else)]
    if ret == -1 {
        let err = io::Error::last_os_error();
        let code = err.raw_os_error().unwrap(); // unwrap () Ok, created from `last_os_error()`
        if code == libc::EOPNOTSUPP {
            // Filesystem does not supported reflinks.
            // No cleanup required, file is left untouched.
        } else if code == libc::EINVAL {
            // Target and link filesizes were no equal, something changed on disk.
        }
        Err(err)
    } else {
        Ok(())
    }
}

// Not kept: owner, xattrs, ACLs, etc.
fn restore_some_metadata(path: &std::path::Path, metadata: &Metadata) -> io::Result<()> {
    let atime = FileTime::from_last_access_time(metadata);
    let mtime = FileTime::from_last_modification_time(metadata);

    let timestamp = filetime::set_file_times(path, atime, mtime);
    if timestamp.is_err() {
        timestamp
    } else {
        fs::set_permissions(&path, metadata.permissions())
    }
}

fn reflink_keep_some_metadata(
    target: &FileMetadata,
    link: &FileMetadata,
    log: &Log,
) -> io::Result<()> {
    let link_path = &link.path.to_path_buf();

    FsCommand::safe_remove(&link.path, |link| safe_reflink(&target.path, link), log)?;

    if let Err(err) = restore_some_metadata(link_path, &link.metadata) {
        log.warn(format!("Failed keep metadata for {}: {:?}", link, err))
    }

    Ok(())
}
