use std::fs;
use std::fs::Metadata;
use std::io;

use filetime::FileTime;

use crate::dedupe::{FileMetadata, FsCommand};
use crate::lock::FileLock;
use crate::log::Log;
use crate::Path as FcPath;

/// Calls OS-specific reflink implementations with an option to call the more generic
/// one during testing one on Linux ("crosstesting").
/// The destination file is allowed to exist.
pub fn reflink(src: &FileMetadata, dest: &FileMetadata, log: &Log) -> io::Result<()> {
    let _ = FileLock::new(&dest.path)?; // don't touch a locked file

    // Remember the original metadata of the parent directory of the destination file:
    let dest_parent = dest.path.parent();
    let dest_parent_metadata = dest_parent.map(|p| p.to_path_buf().metadata());

    // Call reflink:
    let result = {
        if cfg!(any(target_os = "linux", target_os = "android")) && !test::cfg::crosstest() {
            linux_reflink(src, dest, log).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to deduplicate {} -> {}: {}", dest, src, e),
                )
            })
        } else {
            safe_reflink(src, dest, log)
        }
    };

    // Restore the original metadata of the deduplicated file:
    if let Err(e) = restore_some_metadata(&dest.path.to_path_buf(), &dest.metadata) {
        log.warn(format!("Failed keep metadata for {}: {}", dest, e))
    }

    // Restore the original metadata of the deduplicated files's parent directory:
    if let Some(parent) = dest_parent {
        if let Some(metadata) = dest_parent_metadata {
            let result = metadata
                .and_then(|metadata| restore_some_metadata(&parent.to_path_buf(), &metadata));
            if let Err(e) = result {
                log.warn(format!(
                    "Failed keep metadata for {}: {}",
                    parent.display(),
                    e
                ))
            }
        }
    }

    result
}

// Dummy function so tests compile
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn linux_reflink(_target: &FileMetadata, _link: &FileMetadata, _log: &Log) -> io::Result<()> {
    unreachable!()
}

// First reflink (not move) the target file out of the way (this also checks for
// reflink support), then overwrite the existing file to preserve metadata.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_reflink(src: &FileMetadata, dest: &FileMetadata, log: &Log) -> io::Result<()> {
    let tmp = FsCommand::temp_file(&dest.path);
    let std_tmp = tmp.to_path_buf();

    let fs_target = src.path.to_path_buf();
    let std_link = dest.path.to_path_buf();

    let remove_temporary = |temporary| {
        if let Err(e) = FsCommand::remove(&temporary) {
            log.warn(format!(
                "Failed to remove temporary {}: {}",
                temporary.display(),
                e
            ))
        }
    };

    // Backup via reflink, if this fails then the fs does not support reflinking.
    if let Err(e) = reflink_overwrite(&std_link, &std_tmp) {
        remove_temporary(tmp);
        return Err(e);
    }

    match reflink_overwrite(&fs_target, &std_link) {
        Err(e) => {
            if let Err(remove_err) = FsCommand::unsafe_rename(&tmp, &dest.path) {
                log.warn(format!(
                    "Failed to undo deduplication from {} to {}: {}",
                    &dest,
                    tmp.display(),
                    remove_err
                ))
            }
            Err(e)
        }
        Ok(ok) => {
            remove_temporary(tmp);
            Ok(ok)
        }
    }
}

/// Reflink `target` to `link` and expect these two files to be equally sized.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn reflink_overwrite(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
    use nix::request_code_write;
    use std::os::unix::prelude::AsRawFd;

    let src = std::fs::File::open(&target)?;

    // This operation does not require `.truncate(true)` because the files are already of the same size.
    let dest = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&link)?;

    // From /usr/include/linux/fs.h:
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
        if code == libc::EOPNOTSUPP { // 95
             // Filesystem does not supported reflinks.
             // No cleanup required, file is left untouched.
        } else if code == libc::EINVAL { // 22
             // Source filesize was larger than destination.
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

// Reflink which expects the destination to not exist.
fn copy_by_reflink(src: &FcPath, dest: &FcPath) -> io::Result<()> {
    reflink::reflink(&src.to_path_buf(), &dest.to_path_buf()).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "Failed to deduplicate {} -> {}: {}",
                dest.display(),
                src.display(),
                e
            ),
        )
    })
}

// Create a reflink by removing the file and making a reflink copy of the original.
fn safe_reflink(src: &FileMetadata, dest: &FileMetadata, log: &Log) -> io::Result<()> {
    FsCommand::safe_remove(&dest.path, |link| copy_by_reflink(&src.path, link), log)?;
    Ok(())
}

#[cfg(not(test))]
pub mod test {
    pub mod cfg {
        pub const fn crosstest() -> bool {
            false
        }
    }
}

#[cfg(test)]
pub mod test {
    pub mod cfg {
        // Helpers to switch reflink implementations when running tests
        // and to ensure only one reflink test runs at a time.

        use std::sync::Mutex;
        use std::sync::MutexGuard;

        use lazy_static::lazy_static;

        lazy_static! {
            pub static ref CROSSTEST: Mutex<bool> = Mutex::new(false);
            pub static ref SEQUENTIAL_REFLINK_TESTS: Mutex<()> = Mutex::default();
        }

        pub struct CrossTest<'a>(MutexGuard<'a, ()>);
        impl<'a> CrossTest<'a> {
            pub fn new(crosstest: bool) -> CrossTest<'a> {
                let x = CrossTest(SEQUENTIAL_REFLINK_TESTS.lock().unwrap());
                *CROSSTEST.lock().unwrap() = crosstest;
                x
            }
        }

        impl<'a> Drop for CrossTest<'a> {
            fn drop(&mut self) {
                *CROSSTEST.lock().unwrap() = false;
            }
        }

        pub fn crosstest() -> bool {
            *CROSSTEST.lock().unwrap()
        }
    }

    use std::sync::Arc;

    use crate::util::test::{cached_reflink_supported, read_file, with_dir, write_file};

    use super::*;

    // Usually /dev/shm only exists on Linux.
    #[cfg(any(target_os = "linux"))]
    fn test_reflink_command_fails_on_dev_shm_tmpfs() {
        // No `cached_reflink_supported()` check

        if !std::path::Path::new("/dev/shm").is_dir() {
            println!("  Notice: strange Linux without /dev/shm, can't test reflink failure");
            return;
        }

        let test_root = "/dev/shm/tmp.fclones.reflink.testfailure";

        // Usually /dev/shm is mounted as a tmpfs which does not support reflinking, so test there.
        with_dir(&test_root, |root| {
            // Always clean up files in /dev/shm, even after failure
            struct CleanupGuard<'a>(&'a str);
            impl<'a> Drop for CleanupGuard<'a> {
                fn drop(&mut self) {
                    std::fs::remove_dir_all(&self.0).unwrap();
                }
            }
            let _guard = CleanupGuard(&test_root);

            let log = Log::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "foo");

            let file_1 = FileMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = FileMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };

            assert!(
                cmd.execute(&log)
                    .unwrap_err()
                    .to_string()
                    .starts_with("Failed to deduplicate"),
                "Reflink did not fail on /dev/shm (tmpfs), or this mount now supports reflinking"
            );

            assert!(file_path_2.exists());
            assert_eq!(read_file(&file_path_2), "foo");
        })
    }

    #[test]
    #[cfg(any(target_os = "linux"))]
    fn test_reflink_command_failure() {
        {
            let _sequential = cfg::CrossTest::new(false);
            test_reflink_command_fails_on_dev_shm_tmpfs();
        }
        {
            let _sequential = cfg::CrossTest::new(true);
            test_reflink_command_fails_on_dev_shm_tmpfs();
        }
    }

    fn test_reflink_command_with_file_too_large(via_ioctl: bool) {
        if !cached_reflink_supported() {
            return;
        }

        with_dir("dedupe/reflink_too_large", |root| {
            let log = Log::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "too large");

            let file_1 = FileMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = FileMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };

            if via_ioctl {
                assert!(cmd
                    .execute(&log)
                    .unwrap_err()
                    .to_string()
                    .starts_with("Failed to deduplicate"));

                assert!(file_path_1.exists());
                assert!(file_path_2.exists());
                assert_eq!(read_file(&file_path_1), "foo");
                assert_eq!(read_file(&file_path_2), "too large");
            } else {
                cmd.execute(&log).unwrap();

                assert!(file_path_2.exists());
                assert_eq!(read_file(&file_path_2), "foo");
            }
        })
    }

    #[test]
    fn test_reflink_command_works_with_files_too_large_anyos() {
        let _sequential = cfg::CrossTest::new(true);
        test_reflink_command_with_file_too_large(false);
    }

    // This tests the reflink code path (using the reflink crate) usually not used on Linux.
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_command_fails_with_files_too_large_using_ioctl_linux() {
        let _sequential = cfg::CrossTest::new(false);
        test_reflink_command_with_file_too_large(true);
    }

    fn test_reflink_command_fills_file_with_content() {
        if !cached_reflink_supported() {
            return;
        }
        with_dir("dedupe/reflink_test", |root| {
            let log = Log::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "f");

            let file_1 = FileMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = FileMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };
            cmd.execute(&log).unwrap();

            assert!(file_path_1.exists());
            assert!(file_path_2.exists());
            assert_eq!(read_file(&file_path_2), "foo");
        })
    }

    #[test]
    fn test_reflink_command_fills_file_with_content_anyos() {
        let _sequential = cfg::CrossTest::new(false);
        test_reflink_command_fills_file_with_content();
    }

    // This tests the reflink code path (using the reflink crate) usually not used on Linux.
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_command_fills_file_with_content_not_ioctl_linux() {
        let _sequential = cfg::CrossTest::new(true);
        test_reflink_command_fills_file_with_content();
    }
}
