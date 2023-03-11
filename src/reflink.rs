use std::fs;
use std::fs::Metadata;
use std::io;

use filetime::FileTime;

use crate::dedupe::{FsCommand, PathAndMetadata};
use crate::log::{Log, LogExt};

#[cfg(unix)]
#[cfg(any(not(any(target_os = "linux", target_os = "android")), test))]
struct XAttr {
    name: std::ffi::OsString,
    value: Option<Vec<u8>>,
}

/// Calls OS-specific reflink implementations with an option to call the more generic
/// one during testing one on Linux ("crosstesting").
/// The destination file is allowed to exist.
pub fn reflink(src: &PathAndMetadata, dest: &PathAndMetadata, log: &dyn Log) -> io::Result<()> {
    // Remember original metadata of the parent directory:
    let dest_parent = dest.path.parent();
    let dest_parent_metadata = dest_parent.map(|p| p.to_path_buf().metadata());

    // Call reflink:
    let result = {
        if cfg!(any(target_os = "linux", target_os = "android")) && !crosstest() {
            linux_reflink(src, dest, log)
        } else {
            safe_reflink(src, dest, log)
        }
    }
    .map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to deduplicate {dest} -> {src}: {e}"),
        )
    });

    // Restore the original metadata of the deduplicated files's parent directory:
    if let Some(parent) = dest_parent {
        if let Some(metadata) = dest_parent_metadata {
            let result =
                metadata.and_then(|metadata| restore_metadata(&parent.to_path_buf(), &metadata));
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
fn linux_reflink(
    _target: &PathAndMetadata,
    _link: &PathAndMetadata,
    _log: &dyn Log,
) -> io::Result<()> {
    unreachable!()
}

// First reflink (not move) the target file out of the way (this also checks for
// reflink support), then overwrite the existing file to preserve metadata.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_reflink(src: &PathAndMetadata, dest: &PathAndMetadata, log: &dyn Log) -> io::Result<()> {
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

    let src = fs::File::open(target)?;

    // This operation does not require `.truncate(true)` because the files are already of the same size.
    let dest = fs::OpenOptions::new().create(true).write(true).open(link)?;

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

/// Restores file owner and group
#[cfg(unix)]
fn restore_owner(path: &std::path::Path, metadata: &Metadata) -> io::Result<()> {
    use file_owner::PathExt;
    use std::os::unix::fs::MetadataExt;

    let uid = metadata.uid();
    let gid = metadata.gid();
    path.set_group(gid).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to set file group of {}: {}", path.display(), e),
        )
    })?;
    path.set_owner(uid).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to set file owner of {}: {}", path.display(), e),
        )
    })?;
    Ok(())
}

// Not kept: xattrs, ACLs, etc.
fn restore_metadata(path: &std::path::Path, metadata: &Metadata) -> io::Result<()> {
    let atime = FileTime::from_last_access_time(metadata);
    let mtime = FileTime::from_last_modification_time(metadata);

    filetime::set_file_times(path, atime, mtime).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "Failed to set access and modification times for {}: {}",
                path.display(),
                e
            ),
        )
    })?;

    fs::set_permissions(path, metadata.permissions()).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to set permissions for {}: {}", path.display(), e),
        )
    })?;

    #[cfg(unix)]
    restore_owner(path, metadata)?;
    Ok(())
}

#[cfg(unix)]
#[cfg(any(not(any(target_os = "linux", target_os = "android")), test))]
fn get_xattrs(path: &std::path::Path) -> io::Result<Vec<XAttr>> {
    use itertools::Itertools;
    use xattr::FileExt;

    let file = fs::File::open(path)?;
    file.list_xattr()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to list extended attributes of {}: {}",
                    path.display(),
                    e
                ),
            )
        })?
        .map(|name| {
            Ok(XAttr {
                value: file.get_xattr(name.as_os_str()).map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "Failed to read extended attribute {} of {}: {}",
                            name.to_string_lossy(),
                            path.display(),
                            e
                        ),
                    )
                })?,
                name,
            })
        })
        .try_collect()
}

#[cfg(unix)]
#[cfg(any(not(any(target_os = "linux", target_os = "android")), test))]
fn restore_xattrs(path: &std::path::Path, xattrs: Vec<XAttr>) -> io::Result<()> {
    use xattr::FileExt;
    let file = fs::File::open(path)?;
    for name in file.list_xattr()? {
        file.remove_xattr(&name).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to clear extended attribute {} of {}: {}",
                    name.to_string_lossy(),
                    path.display(),
                    e
                ),
            )
        })?;
    }
    for attr in xattrs {
        if let Some(value) = attr.value {
            file.set_xattr(&attr.name, &value).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to set extended attribute {} of {}: {}",
                        attr.name.to_string_lossy(),
                        path.display(),
                        e
                    ),
                )
            })?;
        }
    }
    Ok(())
}

// Reflink which expects the destination to not exist.
#[cfg(any(not(any(target_os = "linux", target_os = "android")), test))]
fn copy_by_reflink(src: &crate::path::Path, dest: &crate::path::Path) -> io::Result<()> {
    reflink::reflink(src.to_path_buf(), dest.to_path_buf())
        .map_err(|e| io::Error::new(e.kind(), format!("Failed to reflink: {e}")))
}

// Create a reflink by removing the file and making a reflink copy of the original.
// After successful copy, attempts to restore the metadata of the file.
// If reflink or metadata restoration fails, moves the original file back to its original place.
#[cfg(any(not(any(target_os = "linux", target_os = "android")), test))]
fn safe_reflink(src: &PathAndMetadata, dest: &PathAndMetadata, log: &dyn Log) -> io::Result<()> {
    let dest_path_buf = dest.path.to_path_buf();
    #[cfg(unix)]
    let dest_xattrs = get_xattrs(&dest_path_buf)?;

    FsCommand::safe_remove(
        &dest.path,
        move |link| {
            copy_by_reflink(&src.path, link)?;

            #[cfg(unix)]
            restore_xattrs(&dest_path_buf, dest_xattrs)?;
            restore_metadata(&dest_path_buf, &dest.metadata)?;
            Ok(())
        },
        log,
    )
}

// Dummy function so non-test cfg compiles
#[cfg(not(any(not(any(target_os = "linux", target_os = "android")), test)))]
fn safe_reflink(_src: &PathAndMetadata, _dest: &PathAndMetadata, _log: &dyn Log) -> io::Result<()> {
    unreachable!()
}

#[cfg(not(test))]
pub const fn crosstest() -> bool {
    false
}

#[cfg(test)]
pub fn crosstest() -> bool {
    test::cfg::crosstest()
}

#[cfg(test)]
pub mod test {

    pub mod cfg {
        // Helpers to switch reflink implementations when running tests
        // and to ensure only one reflink test runs at a time.

        use std::sync::{Mutex, MutexGuard};

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

    use crate::log::StdLog;
    use std::sync::Arc;

    use crate::util::test::{cached_reflink_supported, read_file, with_dir, write_file};

    use super::*;
    use crate::path::Path as FcPath;

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
        with_dir(test_root, |root| {
            // Always clean up files in /dev/shm, even after failure
            struct CleanupGuard<'a>(&'a str);
            impl<'a> Drop for CleanupGuard<'a> {
                fn drop(&mut self) {
                    fs::remove_dir_all(self.0).unwrap();
                }
            }
            let _guard = CleanupGuard(test_root);

            let log = StdLog::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "foo");

            let file_1 = PathAndMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = PathAndMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };

            assert!(
                cmd.execute(true, &log)
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
            let log = StdLog::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "too large");

            let file_1 = PathAndMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = PathAndMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };

            if via_ioctl {
                assert!(cmd
                    .execute(true, &log)
                    .unwrap_err()
                    .to_string()
                    .starts_with("Failed to deduplicate"));

                assert!(file_path_1.exists());
                assert!(file_path_2.exists());
                assert_eq!(read_file(&file_path_1), "foo");
                assert_eq!(read_file(&file_path_2), "too large");
            } else {
                cmd.execute(true, &log).unwrap();

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
            let log = StdLog::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "f");

            let file_1 = PathAndMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = PathAndMetadata::new(FcPath::from(&file_path_2)).unwrap();
            let cmd = FsCommand::RefLink {
                target: Arc::new(file_1),
                link: file_2,
            };
            cmd.execute(true, &log).unwrap();

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
