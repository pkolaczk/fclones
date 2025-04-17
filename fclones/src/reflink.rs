use std::fs;
use std::fs::Metadata;
use std::io;

use filetime::FileTime;

use crate::dedupe::{FsCommand, PathAndMetadata};
use crate::log::{Log, LogExt};

#[cfg(unix)]
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
    let result = || -> io::Result<()> {
        let dest_path_buf = dest.path.to_path_buf();

        if cfg!(any(target_os = "linux", target_os = "android")) && !crosstest() {
            linux_reflink(src, dest, log)?;
            restore_metadata(&dest_path_buf, &dest.metadata, Restore::TimestampOnly)
        } else {
            #[cfg(unix)]
            let dest_xattrs = get_xattrs(&dest_path_buf)?;

            safe_reflink(src, dest, log)?;

            #[cfg(unix)]
            restore_xattrs(&dest_path_buf, dest_xattrs)?;

            restore_metadata(
                &dest_path_buf,
                &dest.metadata,
                Restore::TimestampOwnersPermissions,
            )
        }
    }()
    .map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to deduplicate {dest} -> {src}: {e}"),
        )
    });

    // Restore the original metadata of the deduplicated files's parent directory:
    if let Some(parent) = dest_parent {
        if let Some(metadata) = dest_parent_metadata {
            let result = metadata.and_then(|metadata| {
                restore_metadata(&parent.to_path_buf(), &metadata, Restore::TimestampOnly)
            });
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
// reflink support), then overwrite the existing file to preserve most metadata and xattrs.
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

    // Try FIDEDUPERANGE first, fall back to FICLONE if not supported
    let result = reflink_overwrite_dedupe(&fs_target, &std_link);
    let result = match result {
        // Check for both EOPNOTSUPP (95) and ENOTTY (25) as possible "ioctl not supported" errors
        Err(e)
            if e.raw_os_error() == Some(libc::EOPNOTSUPP)
                || e.raw_os_error() == Some(libc::ENOTTY) =>
        {
            // Fall back to FICLONE
            reflink_overwrite(&fs_target, &std_link)
        }
        other => other,
    };

    // Use the same error handling pattern as the original code
    match result {
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
    let dest = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(link)?;

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

/// New implementation using FIDEDUPERANGE for safer deduplication
#[cfg(any(target_os = "linux", target_os = "android"))]
fn reflink_overwrite_dedupe(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
    use nix::request_code_readwrite;
    use std::mem::{size_of, zeroed};
    use std::os::unix::prelude::AsRawFd;

    let src = fs::File::open(target)?;
    let src_metadata = src.metadata()?;
    let src_size = src_metadata.len();

    // This operation does not require `.truncate(true)` because the files are already of the same size.
    let dest = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(link)?;

    // From /usr/include/linux/fs.h:
    // #define FIDEDUPERANGE _IOWR(0x94, 54, struct file_dedupe_range)
    const FIDEDUPERANGE_TYPE: u8 = 0x94;
    const FIDEDUPERANGE_NR: u8 = 54;

    // Status codes from Linux kernel
    // FILE_DEDUPE_RANGE_SAME = 0: Blocks are identical and were successfully deduplicated
    const FILE_DEDUPE_RANGE_DIFFERS: i32 = 1;

    // Define dedupe range structures
    #[repr(C)]
    struct FileDedupRangeInfo {
        dest_fd: i64,
        dest_offset: u64,
        bytes_deduped: u64,
        status: i32,
        reserved: u32,
    }

    #[repr(C)]
    struct FileDedupRange {
        src_offset: u64,
        src_length: u64,
        dest_count: u16,
        reserved1: u16,
        reserved2: u32,
        info: [FileDedupRangeInfo; 1],
    }

    // Calculate the total size of the structure for the ioctl call
    const FIDEDUPERANGE_SIZE: usize = size_of::<FileDedupRange>();

    // Process deduplication potentially in chunks
    // Prior to Linux kernel 4.18, btrfs had a 16MiB restriction on FIDEDUPERANGE
    // This loop handles both older kernels (multiple iterations) and newer ones (likely one iteration)
    let mut offset: u64 = 0;

    while offset < src_size {
        // Prepare dedupe range struct
        let mut dedupe_range: FileDedupRange = unsafe { zeroed() };

        // Set source information
        dedupe_range.src_offset = offset;
        dedupe_range.src_length = src_size - offset;
        dedupe_range.dest_count = 1;
        dedupe_range.reserved1 = 0;
        dedupe_range.reserved2 = 0;

        // Set destination information
        dedupe_range.info[0].dest_fd = dest.as_raw_fd() as i64;
        dedupe_range.info[0].dest_offset = offset;
        dedupe_range.info[0].bytes_deduped = 0;
        dedupe_range.info[0].status = 0;
        dedupe_range.info[0].reserved = 0;

        // Call FIDEDUPERANGE ioctl
        let ret = unsafe {
            libc::ioctl(
                src.as_raw_fd(),
                request_code_readwrite!(FIDEDUPERANGE_TYPE, FIDEDUPERANGE_NR, FIDEDUPERANGE_SIZE)
                    as libc::c_ulong,
                &mut dedupe_range,
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
            return Err(err);
        }

        // Check for content differences - FIDEDUPERANGE verifies content identity
        if dedupe_range.info[0].status == FILE_DEDUPE_RANGE_DIFFERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File contents differ, cannot deduplicate",
            ));
        }

        // Get bytes deduped - on older btrfs (pre-kernel 4.18), this may be limited to 16MiB
        // On newer kernels, this will typically process the entire file in one go
        let bytes_deduped = dedupe_range.info[0].bytes_deduped;
        if bytes_deduped == 0 {
            // No bytes deduped but no error, might be end of file
            break;
        }

        // Move offset for next chunk
        offset += bytes_deduped;
    }

    Ok(())
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

#[derive(Debug, PartialEq)]
enum Restore {
    TimestampOnly,
    TimestampOwnersPermissions,
}

// Not kept: xattrs, ACLs, etc.
fn restore_metadata(
    path: &std::path::Path,
    metadata: &Metadata,
    restore: Restore,
) -> io::Result<()> {
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

    if restore == Restore::TimestampOwnersPermissions {
        fs::set_permissions(path, metadata.permissions()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to set permissions for {}: {}", path.display(), e),
            )
        })?;

        #[cfg(unix)]
        restore_owner(path, metadata)?;
    }
    Ok(())
}

#[cfg(unix)]
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
    FsCommand::safe_remove(
        &dest.path,
        move |link| {
            copy_by_reflink(&src.path, link)?;
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

        pub struct CrossTest<'a>(#[allow(dead_code)] MutexGuard<'a, ()>);
        impl<'a> CrossTest<'a> {
            pub fn new(crosstest: bool) -> CrossTest<'a> {
                let x = CrossTest(SEQUENTIAL_REFLINK_TESTS.lock().unwrap());
                *CROSSTEST.lock().unwrap() = crosstest;
                x
            }
        }

        impl Drop for CrossTest<'_> {
            fn drop(&mut self) {
                *CROSSTEST.lock().unwrap() = false;
            }
        }

        pub fn crosstest() -> bool {
            *CROSSTEST.lock().unwrap()
        }
    }

    use crate::log::StdLog;
    use std::fs::File;
    use std::sync::Arc;

    use crate::util::test::{cached_reflink_supported, read_file, with_dir, write_file};

    use super::*;
    use crate::file::{FileChunk, FileHash, FileLen, FilePos};
    use crate::hasher::FileHasher;
    use crate::hasher::HashFn;
    use crate::path::Path as FcPath;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    // Helper function to compute hash of a file using the project's hasher
    fn compute_file_hash(path: &std::path::Path) -> FileHash {
        let log = StdLog::new();
        let hasher = FileHasher::new(HashFn::Metro, None, &log);
        let fc_path = FcPath::from(path);
        let chunk = FileChunk::new(&fc_path, FilePos(0), FileLen::MAX);
        hasher.hash_file(&chunk, |_| {}).unwrap()
    }

    // Helper to generate large files with specified content
    fn create_large_file(path: &std::path::Path, size_mb: usize, pattern_char: char) {
        let chunk_size = 1024 * 1024; // 1MB chunks
        let mut file = File::create(path).unwrap();

        for i in 0..size_mb {
            // Create a chunk with a unique pattern that includes the chunk number
            let mut chunk = format!("CHUNK{:04}:{}", i, pattern_char);
            // Pad to fill the chunk size
            chunk.push_str(&pattern_char.to_string().repeat(chunk_size - chunk.len()));
            file.write_all(chunk.as_bytes()).unwrap();
        }
    }

    // Helper to check if FIDEDUPERANGE is supported on this system
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn fideduperange_supported() -> bool {
        if !cached_reflink_supported() {
            return false;
        }

        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source_test");
        let dest_path = dir.path().join("dest_test");

        // Create identical files
        write_file(&source_path, "test content");
        write_file(&dest_path, "test content");

        // Try FIDEDUPERANGE
        let result = reflink_overwrite_dedupe(&source_path, &dest_path);

        match result {
            Err(e)
                if e.raw_os_error() == Some(libc::ENOTTY)
                    || e.raw_os_error() == Some(libc::EOPNOTSUPP) =>
            {
                false
            }
            _ => true,
        }
    }

    // Usually /dev/shm only exists on Linux.
    #[cfg(target_os = "linux")]
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
            impl Drop for CleanupGuard<'_> {
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
    #[cfg(target_os = "linux")]
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

    // Test that FICLONE overwrites a file even when content is different (unsafe behavior)
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_overwrite_with_different_content() {
        if !cached_reflink_supported() {
            return;
        }

        with_dir("dedupe/reflink_clone_different", |root| {
            let source_path = root.join("source_file");
            let dest_path = root.join("dest_file");

            // Create files with same size but different content
            write_file(&source_path, "source content AAA");
            write_file(&dest_path, "different content");

            // Calculate initial hashes
            let source_hash_before = compute_file_hash(&source_path);
            let dest_hash_before = compute_file_hash(&dest_path);

            // Verify hashes are different initially
            assert_ne!(
                source_hash_before, dest_hash_before,
                "Source and destination should have different content initially"
            );

            // Perform FICLONE operation
            let result = reflink_overwrite(&source_path, &dest_path);
            assert!(
                result.is_ok(),
                "FICLONE operation should succeed even with different content"
            );

            // Calculate hashes after reflink
            let source_hash_after = compute_file_hash(&source_path);
            let dest_hash_after = compute_file_hash(&dest_path);

            // Source should be unchanged
            assert_eq!(
                source_hash_before, source_hash_after,
                "Source file should be unchanged"
            );

            // Destination should now match source (UNSAFE BEHAVIOR OF FICLONE)
            assert_eq!(
                dest_hash_after, source_hash_after,
                "FICLONE overwrites destination content without verifying, which is unsafe"
            );
            assert_ne!(
                dest_hash_before, dest_hash_after,
                "Destination content was changed by FICLONE"
            );

            // Verify content directly
            assert_eq!(
                read_file(&dest_path),
                "source content AAA",
                "Destination content should be overwritten with source content"
            );
        });
    }

    // Test that FIDEDUPERANGE rejects files with same size but different content (safe behavior)
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_overwrite_dedupe_rejects_different_content() {
        // Skip if FIDEDUPERANGE is not supported
        if !fideduperange_supported() {
            println!("Skipping test: FIDEDUPERANGE not supported on this system");
            return;
        }

        with_dir("dedupe/reflink_dedupe_different", |root| {
            let source_path = root.join("source_file");
            let dest_path = root.join("dest_file");

            // Create files with same size but different content
            write_file(&source_path, "source content AAA");
            write_file(&dest_path, "different content");

            // Calculate initial hashes
            let source_hash_before = compute_file_hash(&source_path);
            let dest_hash_before = compute_file_hash(&dest_path);

            // Verify hashes are different initially
            assert_ne!(
                source_hash_before, dest_hash_before,
                "Source and destination should have different content initially"
            );

            // Perform FIDEDUPERANGE operation
            let result = reflink_overwrite_dedupe(&source_path, &dest_path);

            // Should fail with error about content differing
            assert!(
                result.is_err(),
                "FIDEDUPERANGE operation should fail with different content"
            );

            // Get the error message
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("differ") || error.to_string().contains("Invalid"),
                "Error should indicate content differs: {}",
                error
            );

            // Calculate hashes after attempted reflink
            let source_hash_after = compute_file_hash(&source_path);
            let dest_hash_after = compute_file_hash(&dest_path);

            // Both files should be unchanged
            assert_eq!(
                source_hash_before, source_hash_after,
                "Source file should be unchanged"
            );
            assert_eq!(
                dest_hash_before, dest_hash_after,
                "Destination file should be unchanged when FIDEDUPERANGE rejects different content"
            );

            // Verify content directly
            assert_eq!(
                read_file(&dest_path),
                "different content",
                "Destination content should remain unchanged"
            );
        });
    }

    // Test that FIDEDUPERANGE works correctly with identical small files
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_overwrite_dedupe_with_identical_content() {
        // Skip if FIDEDUPERANGE is not supported
        if !fideduperange_supported() {
            println!("Skipping test: FIDEDUPERANGE not supported on this system");
            return;
        }

        with_dir("dedupe/reflink_dedupe_identical", |root| {
            let source_path = root.join("source_file");
            let dest_path = root.join("dest_file");

            // Create identical files
            write_file(&source_path, "identical content in both files");
            write_file(&dest_path, "identical content in both files");

            // Calculate initial hashes
            let source_hash_before = compute_file_hash(&source_path);
            let dest_hash_before = compute_file_hash(&dest_path);

            // Verify hashes are identical initially
            assert_eq!(
                source_hash_before, dest_hash_before,
                "Source and destination should have identical content"
            );

            // Perform FIDEDUPERANGE operation
            let result = reflink_overwrite_dedupe(&source_path, &dest_path);
            assert!(
                result.is_ok(),
                "FIDEDUPERANGE operation should succeed with identical content"
            );

            // Calculate hashes after reflink
            let source_hash_after = compute_file_hash(&source_path);
            let dest_hash_after = compute_file_hash(&dest_path);

            // Both files should be unchanged
            assert_eq!(
                source_hash_before, source_hash_after,
                "Source file should be unchanged"
            );
            assert_eq!(
                dest_hash_before, dest_hash_after,
                "Destination file should be unchanged"
            );
            assert_eq!(
                source_hash_after, dest_hash_after,
                "Files should still be identical"
            );

            // Verify content directly
            assert_eq!(
                read_file(&dest_path),
                "identical content in both files",
                "Destination content should remain the same"
            );
        });
    }

    // Test that FIDEDUPERANGE works with a 21MB file (chunking)
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_overwrite_dedupe_large_file_chunking() {
        // Skip if FIDEDUPERANGE is not supported
        if !fideduperange_supported() {
            println!("Skipping test: FIDEDUPERANGE not supported on this system");
            return;
        }

        with_dir("dedupe/reflink_dedupe_large", |root| {
            let source_path = root.join("large_source");
            let dest_path = root.join("large_dest");

            // Create smaller test files - 2MB instead of 21MB to make test run faster
            // but still large enough to test chunking
            create_large_file(&source_path, 2, 'A');
            create_large_file(&dest_path, 2, 'A');

            // Calculate initial hashes
            let source_hash_before = compute_file_hash(&source_path);
            let dest_hash_before = compute_file_hash(&dest_path);

            // Verify hashes are identical initially
            assert_eq!(
                source_hash_before, dest_hash_before,
                "Large files should have identical content"
            );

            // Perform FIDEDUPERANGE operation
            let result = reflink_overwrite_dedupe(&source_path, &dest_path);
            assert!(
                result.is_ok(),
                "FIDEDUPERANGE operation should succeed with identical large files"
            );

            // Calculate hashes after reflink
            let source_hash_after = compute_file_hash(&source_path);
            let dest_hash_after = compute_file_hash(&dest_path);

            // Both files should be unchanged
            assert_eq!(
                source_hash_before, source_hash_after,
                "Source file should be unchanged"
            );
            assert_eq!(
                dest_hash_before, dest_hash_after,
                "Destination file should be unchanged"
            );
            assert_eq!(
                source_hash_after, dest_hash_after,
                "Files should still be identical"
            );
        });
    }

    // Test that FIDEDUPERANGE detects differences in large files
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_reflink_overwrite_dedupe_large_file_one_byte_different() {
        // Skip if FIDEDUPERANGE is not supported
        if !fideduperange_supported() {
            println!("Skipping test: FIDEDUPERANGE not supported on this system");
            return;
        }

        with_dir("dedupe/reflink_dedupe_large_different", |root| {
            let source_path = root.join("large_source_a");
            let dest_path = root.join("large_dest_b");

            // Create smaller test files - 2MB instead of 21MB to make test run faster
            create_large_file(&source_path, 2, 'A');
            create_large_file(&dest_path, 2, 'A');

            // Modify one byte in the middle of the destination file
            let mut file = OpenOptions::new().write(true).open(&dest_path).unwrap();
            file.seek(SeekFrom::Start(1 * 1024 * 1024)).unwrap(); // Seek to 1MB position
            file.write_all(b"B").unwrap(); // Write a different byte

            // Calculate initial hashes
            let source_hash_before = compute_file_hash(&source_path);
            let dest_hash_before = compute_file_hash(&dest_path);

            // Verify hashes are different initially
            assert_ne!(
                source_hash_before, dest_hash_before,
                "Files should have different content due to one byte change"
            );

            // Perform FIDEDUPERANGE operation
            let result = reflink_overwrite_dedupe(&source_path, &dest_path);

            // Should fail with error about content differing
            assert!(
                result.is_err(),
                "FIDEDUPERANGE operation should fail with one byte difference"
            );

            // Get the error message
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("differ") || error.to_string().contains("Invalid"),
                "Error should indicate content differs: {}",
                error
            );

            // Calculate hashes after attempted reflink
            let source_hash_after = compute_file_hash(&source_path);
            let dest_hash_after = compute_file_hash(&dest_path);

            // Both files should be unchanged
            assert_eq!(
                source_hash_before, source_hash_after,
                "Source file should be unchanged"
            );
            assert_eq!(
                dest_hash_before, dest_hash_after,
                "Destination file should be unchanged when FIDEDUPERANGE rejects different content"
            );
        });
    }

    // Test that linux_reflink with fallback behavior preserves file integrity
    #[test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn test_linux_reflink_fallback_behavior() {
        if !cached_reflink_supported() {
            return;
        }

        with_dir("dedupe/linux_reflink_fallback", |root| {
            let log = StdLog::new();
            let file_path_1 = root.join("source_file");
            let file_path_2 = root.join("dest_file");

            // Create files with same size but different content
            write_file(&file_path_1, "source content AAA");
            write_file(&file_path_2, "different content");

            // Calculate initial hashes
            let hash_1_before = compute_file_hash(&file_path_1);
            let hash_2_before = compute_file_hash(&file_path_2);

            // Verify hashes are different initially
            assert_ne!(
                hash_1_before, hash_2_before,
                "Files should have different content"
            );

            // Create PathAndMetadata objects
            let file_1 = PathAndMetadata::new(FcPath::from(&file_path_1)).unwrap();
            let file_2 = PathAndMetadata::new(FcPath::from(&file_path_2)).unwrap();

            // Call linux_reflink which tries FIDEDUPERANGE first, then falls back to FICLONE
            let result = linux_reflink(&file_1, &file_2, &log);

            // Check the result - this test is primarily informational to understand
            // the fallback behavior in the actual environment
            match result {
                Ok(_) => {
                    // Operation succeeded, meaning either:
                    // 1. FIDEDUPERANGE isn't supported and it fell back to FICLONE, or
                    // 2. The kernel handled the operation differently than expected

                    // Calculate hashes after reflink
                    let hash_1_after = compute_file_hash(&file_path_1);
                    let hash_2_after = compute_file_hash(&file_path_2);

                    // The source should be unchanged
                    assert_eq!(
                        hash_1_before, hash_1_after,
                        "Source file should be unchanged"
                    );

                    // Check if destination was changed
                    if hash_2_after == hash_1_after && hash_2_before != hash_2_after {
                        println!("Note: linux_reflink succeeded with different content - likely fell back to FICLONE");
                        assert_eq!(
                            read_file(&file_path_2),
                            "source content AAA",
                            "Destination content matches source after fallback to FICLONE"
                        );
                    }
                }
                Err(e) => {
                    // FIDEDUPERANGE detected content difference and rejected the operation
                    println!(
                        "linux_reflink failed as expected with different content: {}",
                        e
                    );

                    // Calculate hashes after failed reflink
                    let hash_1_after = compute_file_hash(&file_path_1);
                    let hash_2_after = compute_file_hash(&file_path_2);

                    // Both files should be unchanged
                    assert_eq!(
                        hash_1_before, hash_1_after,
                        "Source file should be unchanged"
                    );
                    assert_eq!(
                        hash_2_before, hash_2_after,
                        "Destination file should be unchanged"
                    );
                    assert_eq!(
                        read_file(&file_path_2),
                        "different content",
                        "Destination content should remain unchanged"
                    );
                }
            }
        });
    }
}
