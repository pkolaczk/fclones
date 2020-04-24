use std::cmp::min;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;
use std::thread;

use fasthash::{city::crc::Hasher128, FastHasher, HasherExt};
use jwalk::{Parallelism, WalkDir};
use rayon::ThreadPoolBuilder;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;

/// Return file size in bytes.
/// If file metadata cannot be accessed, print the error to stderr and return `None`.
pub fn file_len(file: &PathBuf) -> Option<u64> {
    match std::fs::metadata(file) {
        Ok(metadata) =>
            Some(metadata.len()),
        Err(e) => {
            eprintln!("Failed to read size of {}: {}", file.display(), e);
            None
        }
    }
}

/// Compute hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
///
/// # Example
/// ```
/// use dff::files::file_hash;
/// use std::path::PathBuf;
/// use std::fs::{File, create_dir_all};
/// use std::io::Write;
///
/// let test_root = PathBuf::from("target/test/file_hash/");
/// create_dir_all(&test_root).unwrap();
/// let file1 = test_root.join("file1");
/// File::create(&file1).unwrap().write_all(b"Test file 1");
/// let file2 = test_root.join("file2");
/// File::create(&file2).unwrap().write_all(b"Test file 2");
///
/// let hash1 = file_hash(&file1, std::u64::MAX).unwrap();
/// let hash2 = file_hash(&file2, std::u64::MAX).unwrap();
/// let hash3 = file_hash(&file2, 8).unwrap();
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash2, hash3);
/// ```
pub fn file_hash(path: &PathBuf, len: u64) -> Option<u128> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Failed to open file {}: {}", path.display(), e);
            return None;
        }
    };
    let mut count: u64 = 0;
    let mut reader = BufReader::with_capacity(4096, file);
    let mut hasher = Hasher128::new();
    while count < len {
        match reader.fill_buf() {
            Ok(&[]) => break,
            Ok(buf) => {
                let to_read = len - count;
                let length = buf.len() as u64;
                let actual_read = min(length, to_read) as usize;
                count += actual_read as u64;
                hasher.write(&buf[0..actual_read]);
                reader.consume(actual_read);
            },
            Err(e) => {
                eprintln!("Error reading file {}: {}", path.display(), e);
                return None;
            }
        }
    }
    Some(hasher.finish_ext())
}

#[derive(Copy, Clone)]
pub struct WalkOpts {
    pub parallelism: usize,
    pub skip_hidden: bool,
    pub follow_links: bool
}

impl WalkOpts {
    pub fn default() -> WalkOpts {
        WalkOpts { parallelism: 8, skip_hidden: false, follow_links: false }
    }
}


/// Walks multiple directory trees in parallel.
/// Inaccessible files are skipped, but errors are printed to stderr.
/// Returns only regular files and symlinks in the output stream.
/// Output order is unspecified.
/// Uses the default global rayon pool to launch tasks.
///
/// # Example
/// ```
/// use std::fs::{File, create_dir_all, remove_dir_all, create_dir};
/// use std::path::PathBuf;
/// use rayon::iter::ParallelIterator;
///
/// use dff::files::{walk_dirs, WalkOpts};
///
/// let test_root = PathBuf::from("target/test/walk/");
/// let dir1 = test_root.join("dir1");
/// let dir2 = test_root.join("dir2");
/// let dir3 = dir2.join("dir3");
/// remove_dir_all(&test_root).unwrap();
/// create_dir_all(&test_root).unwrap();
/// create_dir(&dir1).unwrap();
/// create_dir(&dir2).unwrap();
/// create_dir(&dir3).unwrap();
///
/// File::create(dir1.join("file11.txt")).unwrap();
/// File::create(dir1.join("file12.txt")).unwrap();
/// File::create(dir2.join("file2.txt")).unwrap();
/// File::create(dir3.join("file3.txt")).unwrap();
///
/// let dirs = vec![dir1, dir2];
/// let files = walk_dirs(dirs, WalkOpts::default());
/// assert_eq!(files.count(), 4);
/// ```
///
pub fn walk_dirs(paths: Vec<PathBuf>, opts: WalkOpts) -> impl ParallelIterator<Item=PathBuf> {

    let (tx, rx) = sync_channel(65536);

    // We need to use a separate rayon thread-pool for walking the directories, because
    // otherwise we may get deadlocks caused by blocking on the channel.
    let thread_pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(opts.parallelism)
            .build()
            .unwrap());

    for path in paths {
        let tx = tx.clone();
        let thread_pool = thread_pool.clone();
        thread::spawn(move || {
            WalkDir::new(&path)
                .skip_hidden(opts.skip_hidden)
                .follow_links(opts.follow_links)
                .parallelism(Parallelism::RayonExistingPool(thread_pool))
                .into_iter()
                .for_each(move |entry| match entry {
                    Ok(e) if e.file_type.is_file() || e.file_type.is_symlink() =>
                        tx.send(e.path()).unwrap(),
                    Ok(_) =>
                        (),
                    Err(e) =>
                        eprintln!("Cannot access path {}: {}", path.display(), e)
                });
        });
    }

    rx.into_iter().par_bridge()
}