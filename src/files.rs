use core::fmt;
use std::cmp::min;
use std::fmt::Display;
use std::fs::{File, Metadata};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;
use std::thread;

use bytesize::ByteSize;
use fasthash::{city::crc::Hasher128, FastHasher, HasherExt};
use jwalk::{Parallelism, WalkDir};
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use smallvec::alloc::fmt::Formatter;
use smallvec::alloc::str::FromStr;

/// Represents length of a file.
/// Provides more type safety and nicer formatting over using a raw u64.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FileLen(pub u64);

impl FileLen {
    pub const MAX: FileLen = FileLen(u64::MAX);
}

pub trait AsFileLen {
    fn as_file_len(&self) -> &FileLen;
}

impl AsFileLen for FileLen {
    fn as_file_len(&self) -> &FileLen {
        self
    }
}

impl<T> AsFileLen for (FileLen, T) {
    fn as_file_len(&self) -> &FileLen {
        &self.0
    }
}

impl FromStr for FileLen {

    type Err = <u64 as FromStr>::Err;

    // TODO handle units
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(FileLen(s.parse()?))
    }
}

impl Display for FileLen {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
       write!(f, "{}", ByteSize(self.0))
    }
}

/// Keeps metadata of the file that we are interested in.
/// We don't want to keep unused metadata in memory.
pub struct FileInfo {
    pub path: PathBuf,
    pub file_id: u64,
    pub dev_id: u64,
    pub len: FileLen,
}

impl FileInfo {

    #[cfg(unix)]
    fn for_file(path: PathBuf, metadata: &Metadata) -> FileInfo {
        use std::os::unix::fs::MetadataExt;
        FileInfo {
            path,
            file_id: metadata.ino(),
            dev_id: metadata.dev(),
            len: FileLen(metadata.len())
        }
    }

    #[cfg(windows)]
    fn from_metadata(path: PathBuf, metadata: &Metadata) -> FileInfo {
        use std::os::windows::fs::MetadataExt;
        let hasher = DefaultHasher::new();
        FileInfo {
            path,
            file_id: metadata.file_index().expect(fmt!("Not a file: {}", path)),
            dev_id: metadata.volume_serial_number().expect(fmt!("Not a file: {}", path)),
            len: FileLen(metadata.len())
        }
    }
}

/// Return file information.
/// If file metadata cannot be accessed, print the error to stderr and return `None`.
pub fn file_info(file: PathBuf) -> Option<FileInfo> {
    match std::fs::metadata(&file) {
        Ok(metadata) =>
            Some(FileInfo::for_file(file, &metadata)),
        Err(e) => match e.kind() {
            ErrorKind::NotFound =>
                // file was probably removed while we were scanning, so we don't care -
                // let's pretend we never found it in the first place
                None,
            _ => {
                eprintln!("Failed to read metadata of {}: {}", file.display(), e);
                None
            }
        }
    }
}


#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FileHash(pub u128);

pub trait AsFileHash {
    fn as_file_hash(&self) -> &FileHash;
}

impl AsFileHash for FileHash {
    fn as_file_hash(&self) -> &FileHash {
        self
    }
}

impl<T> AsFileHash for (T, FileHash) {
    fn as_file_hash(&self) -> &FileHash {
        &self.1
    }
}

impl Display for FileHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

/// Compute hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
///
/// # Example
/// ```
/// use dff::files::{file_hash, FileLen};
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
/// let hash1 = file_hash(&file1, FileLen::MAX).unwrap();
/// let hash2 = file_hash(&file2, FileLen::MAX).unwrap();
/// let hash3 = file_hash(&file2, FileLen(8)).unwrap();
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash2, hash3);
/// ```
pub fn file_hash(path: &PathBuf, len: FileLen) -> Option<FileHash> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => return match e.kind() {
            ErrorKind::NotFound =>
                // the file was probably removed while we were scanning, so we don't care -
                // let's pretend we never found it in the first place
                None,
            _ => {
                eprintln!("Failed to open file {}: {}", path.display(), e);
                None
            }
        }
    };
    let mut count: u64 = 0;
    let mut reader = BufReader::with_capacity(4096, file);
    let mut hasher = Hasher128::new();
    while count < len.0 {
        match reader.fill_buf() {
            Ok(&[]) => break,
            Ok(buf) => {
                let to_read = len.0 - count;
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
    Some(FileHash(hasher.finish_ext()))
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
                    Ok(e) if e.file_type.is_file() =>
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