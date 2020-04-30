use core::fmt;
use std::{io, thread};
use std::cell::RefCell;
use std::cmp::min;
use std::fmt::Display;
use std::fs::{File, Metadata, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::ops::{Add, Sub};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::io::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use bytesize::ByteSize;
use fasthash::{city::crc::Hasher128, FastHasher, HasherExt};
use jwalk::{Parallelism, WalkDir};
#[cfg(unix)]
use nix::fcntl::*;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use smallvec::alloc::fmt::Formatter;
use smallvec::alloc::str::FromStr;
use sys_info::mem_info;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FilePos(pub u64);

impl Display for FilePos {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents length of a file.
/// Provides more type safety and nicer formatting over using a raw u64.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FileLen(pub u64);

impl FileLen {
    pub const MAX: FileLen = FileLen(u64::MAX);
    pub fn as_pos(&self) -> FilePos {
        FilePos(self.0)
    }
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

impl Add for FileLen {
    type Output = FileLen;

    fn add(self, rhs: Self) -> Self::Output {
        FileLen(self.0 + rhs.0)
    }
}

impl Sub for FileLen {
    type Output = FileLen;

    fn sub(self, rhs: Self) -> Self::Output {
        FileLen(self.0 - rhs.0)
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

/// Returns file information for the given path.
pub fn file_info(file: PathBuf) -> io::Result<FileInfo> {
    let metadata = std::fs::metadata(&file)?;
    Ok(FileInfo::for_file(file, &metadata))
}

/// Returns file information for the given path.
/// On failure, logs an error to stderr and returns `None`.
pub fn file_info_or_log_err(file: PathBuf) -> Option<FileInfo> {
    match std::fs::metadata(&file) {
        Ok(metadata) => Some(FileInfo::for_file(file, &metadata)),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            eprintln!("Failed to read metadata of {}: {}", file.display(), e);
            None
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

/// Size of the temporary buffer used for file read operations.
/// There is one such buffer per thread.
pub const BUF_LEN: usize = 8192;

#[repr(C, align(4096))]
#[derive(Copy, Clone)]
struct AlignedBuf([u8; BUF_LEN]);

impl AlignedBuf {
    pub fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }

    pub fn len(&self) -> FileLen {
        FileLen(BUF_LEN as u64)
    }
}

thread_local! {
    static BUF: RefCell<AlignedBuf> =
        RefCell::new(AlignedBuf([0; BUF_LEN]));
}

fn to_off_t(offset: u64) -> i64 {
    min(i64::MAX as u64, offset) as i64
}

/// Wrapper for `posix_fadvise`. Ignores errors.
/// This method is used to advise the system, so its failure is not critical to the result of
/// the program. At worst, failure could hurt performance.
#[cfg(unix)]
fn fadvise(file: &File, offset: FilePos, len: FileLen, advice: PosixFadviseAdvice) {
    let _ = posix_fadvise(file.as_raw_fd(),
                          to_off_t(offset.0),
                          to_off_t(len.0),
                          advice);
}

/// Optimizes file read performance based on how many bytes we are planning to read.
/// If we know we'll be reading just one buffer, non zero read-ahead would be a cache waste.
/// On non-Unix systems, does nothing.
/// Failures are not signalled to the caller, but a warning is printed to stderr.
fn configure_readahead(file: &File, offset: FilePos, len: FileLen) {
    if cfg!(unix) {
        let advise = |advice: PosixFadviseAdvice| {
            fadvise(file, offset, len, advice)
        };
        if len.0 <= BUF_LEN as u64 {
            advise(PosixFadviseAdvice::POSIX_FADV_RANDOM)
        } else {
            advise(PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL)
        };
    }
}

/// Tells the system to remove given file fragment from the page cache.
/// On non-Unix systems, does nothing.
fn evict_page_cache(file: &File, offset: FilePos, len: FileLen) {
    if cfg!(unix) {
        fadvise(file, offset, len, PosixFadviseAdvice::POSIX_FADV_DONTNEED);
    }
}

/// Evicts the middle of the file from cache if the system is low on free memory.
/// The purpose of this method is to be nice to the data cached by other processes.
/// This program is likely to be used only once, so there is little value in keeping its
/// data cached for further use.
fn evict_page_cache_if_low_mem(file: &mut File, len: FileLen) {
    if len.0 > BUF_LEN as u64 * 2 {
        let free_ratio = match mem_info() {
            Ok(mem) => mem.free as f32 / mem.total as f32,
            Err(_) => 0.0
        };
        if free_ratio < 0.05 {
            evict_page_cache(
                &file, FilePos(BUF_LEN as u64), len - FileLen(2 * BUF_LEN as u64));
        }
    }
}

/// Opens a file and positions it at the given offset.
/// Additionally, sends the advice to the operating system about how many bytes will be read.
fn open(path: &PathBuf, offset: FilePos, len: FileLen) -> io::Result<File> {
    let mut file = open_noatime(path)?;
    configure_readahead(&file, offset, len);
    if offset.0 > 0 {
        file.seek(SeekFrom::Start(offset.0))?;
    }
    Ok(file)
}

/// Opens a file for read. On unix systems passes O_NOATIME flag to drastically improve
/// performance of reading small files.
fn open_noatime(path: &PathBuf) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    if cfg!(unix) {
        let mut noatime_opts = options.clone();
        noatime_opts.custom_flags(libc::O_NOATIME);
        noatime_opts.open(path)
            // opening with O_NOATIME may fail in some cases for security reasons
            .or_else(|_| options.open(path))
    } else {
        options.open(path)
    }
}

/// Scans up to `len` bytes in a file and sends data to the given consumer.
/// Returns the number of bytes successfully read.
fn scan<F: FnMut(&[u8]) -> ()>(file: &mut File, len: FileLen, mut consumer: F) -> io::Result<u64> {
    BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        let mut read = 0;
        while read < len.0 {
            let remaining = len.0 - read;
            let to_read = min(remaining, buf.len().0) as usize;
            let buf = &mut buf.as_mut()[..to_read];
            match file.read(buf) {
                Ok(0) =>
                    break,
                Ok(actual_read) => {
                    read += actual_read as u64;
                    (consumer)(buf);
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(read)
    })
}

/// Computes hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
///
/// # Example
/// ```
/// use dff::files::{file_hash, FileLen, FilePos};
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
/// let hash1 = file_hash(&file1, FilePos(0), FileLen::MAX).unwrap();
/// let hash2 = file_hash(&file2, FilePos(0), FileLen::MAX).unwrap();
/// let hash3 = file_hash(&file2, FilePos(0), FileLen(8)).unwrap();
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash2, hash3);
/// ```
pub fn file_hash(path: &PathBuf, offset: FilePos, len: FileLen) -> io::Result<FileHash> {
    let mut hasher = Hasher128::new();
    let mut file = open(path, offset, len)?;
    scan(&mut file, len, |buf| hasher.write(buf))?;
    evict_page_cache_if_low_mem(&mut file, len);
    Ok(FileHash(hasher.finish_ext()))
}

/// Computes the file hash or logs an error and returns none if failed.
/// If file is not found, no error is logged and `None` is returned.
pub fn file_hash_or_log_err(path: &PathBuf, offset: FilePos, len: FileLen) -> Option<FileHash> {
    match file_hash(path, offset, len) {
        Ok(hash) => Some(hash),
        Err(e) if e.kind() == ErrorKind::NotFound =>
            None,
        Err(e) => {
            eprintln!("Failed to compute hash of file {}: {}", path.display(), e);
            None
        }
    }
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