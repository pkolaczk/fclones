use core::fmt;
use std::cmp::min;
use std::fmt::Display;
use std::fs::{File, Metadata, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::iter::Sum;
use std::ops::{Add, Mul, Sub};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::io::*;

use bytesize::ByteSize;
use fasthash::{FastHasher, HasherExt, t1ha2::Hasher128};
#[cfg(unix)]
use nix::fcntl::*;
use serde::*;
use smallvec::alloc::fmt::Formatter;
use smallvec::alloc::str::FromStr;
use sys_info::mem_info;

use crate::log::Log;
use crate::path::Path;

/// Represents data position in the file, counted from the beginning of the file, in bytes.
/// Provides more type safety and nicer formatting over using a raw u64.
/// Offsets are formatted as hex.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FilePos(pub u64);

impl FilePos {
    fn zero() -> FilePos {
        FilePos(0)
    }
}

impl Display for FilePos {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl From<u64> for FilePos {
    fn from(p: u64) -> Self {
        FilePos(p)
    }
}

impl From<usize> for FilePos {
    fn from(p: usize) -> Self {
        FilePos(p as u64)
    }
}

impl Into<u64> for FilePos {
    fn into(self) -> u64 {
        self.0
    }
}

impl Into<usize> for FilePos {
    fn into(self) -> usize {
        self.0 as usize
    }
}

impl Into<SeekFrom> for FilePos {
    fn into(self) -> SeekFrom {
        SeekFrom::Start(self.0)
    }
}

impl Add<FileLen> for FilePos {
    type Output = FilePos;
    fn add(self, rhs: FileLen) -> Self::Output {
        FilePos(self.0 + rhs.0)
    }
}

impl Sub<FileLen> for FilePos {
    type Output = FilePos;
    fn sub(self, rhs: FileLen) -> Self::Output {
        FilePos(self.0 - rhs.0)
    }
}

/// Represents length of data, in bytes.
/// Provides more type safety and nicer formatting over using a raw u64.
///
/// # Examples
/// Formatting file length as a human readable string:
/// ```
/// use fclones::files::FileLen;
/// let file_len = FileLen(16000);
/// let human_readable = format!("{}", file_len);
/// assert_eq!(human_readable, "16.0 KB");
/// ```
///
/// `FileLen` can be added directly to `FilePos`:
/// ```
/// use fclones::files::{FileLen, FilePos};
/// assert_eq!(FilePos(1000) + FileLen(64), FilePos(1064));
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize)]
pub struct FileLen(pub u64);

impl FileLen {
    pub const MAX: FileLen = FileLen(u64::MAX);
    pub fn as_pos(&self) -> FilePos {
        FilePos(self.0)
    }
}

impl From<u64> for FileLen {
    fn from(l: u64) -> Self {
        FileLen(l)
    }
}

impl From<usize> for FileLen {
    fn from(l: usize) -> Self {
        FileLen(l as u64)
    }
}

impl Into<u64> for FileLen {
    fn into(self) -> u64 {
        self.0
    }
}

impl Into<usize> for FileLen {
    fn into(self) -> usize {
        self.0 as usize
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

impl Mul<u64> for FileLen {
    type Output = FileLen;
    fn mul(self, rhs: u64) -> Self::Output {
        FileLen(self.0 * rhs)
    }
}

impl Sum<FileLen> for FileLen {
    fn sum<I: Iterator<Item=FileLen>>(iter: I) -> Self {
        iter.fold(FileLen(0), |a, b| a + b)
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
        f.pad(format!("{}", ByteSize(self.0)).as_str())
    }
}

/// Keeps metadata of the file that we are interested in.
/// We don't want to keep unused metadata in memory.
pub struct FileInfo {
    pub path: Path,
    pub file_id: u64,
    pub dev_id: u64,
    pub len: FileLen,
}

impl FileInfo {

    #[cfg(unix)]
    fn for_file(path: Path, metadata: &Metadata) -> FileInfo {
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
        FileInfo {
            path,
            file_id: metadata.file_index().expect(fmt!("Not a file: {}", path)),
            dev_id: metadata.volume_serial_number().expect(fmt!("Not a file: {}", path)),
            len: FileLen(metadata.len())
        }
    }
}

/// Returns file information for the given path.
pub fn file_info(file: Path) -> io::Result<FileInfo> {
    let metadata = std::fs::metadata(&file.to_path_buf())?;
    Ok(FileInfo::for_file(file, &metadata))
}

/// Returns file information for the given path.
/// On failure, logs an error to stderr and returns `None`.
pub fn file_info_or_log_err(file: Path, log: &Log) -> Option<FileInfo> {
    match std::fs::metadata(&file.to_path_buf()) {
        Ok(metadata) => Some(FileInfo::for_file(file, &metadata)),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            log.err(format!("Failed to read metadata of {}: {}", file.display(), e));
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
        f.pad(format!("{:032x}", self.0).as_str())
    }
}

impl Serialize for FileHash {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
        where S: Serializer
    {
        serializer.collect_str(self)
    }
}
/// Size of the temporary buffer used for file read operations.
/// There is one such buffer per thread.
pub const BUF_LEN: usize = 8192;

fn to_off_t(offset: u64) -> i64 {
    min(i64::MAX as u64, offset) as i64
}

/// Wrapper for `posix_fadvise`. Ignores errors.
/// This method is used to advise the system, so its failure is not critical to the result of
/// the program. At worst, failure could hurt performance.
#[cfg(unix)]
fn fadvise(file: &File, offset: FilePos, len: FileLen, advice: PosixFadviseAdvice) {
    let _ = posix_fadvise(file.as_raw_fd(),
                          to_off_t(offset.into()),
                          to_off_t(len.into()),
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
    let buf_len: FileLen = BUF_LEN.into();
    if len > buf_len * 2 {
        let free_ratio = match mem_info() {
            Ok(mem) => mem.free as f32 / mem.total as f32,
            Err(_) => 0.0
        };
        if free_ratio < 0.05 {
            evict_page_cache(&file, FilePos::zero() + buf_len, len - buf_len * 2);
        }
    }
}

/// Opens a file and positions it at the given offset.
/// Additionally, sends the advice to the operating system about how many bytes will be read.
fn open(path: &Path, offset: FilePos, len: FileLen) -> io::Result<File> {
    let mut file = open_noatime(&path)?;
    configure_readahead(&file, offset, len);
    if offset > FilePos::zero() {
        file.seek(offset.into())?;
    }
    Ok(file)
}

/// Opens a file for read. On unix systems passes O_NOATIME flag to drastically improve
/// performance of reading small files.
fn open_noatime(path: &Path) -> io::Result<File> {
    let path = path.to_path_buf();
    let mut options = OpenOptions::new();
    options.read(true);
    if cfg!(unix) {
        let mut noatime_opts = options.clone();
        noatime_opts.custom_flags(libc::O_NOATIME);
        noatime_opts.open(&path)
            // opening with O_NOATIME may fail in some cases for security reasons
            .or_else(|_| options.open(&path))
    } else {
        options.open(&path)
    }
}

/// Scans up to `len` bytes in a file and sends data to the given consumer.
/// Returns the number of bytes successfully read.
fn scan<F: FnMut(&[u8]) -> ()>(file: &mut File, len: FileLen, mut consumer: F) -> io::Result<u64> {
    let mut buf = [0; BUF_LEN];
    let mut read: u64 = 0;
    let len = len.into();
    while read < len {
        let remaining = len - read;
        let to_read = min(remaining, buf.len() as u64) as usize;
        let buf = &mut buf[..to_read];
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
}

/// Computes hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
///
/// # Example
/// ```
/// use fclones::files::{file_hash, FileLen, FilePos};
/// use fclones::path::Path;
/// use std::io::Write;
/// use std::fs::{File, create_dir_all};
/// use std::path::PathBuf;
///
/// let test_root = PathBuf::from("target/test/file_hash/");
/// create_dir_all(&test_root).unwrap();
/// let file1 = test_root.join("file1");
/// File::create(&file1).unwrap().write_all(b"Test file 1");
/// let file2 = test_root.join("file2");
/// File::create(&file2).unwrap().write_all(b"Test file 2");
///
/// let hash1 = file_hash(&Path::from(&file1), FilePos(0), FileLen::MAX, |_|{}).unwrap();
/// let hash2 = file_hash(&Path::from(&file2), FilePos(0), FileLen::MAX, |_|{}).unwrap();
/// let hash3 = file_hash(&Path::from(&file2), FilePos(0), FileLen(8), |_|{}).unwrap();
/// assert_ne!(hash1, hash2);
/// assert_ne!(hash2, hash3);
/// ```
pub fn file_hash(path: &Path, offset: FilePos, len: FileLen, progress: impl Fn(usize))
    -> io::Result<FileHash>
{
    let mut hasher = Hasher128::new();
    let mut file = open(path, offset, len)?;
    scan(&mut file, len, |buf| {
        let mut block_hasher = Hasher128::new();
        block_hasher.write(buf);
        hasher.write_u128(block_hasher.finish_ext());
        (progress)(buf.len());
    })?;
    evict_page_cache_if_low_mem(&mut file, len);
    Ok(FileHash(hasher.finish_ext()))
}

/// Computes the file hash or logs an error and returns none if failed.
/// If file is not found, no error is logged and `None` is returned.
pub fn file_hash_or_log_err(
    path: &Path,
    offset: FilePos,
    len: FileLen,
    progress: impl Fn(usize),
    log: &Log)
    -> Option<FileHash>
{
    match file_hash(path, offset, len, progress) {
        Ok(hash) => Some(hash),
        Err(e) if e.kind() == ErrorKind::NotFound =>
            None,
        Err(e) => {
            log.err(format!("Failed to compute hash of file {}: {}", path.display(), e));
            None
        }
    }
}