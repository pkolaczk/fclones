use core::fmt;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::iter::Sum;
use std::ops::{Add, BitXor, Mul, Sub};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::io::*;

use byte_unit::Byte;
use bytesize::ByteSize;
use metrohash::MetroHash128;
use serde::*;
use smallvec::alloc::fmt::Formatter;
use smallvec::alloc::str::FromStr;
#[cfg(unix)]
use sysinfo::{System, SystemExt};

use crate::device::DiskDevices;
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
        write!(f, "{}", self.0)
    }
}

impl From<u64> for FilePos {
    fn from(pos: u64) -> Self {
        FilePos(pos)
    }
}

impl From<usize> for FilePos {
    fn from(pos: usize) -> Self {
        FilePos(pos as u64)
    }
}

impl From<FilePos> for u64 {
    fn from(pos: FilePos) -> Self {
        pos.0
    }
}

impl From<FilePos> for usize {
    fn from(pos: FilePos) -> Self {
        pos.0 as usize
    }
}

impl From<FilePos> for SeekFrom {
    fn from(pos: FilePos) -> Self {
        SeekFrom::Start(pos.0)
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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Default)]
pub struct FileLen(pub u64);

impl FileLen {
    pub const MAX: FileLen = FileLen(u64::MAX);
    pub fn as_pos(self) -> FilePos {
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

impl From<FileLen> for u64 {
    fn from(l: FileLen) -> Self {
        l.0
    }
}

impl From<FileLen> for usize {
    fn from(l: FileLen) -> Self {
        l.0 as usize
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
    fn sum<I: Iterator<Item = FileLen>>(iter: I) -> Self {
        iter.fold(FileLen(0), |a, b| a + b)
    }
}

impl FromStr for FileLen {
    type Err = byte_unit::ByteError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let b = Byte::from_str(s)?;
        Ok(FileLen(b.get_bytes() as u64))
    }
}

impl Display for FileLen {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", ByteSize(self.0)).as_str())
    }
}

pub trait AsPath {
    fn path(&self) -> &Path;
}

pub trait IntoPath {
    fn into_path(self) -> Path;
}

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub path: Path,
    pub len: FileLen,
    // physical on-disk location of file data for access ordering optimisation
    // the highest 16 bits encode the device id
    pub location: u64,
}

impl AsPath for FileInfo {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl IntoPath for FileInfo {
    fn into_path(self) -> Path {
        self.path
    }
}

#[cfg(unix)]
const OFFSET_MASK: u64 = 0x0000FFFFFFFFFFFF;

#[cfg(unix)]
const DEVICE_MASK: u64 = 0xFFFF000000000000;

impl FileInfo {
    #[cfg(unix)]
    fn new(path: Path, devices: &DiskDevices) -> io::Result<FileInfo> {
        use std::os::unix::fs::MetadataExt;

        match std::fs::metadata(&path.to_path_buf()) {
            Ok(metadata) => {
                let device_index = devices.get_by_path(&path).index as u64;
                Ok(FileInfo {
                    path,
                    len: FileLen(metadata.len()),
                    location: device_index << 48 | metadata.ino() & OFFSET_MASK,
                })
            }
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path.display(), e),
            )),
        }
    }

    #[cfg(windows)]
    fn new(path: Path, devices: &DiskDevices) -> io::Result<FileInfo> {
        let info = File::open(path.to_path_buf()).and_then(|f| winapi_util::file::information(&f));
        match info {
            Ok(info) => {
                let device_index = devices.get_by_path(&path).index as u64;
                Ok(FileInfo {
                    path,
                    len: FileLen(info.file_size()),
                    location: device_index << 24 | info.file_index() & 0xFFFFFFFFFFFF,
                })
            }
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path.display(), e),
            )),
        }
    }

    /// Returns the device index into the `DiskDevices` instance passed at creation
    pub fn get_device_index(&self) -> usize {
        (self.location >> 48) as usize
    }

    #[cfg(target_os = "linux")]
    pub fn fetch_physical_location(&mut self) -> io::Result<u64> {
        let new_location = get_physical_file_location(&self.path())?;
        if let Some(new_location) = new_location {
            self.location = self.location & DEVICE_MASK | (new_location >> 8) & OFFSET_MASK;
        }
        Ok(self.location)
    }
}

/// Returns file information for the given path.
/// On failure, logs an error to stderr and returns `None`.
pub fn file_info_or_log_err(file: Path, devices: &DiskDevices, log: &Log) -> Option<FileInfo> {
    match FileInfo::new(file, devices) {
        Ok(info) => Some(info),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            log.warn(e);
            None
        }
    }
}

/// Useful for identifying files in presence of hardlinks
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId {
    pub inode: u128,
    pub device: u64,
}

impl FileId {
    #[cfg(unix)]
    pub fn new(file: &Path) -> io::Result<FileId> {
        use std::os::unix::fs::MetadataExt;
        match std::fs::metadata(&file.to_path_buf()) {
            Ok(metadata) => Ok(FileId {
                inode: metadata.ino() as u128,
                device: metadata.dev(),
            }),
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", file.display(), e),
            )),
        }
    }

    #[cfg(windows)]
    pub fn new(file: &Path) -> io::Result<FileId> {
        use std::os::windows::io::*;
        use winapi::ctypes::c_void;
        use winapi::um::fileapi::FILE_ID_INFO;
        use winapi::um::minwinbase::FileIdInfo;
        use winapi::um::winbase::GetFileInformationByHandleEx;
        let handle = File::open(file.to_path_buf())?.into_raw_handle();
        unsafe {
            let mut file_id: FILE_ID_INFO = std::mem::zeroed();
            let file_id_ptr = (&mut file_id) as *mut _ as *mut c_void;
            const FILE_ID_SIZE: u32 = std::mem::size_of::<FILE_ID_INFO>() as u32;
            match GetFileInformationByHandleEx(handle, FileIdInfo, file_id_ptr, FILE_ID_SIZE) {
                0 => Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to read file identifier of {}: {}",
                        file.display(),
                        io::Error::last_os_error()
                    ),
                )),
                _ => Ok(FileId {
                    device: file_id.VolumeSerialNumber as u64,
                    inode: u128::from_be_bytes(file_id.FileId.Identifier),
                }),
            }
        }
    }
}

pub fn file_id_or_log_err(file: &Path, log: &Log) -> Option<FileId> {
    match FileId::new(&file) {
        Ok(id) => Some(id),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            log.warn(e);
            None
        }
    }
}

/// Returns the physical offset of the first data block of the file
#[cfg(target_os = "linux")]
pub fn get_physical_file_location(path: &Path) -> io::Result<Option<u64>> {
    let mut extents = fiemap::fiemap(&path.to_path_buf())?;
    match extents.next() {
        Some(fe) => Ok(Some(fe?.fe_physical)),
        None => Ok(None),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
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

impl BitXor for FileHash {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        FileHash(rhs.0 ^ self.0)
    }
}

impl Serialize for FileHash {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

#[cfg(unix)]
fn to_off_t(offset: u64) -> libc::off_t {
    min(libc::off_t::MAX as u64, offset) as libc::off_t
}

/// Wrapper for `posix_fadvise`. Ignores errors.
/// This method is used to advise the system, so its failure is not critical to the result of
/// the program. At worst, failure could hurt performance.
#[cfg(target_os = "linux")]
fn fadvise(file: &File, offset: FilePos, len: FileLen, advice: nix::fcntl::PosixFadviseAdvice) {
    let _ = nix::fcntl::posix_fadvise(
        file.as_raw_fd(),
        to_off_t(offset.into()),
        to_off_t(len.into()),
        advice,
    );
}

/// Optimizes file read performance based on how many bytes we are planning to read.
/// If we know we'll be reading just one buffer, non zero read-ahead would be a cache waste.
/// On non-Unix systems, does nothing.
/// Failures are not signalled to the caller, but a warning is printed to stderr.
#[allow(unused)]
fn configure_readahead(file: &File, offset: FilePos, len: FileLen, cache_policy: Caching) {
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::*;
        let advise = |advice: PosixFadviseAdvice| fadvise(file, offset, len, advice);
        match cache_policy {
            Caching::Default => {}
            Caching::Random => advise(PosixFadviseAdvice::POSIX_FADV_RANDOM),
            Caching::Sequential => advise(PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL),
        };
    }
}

/// Tells the system to remove given file fragment from the page cache.
/// On non-Unix systems, does nothing.
#[allow(unused)]
fn evict_page_cache(file: &File, offset: FilePos, len: FileLen) {
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::*;
        fadvise(file, offset, len, PosixFadviseAdvice::POSIX_FADV_DONTNEED);
    }
}

/// Evicts the middle of the file from cache if the system is low on free memory.
/// The purpose of this method is to be nice to the data cached by other processes.
/// This program is likely to be used only once, so there is little value in keeping its
/// data cached for further use.
#[allow(unused)]
fn evict_page_cache_if_low_mem(file: &mut File, len: FileLen) {
    #[cfg(target_os = "linux")]
    {
        let skipped_prefix_len = FileLen(256 * 1024);
        if len > skipped_prefix_len {
            let mut system = System::new();
            system.refresh_memory();
            let free_mem = system.get_free_memory();
            let total_mem = system.get_total_memory();
            let free_ratio = free_mem as f32 / total_mem as f32;
            if free_ratio < 0.05 {
                evict_page_cache(
                    &file,
                    FilePos::zero() + skipped_prefix_len,
                    len - skipped_prefix_len,
                );
            }
        }
    }
}

/// Determines how page cache should be used when computing a file hash.
#[derive(Debug, Copy, Clone)]
pub enum Caching {
    /// Don't send any special cache advice to the OS
    Default,
    /// Prefetch as much data as possible into cache, to speed up sequential access
    Sequential,
    /// Don't prefetch data into RAM, but keep read data in cache. Use when reading a tiny part
    /// of the file.
    Random,
}

/// Opens a file and positions it at the given offset.
/// Additionally, sends the advice to the operating system about how many bytes will be read.
fn open(path: &Path, offset: FilePos, len: FileLen, cache_policy: Caching) -> io::Result<File> {
    let mut file = open_noatime(&path)?;
    configure_readahead(&file, offset, len, cache_policy);
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
    #[cfg(target_os = "linux")]
    {
        let mut noatime_opts = options.clone();
        noatime_opts.custom_flags(libc::O_NOATIME);
        noatime_opts
            .open(&path)
            // opening with O_NOATIME may fail in some cases for security reasons
            .or_else(|_| options.open(&path))
    }
    #[cfg(not(target_os = "linux"))]
    {
        options.open(&path)
    }
}

thread_local! {
    static BUF: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

/// Scans up to `len` bytes in a file and sends data to the given consumer.
/// Returns the number of bytes successfully read.
fn scan<F: FnMut(&[u8])>(
    stream: &mut impl Read,
    len: FileLen,
    buf_len: usize,
    mut consumer: F,
) -> io::Result<u64> {
    BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        let new_len = max(buf.len(), buf_len);
        buf.resize(new_len, 0);
        let mut read: u64 = 0;
        let len = len.into();
        while read < len {
            let remaining = len - read;
            let to_read = min(remaining, buf.len() as u64) as usize;
            let buf = &mut buf[..to_read];
            match stream.read(buf) {
                Ok(0) => break,
                Ok(actual_read) => {
                    read += actual_read as u64;
                    (consumer)(&buf[..actual_read]);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(read)
    })
}

/// Computes the hash value over at most `len` bytes of the stream.
/// Returns the number of the bytes read and a 128-bit hash value.
pub fn stream_hash(
    stream: &mut impl Read,
    len: FileLen,
    buf_len: usize,
    progress: impl Fn(usize),
) -> io::Result<(FileLen, FileHash)> {
    let mut hasher = MetroHash128::new();
    let mut read_len: FileLen = FileLen(0);
    scan(stream, len, buf_len, |buf| {
        hasher.write(buf);
        read_len = read_len + FileLen(buf.len() as u64);
        (progress)(buf.len());
    })?;
    let (a, b) = hasher.finish128();
    Ok((read_len, FileHash(((a as u128) << 64) | b as u128)))
}

/// Computes hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
pub fn file_hash(
    path: &Path,
    offset: FilePos,
    len: FileLen,
    buf_len: usize,
    cache_policy: Caching,
    progress: impl Fn(usize),
) -> io::Result<FileHash> {
    let mut file = open(path, offset, len, cache_policy)?;
    let hash = stream_hash(&mut file, len, buf_len, progress)?.1;
    evict_page_cache_if_low_mem(&mut file, len);
    Ok(hash)
}

/// Computes the file hash or logs an error and returns none if failed.
/// If file is not found, no error is logged and `None` is returned.
pub fn file_hash_or_log_err(
    path: &Path,
    offset: FilePos,
    len: FileLen,
    buf_len: usize,
    caching: Caching,
    progress: impl Fn(usize),
    log: &Log,
) -> Option<FileHash> {
    match file_hash(path, offset, len, buf_len, caching, progress) {
        Ok(hash) => Some(hash),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            log.warn(format!(
                "Failed to compute hash of file {}: {}",
                path.display(),
                e
            ));
            None
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use crate::path::Path;
    use std::fs::{create_dir_all, File};
    use std::io::Write;
    use std::path::PathBuf;

    #[test]
    fn test_format_bytes() {
        let file_len = FileLen(16000);
        let human_readable = format!("{}", file_len);
        assert_eq!(human_readable, "16.0 KB");
    }

    #[test]
    fn test_file_hash() {
        let test_root = PathBuf::from("target/test/file_hash/");
        create_dir_all(&test_root).unwrap();
        let file1 = test_root.join("file1");
        File::create(&file1)
            .unwrap()
            .write_all(b"Test file 1")
            .unwrap();
        let file2 = test_root.join("file2");
        File::create(&file2)
            .unwrap()
            .write_all(b"Test file 2")
            .unwrap();

        let hash1 = file_hash(
            &Path::from(&file1),
            FilePos(0),
            FileLen::MAX,
            4096,
            Caching::Default,
            |_| {},
        )
        .unwrap();
        let hash2 = file_hash(
            &Path::from(&file2),
            FilePos(0),
            FileLen::MAX,
            4096,
            Caching::Default,
            |_| {},
        )
        .unwrap();
        let hash3 = file_hash(
            &Path::from(&file2),
            FilePos(0),
            FileLen(8),
            4096,
            Caching::Default,
            |_| {},
        )
        .unwrap();
        assert_ne!(hash1, hash2);
        assert_ne!(hash2, hash3);
    }
}
