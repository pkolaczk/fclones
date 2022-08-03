//! Type-safe wrappers for file position and length and other
//! file-system related utilities.

use core::fmt;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{ErrorKind, SeekFrom};
use std::iter::Sum;
use std::ops::{Add, AddAssign, BitXor, Deref, Mul, Sub};
use std::{fs, io};

use byte_unit::Byte;
use bytesize::ByteSize;
use rayon::iter::{IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator};
use serde::*;
use smallvec::alloc::fmt::Formatter;
use smallvec::alloc::str::FromStr;

use crate::device::DiskDevices;
use crate::group::FileGroup;
use crate::log::Log;
use crate::path::Path;

/// Represents data position in the file, counted from the beginning of the file, in bytes.
/// Provides more type safety and nicer formatting over using a raw u64.
/// Offsets are formatted as hex.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct FilePos(pub u64);

impl FilePos {
    pub fn zero() -> FilePos {
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
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Deserialize, Serialize, Default,
)]
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

impl AddAssign for FileLen {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0
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

/// A file chunk to be hashed
pub struct FileChunk<'a> {
    pub path: &'a Path,
    pub pos: FilePos,
    pub len: FileLen,
}

impl FileChunk<'_> {
    pub fn new(path: &Path, pos: FilePos, len: FileLen) -> FileChunk<'_> {
        FileChunk { path, pos, len }
    }
}

#[cfg(unix)]
pub type InodeId = u64;
#[cfg(windows)]
pub type InodeId = u128;

/// Useful for identifying files in presence of hardlinks
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FileId {
    pub device: u64,
    pub inode: InodeId,
}

impl FileId {
    #[cfg(unix)]
    pub fn new(file: &Path) -> io::Result<FileId> {
        use std::os::unix::fs::MetadataExt;
        match fs::metadata(&file.to_path_buf()) {
            Ok(metadata) => Ok(FileId {
                inode: metadata.ino(),
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
        Self::from_file(&fs::File::open(file.to_path_buf())?).map_err(|_| {
            io::Error::new(
                ErrorKind::Other,
                format!(
                    "Failed to read file identifier of {}: {}",
                    file.display(),
                    io::Error::last_os_error()
                ),
            )
        })
    }

    #[cfg(windows)]
    pub fn from_file(file: &fs::File) -> io::Result<FileId> {
        use std::os::windows::io::*;
        use winapi::ctypes::c_void;
        use winapi::um::fileapi::FILE_ID_INFO;
        use winapi::um::minwinbase::FileIdInfo;
        use winapi::um::winbase::GetFileInformationByHandleEx;
        let handle = file.as_raw_handle();
        unsafe {
            let mut file_id: FILE_ID_INFO = std::mem::zeroed();
            let file_id_ptr = (&mut file_id) as *mut _ as *mut c_void;
            const FILE_ID_SIZE: u32 = std::mem::size_of::<FILE_ID_INFO>() as u32;
            match GetFileInformationByHandleEx(handle, FileIdInfo, file_id_ptr, FILE_ID_SIZE) {
                0 => Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to read file identifier: {}",
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

    #[cfg(unix)]
    pub fn from_metadata(metadata: &fs::Metadata) -> FileId {
        use std::os::unix::fs::MetadataExt;
        FileId {
            inode: metadata.ino(),
            device: metadata.dev(),
        }
    }

    pub fn of(f: impl AsRef<FileId>) -> FileId {
        *f.as_ref()
    }
}

/// Convenience wrapper for accessing OS-dependent metadata like inode and device-id
#[derive(Debug)]
pub struct FileMetadata {
    id: FileId,
    metadata: fs::Metadata,
}

impl FileMetadata {
    pub fn new(path: &Path) -> io::Result<FileMetadata> {
        let path_buf = path.to_path_buf();
        let metadata = fs::metadata(&path_buf)?;
        #[cfg(unix)]
        let id = FileId::from_metadata(&metadata);
        #[cfg(windows)]
        let id = FileId::new(&path)?;
        Ok(FileMetadata { id, metadata })
    }

    pub fn len(&self) -> FileLen {
        FileLen(self.metadata.len())
    }

    pub fn file_id(&self) -> FileId {
        self.id
    }

    pub fn device_id(&self) -> u64 {
        self.id.device
    }

    pub fn inode_id(&self) -> InodeId {
        self.id.inode
    }
}

impl Deref for FileMetadata {
    type Target = fs::Metadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl AsRef<FileId> for FileMetadata {
    fn as_ref(&self) -> &FileId {
        &self.id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: Path,
    pub id: FileId,
    pub(crate) len: FileLen,
    // physical on-disk location of file data for access ordering optimisation
    // the highest 16 bits encode the device id
    pub(crate) location: u64,
}

const OFFSET_MASK: u64 = 0x0000FFFFFFFFFFFF;

#[cfg(target_os = "linux")]
const DEVICE_MASK: u64 = 0xFFFF000000000000;

impl FileInfo {
    fn new(path: Path, devices: &DiskDevices) -> io::Result<FileInfo> {
        let device_index = devices.get_by_path(&path).index as u64;
        let metadata = FileMetadata::new(&path)?;
        let file_len = metadata.len();
        let id = metadata.id;
        let inode_id = metadata.inode_id();
        Ok(FileInfo {
            path,
            id,
            len: file_len,
            location: device_index << 48 | inode_id as u64 & OFFSET_MASK,
        })
    }

    /// Returns the device index into the `DiskDevices` instance passed at creation
    pub fn get_device_index(&self) -> usize {
        (self.location >> 48) as usize
    }

    #[cfg(target_os = "linux")]
    pub fn fetch_physical_location(&mut self) -> io::Result<u64> {
        let new_location = get_physical_file_location(self.as_ref())?;
        if let Some(new_location) = new_location {
            self.location = self.location & DEVICE_MASK | (new_location >> 8) & OFFSET_MASK;
        }
        Ok(self.location)
    }
}

impl AsRef<FileId> for FileInfo {
    fn as_ref(&self) -> &FileId {
        &self.id
    }
}

impl AsRef<Path> for FileInfo {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl From<FileInfo> for Path {
    fn from(info: FileInfo) -> Self {
        info.path
    }
}

/// Returns file information for the given path.
/// On failure, logs an error to stderr and returns `None`.
pub(crate) fn file_info_or_log_err(
    file: Path,
    devices: &DiskDevices,
    log: &Log,
) -> Option<FileInfo> {
    match FileInfo::new(file, devices) {
        Ok(info) => Some(info),
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            log.warn(e);
            None
        }
    }
}

/// Returns the physical offset of the first data block of the file
#[cfg(target_os = "linux")]
pub(crate) fn get_physical_file_location(path: &Path) -> io::Result<Option<u64>> {
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

impl<'de> Deserialize<'de> for FileHash {
    fn deserialize<D>(deserializer: D) -> Result<FileHash, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let hash_value = u128::from_str_radix(s.as_str(), 16).map_err(serde::de::Error::custom)?;
        Ok(FileHash(hash_value))
    }
}

/// Makes it possible to operate generically on collections of files, regardless
/// of the way how the collection is implemented. We sometimes need to work on grouped files
/// but sometimes we just have a flat vector.
pub(crate) trait FileCollection {
    /// Returns the number of files in the collection
    fn count(&self) -> usize;
    /// Returns the total size of files in the collection
    fn total_size(&self) -> FileLen;
    /// Performs given action on each file in the collection
    fn for_each_mut<OP>(&mut self, op: OP)
    where
        OP: Fn(&mut FileInfo) + Sync + Send;
}

impl FileCollection for Vec<FileInfo> {
    fn count(&self) -> usize {
        self.len()
    }

    fn total_size(&self) -> FileLen {
        self.par_iter().map(|f| f.len).sum()
    }

    fn for_each_mut<OP>(&mut self, op: OP)
    where
        OP: Fn(&mut FileInfo) + Sync + Send,
    {
        self.par_iter_mut().for_each(op)
    }
}

impl FileCollection for Vec<FileGroup<FileInfo>> {
    fn count(&self) -> usize {
        self.iter().map(|g| g.file_count()).sum()
    }

    fn total_size(&self) -> FileLen {
        self.par_iter().map(|g| g.total_size()).sum()
    }

    fn for_each_mut<OP>(&mut self, op: OP)
    where
        OP: Fn(&mut FileInfo) + Sync + Send,
    {
        self.par_iter_mut().flat_map(|g| &mut g.files).for_each(op)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum FileAccess {
    Sequential,
    Random,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_bytes() {
        let file_len = FileLen(16000);
        let human_readable = format!("{}", file_len);
        assert_eq!(human_readable, "16.0 KB");
    }
}
