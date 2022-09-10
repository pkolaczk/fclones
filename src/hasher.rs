use byteorder::{LittleEndian, ReadBytesExt};
use std::cell::RefCell;
use std::cmp::{max, min};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io;
use std::io::{ErrorKind, Read, Seek};
use std::str::FromStr;

use metrohash::MetroHash128;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};

use crate::cache::{HashCache, Key};
use crate::file::{FileAccess, FileChunk, FileHash, FileLen, FileMetadata, FilePos};
use crate::log::Log;
use crate::path::Path;
use crate::transform::Transform;
use crate::Error;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum HashFn {
    #[default]
    Metro128,
    Blake3,
    Sha256,
    Sha512,
}

impl HashFn {
    pub fn variants() -> Vec<&'static str> {
        vec!["metro128", "blake3", "sha256", "sha512"]
    }
}

impl FromStr for HashFn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "metro128" => Ok(Self::Metro128),
            "blake3" => Ok(Self::Blake3),
            "sha256" => Ok(Self::Sha256),
            "sha512" => Ok(Self::Sha512),
            _ => Err(format!("Unknown hash algorithm: {}", s)),
        }
    }
}

/// Computes the hash of a data stream
trait StreamHasher {
    fn new() -> Self;
    fn update(&mut self, bytes: &[u8]);
    fn finish(self) -> FileHash;
}

impl StreamHasher for MetroHash128 {
    fn new() -> Self {
        MetroHash128::new()
    }

    fn update(&mut self, bytes: &[u8]) {
        self.write(bytes)
    }

    fn finish(self) -> FileHash {
        let (a, b) = self.finish128();
        FileHash(((a as u128) << 64) | b as u128)
    }
}

impl StreamHasher for blake3::Hasher {
    fn new() -> Self {
        blake3::Hasher::new()
    }

    fn update(&mut self, bytes: &[u8]) {
        blake3::Hasher::update(self, bytes);
    }

    fn finish(self) -> FileHash {
        FileHash(
            self.finalize()
                .as_bytes()
                .as_slice()
                .read_u128::<LittleEndian>()
                .unwrap(),
        )
    }
}

impl StreamHasher for Sha512 {
    fn new() -> Self {
        <Sha512 as Digest>::new()
    }

    fn update(&mut self, bytes: &[u8]) {
        Digest::update(self, bytes);
    }

    fn finish(self) -> FileHash {
        let result = self.finalize();
        FileHash(result.as_slice().read_u128::<LittleEndian>().unwrap())
    }
}

impl StreamHasher for Sha256 {
    fn new() -> Self {
        <Sha256 as Digest>::new()
    }

    fn update(&mut self, bytes: &[u8]) {
        Digest::update(self, bytes);
    }

    fn finish(self) -> FileHash {
        let result = self.finalize();
        FileHash(result.as_slice().read_u128::<LittleEndian>().unwrap())
    }
}

/// Hashes file contents
pub struct FileHasher<'a> {
    pub(crate) algorithm: HashFn,
    pub(crate) buf_len: usize,
    pub(crate) cache: Option<HashCache>,
    pub(crate) transform: Option<Transform>,
    pub(crate) log: &'a Log,
}

impl FileHasher<'_> {
    /// Creates a hasher with no caching
    pub fn new(algorithm: HashFn, transform: Option<Transform>, log: &Log) -> FileHasher<'_> {
        FileHasher {
            algorithm,
            buf_len: 65536,
            cache: None,
            transform,
            log,
        }
    }

    /// Creates a default hasher with caching enabled
    pub fn new_cached(
        algorithm: HashFn,
        transform: Option<Transform>,
        log: &Log,
    ) -> Result<FileHasher<'_>, Error> {
        let transform_command_str = transform.as_ref().map(|t| t.command_str.as_str());
        let cache = HashCache::open_default(transform_command_str, algorithm)?;
        Ok(FileHasher {
            algorithm,
            buf_len: 65536,
            cache: Some(cache),
            transform,
            log,
        })
    }

    /// Computes the file hash or logs an error and returns none if failed.
    /// If file is not found, no error is logged and `None` is returned.
    pub fn hash_file(
        &self,
        chunk: &FileChunk<'_>,
        progress: impl Fn(usize),
    ) -> io::Result<FileHash> {
        let cache = self.cache.as_ref();
        let metadata = cache.and_then(|_| FileMetadata::new(chunk.path).ok());
        let metadata = metadata.as_ref();
        let key = cache
            .zip(metadata.as_ref())
            .and_then(|(c, m)| c.key(chunk, m).ok());
        let key = key.as_ref();
        let hash = self.load_hash(key, metadata);
        if let Some((_, hash)) = hash {
            progress(chunk.len.0 as usize);
            return Ok(hash);
        }
        let hash = match self.algorithm {
            HashFn::Metro128 => file_hash::<MetroHash128>(chunk, self.buf_len, progress),
            HashFn::Blake3 => file_hash::<blake3::Hasher>(chunk, self.buf_len, progress),
            HashFn::Sha256 => file_hash::<Sha256>(chunk, self.buf_len, progress),
            HashFn::Sha512 => file_hash::<Sha512>(chunk, self.buf_len, progress),
        }?;
        self.store_hash(key, metadata, chunk.len, hash);
        Ok(hash)
    }

    pub fn hash_file_or_log_err(
        &self,
        chunk: &FileChunk<'_>,
        progress: impl Fn(usize),
    ) -> Option<FileHash> {
        match self.hash_file(chunk, progress) {
            Ok(hash) => Some(hash),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => {
                self.log.warn(format!(
                    "Failed to compute hash of file {}: {}",
                    chunk.path.to_escaped_string(),
                    e
                ));
                None
            }
        }
    }

    /// Just like `hash_file`, but transforms the file before hashing.
    pub fn hash_transformed(
        &self,
        chunk: &FileChunk<'_>,
        progress: impl Fn(usize),
    ) -> io::Result<(FileLen, FileHash)> {
        assert_eq!(chunk.pos, FilePos::zero());
        assert!(self.transform.is_some());

        let transform = self.transform.as_ref().unwrap();
        let cache = self.cache.as_ref();
        let metadata = cache.and_then(|_| FileMetadata::new(chunk.path).ok());
        let metadata = metadata.as_ref();
        let key = cache
            .zip(metadata.as_ref())
            .and_then(|(c, m)| c.key(chunk, m).ok());
        let key = key.as_ref();
        let hash = self.load_hash(key, metadata);
        if let Some(hash) = hash {
            progress(chunk.len.0 as usize);
            return Ok(hash);
        }

        let mut transform_output = transform.run(chunk.path)?;
        let hash_input = &mut transform_output.out_stream;

        // Transformed file may have a different length, so we cannot use stream_hash progress
        // reporting, as it would report progress of the transformed stream. Instead we advance
        // progress after doing the full file.
        let hash = match self.algorithm {
            HashFn::Metro128 => {
                stream_hash::<MetroHash128>(hash_input, chunk.len, self.buf_len, |_| {})
            }
            HashFn::Blake3 => {
                stream_hash::<blake3::Hasher>(hash_input, chunk.len, self.buf_len, |_| {})
            }
            HashFn::Sha256 => stream_hash::<Sha256>(hash_input, chunk.len, self.buf_len, |_| {}),
            HashFn::Sha512 => stream_hash::<Sha512>(hash_input, chunk.len, self.buf_len, |_| {}),
        };
        progress(chunk.len.0 as usize);

        let hash = hash?;
        let exit_status = transform_output.child.lock().unwrap().wait()?;
        if !exit_status.success() {
            let captured_err = transform_output
                .err_stream
                .take()
                .unwrap()
                .join()
                .unwrap_or_else(|_| "".to_owned());
            let captured_err = format_output_stream(captured_err.as_str());
            return match exit_status.code() {
                Some(exit_code) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "{} failed with non-zero status code: {}{}",
                        transform.program, exit_code, captured_err
                    ),
                )),
                None => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{} failed{}", transform.program, captured_err),
                )),
            };
        }

        self.store_hash(key, metadata, hash.0, hash.1);
        Ok(hash)
    }

    pub fn hash_transformed_or_log_err(
        &self,
        chunk: &FileChunk<'_>,
        progress: impl Fn(usize),
    ) -> Option<(FileLen, FileHash)> {
        match self.hash_transformed(chunk, progress) {
            Ok(hash) => Some(hash),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => {
                self.log.warn(format!(
                    "Failed to compute hash of file {}: {}",
                    chunk.path.to_escaped_string(),
                    e
                ));
                None
            }
        }
    }

    /// Loads hash from the cache.
    /// If the hash is not present in the cache, returns `None`.
    /// If the operation fails (e.g. corrupted cache), logs a warning and returns `None`.
    fn load_hash(
        &self,
        key: Option<&Key>,
        metadata: Option<&FileMetadata>,
    ) -> Option<(FileLen, FileHash)> {
        self.cache
            .as_ref()
            .zip(key)
            .zip(metadata)
            .and_then(|((cache, key), metadata)| match cache.get(key, metadata) {
                Ok(len_and_hash) => len_and_hash,
                Err(e) => {
                    self.log.warn(format!(
                        "Failed to load hash of file id = {} from the cache: {}",
                        key, e
                    ));
                    None
                }
            })
    }

    /// Stores the hash in the cache.
    /// If the operation fails (e.g. no space on drive), logs a warning.
    fn store_hash(
        &self,
        key: Option<&Key>,
        metadata: Option<&FileMetadata>,
        data_len: FileLen,
        hash: FileHash,
    ) {
        if let Some(((cache, key), metadata)) =
            self.cache.as_ref().zip(key.as_ref()).zip(metadata.as_ref())
        {
            if let Err(e) = cache.put(key, metadata, data_len, hash) {
                self.log.warn(format!(
                    "Failed to store hash of file {} in the cache: {}",
                    key, e
                ))
            }
        };
    }
}

fn format_output_stream(output: &str) -> String {
    let output = output.trim().to_string();
    if output.is_empty() {
        output
    } else {
        format!("\n{}\n", output)
    }
}

#[cfg(target_os = "linux")]
fn to_off_t(offset: u64) -> libc::off_t {
    min(libc::off_t::MAX as u64, offset) as libc::off_t
}

/// Wrapper for `posix_fadvise`. Ignores errors.
/// This method is used to advise the system, so its failure is not critical to the result of
/// the program. At worst, failure could hurt performance.
#[cfg(target_os = "linux")]
fn fadvise(file: &File, offset: FilePos, len: FileLen, advice: nix::fcntl::PosixFadviseAdvice) {
    use std::os::unix::io::AsRawFd;
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
fn configure_readahead(file: &File, offset: FilePos, len: FileLen, access: FileAccess) {
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::*;
        let advise = |advice: PosixFadviseAdvice| fadvise(file, offset, len, advice);
        match access {
            FileAccess::Random => advise(PosixFadviseAdvice::POSIX_FADV_RANDOM),
            FileAccess::Sequential => advise(PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL),
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
        use sysinfo::{System, SystemExt};

        let skipped_prefix_len = FileLen(256 * 1024);
        if len > skipped_prefix_len {
            let mut system = System::new();
            system.refresh_memory();
            let free_mem = system.free_memory();
            let total_mem = system.total_memory();
            let free_ratio = free_mem as f32 / total_mem as f32;
            if free_ratio < 0.05 {
                evict_page_cache(
                    file,
                    FilePos::zero() + skipped_prefix_len,
                    len - skipped_prefix_len,
                );
            }
        }
    }
}

/// Opens a file and positions it at the given offset.
/// Additionally, sends the advice to the operating system about how many bytes will be read.
fn open(path: &Path, offset: FilePos, len: FileLen, access_type: FileAccess) -> io::Result<File> {
    let mut file = open_noatime(path)?;
    configure_readahead(&file, offset, len, access_type);
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
        use std::os::unix::fs::OpenOptionsExt;
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
fn stream_hash<H: StreamHasher>(
    stream: &mut impl Read,
    len: FileLen,
    buf_len: usize,
    progress: impl Fn(usize),
) -> io::Result<(FileLen, FileHash)> {
    let mut hasher = H::new();
    let mut read_len: FileLen = FileLen(0);
    scan(stream, len, buf_len, |buf| {
        hasher.update(buf);
        read_len += FileLen(buf.len() as u64);
        (progress)(buf.len());
    })?;
    Ok((read_len, hasher.finish()))
}

/// Computes hash of initial `len` bytes of a file.
/// If the file does not exist or is not readable, print the error to stderr and return `None`.
/// The returned hash is not cryptograhically secure.
fn file_hash<H: StreamHasher>(
    chunk: &FileChunk<'_>,
    buf_len: usize,
    progress: impl Fn(usize),
) -> io::Result<FileHash> {
    let access = if chunk.len.0 < 64 * 1024 {
        FileAccess::Random
    } else {
        FileAccess::Sequential
    };
    let mut file = open(chunk.path, chunk.pos, chunk.len, access)?;
    let hash = stream_hash::<H>(&mut file, chunk.len, buf_len, progress)?.1;
    evict_page_cache_if_low_mem(&mut file, chunk.len);
    Ok(hash)
}

#[cfg(test)]
mod test {
    use metrohash::MetroHash128;
    use sha2::{Sha256, Sha512};
    use std::fs::{create_dir_all, File};
    use std::io::Write;
    use std::path::PathBuf;

    use crate::file::{FileChunk, FileLen, FilePos};
    use crate::hasher::{file_hash, StreamHasher};
    use crate::path::Path;

    fn test_file_hash<H: StreamHasher>() {
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

        let file1 = Path::from(&file1);
        let file2 = Path::from(&file2);
        let chunk1 = FileChunk::new(&file1, FilePos(0), FileLen::MAX);
        let chunk2 = FileChunk::new(&file2, FilePos(0), FileLen::MAX);
        let chunk3 = FileChunk::new(&file2, FilePos(0), FileLen(8));

        let hash1 = file_hash::<H>(&chunk1, 4096, |_| {}).unwrap();
        let hash2 = file_hash::<H>(&chunk2, 4096, |_| {}).unwrap();
        let hash3 = file_hash::<H>(&chunk3, 4096, |_| {}).unwrap();

        assert_ne!(hash1, hash2);
        assert_ne!(hash2, hash3);
    }

    #[test]
    fn test_file_hash_metro_128() {
        test_file_hash::<MetroHash128>()
    }

    #[test]
    fn test_file_hash_blake3() {
        test_file_hash::<blake3::Hasher>()
    }

    #[test]
    fn test_file_hash_sha_256() {
        test_file_hash::<Sha256>()
    }

    #[test]
    fn test_file_hash_sha_512() {
        test_file_hash::<Sha512>()
    }
}
