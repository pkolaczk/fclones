//! Persistent caching of file hashes

use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sled::IVec;

use crate::error::Error;
use crate::file::{FileChunk, FileHash, FileLen, FileMetadata, FilePos};
use crate::hasher::HashAlgorithm;
use crate::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct Key {
    file_id: u128,
    device_id: u64,
    chunk_pos: FilePos,
    chunk_len: FileLen,
    algorithm: HashAlgorithm,
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.device_id, self.file_id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedFileInfo {
    modified_timestamp_us: u128,
    len: FileLen,
    hash: FileHash,
}

/// Caches file hashes to avoid repeated computations in subsequent runs of fclones.
///
/// Most files don't change very frequently so their hashes don't change.
/// Usually it is a lot faster to retrieve the hash from an embedded database that to compute
/// them from file data.
pub struct HashCache {
    cache: sled::Db,
}

impl HashCache {
    /// Opens the file hash database located in the given directory.
    /// If the database doesn't exist yet, creates a new one.
    pub fn open(database_path: &Path) -> Result<HashCache, Error> {
        create_dir_all(&database_path.to_path_buf()).map_err(|e| {
            format!(
                "Count not create hash database directory {}: {}",
                database_path.to_escaped_string(),
                e
            )
        })?;
        let cache = sled::open(&database_path.to_path_buf()).map_err(|e| {
            format!(
                "Failed to open hash database at {}: {}",
                database_path.to_escaped_string(),
                e
            )
        })?;
        Ok(HashCache { cache })
    }

    /// Opens the file hash database located in `fclones` subdir of user cache directory.
    /// If the database doesn't exist yet, creates a new one.
    pub fn open_default() -> Result<HashCache, Error> {
        let cache_dir =
            dirs::cache_dir().ok_or("Could not obtain user cache directory from the system.")?;
        let hash_db_path = cache_dir.join("fclones");
        Self::open(&Path::from(hash_db_path))
    }

    /// Stores the file hash plus some file metadata in the cache.
    pub fn put(&self, key: &Key, file: &FileMetadata, hash: FileHash) -> Result<(), Error> {
        let value = CachedFileInfo {
            modified_timestamp_us: file
                .modified()
                .map_err(|e| format!("Unable to get file modification timestamp: {}", e))?
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_micros(),
            len: file.len(),
            hash,
        };

        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();
        self.cache
            .insert(key, value)
            .map_err(|e| format!("Failed to write entry to cache: {}", e))?;
        Ok(())
    }

    /// Retrieves the cached hash of a file.
    ///
    /// Returns `Ok(None)` if file is not present in the cache or if its current length
    /// or its current modification time do not match the file length and modification time
    /// recorded at insertion time.
    pub fn get(&self, key: &Key, metadata: &FileMetadata) -> Result<Option<FileHash>, Error> {
        let key = bincode::serialize(&key).unwrap();
        let value: Option<IVec> = self
            .cache
            .get(key)
            .map_err(|e| format!("Failed to retrieve entry from cache: {}", e))?;
        let value = match value {
            Some(v) => v,
            None => return Ok(None), // not found in cache
        };
        let value: CachedFileInfo = bincode::deserialize(&value)
            .map_err(|e| format!("Failed to deserialize value from cache: {}", e))?;

        let modified = metadata
            .modified()
            .map_err(|e| format!("Unable to get file modification timestamp: {}", e))?
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros();

        if value.modified_timestamp_us != modified || value.len != metadata.len() {
            Ok(None) // found in cache, but the file has changed since it was cached
        } else {
            Ok(Some(value.hash))
        }
    }

    /// Returns the cache key for a file.
    ///
    /// Using file identifiers as cache keys instead of paths allows the user for moving or renaming
    /// files without losing their cached hash data.
    pub fn key(
        &self,
        chunk: &FileChunk<'_>,
        metadata: &FileMetadata,
        algorithm: HashAlgorithm,
    ) -> Result<Key, Error> {
        let key = Key {
            file_id: metadata
                .inode_id()
                .map_err(|e| format!("Unable to get file id: {}", e))?,
            device_id: metadata
                .device_id()
                .map_err(|e| format!("Unable to get device id: {}", e))?,
            chunk_pos: chunk.pos,
            chunk_len: chunk.len,
            algorithm,
        };
        Ok(key)
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;

    use crate::cache::HashCache;
    use crate::file::{FileChunk, FileHash, FileLen, FileMetadata, FilePos};
    use crate::hasher::HashAlgorithm;
    use crate::path::Path;
    use crate::util::test::{create_file, with_dir};

    #[test]
    fn return_cached_hash_if_file_hasnt_changed() {
        with_dir("cache/return_cached_hash_if_file_hasnt_changed", |root| {
            let path = root.join("file");
            create_file(&path);
            let path = Path::from(&path);
            let metadata = FileMetadata::new(&path).unwrap();
            let chunk = FileChunk::new(&path, FilePos(0), FileLen(1000));

            let cache_path = Path::from(root.join("cache"));
            let cache = HashCache::open(&cache_path).unwrap();
            let key = cache
                .key(&chunk, &metadata, HashAlgorithm::MetroHash128)
                .unwrap();
            let orig_hash = FileHash(12345);

            cache.put(&key, &metadata, orig_hash).unwrap();
            let cached_hash = cache.get(&key, &metadata).unwrap();

            assert_eq!(cached_hash, Some(orig_hash))
        });
    }

    #[test]
    fn return_none_if_file_has_changed() {
        with_dir("cache/return_none_if_file_has_changed", |root| {
            let path = root.join("file");
            create_file(&path);
            let path = Path::from(&path);
            let metadata = FileMetadata::new(&path).unwrap();
            let chunk = FileChunk::new(&path, FilePos(0), FileLen(1000));

            let cache_path = Path::from(root.join("cache"));
            let cache = HashCache::open(&cache_path).unwrap();
            let key = cache
                .key(&chunk, &metadata, HashAlgorithm::MetroHash128)
                .unwrap();
            cache.put(&key, &metadata, FileHash(12345)).unwrap();

            // modify the file
            use std::io::Write;

            let mut f = OpenOptions::new()
                .append(true)
                .open(path.to_path_buf())
                .unwrap();
            write!(f, "text").unwrap();
            drop(f);

            let metadata = FileMetadata::new(&path).unwrap();
            let cached_hash = cache.get(&key, &metadata).unwrap();
            assert_eq!(cached_hash, None)
        });
    }

    #[test]
    fn return_none_if_asked_for_a_different_chunk() {
        with_dir("cache/return_none_if_asked_for_a_different_chunk", |root| {
            let path = root.join("file");
            create_file(&path);
            let path = Path::from(&path);
            let metadata = FileMetadata::new(&path).unwrap();
            let chunk = FileChunk::new(&path, FilePos(0), FileLen(1000));

            let cache_path = Path::from(root.join("cache"));
            let cache = HashCache::open(&cache_path).unwrap();
            let key = cache
                .key(&chunk, &metadata, HashAlgorithm::MetroHash128)
                .unwrap();

            cache.put(&key, &metadata, FileHash(12345)).unwrap();

            let chunk = FileChunk::new(&path, FilePos(1000), FileLen(2000));
            let key = cache
                .key(&chunk, &metadata, HashAlgorithm::MetroHash128)
                .unwrap();
            let cached_hash = cache.get(&key, &metadata).unwrap();

            assert_eq!(cached_hash, None)
        });
    }
}
