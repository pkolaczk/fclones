//! Persistent caching of file hashes

use crossbeam_channel::RecvTimeoutError;
use std::fmt::{Display, Formatter};
use std::fs::create_dir_all;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::file::{FileChunk, FileHash, FileId, FileLen, FileMetadata, FilePos};
use crate::hasher::HashFn;
use crate::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct Key {
    file_id: FileId,
    chunk_pos: FilePos,
    chunk_len: FileLen,
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file_id.device, self.file_id.inode)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedFileInfo {
    modified_timestamp_ms: u64,
    file_len: FileLen,
    data_len: FileLen,
    hash: FileHash,
}

type InnerCache = typed_sled::Tree<Key, CachedFileInfo>;

const FLUSH_INTERVAL: Duration = Duration::from_millis(1000);

/// Caches file hashes to avoid repeated computations in subsequent runs of fclones.
///
/// Most files don't change very frequently so their hashes don't change.
/// Usually it is a lot faster to retrieve the hash from an embedded database that to compute
/// them from file data.
pub struct HashCache {
    cache: Arc<InnerCache>,
    flusher: HashCacheFlusher,
}

impl HashCache {
    /// Opens the file hash database located in the given directory.
    /// If the database doesn't exist yet, creates a new one.
    pub fn open(
        database_path: &Path,
        transform: Option<&str>,
        algorithm: HashFn,
    ) -> Result<HashCache, Error> {
        create_dir_all(database_path.to_path_buf()).map_err(|e| {
            format!(
                "Count not create hash database directory {}: {}",
                database_path.to_escaped_string(),
                e
            )
        })?;
        let db = sled::open(database_path.to_path_buf()).map_err(|e| {
            format!(
                "Failed to open hash database at {}: {}",
                database_path.to_escaped_string(),
                e
            )
        })?;

        let tree_id = format!("hash_db:{:?}:{}", algorithm, transform.unwrap_or("<none>"));
        let cache = Arc::new(typed_sled::Tree::open(&db, tree_id));
        let flusher = HashCacheFlusher::start(&cache);
        Ok(HashCache { cache, flusher })
    }

    /// Opens the file hash database located in `fclones` subdir of user cache directory.
    /// If the database doesn't exist yet, creates a new one.
    pub fn open_default(transform: Option<&str>, algorithm: HashFn) -> Result<HashCache, Error> {
        let cache_dir =
            dirs::cache_dir().ok_or("Could not obtain user cache directory from the system.")?;
        let hash_db_path = cache_dir.join("fclones");
        Self::open(&Path::from(hash_db_path), transform, algorithm)
    }

    /// Stores the file hash plus some file metadata in the cache.
    pub fn put(
        &self,
        key: &Key,
        file: &FileMetadata,
        data_len: FileLen,
        hash: FileHash,
    ) -> Result<(), Error> {
        let value = CachedFileInfo {
            modified_timestamp_ms: file
                .modified()
                .map_err(|e| format!("Unable to get file modification timestamp: {e}"))?
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64,
            file_len: file.len(),
            data_len,
            hash,
        };

        self.cache
            .insert(key, &value)
            .map_err(|e| format!("Failed to write entry to cache: {e}"))?;

        // Check for cache flush errors. If there were errors, report them to the caller.
        match self.flusher.err_channel.try_recv() {
            Ok(err) => Err(err),
            Err(_) => Ok(()),
        }
    }

    /// Retrieves the cached hash of a file.
    ///
    /// Returns `Ok(None)` if file is not present in the cache or if its current length
    /// or its current modification time do not match the file length and modification time
    /// recorded at insertion time.
    pub fn get(
        &self,
        key: &Key,
        metadata: &FileMetadata,
    ) -> Result<Option<(FileLen, FileHash)>, Error> {
        let value = self
            .cache
            .get(key)
            .map_err(|e| format!("Failed to retrieve entry from cache: {e}"))?;
        let value = match value {
            Some(v) => v,
            None => return Ok(None), // not found in cache
        };

        let modified = metadata
            .modified()
            .map_err(|e| format!("Unable to get file modification timestamp: {e}"))?
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        if value.modified_timestamp_ms != modified || value.file_len != metadata.len() {
            Ok(None) // found in cache, but the file has changed since it was cached
        } else {
            Ok(Some((value.data_len, value.hash)))
        }
    }

    /// Returns the cache key for a file.
    ///
    /// Using file identifiers as cache keys instead of paths allows the user for moving or renaming
    /// files without losing their cached hash data.
    pub fn key(&self, chunk: &FileChunk<'_>, metadata: &FileMetadata) -> Result<Key, Error> {
        let key = Key {
            file_id: metadata.file_id(),
            chunk_pos: chunk.pos,
            chunk_len: chunk.len,
        };
        Ok(key)
    }

    /// Flushes all unwritten data and closes the cache.
    pub fn close(self) -> Result<(), Error> {
        self.cache
            .flush()
            .map_err(|e| format!("Failed to flush cache: {e}"))?;
        Ok(())
    }
}

/// Periodically flushes the cache in a background thread
struct HashCacheFlusher {
    thread_handle: Option<JoinHandle<()>>,
    control_channel: Option<crossbeam_channel::Sender<()>>,
    err_channel: crossbeam_channel::Receiver<Error>,
}

impl HashCacheFlusher {
    fn start(cache: &Arc<InnerCache>) -> HashCacheFlusher {
        let cache = Arc::downgrade(cache);
        let (ctrl_tx, ctrl_rx) = crossbeam_channel::bounded::<()>(1);
        let (err_tx, err_rx) = crossbeam_channel::bounded(1);

        let thread_handle = thread::spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = ctrl_rx.recv_timeout(FLUSH_INTERVAL) {
                if let Some(cache) = cache.upgrade() {
                    if let Err(e) = cache.flush() {
                        err_tx
                            .send(format!("Failed to flush the hash cache: {e}").into())
                            .unwrap_or_default();
                        return;
                    }
                }
            }
        });

        HashCacheFlusher {
            thread_handle: Some(thread_handle),
            control_channel: Some(ctrl_tx),
            err_channel: err_rx,
        }
    }
}

impl Drop for HashCacheFlusher {
    fn drop(&mut self) {
        // Signal the flusher thread to exit:
        drop(self.control_channel.take());
        // Wait for the flusher thread to exit:
        self.thread_handle.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;

    use crate::cache::HashCache;
    use crate::file::{FileChunk, FileHash, FileLen, FileMetadata, FilePos};
    use crate::hasher::HashFn;
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
            let cache = HashCache::open(&cache_path, None, HashFn::Metro).unwrap();
            let key = cache.key(&chunk, &metadata).unwrap();
            let orig_hash = FileHash::from(12345);

            let data_len = FileLen(200);
            cache
                .put(&key, &metadata, data_len, orig_hash.clone())
                .unwrap();
            let cached_hash = cache.get(&key, &metadata).unwrap();

            assert_eq!(cached_hash, Some((data_len, orig_hash)))
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
            let cache = HashCache::open(&cache_path, None, HashFn::Metro).unwrap();
            let key = cache.key(&chunk, &metadata).unwrap();
            cache
                .put(&key, &metadata, chunk.len, FileHash::from(12345))
                .unwrap();

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
            let cache = HashCache::open(&cache_path, None, HashFn::Metro).unwrap();
            let key = cache.key(&chunk, &metadata).unwrap();

            cache
                .put(&key, &metadata, chunk.len, FileHash::from(12345))
                .unwrap();

            let chunk = FileChunk::new(&path, FilePos(1000), FileLen(2000));
            let key = cache.key(&chunk, &metadata).unwrap();
            let cached_hash = cache.get(&key, &metadata).unwrap();

            assert_eq!(cached_hash, None)
        });
    }

    #[test]
    fn return_none_if_different_transform_was_used() {
        with_dir(
            "cache/return_none_if_different_transform_was_used",
            |root| {
                let path = root.join("file");
                create_file(&path);
                let path = Path::from(&path);
                let metadata = FileMetadata::new(&path).unwrap();
                let chunk = FileChunk::new(&path, FilePos(0), FileLen(1000));

                let cache_path = Path::from(root.join("cache"));
                let cache = HashCache::open(&cache_path, None, HashFn::Metro).unwrap();
                let key = cache.key(&chunk, &metadata).unwrap();

                let orig_hash = FileHash::from(12345);
                let data_len = FileLen(200);
                cache
                    .put(&key, &metadata, data_len, orig_hash.clone())
                    .unwrap();
                let cached_hash = cache.get(&key, &metadata).unwrap();
                assert_eq!(cached_hash, Some((data_len, orig_hash)));
                drop(cache); // unlock the db so we can open another cache

                let cache = HashCache::open(&cache_path, Some("transform"), HashFn::Metro).unwrap();
                let cached_hash = cache.get(&key, &metadata).unwrap();
                assert_eq!(cached_hash, None);
            },
        );
    }
}
