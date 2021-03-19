use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::mpsc::{channel, Receiver, Sender};

use crossbeam_utils::thread;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::prelude::ParallelSliceMut;
use serde::*;
use smallvec::SmallVec;

use crate::device::{DiskDevices, DiskDevice};
use crate::files::{AsPath, FileHash, FileInfo, FileLen};
use rayon::ThreadPool;

/// Groups items by key.
/// After all items have been added, this structure can be transformed into
/// an iterator over groups.
/// The order of groups in the output iterator is not defined.
/// The order of items in each group matches the order of adding the items by a thread.
///
/// Internally uses a hash map.
/// The amortized complexity of adding an item is O(1).
/// The complexity of reading all groups is O(N).
///
/// # Example
/// ```
/// use smallvec::SmallVec;
/// use fclones::group::GroupMap;
/// let mut map = GroupMap::new(|item: (u32, u32)| (item.0, item.1));
/// map.add((1, 10));
/// map.add((2, 20));
/// map.add((1, 11));
/// map.add((2, 21));
///
/// let mut groups: Vec<_> = map.into_iter().collect();
///
/// groups.sort_by_key(|item| item.0);
/// assert_eq!(groups[0], (1, SmallVec::from_vec(vec![10, 11])));
/// assert_eq!(groups[1], (2, SmallVec::from_vec(vec![20, 21])));
/// ```
///
pub struct GroupMap<T, K, V, F>
where
    K: PartialEq + Hash,
    F: Fn(T) -> (K, V),
{
    item_type: PhantomData<T>,
    groups: HashMap<K, SmallVec<[V; 1]>>,
    split_fn: F,
}

impl<T, K, V, F> GroupMap<T, K, V, F>
where
    K: Eq + Hash,
    F: Fn(T) -> (K, V),
{
    /// Creates a new empty map.
    ///
    /// # Arguments
    /// * `split_fn` - a function generating the key-value pair for each input item
    pub fn new(split_fn: F) -> GroupMap<T, K, V, F> {
        GroupMap {
            item_type: PhantomData,
            groups: HashMap::new(),
            split_fn,
        }
    }

    pub fn with_capacity(capacity: usize, split_fn: F) -> GroupMap<T, K, V, F> {
        GroupMap {
            item_type: PhantomData,
            groups: HashMap::with_capacity(capacity),
            split_fn,
        }
    }

    /// Adds an item to the map.
    /// Note, this doesn't take `&mut self` so this can be called from safely from many threads.
    pub fn add(&mut self, item: T) {
        let (key, new_item) = (self.split_fn)(item);
        self.groups.entry(key).or_default().push(new_item);
    }

    /// Returns number of groups larger than given min_size
    pub fn group_count(&self, min_size: usize) -> usize {
        self.groups.iter().filter(|g| g.1.len() >= min_size).count()
    }
}

impl<T, K, V, F> IntoIterator for GroupMap<T, K, V, F>
where
    K: Eq + Hash,
    F: Fn(T) -> (K, V),
{
    type Item = (K, SmallVec<[V; 1]>);
    type IntoIter = <HashMap<K, SmallVec<[V; 1]>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.groups.into_iter()
    }
}

/// Represents a group of files that have something in common, e.g. same size or same hash
#[derive(Serialize, Debug)]
pub struct FileGroup<F> {
    /// Length of each file
    pub file_len: FileLen,
    /// Hash of a part or the whole of the file
    pub file_hash: FileHash,
    /// Group of files with the same length and hash
    pub files: Vec<F>,
}

impl<F> FileGroup<F>
where
    F: Send,
{
    /// Splits a single group into 0 to n groups based on the given hash function if `cond` is true.
    /// If `cond` is false, then `self` is returned as the only item.
    /// Files hashing to `None` are removed from the output.
    /// An empty vector may be returned if all files hash to `None`.
    pub fn split<H>(self, cond: bool, hash: H) -> Vec<FileGroup<F>>
    where
        H: Fn(&F) -> Option<FileHash> + Sync + Send,
    {
        if cond {
            let file_len = self.file_len;
            let mut hashed = self
                .files
                .into_par_iter()
                .filter_map(|f: F| (hash)(&f).map(|h| (h, f)))
                .collect::<Vec<_>>();

            hashed.sort_by_key(|(h, _p)| h.0);
            hashed
                .into_iter()
                .group_by(|(h, _)| *h)
                .into_iter()
                .map(|(hash, group)| FileGroup {
                    file_len,
                    file_hash: hash,
                    files: group.map(|(_hash, path)| path).collect(),
                })
                .collect()
        } else {
            vec![self]
        }
    }
}

/// Computes metrics for reporting summaries of each processing stage.
pub trait GroupedFileSetMetrics {
    /// Returns the total count of the files
    fn total_count(self) -> usize;

    /// Returns the sum of file lengths
    fn total_size(self) -> FileLen;

    /// Returns the total count of redundant files
    /// # Arguments
    /// * `max_rf` - maximum number of replicas allowed (they won't be counted as redundant)
    fn selected_count(self, rf_over: usize, rf_under: usize) -> usize;

    /// Returns the amount of data in redundant files
    /// # Arguments
    /// * `max_rf` - maximum number of replicas allowed (they won't be counted as redundant)
    fn selected_size(self, rf_over: usize, rf_under: usize) -> FileLen;
}

impl<'a, I, F> GroupedFileSetMetrics for I
where
    I: IntoIterator<Item = &'a FileGroup<F>> + 'a,
    F: 'a,
{
    fn total_count(self) -> usize {
        self.into_iter().map(|g| g.files.len()).sum()
    }

    fn total_size(self) -> FileLen {
        self.into_iter()
            .map(|g| g.file_len * g.files.len() as u64)
            .sum()
    }

    fn selected_count(self, rf_over: usize, rf_under: usize) -> usize {
        self.into_iter()
            .filter(|&g| g.files.len() < rf_under)
            .map(|g| g.files.len().saturating_sub(rf_over))
            .sum()
    }

    fn selected_size(self, rf_over: usize, rf_under: usize) -> FileLen {
        self.into_iter()
            .filter(|&g| g.files.len() < rf_under)
            .map(|g| g.file_len * g.files.len().saturating_sub(rf_over) as u64)
            .sum()
    }
}

/// Helper struct to preserve the original file hash and keep it together with file information
/// Sometimes the old hash must be taken into account, e.g. when combining the prefix hash with
/// the suffix hash.
pub struct HashedFileInfo {
    file_hash: FileHash,
    file_info: FileInfo,
}

/// Partitions files into separate vectors, where each vector holds files persisted
/// on the same disk device. The vectors are returned in the same order as devices.
fn partition_by_devices(
    files: Vec<FileGroup<FileInfo>>,
    devices: &DiskDevices,
) -> Vec<Vec<HashedFileInfo>> {
    let mut result: Vec<Vec<HashedFileInfo>> = Vec::with_capacity(devices.len());
    for _ in 0..devices.len() {
        result.push(Vec::new());
    }
    for g in files {
        for f in g.files {
            let device = devices.get_by_path(&f.path());
            result[device.index].push(HashedFileInfo {
                file_hash: g.file_hash,
                file_info: f,
            });
        }
    }
    result
}

/// Iterates over grouped files, in parallel
pub fn flat_iter(files: &[FileGroup<FileInfo>]) -> impl ParallelIterator<Item = &FileInfo> {
    files.par_iter().flat_map(|g| &g.files)
}

/// Groups files by length and hash computed by given `hash_fn`.
/// Runs in parallel on dedicated thread pools as given by `thead_pool_fn`.
/// Files on different devices are hashed separately from each other.
/// File hashes within a single device are computed in the order given by
/// their `location` field to minimize seek latency.
///
/// Caveats: the original grouping is lost. It is possible for two files that
/// were in the different groups to end up in the same group if they have the same length
/// and they hash to the same value. If you don't want this, you need to combine the old
/// hash with the new hash in the provided `hash_fn`.
pub fn rehash<H, T>(
    files: Vec<FileGroup<FileInfo>>,
    devices: &DiskDevices,
    thread_pool_fn: T,
    hash_fn: H,
    min_group_size: usize
) -> Vec<FileGroup<FileInfo>>
where
    H: Fn(&HashedFileInfo) -> Option<FileHash> + Sync + Send,
    T: Fn(&DiskDevice) -> &ThreadPool + Sync + Send
{
    // to allow sharing functions between threads, take their references
    let hash_fn = &hash_fn;
    let thread_pool_fn = &thread_pool_fn;

    let files = partition_by_devices(files, &devices);

    // Sender/Receiver pair used to
    let (tx, rx): (Sender<HashedFileInfo>, Receiver<HashedFileInfo>) = channel();

    // target map that will receive the hashed files and group them by hashes
    let mut groups =
        GroupMap::new(|f: HashedFileInfo| ((f.file_info.len, f.file_hash), f.file_info));
    let groups_ref = &mut groups;

    // Scope needed so threads can access shared stuff like groups or shared functions
    // The threads we launch are guaranteed to not live longer than this scope
    thread::scope(move |s| {
        // Process all files in background
        for (mut files, device) in files.into_iter().zip(devices.iter()) {
            let tx = tx.clone();

            // Launch a separate thread for each device, so we can process
            // files on each device independently
            s.spawn(move |_| {
                // Sort files by their physical location, to reduce disk seek latency
                files.par_sort_unstable_by_key(|f| f.file_info.location);

                // Run hashing on the thread-pool dedicated to the device
                let thread_pool = thread_pool_fn(&device);
                thread_pool.install(|| {
                    files
                        .into_par_iter()
                        .filter_map(|mut f| {
                            hash_fn(&f).map(|h| {
                                f.file_hash = h;
                                f
                            })
                        })
                        .for_each_with(tx, |tx, hashed_file| tx.send(hashed_file).unwrap());
                });
            });
        }
        // Drop the original tx, so all tx are closed when the threads finish and
        // the next while loop will eventually exit
        drop(tx);

        // Collect the results from all threads and group them.
        // Note that this will happen as soon as data are available
        while let Ok(hashed_file) = rx.recv() {
            groups_ref.add(hashed_file);
        }
    })
    .unwrap();

    // Convert the hashmap into vector, leaving only large-enough groups:
    groups
        .into_iter()
        .filter(|(_, files)| files.len() >= min_group_size)
        .map(|((len, hash), files)| FileGroup {
            file_len: len,
            file_hash: hash,
            files: files.to_vec(),
        })
        .collect()
}
