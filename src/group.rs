use std::collections::BTreeMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};

use crossbeam_utils::thread;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::prelude::ParallelSliceMut;
use serde::*;
use smallvec::SmallVec;
use sysinfo::DiskType;

use crate::device::DiskDevices;
use crate::files::{FileHash, FileInfo, FileLen};
use crate::semaphore::Semaphore;

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
    groups: BTreeMap<K, SmallVec<[V; 1]>>,
    split_fn: F,
}

impl<T, K, V, F> GroupMap<T, K, V, F>
where
    K: Eq + Hash + Ord,
    F: Fn(T) -> (K, V),
{
    /// Creates a new empty map.
    ///
    /// # Arguments
    /// * `split_fn` - a function generating the key-value pair for each input item
    pub fn new(split_fn: F) -> GroupMap<T, K, V, F> {
        GroupMap {
            item_type: PhantomData,
            groups: BTreeMap::new(),
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
    type IntoIter = <BTreeMap<K, SmallVec<[V; 1]>> as IntoIterator>::IntoIter;

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
struct HashedFileInfo {
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
            let device = &devices[f.get_device_index()];
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

#[derive(Copy, Clone, Debug)]
pub enum AccessType {
    Sequential,
    Random,
}

/// Groups files by length and hash computed by given `hash_fn`.
/// Runs in parallel on dedicated thread pools.
/// Files on different devices are hashed separately from each other.
/// File hashes within a single device are computed in the order given by
/// their `location` field to minimize seek latency.
///
/// Caveats: the original grouping is lost. It is possible for two files that
/// were in the different groups to end up in the same group if they have the same length
/// and they hash to the same value. If you don't want this, you need to combine the old
/// hash with the new hash in the provided `hash_fn`.
pub fn rehash<'a, F1, F2, H>(
    groups: Vec<FileGroup<FileInfo>>,
    group_pre_filter: F1,
    group_post_filter: F2,
    devices: &DiskDevices,
    access_type: AccessType,
    hash_fn: H,
) -> Vec<FileGroup<FileInfo>>
where
    F1: Fn(&FileGroup<FileInfo>) -> bool,
    F2: Fn(&FileGroup<FileInfo>) -> bool,
    H: Fn((&mut FileInfo, FileHash)) -> Option<FileHash> + Sync + Send + 'a,
{

    // Allow sharing the hash function between threads:
    type HT<'a> = dyn Fn((&mut FileInfo, FileHash)) -> Option<FileHash> + Sync + Send + 'a;
    let hash_fn: &HT<'a> = &hash_fn;

    let (tx, rx): (Sender<HashedFileInfo>, Receiver<HashedFileInfo>) = channel();

    // There is no point in processing groups containing a single file.
    // Normally when searching for duplicates such groups are filtered out automatically after
    // each stage, however they are possible when searching for unique files.
    let (groups_to_process, groups_to_pass): (Vec<_>, Vec<_>) =
        groups.into_iter().partition(group_pre_filter);

    // This way we can split processing to separate thread-pools, one per device:
    let files = partition_by_devices(groups_to_process, &devices);
    let mut hash_map = GroupMap::new(|f: HashedFileInfo| {
        ((f.file_info.len, f.file_hash), f.file_info)
    });
    let hash_map_ref = &mut hash_map;

    // Scope needed so threads can access shared stuff like groups or shared functions.
    // The threads we launch are guaranteed to not live longer than this scope.
    thread::scope(move |s| {
        // Process all files in background
        for (mut files, device) in files.into_iter().zip(devices.iter()) {
            if files.is_empty() {
                continue;
            }

            let tx = tx.clone();

            // Launch a separate thread for each device, so we can process
            // files on each device independently
            s.spawn(move |_| {
                // Sort files by their physical location, to reduce disk seek latency
                if device.disk_type != DiskType::SSD {
                    files.par_sort_unstable_by_key(|f| f.file_info.location);
                }

                // Some devices like HDDs may benefit from different amount of parallelism
                // depending on the access type. Therefore we chose a thread pool appropriate
                // for the access type
                let thread_pool = match access_type {
                    AccessType::Sequential => device.seq_thread_pool(),
                    AccessType::Random => device.rand_thread_pool(),
                };

                let thread_count = thread_pool.current_num_threads() as isize;

                // Limit the number of tasks spawned at once into the thread-pool.
                // Each task creates a heap allocation and reserves memory in the queue.
                // It is more memory efficient to keep these tasks as long as possible
                // in our vector. Without this limit we observed over 20% more memory use
                // when processing 1M of files.
                let semaphore = Arc::new(Semaphore::new(8 * thread_count));

                // Run hashing on the thread-pool dedicated to the device
                for mut f in files {
                    let tx = tx.clone();
                    let guard = semaphore.clone().access_owned();

                    // Spawning a task into a thread-pool requires a static lifetime,
                    // because generally the task could outlive caller's stack frame.
                    // However, this is not the case for rehash function, because
                    // we don't exit before all tasks are processed.
                    // In the perfect world we should use scopes for that. Unfortunately
                    // the current implementation of rayon scopes runs the scope body
                    // on one of the thread-pool worker threads, so it is not possible
                    // to safely block inside the scope, because that leads to deadlock
                    // when the pool has only one thread.
                    let hash_fn: &HT<'static> = unsafe {  std::mem::transmute(hash_fn) };
                    thread_pool.spawn_fifo(move || {
                        if let Some(hash) = hash_fn((&mut f.file_info, f.file_hash)) {
                            f.file_hash = hash;
                            tx.send(f).unwrap();
                        }
                        // This forces moving the guard into this task and be released when
                        // the task is done
                        drop(guard);
                    });
                }
            });
        }
        // Drop the original tx, so all tx are closed when the threads finish and
        // the next while loop will eventually exit
        drop(tx);

        // Collect the results from all threads and group them.
        // Note that this will happen as soon as data are available
        while let Ok(hashed_file) = rx.recv() {
            hash_map_ref.add(hashed_file);
        }
    })
    .unwrap();

    // Convert the hashmap into vector, leaving only large-enough groups:
    hash_map
        .into_iter()
        .map(|((len, hash), files)| FileGroup {
            file_len: len,
            file_hash: hash,
            files: files.to_vec(),
        })
        .filter(group_post_filter)
        .chain(groups_to_pass.into_iter())
        .collect()
}
