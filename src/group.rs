use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::*;

use crate::files::{FileHash, FileLen};
use std::cmp::{max, min};
use smallvec::SmallVec;

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
    /// Min length of the prefix such that all files in the group have the same prefix
    pub prefix_len: FileLen,
    /// Hash of a part or the whole of the file
    pub hash: FileHash,

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
    pub fn split<H>(self, cond: bool, prefix_len: FileLen, hash: H) -> Vec<FileGroup<F>>
    where
        H: Fn(&F) -> Option<FileHash> + Sync + Send,
    {
        if cond {
            let file_len = self.file_len;
            let prefix_len = min(self.file_len, max(self.prefix_len, prefix_len));

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
                    prefix_len,
                    hash,
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
