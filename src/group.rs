use std::hash::Hash;
use std::marker::PhantomData;
use std::path::PathBuf;

use dashmap::DashMap;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::files::{FileHash, FileLen};

/// Groups items by key.
/// After all items have been added, this structure can be transformed into
/// an iterator over groups.
/// The order of groups in the output iterator is not defined.
/// The order of items in each group matches the order of adding the items by a thread.
/// Items can be added from multiple threads.
///
/// Internally uses a concurrent hash map.
/// The amortized complexity of adding an item is O(1).
/// The complexity of reading all groups is O(N).
///
/// # Example
/// ```
/// use fclones::group::GroupMap;
/// let map = GroupMap::new(|item: (u32, u32)| (item.0, item.1));
/// map.add((1, 10));
/// map.add((2, 20));
/// map.add((1, 11));
/// map.add((2, 21));
///
/// let mut groups: Vec<_> = map.into_iter().collect();
///
/// groups.sort_by_key(|item| item.0);
/// assert_eq!(groups[0], (1, vec![10, 11]));
/// assert_eq!(groups[1], (2, vec![20, 21]));
/// ```
///
pub struct GroupMap<T, K, V, F>
    where K: PartialEq + Hash,
          F: Fn(T) -> (K, V)
{
    item_type: PhantomData<T>,
    groups: DashMap<K, Vec<V>>,
    split_fn: F,
}

impl<T, K, V, F> GroupMap<T, K, V, F>
    where K: Eq + Hash,
          F: Fn(T) -> (K, V),
{
    /// Creates a new empty map.
    ///
    /// # Arguments
    /// * `split_fn` - a function generating the key-value pair for each input item
    pub fn new(split_fn: F) -> GroupMap<T, K, V, F> {
        GroupMap { item_type: PhantomData, groups: DashMap::new(), split_fn }
    }

    /// Adds an item to the map.
    /// Note, this doesn't take `&mut self` so this can be called from safely from many threads.
    pub fn add(&self, item: T) {
        let (key, new_item) = (self.split_fn)(item);
        self.groups
            .entry(key)
            .or_insert(vec![])
            .push(new_item);
    }
}

impl<T, K, V, F> IntoIterator for GroupMap<T, K, V, F>
    where K: Eq + Hash,
          F: Fn(T) -> (K, V),
{
    type Item = (K, Vec<V>);
    type IntoIter = <DashMap<K, Vec<V>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.groups.into_iter()
    }
}

/// Implemented by collections of items that can be grouped in parallel according to some key.
/// The value of the key for each item is provided by a key-generating function.
///
/// # Example
/// ```
/// use rayon::prelude::*;
/// use fclones::group::*;
/// use std::convert::identity;
///
/// let mut groups: Vec<_> = vec![(1, 10), (2, 20), (1, 11), (2, 21)]
///     .into_par_iter()
///     .group_by_key(identity)
///     .into_iter()
///     .collect();
///
/// // The results may come in any order, so let's make them deterministic:
/// groups.sort_by_key(|item| item.0);
/// groups[0].1.sort();
/// groups[1].1.sort();
///
/// assert_eq!(groups[0], (1, vec![10, 11]));
/// assert_eq!(groups[1], (2, vec![20, 21]));
///
/// ```
pub trait GroupByKey<T> {
    fn group_by_key<K, V, F>(self, f: F) -> GroupMap<T, K, V, F>
        where K: Eq + Hash + Sync + Send,
              V: Send + Sync,
              F: (Fn(T) -> (K, V)) + Sync;
}

impl<T, In> GroupByKey<T> for In
    where T: Sync + Send, In: ParallelIterator<Item=T>
{
    fn group_by_key<K, V, F>(self, f: F) -> GroupMap<T, K, V, F>
        where K: Eq + Hash + Sync + Send,
              V: Send + Sync,
              F: (Fn(T) -> (K, V)) + Sync
    {
        let grouping_map = GroupMap::new(f);
        self.for_each(|item| grouping_map.add(item));
        grouping_map
    }
}


/// Represents a group of files that have something in common, e.g. same size or same hash
pub struct FileGroup {
    pub len: FileLen,
    pub hash: Option<FileHash>,
    pub files: Vec<PathBuf>
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


impl<'a, I> GroupedFileSetMetrics for I
    where I: IntoIterator<Item=&'a FileGroup> + 'a
{
    fn total_count(self) -> usize {
        self.into_iter().map(|g| g.files.len()).sum()
    }

    fn total_size(self) -> FileLen {
        self.into_iter().map(|g| g.len * g.files.len() as u64).sum()
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
            .map(|g| g.len * g.files.len().saturating_sub(rf_over) as u64)
            .sum()
    }
}

pub trait SplitGroups<G, H> {

    /// Splits a collection of file groups into a collection of smaller
    /// file groups according to file hashes generated by `hash` function.
    /// The items for which the grouping function returns `None` are not further processed.
    ///
    /// This method does not merge groups together. If items in two different input groups
    /// generate the same key, they will remain in separate groups in the output.
    ///
    /// The operation is performed in parallel, so all arguments must be thread-safe.
    /// The order of items in the output is unspecified and must not be relied upon.
    ///
    /// # Example
    /// ```
    /// use rayon::iter::ParallelIterator;
    /// use fclones::files::{FileLen, FileHash};
    /// use std::path::PathBuf;
    /// use fasthash::spooky::Hasher128;
    /// use fasthash::{FastHasher, HasherExt};
    /// use std::hash::Hasher;
    /// use fclones::group::{FileGroup, SplitGroups};
    ///
    /// let g = FileGroup { len: FileLen(10), hash: None, files: vec![
    ///     PathBuf::from("file1"),
    ///     PathBuf::from("file2")
    /// ]};
    ///
    /// fn hash(len: FileLen, hash: Option<FileHash>, path: &PathBuf) -> Option<FileHash> {
    ///     let mut hasher = Hasher128::new();
    ///     hasher.write(path.to_str().unwrap().as_bytes());
    ///     Some(FileHash(hasher.finish_ext()))
    /// }
    ///
    /// let mut groups = vec![g].split(1, hash);
    /// groups.sort_by_key(|g| g.files[0].clone());
    /// assert_eq!(groups[0].len, FileLen(10));
    /// assert_eq!(groups[0].files, vec![PathBuf::from("file1")]);
    /// assert_eq!(groups[1].len, FileLen(10));
    /// assert_eq!(groups[1].files, vec![PathBuf::from("file2")]);
    /// ```
    fn split(self, min_group_size: usize, hash: H) -> Vec<G>;
}

impl<H> SplitGroups<FileGroup, H> for Vec<FileGroup>
    where H: Fn(FileLen, Option<FileHash>, &PathBuf) -> Option<FileHash> + Sync + Send
{
    fn split(self, min_group_size: usize, hash: H) -> Vec<FileGroup> {
        self.into_par_iter()
            .flat_map(|g: FileGroup| {
                if g.files.len() <= 1 {
                    vec![g]
                } else {
                    split_single(g, &hash)
                }
            })
            .filter(|g| g.files.len() >= min_group_size)
            .collect()
    }
}

fn split_single<H>(g: FileGroup, hash: &H) -> Vec<FileGroup>
where H: Fn(FileLen, Option<FileHash>, &PathBuf) -> Option<FileHash> + Sync + Send
{
    let file_len = g.len;
    let file_hash = g.hash;
    g.files
        .into_par_iter()
        .filter_map(|p: PathBuf| (hash)(file_len, file_hash, &p).map(|h: FileHash| (h, p)))
        .collect::<Vec<_>>()
        .into_iter()
        .group_by(|(h, _p)| *h)
        .into_iter()
        .map(|(hash, group)| FileGroup {
            len: file_len,
            hash: Some(hash),
            files: group.map(|(_hash, path)| path).collect()
        })
        .collect()
}