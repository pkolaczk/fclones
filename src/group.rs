use chashmap::CHashMap;
use either::Either;
use either::Either::{Left, Right};
use std::hash::Hash;
use rayon::iter::ParallelIterator;

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
/// use dff::group::GroupMap;
/// let map = GroupMap::new(|item: &(u32, u32)| item.0);
/// map.add((1, 10));
/// map.add((2, 20));
/// map.add((1, 11));
/// map.add((2, 21));
///
/// let mut groups: Vec<(u32, Vec<(u32, u32)>)> = map.into_iter().collect();
///
/// groups.sort_by_key(|item| item.0);
/// assert_eq!(groups[0], (1, vec![(1, 10), (1, 11)]));
/// assert_eq!(groups[1], (2, vec![(2, 20), (2, 21)]));
/// ```
///
pub struct GroupMap<K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(&V) -> K
{
    groups: CHashMap<K, Vec<V>>,
    key_by: F,
}

impl<K, V, F> GroupMap<K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(&V) -> K
{
    /// Creates a new empty map.
    pub fn new(key_fn: F) -> GroupMap<K, V, F> {
        GroupMap { groups: CHashMap::new(), key_by: key_fn }
    }

    /// Adds an item to the map.
    /// Note, this doesn't require `&mut self` so this can be called
    /// from safely from many threads.
    pub fn add(&self, item: V) {
        let key = (self.key_by)(&item);
        // TODO: Find a better concurrent map implementation that provides atomic `get_or_insert`
        // We can't add the item directly in the `upsert` method, because we'd have to move it
        // to both arguments and it would have to implement Copy
        self.groups.upsert(key, || vec![], |v| ());
        self.groups.get_mut(&key).unwrap().push(item)
    }
}

impl<K, V, F> IntoIterator for GroupMap<K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(&V) -> K
{
    type Item = (K, Vec<V>);
    type IntoIter = chashmap::IntoIter<K, Vec<V>>;

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
/// use dff::group::*;
///
/// let mut groups: Vec<(u32, Vec<(u32, u32)>)> = vec![(1, 10), (2, 20), (1, 11), (2, 21)]
///     .into_par_iter()
///     .group_by_key(|&item| item.0)
///     .into_iter()
///     .collect();
///
/// // The results may come in any order, so let's make them deterministic:
/// groups.sort_by_key(|item| item.0);
/// groups[0].1.sort_by_key(|item| item.1);
/// groups[1].1.sort_by_key(|item| item.1);
///
/// assert_eq!(groups[0], (1, vec![(1, 10), (1, 11)]));
/// assert_eq!(groups[1], (2, vec![(2, 20), (2, 21)]));
///
/// ```
pub trait GroupBy<V> {
    fn group_by_key<K, F>(self, f: F) -> GroupMap<K, V, F>
        where K: PartialEq + Hash + Copy + Sized + Sync + Send,
              F: (Fn(&V) -> K) + Sync;
}

impl<V, In> GroupBy<V> for In
    where V: Sync + Send, In: ParallelIterator<Item=V>
{
    fn group_by_key<K, F>(self, f: F) -> GroupMap<K, V, F>
        where K: PartialEq + Hash + Copy + Sized + Sync + Send,
              F: (Fn(&V) -> K) + Sync
    {
        let grouping_map = GroupMap::new(f);
        self.for_each(|item| grouping_map.add(item));
        grouping_map
    }
}
