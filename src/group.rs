use std::hash::Hash;
use std::marker::PhantomData;

use chashmap::CHashMap;
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
/// let map = GroupMap::new(|item: (u32, u32)| (item.0, item.1));
/// map.add((1, 10));
/// map.add((2, 20));
/// map.add((1, 11));
/// map.add((2, 21));
///
/// let mut groups: Vec<(u32, Vec<u32>)> = map.into_iter().collect();
///
/// groups.sort_by_key(|item| item.0);
/// assert_eq!(groups[0], (1, vec![10, 11]));
/// assert_eq!(groups[1], (2, vec![20, 21]));
/// ```
///
pub struct GroupMap<T, K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(T) -> (K, V)
{
    item_type: PhantomData<T>,
    groups: CHashMap<K, Vec<V>>,
    split_fn: F,
}

impl<T, K, V, F> GroupMap<T, K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(T) -> (K, V),
{
    /// Creates a new empty map.
    ///
    /// # Arguments
    /// * `split_fn` - a function generating the key-value pair for each input item
    pub fn new(split_fn: F) -> GroupMap<T, K, V, F> {
        GroupMap { item_type: PhantomData, groups: CHashMap::new(), split_fn }
    }

    /// Adds an item to the map.
    /// Note, this doesn't take `&mut self` so this can be called from safely from many threads.
    pub fn add(&self, item: T) {
        let (key, new_item) = (self.split_fn)(item);
        // TODO: Find a better concurrent map implementation that provides atomic `get_or_insert`
        // We can't add the item directly in the `upsert` method, because we'd have to move it
        // to both arguments and it would have to implement Copy
        self.groups.upsert(key, || vec![], |_| ());
        self.groups.get_mut(&key).unwrap().push(new_item)
    }
}

impl<T, K, V, F> IntoIterator for GroupMap<T, K, V, F>
    where K: PartialEq + Hash + Copy,
          F: Fn(T) -> (K, V),
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
/// use std::convert::identity;
///
/// let mut groups: Vec<(u32, Vec<u32>)> = vec![(1, 10), (2, 20), (1, 11), (2, 21)]
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
pub trait GroupBy<T> {
    fn group_by_key<K, V, F>(self, f: F) -> GroupMap<T, K, V, F>
        where K: PartialEq + Hash + Copy + Sized + Sync + Send,
              V: Send + Sync,
              F: (Fn(T) -> (K, V)) + Sync;
}

impl<T, In> GroupBy<T> for In
    where T: Sync + Send, In: ParallelIterator<Item=T>
{
    fn group_by_key<K, V, F>(self, f: F) -> GroupMap<T, K, V, F>
        where K: PartialEq + Hash + Copy + Sized + Sync + Send,
              V: Send + Sync,
              F: (Fn(T) -> (K, V)) + Sync
    {
        let grouping_map = GroupMap::new(f);
        self.for_each(|item| grouping_map.add(item));
        grouping_map
    }
}
