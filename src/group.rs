use std::collections::BTreeMap;
use std::hash::Hash;
use std::marker::PhantomData;

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
pub(crate) struct GroupMap<T, K, V, F>
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

#[cfg(test)]
mod test {

    #[test]
    fn items_should_be_split_into_groups() {
        use super::GroupMap;
        use smallvec::SmallVec;

        let mut map = GroupMap::new(|item: (u32, u32)| (item.0, item.1));
        map.add((1, 10));
        map.add((2, 20));
        map.add((1, 11));
        map.add((2, 21));

        let mut groups: Vec<_> = map.into_iter().collect();

        groups.sort_by_key(|item| item.0);
        assert_eq!(groups[0], (1, SmallVec::from_vec(vec![10, 11])));
        assert_eq!(groups[1], (2, SmallVec::from_vec(vec![20, 21])));
    }
}
