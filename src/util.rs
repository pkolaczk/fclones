use serde::{Serialize, Serializer};
use std::cell::Cell;

/// Allows for serializing iterators
pub struct IteratorWrapper<T>(pub Cell<Option<T>>);

impl<I, P> Serialize for IteratorWrapper<I>
where
    I: IntoIterator<Item = P>,
    P: Serialize,
{
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.take().unwrap())
    }
}

/// Sorts an array using a key generation function that can fail.
/// Items for which the key could not be obtained are sorted last.
/// Returns vector of errors encountered when obtaining the keys.
pub fn fallible_sort_by_key<T, K, E>(v: &mut Vec<T>, f: impl Fn(&T) -> Result<K, E>) -> Vec<E>
where
    K: Ord,
{
    let mut errors: Vec<E> = Vec::new();
    v.sort_by_key(|t| f(t).map_err(|e| errors.push(e)).ok());
    errors
}

#[cfg(test)]
pub mod test {
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::PathBuf;

    /// Runs test code that needs access to temporary file storage.
    /// Makes sure the test root directory exists and is empty.
    /// Deletes the directory and its contents after the test unless test fails.
    pub fn with_dir<F>(test_root: &str, test_code: F)
    where
        F: FnOnce(&PathBuf),
    {
        let test_root = PathBuf::from(test_root);
        remove_dir_all(&test_root).ok();
        create_dir_all(&test_root).unwrap();
        (test_code)(&test_root.canonicalize().unwrap());
        remove_dir_all(&test_root).unwrap();
    }
}
