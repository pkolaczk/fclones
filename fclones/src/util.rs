use itertools::Itertools;
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
pub fn try_sort_by_key<T, K, E>(v: &mut [T], f: impl Fn(&T) -> Result<K, E>) -> Vec<E>
where
    K: Ord,
{
    let mut errors: Vec<E> = Vec::new();
    v.sort_by_key(|t| f(t).map_err(|e| errors.push(e)).ok());
    errors
}

/// Reduces the elements to a single one, by repeatedly applying a reducing operation.
/// If the iterator is empty, returns `Ok(None)`; otherwise, returns the result of the reduction.
/// If any of the elements is `Err`, returns the first `Err`.
pub fn reduce_results<I, T, E, F>(mut iter: I, f: F) -> Result<Option<T>, E>
where
    I: Iterator<Item = Result<T, E>>,
    F: Fn(T, T) -> T,
{
    iter.fold_ok(None, |res, item| match res {
        Some(res) => Some(f(res, item)),
        None => Some(item),
    })
}

/// Finds the minimum value.
/// If any of the values is `Err`, returns the first `Err`.
/// If the input iterator is empty, returns `Ok(None)`.
pub fn min_result<I, T, E>(iter: I) -> Result<Option<T>, E>
where
    I: Iterator<Item = Result<T, E>>,
    T: Ord,
{
    reduce_results(iter, T::min)
}

/// Finds the maximum value.
/// If any of the values is `Err`, returns the first `Err`.
/// If the input iterator is empty, returns `Ok(None)`.
pub fn max_result<I, T, E>(iter: I) -> Result<Option<T>, E>
where
    I: Iterator<Item = Result<T, E>>,
    T: Ord,
{
    reduce_results(iter, T::max)
}

/// Utility functions intended for testing.
/// Beware they typically panic instead of returning `Err`.
#[cfg(test)]
pub mod test {
    use std::fs::{create_dir_all, remove_dir_all, File};
    use std::io::{BufReader, Read, Write};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::SystemTime;
    use std::{fs, thread};

    use super::*;
    use lazy_static::lazy_static;

    #[derive(Debug, PartialEq, Eq)]
    enum FsSupportsReflink {
        Untested,
        Supported(bool),
    }

    lazy_static! {
        static ref REFLINK_SUPPORTED: Mutex<FsSupportsReflink> =
            Mutex::new(FsSupportsReflink::Untested);
    }

    /// Runs test code that needs access to temporary file storage.
    /// Makes sure the test root directory exists and is empty.
    /// Returns the return value of the test code and recursively deletes
    /// the directory after the test, unless the test fails.
    pub fn with_dir<F, R>(test_root: &str, test_code: F) -> R
    where
        F: FnOnce(&PathBuf) -> R,
    {
        let test_root = PathBuf::from("target/test").join(test_root);

        // Quick sanity check: Joining a relative with an absolute path
        // returns an absolute path.
        if test_root.is_absolute() && !test_root.starts_with("/dev/shm/") {
            panic!("Internal test error: only use relative paths!");
        }

        remove_dir_all(&test_root).ok();
        create_dir_all(&test_root).unwrap();

        let ret = test_code(&test_root.canonicalize().unwrap());

        remove_dir_all(&test_root).ok();
        ret
    }

    /// Creates a new empty file.
    /// If the file existed before, it will be first removed so that the creation time
    /// is updated.
    pub fn create_file(path: &std::path::Path) {
        let _ = fs::remove_file(path);
        File::create(path).unwrap();
    }

    /// Creates a new empty file with creation time after (not equal) the given time.
    ///
    /// This function is used to create multiple files differing by creation time.
    /// It adapts to the operating system timer resolution.
    ///
    /// Returns the creation time of the newly created file.
    /// Panics if `time` is in future or if file could not be created.
    pub fn create_file_newer_than(f: &PathBuf, time: SystemTime) -> SystemTime {
        assert!(SystemTime::now() >= time);
        let mut delay = std::time::Duration::from_millis(1);
        loop {
            thread::sleep(delay);
            create_file(f);
            let ctime = fs::metadata(f).unwrap().modified().unwrap();
            if ctime != time {
                return ctime;
            }
            delay *= 2;
        }
    }

    /// Writes contents to a new file. Overwrites file if it exists.
    /// Panics on errors.
    pub fn write_file(path: &std::path::Path, content: &str) {
        let mut f = File::create(path).unwrap();
        write!(&mut f, "{content}").unwrap();
    }

    /// Reads contents of a file to a string.
    /// Panics on errors.
    pub fn read_file(path: &std::path::Path) -> String {
        let f = File::open(path).unwrap();
        let mut r = BufReader::new(f);
        let mut result = String::new();
        r.read_to_string(&mut result).unwrap();
        result
    }

    pub fn cached_reflink_supported() -> bool {
        let mut guard = REFLINK_SUPPORTED.lock().unwrap();

        match *guard {
            FsSupportsReflink::Untested => {
                with_dir("fs_supports_reflink", |test_dir| {
                    let src_file = test_dir.join("src_file");
                    let dest_file = test_dir.join("dest_file");
                    write_file(&src_file, "1");

                    let result = reflink::reflink(src_file, dest_file).is_ok();
                    *guard = FsSupportsReflink::Supported(result);

                    if !result {
                        println!("  Notice: filesystem does not support reflinks, skipping related tests")
                    }
                    result
                })
            }
            FsSupportsReflink::Supported(val) => val,
        }
    }

    #[test]
    fn min_result_should_return_none_if_no_elements() {
        let elements: Vec<Result<i64, &str>> = vec![];
        assert_eq!(min_result(elements.into_iter()), Ok(None));
    }

    #[test]
    fn min_result_should_return_min_if_all_ok() {
        let elements: Vec<Result<i64, &str>> = vec![Ok(1), Ok(3), Ok(2)];
        assert_eq!(min_result(elements.into_iter()), Ok(Some(1)));
    }

    #[test]
    fn min_result_should_return_err_if_at_least_one_err() {
        let elements: Vec<Result<i64, &str>> = vec![Ok(1), Ok(3), Err("error"), Ok(2)];
        assert_eq!(min_result(elements.into_iter()), Err("error"));
    }

    #[test]
    fn max_result_should_return_max_if_all_ok() {
        let elements: Vec<Result<i64, &str>> = vec![Ok(1), Ok(3), Ok(2)];
        assert_eq!(max_result(elements.into_iter()), Ok(Some(3)));
    }
}
