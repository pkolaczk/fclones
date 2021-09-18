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

    use lazy_static::lazy_static;

    #[derive(Debug, PartialEq, Eq)]
    enum FsReflink {
        Untested,
        Supported(bool),
    }

    lazy_static! {
        static ref REFLINK_SUPPORTED: Mutex<FsReflink> = Mutex::new(FsReflink::Untested);
    }

    /// Runs test code that needs access to temporary file storage.
    /// Makes sure the test root directory exists and is empty.
    /// Deletes the directory and its contents after the test unless test fails.
    pub fn with_dir<F>(test_root: &str, test_code: F)
    where
        F: FnOnce(&PathBuf),
    {
        let test_root = PathBuf::from("target/test").join(test_root);
        remove_dir_all(&test_root).ok();
        create_dir_all(&test_root).unwrap();
        (test_code)(&test_root.canonicalize().unwrap());
        remove_dir_all(&test_root).unwrap();
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
            create_file(&f);
            let ctime = fs::metadata(&f).unwrap().modified().unwrap();
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
        write!(&mut f, "{}", content).unwrap();
    }

    /// Reads contents of a file to a string.
    /// Panics on errors.
    pub fn read_file(path: &std::path::Path) -> String {
        let f = File::open(&path).unwrap();
        let mut r = BufReader::new(f);
        let mut result = String::new();
        r.read_to_string(&mut result).unwrap();
        result
    }

    pub fn cached_reflink_supported() -> bool {
        let mut guard = REFLINK_SUPPORTED.lock().unwrap();

        match *guard {
            FsReflink::Untested => {
                let test_dir = PathBuf::from("target/test/have_reflink_support");
                create_dir_all(&test_dir).expect("create_dir_all failed");
                let src_file = test_dir.join("src_file");
                let dest_file = test_dir.join("dest_file");
                create_file(&src_file);
                let result = reflink::reflink(src_file, dest_file).is_ok();
                remove_dir_all(&test_dir).unwrap();
                *guard = FsReflink::Supported(result);
                if !result {
                    println!("Notice: filesystem does not support reflinks, skipping related tests")
                }
                result
            }
            FsReflink::Supported(val) => val,
        }
    }
}
