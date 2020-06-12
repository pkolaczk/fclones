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
