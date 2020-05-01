use std::path::PathBuf;

use rayon::Scope;

pub struct Walk<'a> {
    pub skip_hidden: bool,
    pub follow_links: bool,
    pub logger: &'a (dyn Fn(String) + Sync + Send)
}

impl<'a> Walk<'a> {

    /// Creates a default walk with empty root dirs, no link following and logger set to sdterr
    pub fn new() -> Walk<'a> {
        Walk {
            skip_hidden: false,
            follow_links: false,
            logger: &|s| eprintln!("{}", s),
        }
    }

    /// Walks multiple directories recursively in parallel and sends found files to `consumer`.
    /// The `consumer` must be able to receive items from many threads.
    /// Inaccessible files are skipped, but errors are printed to stderr.
    /// The order of files is not specified and may be different every time.
    ///
    /// # Example
    /// ```
    /// use dff::walk::*;
    /// use std::fs::{File, create_dir_all, remove_dir_all, create_dir};
    /// use std::path::PathBuf;
    /// use std::sync::Mutex;
    ///
    /// let test_root = PathBuf::from("target/test/walk/");
    /// let dir1 = test_root.join("dir1");
    /// let dir2 = test_root.join("dir2");
    /// let dir3 = dir2.join("dir3");
    /// let file11 = dir1.join("file11.txt");
    /// let file12 = dir1.join("file12.txt");
    /// let file2 = dir2.join("file2.txt");
    /// let file3 = dir3.join("file3.txt");
    ///
    /// remove_dir_all(&test_root).unwrap();
    /// create_dir_all(&test_root).unwrap();
    /// create_dir(&dir1).unwrap();
    /// create_dir(&dir2).unwrap();
    /// create_dir(&dir3).unwrap();
    /// File::create(&file11).unwrap();
    /// File::create(&file12).unwrap();
    /// File::create(&file2).unwrap();
    /// File::create(&file3).unwrap();
    ///
    /// let receiver = Mutex::new(Vec::<PathBuf>::new());
    /// let mut walk = Walk::new();
    /// walk.run(vec![dir1, dir2], |path| {
    ///     receiver.lock().unwrap().push(path)
    /// });
    ///
    /// let mut results = receiver.lock().unwrap();
    /// results.sort();
    /// assert_eq!(*results, vec![file11, file12, file3, file2]);
    /// ```
    pub fn run<F>(&self, roots: Vec<PathBuf>, consumer: F)
        where F: Fn(PathBuf) + Sync + Send {

        let consumer = &consumer;
        rayon::scope( move |s| {
            for p in roots {
                s.spawn( move |s| self.walk_dir(s, p, consumer))
            }
        });
    }

    fn walk_dir<'s, 'w, F>(&'w self, s: &'s Scope<'w>, path: PathBuf, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send {

        match std::fs::read_dir(&path) {
            Ok(rd) => for entry in rd {
                let entry = entry.unwrap();
                let file_type = entry.file_type().unwrap();
                if file_type.is_file() {
                    (consumer)(entry.path())
                } else if file_type.is_dir() && (!file_type.is_symlink() || self.follow_links) {
                    s.spawn(move |s| self.walk_dir(s, entry.path(), consumer))
                }
            },
            Err(e) =>
                (self.logger)(format!("Failed to read dir {}: {}", path.display(), e))
        }
    }

}
