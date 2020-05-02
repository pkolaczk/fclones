use std::path::PathBuf;

use rayon::Scope;
use std::fs::{FileType, DirEntry};
use std::io;

/// Provides an abstraction over `PathBuf` and `DirEntry` that allows
/// to distinguish file type and wrap its path.
enum Entry {
    File(PathBuf),
    Dir(PathBuf),
    SymLink(PathBuf),
    Other(PathBuf)
}

impl Entry {

    pub fn new(file_type: FileType, path: PathBuf) -> Entry {
        if file_type.is_file() { Entry::File(path) }
        else if file_type.is_dir() { Entry::Dir(path) }
        else if file_type.is_symlink() { Entry::SymLink(path) }
        else { Entry::Other(path) }
    }

    pub fn from_path(path: PathBuf) -> io::Result<Entry> {
        path.metadata().map(|meta| Entry::new(meta.file_type(), path))
    }

    pub fn from_dir_entry(dir_entry: DirEntry) -> io::Result<Entry> {
        let path = dir_entry.path();
        dir_entry.file_type().map(|ft| Entry::new(ft, path))
    }
}

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

    fn handle_err<T>(&self, value: io::Result<T>) -> Option<T> {
        match value {
            Ok(value) => Some(value),
            Err(e) => { (self.logger)(format!("{}", e)); None }
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
    /// walk.run(vec![test_root], |path| {
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
                s.spawn( move |s| {
                    self.handle_err(Entry::from_path(p))
                        .into_iter()
                        .for_each(|entry| {
                            self.walk_dir(s, entry, consumer)
                        })
                })
            }
        });
    }

    fn walk_dir<'s, 'w, F>(&'w self, s: &'s Scope<'w>, entry: Entry, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send {

        match entry {
            Entry::File(path) => {
                (consumer)(path)
            },
            Entry::Dir(path) => match std::fs::read_dir(&path) {
                Ok(rd) => for entry in rd {
                    let entry = entry.unwrap();
                    let entry = Entry::from_dir_entry(entry).unwrap();
                    s.spawn(move |s| self.walk_dir(s, entry, consumer))
                },
                Err(e) =>
                    (self.logger)(format!("Failed to read dir {}: {}", path.display(), e))
            },
            Entry::SymLink(_) => {}, // TODO
            Entry::Other(_) => {}
        }
    }
}
