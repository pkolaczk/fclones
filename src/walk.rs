use std::path::PathBuf;

use rayon::Scope;
use std::fs::{FileType, DirEntry, ReadDir, symlink_metadata, read_link};
use std::io;
use std::env::current_dir;

/// Provides an abstraction over `PathBuf` and `DirEntry` that allows
/// to distinguish file type and wrap its path.
#[derive(Debug)]
enum Entry {
    File(PathBuf),
    Dir(PathBuf),
    SymLink(PathBuf),
    Other(PathBuf)
}

impl Entry {

    pub fn new(file_type: FileType, path: PathBuf) -> Entry {
        if file_type.is_symlink() { Entry::SymLink(path) }
        else if file_type.is_file() { Entry::File(path) }
        else if file_type.is_dir() { Entry::Dir(path) }
        else { Entry::Other(path) }
    }

    pub fn from_path(path: PathBuf) -> io::Result<Entry> {
        symlink_metadata(&path).map(|meta| Entry::new(meta.file_type(), path))
    }

    pub fn from_dir_entry(dir_entry: DirEntry) -> io::Result<Entry> {
        let path = dir_entry.path();
        dir_entry.file_type().map(|ft| Entry::new(ft, path))
    }
}

pub struct Walk<'a> {
    pub skip_hidden: bool,
    pub follow_links: bool,
    pub logger: &'a (dyn Fn(String) + Sync + Send),
    pub base_dir: PathBuf
}

impl<'a> Walk<'a> {

    /// Creates a default walk with empty root dirs, no link following and logger set to sdterr
    pub fn new() -> Walk<'a> {
        Walk {
            skip_hidden: false,
            follow_links: false,
            logger: &|s| eprintln!("{}", s),
            base_dir: current_dir().unwrap_or_default()
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
    /// remove_dir_all(&test_root).ok();
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
    /// walk.run(vec![test_root.clone()], |path| {
    ///     receiver.lock().unwrap().push(path)
    /// });
    ///
    /// let mut results = receiver.lock().unwrap();
    /// results.sort();
    /// assert_eq!(*results, vec![file11, file12, file3, file2]);
    ///
    /// remove_dir_all(&test_root).ok();
    /// ```
    pub fn run<F>(&self, roots: Vec<PathBuf>, consumer: F)
        where F: Fn(PathBuf) + Sync + Send {

        let consumer = &consumer;
        rayon::scope( move |s| {
            for p in roots {
                let p = self.absolute(p);
                s.spawn( move |s| self.visit_path(s, p, consumer))
            }
        });
    }

    /// Visits path of any type (can be a symlink target, file or dir)
    fn visit_path<'s, 'w, F>(&'w self, s: &'s Scope<'w>, path: PathBuf, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send
    {
        Entry::from_path(path.clone())
            .map_err(|e|
                (self.logger)(format!("Failed to stat {}: {}", path.display(), e)))
            .into_iter()
            .for_each(|entry| self.visit_entry(s, entry, consumer))
    }

    /// Visits a path that was already converted to an `Entry` so the entry type is known.
    /// Faster than `visit_path` because it doesn't need to call `stat` internally.
    fn visit_entry<'s, 'w, F>(&'w self, s: &'s Scope<'w>, entry: Entry, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send
    {
        match entry {
            Entry::File(path) =>
                (consumer)(self.relative(path)),
            Entry::Dir(path) =>
                self.visit_dir(s, &path, consumer),
            Entry::SymLink(path) =>
                self.visit_link(s, &path, consumer ),
            Entry::Other(_path) => {}
        }
    }

    /// Resolves a symbolic link.
    /// If `follow_links` is set to false, does nothing.
    fn visit_link<'s, 'w, F>(&'w self, s: &'s Scope<'w>, path: &PathBuf, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send
    {
        if self.follow_links {
            match self.resolve_link(&path) {
                Ok(target) =>
                    self.visit_path(s, target, consumer),
                Err(e) =>
                    (self.logger)(format!("Failed to read link {}: {}", path.display(), e))
            }
        }
    }

    /// Reads the contents of the directory pointed to by `path`
    /// and recursively visits each child entry
    fn visit_dir<'s, 'w, F>(&'w self, s: &'s Scope<'w>, path: &PathBuf, consumer: &'w F)
        where F: Fn(PathBuf) + Sync + Send
    {
        match std::fs::read_dir(&path) {
            Ok(rd) => {
                for entry in Self::sorted_entries(rd) {
                    s.spawn(move |s| self.visit_entry(s, entry, consumer))
                }
            },
            Err(e) =>
                (self.logger)(format!("Failed to read dir {}: {}", path.display(), e))
        }
    }

    /// Sorts dir entries so that regular files are at the end.
    /// Because each worker's queue is a LIFO, the files would be picked up first and the
    /// dirs would be on the other side, amenable for stealing by other workers.
    fn sorted_entries(rd: ReadDir) -> impl Iterator<Item = Entry> {
        let mut files = vec![];
        let mut links = vec![];
        let mut dirs = vec![];
        rd.into_iter()
            .filter_map(|e| e.ok())
            .filter_map(|e| Entry::from_dir_entry(e).ok())
            .for_each(|e| match e {
                Entry::File(_) => files.push(e),
                Entry::SymLink(_) => links.push(e),
                Entry::Dir(_) => dirs.push(e),
                Entry::Other(_) => {}
            });
        dirs.into_iter().chain(links).chain(files)
    }

    /// Returns the absolute target path of a symbolic link
    fn resolve_link(&self, link: &PathBuf) -> io::Result<PathBuf> {
        let target = read_link(link)?;
        let resolved =
            if target.is_relative() {
                link.parent().unwrap().join(target)
            } else {
                target
            };
        Ok(self.absolute(resolved))
    }

    /// Returns absolute canonical path. Relative paths are resolved against `self.base_dir`.
    fn absolute(&self, path: PathBuf) -> PathBuf {
        if path.is_relative() {
            self.base_dir.join(path).canonicalize().unwrap()
        } else {
            path.canonicalize().unwrap()
        }
    }

    /// Returns a path relative to the base dir.
    fn relative(&self, path: PathBuf) -> PathBuf {
        path.strip_prefix(self.base_dir.clone())
            .map(|p| p.to_path_buf())
            .unwrap_or(path)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use std::fs::{remove_dir_all, create_dir_all, File, create_dir};
    use std::sync::Mutex;

    #[test]
    fn list_files() {
        with_dir("target/test/walk/1/", |test_root| {
            let file1 = test_root.join("file1.txt");
            let file2 = test_root.join("file2.txt");
            File::create(&file1).unwrap();
            File::create(&file2).unwrap();
            let walk = Walk::new();
            assert_eq!(run_walk(walk, test_root.clone()), vec![file1, file2]);
        });
    }

    #[test]
    fn descend_into_nested_dirs() {
        with_dir("target/test/walk/2/", |test_root| {
            let dir = test_root.join("dir");
            create_dir(&dir).unwrap();
            let file = dir.join("file.txt");
            File::create(&file).unwrap();
            let walk = Walk::new();
            assert_eq!(run_walk(walk, test_root.clone()), vec![file]);
        });
    }

    #[test]
    #[cfg(unix)]
    fn follow_rel_file_sym_links() {
        with_dir("target/test/walk/3/", |test_root| {
            use std::os::unix::fs::symlink;
            let file = test_root.join("file.txt");
            let link = test_root.join("link");
            File::create(&file).unwrap();
            symlink(PathBuf::from("file.txt"), &link).unwrap(); // link -> file.txt
            let mut walk = Walk::new();
            walk.follow_links = true;
            assert_eq!(run_walk(walk, link), vec![file]);
        });
    }

    #[test]
    #[cfg(unix)]
    fn follow_rel_dir_sym_links() {
        with_dir("target/test/walk/4/", |test_root| {
            use std::os::unix::fs::symlink;
            let dir = test_root.join("dir");
            let link = test_root.join("link");
            let file = dir.join("file.txt");

            create_dir(&dir).unwrap();
            File::create(&file).unwrap();
            symlink(PathBuf::from("dir"), &link).unwrap(); // link -> dir

            let mut walk = Walk::new();
            walk.follow_links = true;
            assert_eq!(run_walk(walk, link), vec![file]);
        });
    }

    #[test]
    #[cfg(unix)]
    fn follow_abs_dir_sym_links() {
        with_dir("target/test/walk/5/", |test_root| {
            use std::os::unix::fs::symlink;
            let dir = test_root.join("dir");
            let link = test_root.join("link");
            let file = dir.join("file.txt");

            create_dir(&dir).unwrap();
            File::create(&file).unwrap();
            symlink(dir.canonicalize().unwrap(), &link).unwrap(); // link -> absolute path to dir

            let mut walk = Walk::new();
            walk.follow_links = true;
            assert_eq!(run_walk(walk, link), vec![file]);
        });
    }


    fn with_dir<F>(test_root: &str, test_code: F)
        where F: FnOnce(&PathBuf)
    {
        let test_root = PathBuf::from(test_root);
        remove_dir_all(&test_root).ok();
        create_dir_all(&test_root).unwrap();
        (test_code)(&test_root);
        remove_dir_all(&test_root).unwrap();
    }

    fn run_walk(walk: Walk, root: PathBuf) -> Vec<PathBuf> {
        let results = Mutex::new(Vec::new());
        walk.run(vec![root], |path|
            results.lock().unwrap().push(path));

        let mut results = results.into_inner().unwrap();
        results.sort();
        results
    }
}