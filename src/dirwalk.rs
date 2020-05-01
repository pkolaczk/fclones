use std::path::PathBuf;

use rayon::Scope;

#[derive(Copy, Clone)]
pub struct WalkOpts {
    pub skip_hidden: bool,
    pub follow_links: bool
}

impl WalkOpts {
    pub fn default() -> WalkOpts {
        WalkOpts { skip_hidden: false, follow_links: false }
    }
}

fn walk_task<'s, 'w, F>(s: &'s Scope<'w>, path: PathBuf, opts: &'w WalkOpts, consumer: &'w F)
    where F: Fn(PathBuf) + Sync + Send {

    match std::fs::read_dir(&path) {
        Ok(rd) => for entry in rd {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            if file_type.is_file() {
                (consumer)(entry.path())
            } else if file_type.is_dir() && (!file_type.is_symlink() || opts.follow_links) {
                s.spawn(move |s| walk_task(s, entry.path(), opts, consumer))
            }
        },
        Err(e) =>
            eprintln!("Failed to read dir {}: {}", path.display(), e)
    }
}

/// Walks multiple directories recursively in parallel and sends found files to `consumer`.
/// The `consumer` must be able to receive items from many threads.
/// Inaccessible files are skipped, but errors are printed to stderr.
/// The order of files is not specified and may be different every time.
///
/// # Example
/// ```
/// use dff::dirwalk::*;
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
/// walk(vec![dir1, dir2], &WalkOpts::default(), |path| {
///     receiver.lock().unwrap().push(path)
/// });
///
/// let mut results = receiver.lock().unwrap();
/// results.sort();
/// assert_eq!(*results, vec![file11, file12, file3, file2]);
/// ```
pub fn walk<F>(paths: Vec<PathBuf>, opts: &WalkOpts, consumer: F)
    where F: Fn(PathBuf) + Sync + Send {

    let consumer = &consumer;
    rayon::scope( move |s| {
        for p in paths {
            s.spawn( move |s| walk_task(s, p, opts, consumer))
        }
    });
}

/// Walks directory tree rooted at `path` in parallel and sends file paths to `consumer`.
/// The `consumer` must be able to receive items from many threads.
/// Inaccessible files are skipped, but errors are printed to stderr.
/// The order of files is not specified and may be different every time.
pub fn walk_single<F>(path: PathBuf, opts: &WalkOpts, consumer: F)
    where F: Fn(PathBuf) + Sync + Send {
    walk(vec![path], opts, consumer)
}
