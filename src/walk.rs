use std::default::Default;
use std::env::current_dir;
use std::fs::{read_link, symlink_metadata, DirEntry, FileType, ReadDir};
use std::sync::Arc;
use std::{fs, io};

use dashmap::DashSet;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use rayon::Scope;

use crate::log::Log;
use crate::path::Path;
use crate::selector::PathSelector;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryType {
    File,
    Dir,
    SymLink,
    Other,
}

impl EntryType {
    pub fn from(file_type: FileType) -> EntryType {
        if file_type.is_symlink() {
            EntryType::SymLink
        } else if file_type.is_file() {
            EntryType::File
        } else if file_type.is_dir() {
            EntryType::Dir
        } else {
            EntryType::Other
        }
    }
}

/// A path to a file, directory or symbolic link.
/// Provides an abstraction over `Path` and `DirEntry`
#[derive(Debug)]
struct Entry {
    tpe: EntryType,
    path: Path,
}

impl Entry {
    pub fn new(file_type: FileType, path: Path) -> Entry {
        Entry {
            tpe: EntryType::from(file_type),
            path,
        }
    }

    pub fn from_path(path: Path) -> io::Result<Entry> {
        symlink_metadata(&path.to_path_buf()).map(|meta| Entry::new(meta.file_type(), path))
    }

    pub fn from_dir_entry(base: &Arc<Path>, dir_entry: DirEntry) -> io::Result<Entry> {
        let path = base.join(Path::from(dir_entry.file_name()));
        dir_entry.file_type().map(|ft| Entry::new(ft, path))
    }
}

#[derive(Clone)]
struct IgnoreStack(Arc<Vec<Gitignore>>);

impl IgnoreStack {
    /// Returns ignore stack initialized with global gitignore settings.
    fn new(log: Option<&Log>) -> Self {
        let gitignore = GitignoreBuilder::new("/").build_global();
        if let Some(err) = gitignore.1 {
            if let Some(log) = log {
                log.warn(format!("Error loading global gitignore rules: {}", err))
            }
        }
        IgnoreStack(Arc::new(vec![gitignore.0]))
    }

    /// Returns an empty gitignore stack that ignores no files.
    pub fn empty() -> IgnoreStack {
        IgnoreStack(Arc::new(vec![]))
    }

    /// If .gitignore file exists in given dir, creates a `Gitignore` struct for it
    /// and returns the stack with the new `Gitignore` appended. Otherwise returns a cloned self.
    pub fn push(&self, dir: &Path, log: Option<&Log>) -> IgnoreStack {
        let mut path = Arc::new(dir.clone()).resolve(Path::from(".gitignore"));
        let mut path_buf = path.to_path_buf();
        if !path_buf.is_file() {
            path = Arc::new(dir.clone()).resolve(Path::from(".fdignore"));
            path_buf = path.to_path_buf();
        }
        if !path_buf.is_file() {
            return self.clone();
        }
        let gitignore = Gitignore::new(&path_buf);
        if let Some(err) = gitignore.1 {
            if let Some(log) = log {
                log.warn(format!(
                    "Error while loading ignore file {}: {}",
                    path.display(),
                    err
                ))
            }
        }
        let mut stack = self.0.as_ref().clone();
        stack.push(gitignore.0);
        IgnoreStack(Arc::new(stack))
    }

    /// Returns true if any of the gitignore files in the stack selects given path
    pub fn matches(&self, path: &Path, is_dir: bool) -> bool {
        // this is on critical performance path, so avoid unnecessary to_path_buf conversion
        if self.0.is_empty() {
            return false;
        }
        let path = path.to_path_buf();
        self.0
            .iter()
            .any(|gitignore| gitignore.matched(&path, is_dir).is_ignore())
    }
}

/// Describes walk configuration.
/// Many walks can be initiated from the same instance.
pub struct Walk<'a> {
    /// Relative root paths are resolved against this dir.
    pub base_dir: Arc<Path>,
    /// Maximum allowed directory nesting level. 0 means "do not descend to directories at all".
    pub depth: usize,
    /// Include hidden files.
    pub hidden: bool,
    /// Resolve symlinks to dirs and files.
    /// For a symlink to a file, report the target file, unless `report_links` is true.
    pub follow_links: bool,
    /// Don't follow symlinks to files, but report them.
    pub report_links: bool,
    /// Don't honor .gitignore and .fdignore.
    pub no_ignore: bool,
    /// Controls selecting or ignoring files by matching file and path names with regexes / globs.
    pub path_selector: PathSelector,
    /// The function to call for each visited file. The directories are not reported.
    pub on_visit: &'a (dyn Fn(&Path) + Sync + Send),
    /// Warnings about inaccessible files or dirs are logged here, if defined.
    pub log: Option<&'a Log>,
}

/// Private shared state scoped to a single `run` invocation.
struct WalkState<F> {
    pub consumer: F,
    pub visited: DashSet<u128>,
}

impl<'a> Walk<'a> {
    /// Creates a default walk with empty root dirs, no link following and logger set to sdterr
    pub fn new() -> Walk<'a> {
        let base_dir = Path::from(&current_dir().unwrap_or_default());
        Walk {
            base_dir: Arc::new(base_dir.clone()),
            depth: usize::MAX,
            hidden: false,
            follow_links: false,
            report_links: false,
            no_ignore: false,
            path_selector: PathSelector::new(base_dir),
            on_visit: &|_| {},
            log: None,
        }
    }

    /// Walks multiple directories recursively in parallel and sends found files to `consumer`.
    /// Input paths can be relative to the current working directory,
    /// but produced paths are absolute.
    /// The `consumer` must be able to receive items from many threads.
    /// Inaccessible files are skipped, but errors are printed to stderr.
    /// The order of files is not specified and may be different every time.
    pub fn run<I, F>(&self, roots: I, consumer: F)
    where
        I: IntoIterator<Item = Path> + Send,
        F: Fn(Path) + Sync + Send,
    {
        let state = WalkState {
            consumer,
            visited: DashSet::new(),
        };
        rayon::scope(|scope| {
            let ignore = if self.no_ignore {
                IgnoreStack::empty()
            } else {
                IgnoreStack::new(self.log)
            };

            for p in roots.into_iter() {
                let p = self.absolute(p);
                let ignore = ignore.clone();
                match fs::metadata(&p.to_path_buf()) {
                    Ok(metadata) if metadata.is_dir() && self.depth == 0 => self.log_warn(format!(
                        "Skipping directory {} because recursive scan is disabled.",
                        p.display()
                    )),
                    _ => scope.spawn(|scope| self.visit_path(p, scope, 0, ignore, &state)),
                }
            }
        });
    }

    /// Visits path of any type (can be a symlink target, file or dir)
    fn visit_path<'s, 'w, F>(
        &'s self,
        path: Path,
        scope: &Scope<'w>,
        level: usize,
        gitignore: IgnoreStack,
        state: &'w WalkState<F>,
    ) where
        F: Fn(Path) + Sync + Send,
        's: 'w,
    {
        if self.path_selector.matches_dir(&path) {
            Entry::from_path(path.clone())
                .map_err(|e| self.log_warn(format!("Failed to stat {}: {}", path.display(), e)))
                .into_iter()
                .for_each(|entry| self.visit_entry(entry, scope, level, gitignore.clone(), state))
        }
    }

    /// Visits a path that was already converted to an `Entry` so the entry type is known.
    /// Faster than `visit_path` because it doesn't need to call `stat` internally.
    fn visit_entry<'s, 'w, F>(
        &'s self,
        entry: Entry,
        scope: &Scope<'w>,
        level: usize,
        gitignore: IgnoreStack,
        state: &'w WalkState<F>,
    ) where
        F: Fn(Path) + Sync + Send,
        's: 'w,
    {
        // For progress reporting
        (self.on_visit)(&entry.path);

        // Skip hidden files
        if !self.hidden {
            if let Some(name) = entry.path.file_name_cstr() {
                if name.to_string_lossy().starts_with('.') {
                    return;
                }
            }
        }

        // Skip already visited paths. We're checking only when follow_links is true,
        // because inserting into a shared hash set is costly.
        if self.follow_links && !state.visited.insert(entry.path.hash128()) {
            return;
        }

        // Skip entries ignored by .gitignore
        if !self.no_ignore && gitignore.matches(&entry.path, entry.tpe == EntryType::Dir) {
            return;
        }

        match entry.tpe {
            EntryType::File => self.visit_file(entry.path, state),
            EntryType::Dir => self.visit_dir(entry.path, scope, level, gitignore, state),
            EntryType::SymLink => self.visit_link(entry.path, scope, level, gitignore, state),
            EntryType::Other => {}
        }
    }

    /// If file matches selection criteria, sends it to the consumer
    fn visit_file<F>(&self, path: Path, state: &WalkState<F>)
    where
        F: Fn(Path) + Sync + Send,
    {
        if self.path_selector.matches_full_path(&path) {
            (state.consumer)(path)
        }
    }

    /// Resolves a symbolic link.
    /// If `follow_links` is set to false, does nothing.
    fn visit_link<'s, 'w, F>(
        &'s self,
        path: Path,
        scope: &Scope<'w>,
        level: usize,
        gitignore: IgnoreStack,
        state: &'w WalkState<F>,
    ) where
        F: Fn(Path) + Sync + Send,
        's: 'w,
    {
        if self.follow_links || self.report_links {
            match self.resolve_link(&path) {
                Ok((_, EntryType::File)) if self.report_links => self.visit_file(path, state),
                Ok((target, _)) => {
                    if self.follow_links {
                        self.visit_path(target, scope, level, gitignore, state)
                    }
                }
                Err(e) => self.log_warn(format!("Failed to read link {}: {}", path.display(), e)),
            }
        }
    }

    /// Reads the contents of the directory pointed to by `path`
    /// and recursively visits each child entry
    fn visit_dir<'s, 'w, F>(
        &'s self,
        path: Path,
        scope: &Scope<'w>,
        level: usize,
        gitignore: IgnoreStack,
        state: &'w WalkState<F>,
    ) where
        F: Fn(Path) + Sync + Send,
        's: 'w,
    {
        if level > self.depth {
            return;
        }
        if !self.path_selector.matches_dir(&path) {
            return;
        }

        let gitignore = if self.no_ignore {
            gitignore
        } else {
            gitignore.push(&path, self.log)
        };

        match fs::read_dir(path.to_path_buf()) {
            Ok(rd) => {
                for entry in Self::sorted_entries(path, rd) {
                    let gitignore = gitignore.clone();
                    scope.spawn(move |s| self.visit_entry(entry, s, level + 1, gitignore, state))
                }
            }
            Err(e) => self.log_warn(format!("Failed to read dir {}: {}", path.display(), e)),
        }
    }

    #[cfg(unix)]
    fn sort_dir_entries_by_inode(entries: &mut Vec<DirEntry>) {
        use rayon::prelude::ParallelSliceMut;
        use std::os::unix::fs::DirEntryExt;
        entries.par_sort_unstable_by_key(|entry| entry.ino())
    }

    #[cfg(not(unix))]
    fn sort_dir_entries_by_inode(_: &mut Vec<DirEntry>) {
        // do nothing
    }

    /// Sorts dir entries so that regular files are at the end.
    /// Because each worker's queue is a LIFO, the files would be picked up first and the
    /// dirs would be on the other side, amenable for stealing by other workers.
    fn sorted_entries(parent: Path, rd: ReadDir) -> impl Iterator<Item = Entry> {
        let mut files = vec![];
        let mut links = vec![];
        let mut dirs = vec![];
        let path = Arc::new(parent);
        let mut entries: Vec<DirEntry> = rd.filter_map(|e| e.ok()).collect();
        // Accessing entries in the order of identifiers should be faster on rotational drives
        Self::sort_dir_entries_by_inode(&mut entries);
        entries
            .into_iter()
            .filter_map(|e| Entry::from_dir_entry(&path, e).ok())
            .for_each(|e| match e.tpe {
                EntryType::File => files.push(e),
                EntryType::SymLink => links.push(e),
                EntryType::Dir => dirs.push(e),
                EntryType::Other => {}
            });
        dirs.into_iter().chain(links).chain(files)
    }

    /// Returns the absolute target path of a symbolic link with the type of the target
    fn resolve_link(&self, link: &Path) -> io::Result<(Path, EntryType)> {
        let link_buf = link.to_path_buf();
        let target = read_link(&link_buf)?;
        let entry_type = EntryType::from(link_buf.metadata()?.file_type());
        let target = Path::from(target);
        let resolved = if target.is_relative() {
            link.parent().unwrap().join(target)
        } else {
            target
        };
        Ok((self.absolute(resolved), entry_type))
    }

    /// Returns absolute path with removed `.` and `..` components.
    /// Relative paths are resolved against `self.base_dir`.
    /// Symbolic links to directories are resolved.
    /// File symlinks are not resolved, because we need
    fn absolute(&self, mut path: Path) -> Path {
        if path.is_relative() {
            path = self.base_dir.join(path)
        }
        if path.to_path_buf().is_file() {
            // for files we are sure there will be a parent and a file name
            let parent = path.parent().unwrap().canonicalize();
            let file_name = path.file_name().unwrap();
            Arc::new(parent).join(Path::from(file_name))
        } else {
            path.canonicalize()
        }
    }

    /// Logs a warning
    fn log_warn(&self, msg: String) {
        self.log.iter().for_each(|l| l.warn(&msg))
    }
}

impl<'a> Default for Walk<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use std::fs::{create_dir, File};
    use std::path::PathBuf;
    use std::sync::Mutex;

    use crate::util::test::*;

    use super::*;

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
    fn report_rel_file_sym_links() {
        with_dir("target/test/walk/report_symlinks/", |test_root| {
            use std::os::unix::fs::symlink;
            let file = test_root.join("file.txt");
            let link1 = test_root.join("link1");
            let link2 = test_root.join("link2");
            File::create(&file).unwrap();
            symlink(PathBuf::from("file.txt"), &link1).unwrap(); // link1 -> file.txt
            symlink(PathBuf::from("link1"), &link2).unwrap(); // link2 -> link1

            let mut walk1 = Walk::new();
            walk1.report_links = true;
            assert_eq!(run_walk(walk1, link1.clone()), vec![link1]);

            // a link to a link should also be reported
            let mut walk2 = Walk::new();
            walk2.report_links = true;
            assert_eq!(run_walk(walk2, link2.clone()), vec![link2]);
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

    #[test]
    #[cfg(unix)]
    fn sym_link_cycles() {
        with_dir("target/test/walk/6/", |test_root| {
            use std::os::unix::fs::symlink;
            let dir = test_root.join("dir");
            let link = dir.join("link");
            let file = dir.join("file.txt");

            create_dir(&dir).unwrap();
            File::create(&file).unwrap();
            // create a link back to the top level, so a cycle is formed
            symlink(test_root.canonicalize().unwrap(), &link).unwrap();

            let mut walk = Walk::new();
            walk.follow_links = true;
            assert_eq!(run_walk(walk, test_root.clone()), vec![file]);
        });
    }

    #[test]
    fn skip_hidden() {
        with_dir("target/test/walk/skip_hidden/", |test_root| {
            let hidden_dir = test_root.join(".dir");
            create_dir(&hidden_dir).unwrap();
            let hidden_file_1 = hidden_dir.join("file.txt");
            let hidden_file_2 = test_root.join(".file.txt");
            File::create(&hidden_file_1).unwrap();
            File::create(&hidden_file_2).unwrap();

            let mut walk = Walk::new();
            walk.hidden = false;
            assert!(run_walk(walk, test_root.clone()).is_empty());

            let mut walk = Walk::new();
            walk.hidden = true;
            assert_eq!(run_walk(walk, test_root.clone()).len(), 2);
        });
    }

    fn respect_ignore(root: &str, ignore_file: &str) {
        with_dir(root, |test_root| {
            use std::io::Write;
            let mut gitignore = File::create(test_root.join(ignore_file)).unwrap();
            writeln!(gitignore, "foo/").unwrap();
            writeln!(gitignore, "*.log").unwrap();
            writeln!(gitignore, "**/bar").unwrap();
            drop(gitignore);

            create_dir(&test_root.join("foo")).unwrap();
            create_file(&test_root.join("foo").join("bar"));
            create_file(&test_root.join("bar.log"));
            create_dir(&test_root.join("dir")).unwrap();
            create_dir(&test_root.join("dir").join("bar")).unwrap();
            create_file(&test_root.join("dir").join("bar").join("file"));

            let walk = Walk::new();
            assert!(run_walk(walk, test_root.clone()).is_empty());

            let mut walk = Walk::new();
            walk.no_ignore = true;
            assert_eq!(run_walk(walk, test_root.clone()).len(), 3)
        });
    }

    #[test]
    fn respect_gitignore() {
        respect_ignore("target/test/walk/gitignore/", ".gitignore")
    }

    #[test]
    fn respect_fdignore() {
        respect_ignore("target/test/walk/fdignore/", ".fdignore")
    }

    fn run_walk(walk: Walk, root: PathBuf) -> Vec<PathBuf> {
        let results = Mutex::new(Vec::new());
        walk.run(vec![Path::from(root)], |path| {
            results.lock().unwrap().push(path.to_path_buf())
        });

        let mut results = results.into_inner().unwrap();
        results.sort();
        results
    }
}
