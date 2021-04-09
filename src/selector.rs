use std::path::MAIN_SEPARATOR;
use std::sync::Arc;

use crate::path::Path;
use crate::pattern::Pattern;

/// Stores glob patterns working together as a path selector.
///
/// A path is selected only if it matches at least one include pattern
/// and doesn't match any exclude patterns.
/// An empty include pattern vector matches all paths.
#[derive(Debug, Clone)]
pub struct PathSelector {
    base_dir: Arc<Path>,
    included_names: Vec<Pattern>,
    included_paths: Vec<Pattern>,
    excluded_paths: Vec<Pattern>,
}

impl PathSelector {
    /// Creates a new selector that matches all paths.
    pub fn new(base_dir: Path) -> PathSelector {
        PathSelector {
            base_dir: Arc::new(base_dir),
            included_names: vec![],
            included_paths: vec![],
            excluded_paths: vec![],
        }
    }

    pub fn include_names(mut self, pat: Vec<Pattern>) -> PathSelector {
        self.included_names = pat;
        self
    }

    pub fn include_paths(mut self, pat: Vec<Pattern>) -> PathSelector {
        self.included_paths = pat
            .into_iter()
            .map(|p| Self::abs_pattern(&self.base_dir, p))
            .collect();
        self
    }

    pub fn exclude_paths(mut self, pat: Vec<Pattern>) -> PathSelector {
        self.excluded_paths = pat
            .into_iter()
            .map(|p| Self::abs_pattern(&self.base_dir, p))
            .collect();
        self
    }

    /// Returns true if the given path fully matches this selector.
    pub fn matches_full_path(&self, path: &Path) -> bool {
        self.with_absolute_path(path, |path| {
            let name = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let name = name.as_ref();
            let path = path.to_string_lossy();
            (self.included_names.is_empty() || self.included_names.iter().any(|p| p.matches(name)))
                && (self.included_paths.is_empty()
                    || self.included_paths.iter().any(|p| p.matches(&path)))
                && self.excluded_paths.iter().all(|p| !p.matches(&path))
        })
    }

    /// Returns true if the given directory may contain matching paths.
    /// Used to decide whether the directory walk should descend to that directory.
    /// The directory should be allowed only if:
    /// 1. all its components match a prefix of at least one include filter,
    /// 2. it doesn't match any of the exclude filters ending with `**` pattern.
    pub fn matches_dir(&self, path: &Path) -> bool {
        self.with_absolute_path(path, |path| {
            let mut path = path.to_string_lossy();
            if !path.ends_with(MAIN_SEPARATOR) {
                path.push(MAIN_SEPARATOR);
            }
            (self.included_paths.is_empty()
                || self
                    .included_paths
                    .iter()
                    .any(|p| p.matches_partially(&path)))
                && self.excluded_paths.iter().all(|p| !p.matches_prefix(&path))
        })
    }

    /// Executes given code with a reference to an absolute path.
    /// If `path` is already absolute, a direct reference is provided and no allocations happen.
    /// If `path` is relative, it would be appended to the `self.base_path` first and a reference
    /// to the temporary result will be provided.
    fn with_absolute_path<F, R>(&self, path: &Path, f: F) -> R
    where
        F: Fn(&Path) -> R,
    {
        if path.is_absolute() {
            (f)(path)
        } else {
            (f)(&self.base_dir.join(path))
        }
    }

    /// Returns an absolute pattern.
    /// If pattern is relative (i.e. does not start with fs root), then the base_dir is appended.
    fn abs_pattern(base_dir: &Path, pattern: Pattern) -> Pattern {
        if Self::is_absolute(&pattern) {
            pattern
        } else {
            let base_dir_pat = base_dir.to_string_lossy();
            let base_dir_pat = base_dir_pat.replace("\u{FFFD}", "?");
            let base_dir_pat = Pattern::literal(Self::append_sep(base_dir_pat).as_str());
            base_dir_pat + pattern
        }
    }

    /// Appends path separator if the string doesn't end with one already
    fn append_sep(s: String) -> String {
        if s.ends_with(MAIN_SEPARATOR) {
            s
        } else {
            s + MAIN_SEPARATOR.to_string().as_str()
        }
    }

    ///  Returns true if pattern can match absolute paths
    fn is_absolute(pattern: &Pattern) -> bool {
        let s = pattern.to_string();
        s.starts_with(".*") || Path::from(s).is_absolute()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn match_all() {
        let selector = PathSelector::new(Path::from("/test"));
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar")));
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar/baz")));
        assert!(selector.matches_full_path(&Path::from("/test/anything123")));
    }

    #[test]
    fn include_absolute() {
        let selector = PathSelector::new(Path::from("/test"))
            .include_paths(vec![Pattern::glob("/test/foo/**").unwrap()]);
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar")));
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar/baz")));
        assert!(!selector.matches_full_path(&Path::from("/test/bar")));
    }

    #[test]
    fn include_relative() {
        let selector = PathSelector::new(Path::from("/test"))
            .include_paths(vec![Pattern::glob("foo/**").unwrap()]);

        // matching:
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar")));
        assert!(selector.matches_full_path(&Path::from("/test/foo/bar/baz")));
        assert!(selector.matches_full_path(&Path::from("foo/bar")));
        assert!(selector.matches_full_path(&Path::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_full_path(&Path::from("bar")));
        assert!(!selector.matches_full_path(&Path::from("/bar")));
        assert!(!selector.matches_full_path(&Path::from("/test/bar")));
    }

    #[test]
    fn include_relative_root_base() {
        let selector = PathSelector::new(Path::from("/"))
            .include_paths(vec![Pattern::glob("foo/**").unwrap()]);

        // matching:
        assert!(selector.matches_full_path(&Path::from("/foo/bar")));
        assert!(selector.matches_full_path(&Path::from("/foo/bar/baz")));
        assert!(selector.matches_full_path(&Path::from("foo/bar")));
        assert!(selector.matches_full_path(&Path::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_full_path(&Path::from("bar")));
        assert!(!selector.matches_full_path(&Path::from("/bar")));
        assert!(!selector.matches_full_path(&Path::from("/test/bar")));
    }

    #[test]
    fn include_exclude() {
        let selector = PathSelector::new(Path::from("/"))
            .include_paths(vec![Pattern::glob("/foo/**").unwrap()])
            .exclude_paths(vec![Pattern::glob("/foo/b*/**").unwrap()]);

        // matching:
        assert!(selector.matches_full_path(&Path::from("/foo/foo")));
        assert!(selector.matches_full_path(&Path::from("/foo/foo/foo")));

        // not matching:
        assert!(!selector.matches_full_path(&Path::from("/foo/bar/baz")));
        assert!(!selector.matches_full_path(&Path::from("/test/bar")));
    }

    #[test]
    fn prefix_wildcard() {
        let selector = PathSelector::new(Path::from("/"))
            .include_paths(vec![Pattern::glob("**/public-?.jpg").unwrap()])
            .exclude_paths(vec![Pattern::glob("**/private-?.jpg").unwrap()]);

        println!("{:?}", selector);

        // matching absolute:
        assert!(selector.matches_full_path(&Path::from("/public-1.jpg")));
        assert!(selector.matches_full_path(&Path::from("/foo/public-2.jpg")));
        assert!(selector.matches_full_path(&Path::from("/foo/foo/public-3.jpg")));

        // matching relative:
        assert!(selector.matches_full_path(&Path::from("foo/public-2.jpg")));
        assert!(selector.matches_full_path(&Path::from("foo/foo/public-3.jpg")));

        // not matching absolute:
        assert!(!selector.matches_full_path(&Path::from("/something-else.jpg")));
        assert!(!selector.matches_full_path(&Path::from("/private-1.jpg")));
        assert!(!selector.matches_full_path(&Path::from("/foo/private-2.jpg")));
        assert!(!selector.matches_full_path(&Path::from("/foo/foo/private-3.jpg")));

        // not matching relative:
        assert!(!selector.matches_full_path(&Path::from("something-else.jpg")));
        assert!(!selector.matches_full_path(&Path::from("private-1.jpg")));
        assert!(!selector.matches_full_path(&Path::from("foo/private-2.jpg")));
        assert!(!selector.matches_full_path(&Path::from("foo/foo/private-3.jpg")));
    }

    #[test]
    fn matches_dir() {
        let selector = PathSelector::new(Path::from("/"))
            .include_paths(vec![Pattern::glob("/test[1-9]/**").unwrap()])
            .exclude_paths(vec![Pattern::glob("/test[1-9]/foo/**").unwrap()]);

        assert!(selector.matches_dir(&Path::from("/")));
        assert!(selector.matches_dir(&Path::from("/test1")));
        assert!(selector.matches_dir(&Path::from("/test2/bar")));

        assert!(!selector.matches_dir(&Path::from("/test3/foo")));
        assert!(!selector.matches_dir(&Path::from("/test3/foo/bar/baz")));
    }
}
