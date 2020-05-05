use glob::Pattern;
use std::path::{PathBuf, MAIN_SEPARATOR};

/// Stores glob patterns working together as a path selector.
///
/// A path is selected only if it matches at least one include pattern
/// and doesn't match any exclude patterns.
/// An empty include pattern vector matches all paths.
pub struct PathSelector {
    base_dir: PathBuf,
    include_pat: Vec<Pattern>,
    exclude_pat: Vec<Pattern>,
    excluded_prefixes: Vec<PathBuf>
}

impl PathSelector {

    /// Creates a new selector with given include and exclude patterns.
    pub fn new(base_dir: PathBuf, includes: Vec<Pattern>, excludes: Vec<Pattern>) -> PathSelector {
        let includes = includes.into_iter().map(|pat|
            Self::abs_pattern(&base_dir, pat)).collect();
        let excludes = excludes.into_iter().map(|pat|
            Self::abs_pattern(&base_dir, pat)).collect();

        PathSelector {
            base_dir,
            include_pat: includes,
            exclude_pat: excludes,
            excluded_prefixes: vec![]
        }
    }

    /// Creates a new selector that matches all paths.
    pub fn match_all() -> PathSelector {
        PathSelector {
            base_dir: "/".into(),
            include_pat: vec![],
            exclude_pat: vec![],
            excluded_prefixes: vec![]
        }
    }

    /// Returns true if the given path fully matches this selector.
    /// The path must be absolute.
    pub fn matches_fully(&self, path: &PathBuf) -> bool {
        self.with_absolute_path(path, |path| {
            (self.include_pat.is_empty()
                || self.include_pat.iter().any(|p| p.matches_path(path)))
                && self.exclude_pat.iter().all(|p| !p.matches_path(path))
        })
    }

    /// Executes given code with a reference to an absolute path.
    /// If `path` is already absolute, a direct reference is provided and no allocations happen.
    /// If `path` is relative, it would be appended to the `self.base_path` first and a reference
    /// to the temporary result will be provided.
    fn with_absolute_path<F, R>(&self, path: &PathBuf, f: F) -> R
        where F: Fn(&PathBuf) -> R
    {
        if path.is_absolute() {
            (f)(path)
        } else {
            (f)(&self.base_dir.join(path))
        }
    }

    /// Returns an absolute pattern.
    /// If pattern is relative (i.e. does not start with fs root), then the base_dir is appended.
    fn abs_pattern(base_dir: &PathBuf, pattern: Pattern) -> Pattern {
        if Self::is_absolute(&pattern) {
            pattern
        } else {
            let base_dir_pat = Pattern::escape(base_dir.to_string_lossy().as_ref());
            let base_dir_pat = base_dir_pat.replace("\u{FFFD}", "?");
            let new_pattern = Self::append_sep(base_dir_pat) + pattern.as_str();
            Pattern::new(new_pattern.as_str())
                .expect(format!("Failed to create glob pattern from {}", new_pattern).as_str())
        }
    }

    fn append_sep(s: String) -> String {
        if s.ends_with(MAIN_SEPARATOR) { s } else { s + MAIN_SEPARATOR.to_string().as_str() }
    }

    fn is_absolute(pattern: &Pattern) -> bool {
        let s = pattern.as_str();
        PathBuf::from(s).is_absolute()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn match_all() {

    }

    #[test]
    fn include_absolute() {
        let base_dir = PathBuf::from("/test");
        let includes = vec![Pattern::new("/test/foo/**").unwrap()];
        let excludes = vec![];
        let selector = PathSelector::new(base_dir, includes, excludes);

        assert!(selector.matches_fully(&PathBuf::from("/test/foo/")));
        assert!(selector.matches_fully(&PathBuf::from("/test/foo/bar")));
        assert!(selector.matches_fully(&PathBuf::from("/test/foo/bar/baz")));
        assert!(!selector.matches_fully(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_relative() {
        let base_dir = PathBuf::from("/test");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_fully(&PathBuf::from("/test/foo/")));
        assert!(selector.matches_fully(&PathBuf::from("/test/foo/bar")));
        assert!(selector.matches_fully(&PathBuf::from("/test/foo/bar/baz")));
        assert!(selector.matches_fully(&PathBuf::from("foo/")));
        assert!(selector.matches_fully(&PathBuf::from("foo/bar")));
        assert!(selector.matches_fully(&PathBuf::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_fully(&PathBuf::from("bar")));
        assert!(!selector.matches_fully(&PathBuf::from("/bar")));
        assert!(!selector.matches_fully(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_relative_root_base() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_fully(&PathBuf::from("/foo/")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/bar")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/bar/baz")));
        assert!(selector.matches_fully(&PathBuf::from("foo/")));
        assert!(selector.matches_fully(&PathBuf::from("foo/bar")));
        assert!(selector.matches_fully(&PathBuf::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_fully(&PathBuf::from("bar")));
        assert!(!selector.matches_fully(&PathBuf::from("/bar")));
        assert!(!selector.matches_fully(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_exclude() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![Pattern::new("foo/b*/**").unwrap()];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_fully(&PathBuf::from("/foo/")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/foo")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/foo/foo")));

        // not matching:
        assert!(!selector.matches_fully(&PathBuf::from("/foo/bar/")));
        assert!(!selector.matches_fully(&PathBuf::from("/foo/bar/baz")));
        assert!(!selector.matches_fully(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn prefix_wildcard() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("**/public-?.jpg").unwrap()];
        let excludes = vec![Pattern::new("**/private-?.jpg").unwrap()];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching absolute:
        assert!(selector.matches_fully(&PathBuf::from("/public-1.jpg")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/public-2.jpg")));
        assert!(selector.matches_fully(&PathBuf::from("/foo/foo/public-3.jpg")));

        // matching relative:
        assert!(selector.matches_fully(&PathBuf::from("public-1.jpg")));
        assert!(selector.matches_fully(&PathBuf::from("foo/public-2.jpg")));
        assert!(selector.matches_fully(&PathBuf::from("foo/foo/public-3.jpg")));

        // not matching absolute:
        assert!(!selector.matches_fully(&PathBuf::from("/something-else.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("/private-1.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("/foo/private-2.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("/foo/foo/private-3.jpg")));

        // not matching relative:
        assert!(!selector.matches_fully(&PathBuf::from("something-else.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("private-1.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("foo/private-2.jpg")));
        assert!(!selector.matches_fully(&PathBuf::from("foo/foo/private-3.jpg")));
    }

}
