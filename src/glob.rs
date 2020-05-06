use glob::Pattern;
use std::path::{PathBuf, MAIN_SEPARATOR};

/// Stores glob patterns working together as a path selector.
///
/// A path is selected only if it matches at least one include pattern
/// and doesn't match any exclude patterns.
/// An empty include pattern vector matches all paths.
pub struct PathSelector {
    base_dir: PathBuf,
    included_files: Vec<Pattern>,
    excluded_files: Vec<Pattern>,
    included_dirs: Vec<Vec<Pattern>>,
    excluded_dirs: Vec<Pattern>,
}

impl PathSelector {

    /// Creates a new selector with given include and exclude patterns.
    pub fn new(base_dir: PathBuf, includes: Vec<Pattern>, excludes: Vec<Pattern>) -> PathSelector {
        let includes: Vec<Pattern> = includes.into_iter().map(|pat|
            Self::abs_pattern(&base_dir, pat)).collect();
        let excludes: Vec<Pattern> = excludes.into_iter().map(|pat|
            Self::abs_pattern(&base_dir, pat)).collect();

        PathSelector {
            base_dir,
            included_dirs: includes.iter()
                .map(Self::split_non_recursive_prefix)
                .collect(),
            excluded_dirs: excludes.iter()
                .filter(|&p| Self::has_match_all_suffix(p))
                .flat_map(|p| vec![p.clone(), Self::strip_match_all_suffix(&p)])
                .collect(),
            included_files: includes,
            excluded_files: excludes,
        }
    }

    /// Creates a new selector that matches all paths.
    pub fn match_all() -> PathSelector {
        PathSelector {
            base_dir: "/".into(),
            included_files: vec![],
            excluded_files: vec![],
            included_dirs: vec![],
            excluded_dirs: vec![]
        }
    }

    /// Returns true if the given path fully matches this selector.
    /// The path must be absolute.
    pub fn matches_full_path(&self, path: &PathBuf) -> bool {
        self.with_absolute_path(path, |path| {
            (self.included_files.is_empty()
                || self.included_files.iter().any(|p| p.matches_path(path)))
                && self.excluded_files.iter().all(|p| !p.matches_path(path))
        })
    }

    /// Returns true if the given directory may contain matching paths.
    /// Used to decide whether the directory walk should descend to that directory.
    /// The directory should be allowed only if:
    /// 1. all its components match a prefix of at least one include filter,
    /// 2. it doesn't match any of the exclude filters ending with `**` pattern.
    pub fn matches_dir(&self, path: &PathBuf) -> bool {
        self.with_absolute_path(path, |path| {
            (self.included_dirs.is_empty()
                || self.included_dirs.iter().any(|v| Self::prefix_matches_path(v, path)))
                && self.excluded_dirs.iter().all(|p| !p.matches_path(path))
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

    /// Appends path separator if the string doesn't end with one already
    fn append_sep(s: String) -> String {
        if s.ends_with(MAIN_SEPARATOR) { s } else { s + MAIN_SEPARATOR.to_string().as_str() }
    }

    ///  Returns true if pattern can match absolute paths
    fn is_absolute(pattern: &Pattern) -> bool {
        let s = pattern.as_str();
        PathBuf::from(s).is_absolute()
    }

    /// Finds the longest prefix of the `pattern` that doesn't contain recursive wildcard `**`.
    /// Then splits the prefix into separate patterns, one per each path component.
    fn split_non_recursive_prefix(pattern: &Pattern) -> Vec<Pattern> {
        let path = PathBuf::from(pattern.as_str());
        path.components()
            .into_iter()
            .take_while(|c| c.as_os_str() != "**")
            .map(|c| Pattern::new(c.as_os_str().to_str().unwrap()).unwrap())
            .collect()
    }

    /// Returns true if pattern has a "match all" wildcard (`**`) at the end
    fn has_match_all_suffix(pattern: &Pattern) -> bool {
        let s = pattern.as_str();
        s.ends_with("/**") || s.ends_with("/**/*")
    }

    /// Drops the "match all" wildcard (`**`) from the end of the pattern
    fn strip_match_all_suffix(pattern: &Pattern) -> Pattern {
        Pattern::new(pattern.as_str()
            .trim_end_matches("/**/*")
            .trim_end_matches("/**")).unwrap()
    }

    /// Returns true if all components of the path match corresponding initial patterns in
    /// the `pattern` vector.
    fn prefix_matches_path(pattern: &Vec<Pattern>, path: &PathBuf) -> bool {
        pattern.iter().zip(path.iter())
            .all(|(p, c)| p.matches(c.to_string_lossy().as_ref()))
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

        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/bar")));
        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/bar/baz")));
        assert!(!selector.matches_full_path(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_relative() {
        let base_dir = PathBuf::from("/test");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/bar")));
        assert!(selector.matches_full_path(&PathBuf::from("/test/foo/bar/baz")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/bar")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_full_path(&PathBuf::from("bar")));
        assert!(!selector.matches_full_path(&PathBuf::from("/bar")));
        assert!(!selector.matches_full_path(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_relative_root_base() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_full_path(&PathBuf::from("/foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/bar")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/bar/baz")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/bar")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/bar/baz")));

        // not matching:
        assert!(!selector.matches_full_path(&PathBuf::from("bar")));
        assert!(!selector.matches_full_path(&PathBuf::from("/bar")));
        assert!(!selector.matches_full_path(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn include_exclude() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("foo/**").unwrap()];
        let excludes = vec![Pattern::new("foo/b*/**").unwrap()];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching:
        assert!(selector.matches_full_path(&PathBuf::from("/foo/")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/foo")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/foo/foo")));

        // not matching:
        assert!(!selector.matches_full_path(&PathBuf::from("/foo/bar/")));
        assert!(!selector.matches_full_path(&PathBuf::from("/foo/bar/baz")));
        assert!(!selector.matches_full_path(&PathBuf::from("/test/bar")));
    }

    #[test]
    fn prefix_wildcard() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("**/public-?.jpg").unwrap()];
        let excludes = vec![Pattern::new("**/private-?.jpg").unwrap()];
        let selector = PathSelector::new(base_dir, includes, excludes);

        // matching absolute:
        assert!(selector.matches_full_path(&PathBuf::from("/public-1.jpg")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/public-2.jpg")));
        assert!(selector.matches_full_path(&PathBuf::from("/foo/foo/public-3.jpg")));

        // matching relative:
        assert!(selector.matches_full_path(&PathBuf::from("public-1.jpg")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/public-2.jpg")));
        assert!(selector.matches_full_path(&PathBuf::from("foo/foo/public-3.jpg")));

        // not matching absolute:
        assert!(!selector.matches_full_path(&PathBuf::from("/something-else.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("/private-1.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("/foo/private-2.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("/foo/foo/private-3.jpg")));

        // not matching relative:
        assert!(!selector.matches_full_path(&PathBuf::from("something-else.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("private-1.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("foo/private-2.jpg")));
        assert!(!selector.matches_full_path(&PathBuf::from("foo/foo/private-3.jpg")));
    }

    #[test]
    fn matches_dir() {
        let base_dir = PathBuf::from("/");
        let includes = vec![Pattern::new("/test[1-9]/**").unwrap()];
        let excludes = vec![Pattern::new("/test[1-9]/foo/**").unwrap()];
        let selector = PathSelector::new(base_dir, includes, excludes);

        assert!(selector.matches_dir(&PathBuf::from("/")));
        assert!(selector.matches_dir(&PathBuf::from("/test1")));
        assert!(selector.matches_dir(&PathBuf::from("/test2/bar")));

        assert!(!selector.matches_dir(&PathBuf::from("/test999")));
        assert!(!selector.matches_dir(&PathBuf::from("/test999/bar")));
        assert!(!selector.matches_dir(&PathBuf::from("/test3/foo")));
        assert!(!selector.matches_dir(&PathBuf::from("/test3/foo/bar/baz")));
    }

}
