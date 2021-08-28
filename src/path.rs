//! Memory-efficient file path representation.

use std::ffi::{CStr, CString, OsString};
use std::fmt;
use std::fmt::Display;
use std::hash::Hash;
use std::path::{Component, PathBuf};
use std::sync::Arc;

use metrohash::MetroHash128;
use nom::lib::std::fmt::Formatter;
use serde::{Serialize, Serializer};
use smallvec::SmallVec;

use crate::path::string::{c_to_os_str, os_to_c_str};

#[cfg(unix)]
pub const PATH_ESCAPE_CHAR: &str = "\\";

#[cfg(windows)]
pub const PATH_ESCAPE_CHAR: &str = "^";

/// Memory-efficient file path representation.
///
/// When storing multiple paths with common parent, the standard [`std::path::PathBuf`]
/// would keep the parent path text duplicated in memory, wasting a lot of memory.
/// This structure here shares the common parent between many paths by reference-counted
/// references.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Path {
    parent: Option<Arc<Path>>,
    component: CString,
}

impl Path {
    pub fn canonicalize(&self) -> Path {
        let path_buf = self.to_path_buf();
        match dunce::canonicalize(path_buf.clone()) {
            Ok(p) => Path::from(p),
            Err(_) => Path::from(path_buf),
        }
    }

    #[cfg(unix)]
    pub fn is_absolute(&self) -> bool {
        let root = self.root().component.as_bytes();
        root == b"/"
    }

    #[cfg(windows)]
    pub fn is_absolute(&self) -> bool {
        let root = self.root().component.as_bytes();
        root[root.len() - 1] == b':'
    }

    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    /// Moves this [`Path`] under an [`Arc`].
    /// You need to wrap [`Path`] before joining anything to it.
    pub fn share(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Copies this path from under an [`Arc`].
    /// Generally cheap, because only the last component is copied.
    pub fn unshare(self: &Arc<Path>) -> Path {
        self.as_ref().clone()
    }

    /// Creates an owned [`Path`] with `path` adjoined to `self`.
    /// The `path` must be relative.
    pub fn join<P: AsRef<Path>>(self: &Arc<Path>, path: P) -> Path {
        let path = path.as_ref();
        assert!(path.is_relative());
        let components = path.components();
        let mut iter = components.iter();
        let mut result = self.push(CString::from(*iter.next().unwrap()));

        for &c in iter {
            result = Arc::new(result).push(CString::from(c));
        }
        result
    }

    /// Returns the name of the last component of this path or None
    /// if the path is directory (e.g. root dir or parent dir).
    pub fn file_name(&self) -> Option<OsString> {
        match self.component.as_bytes() {
            b"/" => None,
            b".." => None,
            b"." => None,
            _ => Some(c_to_os_str(self.component.as_c_str())),
        }
    }

    /// Returns the name of the last component of this path or None
    /// if the path is directory (e.g. root dir or parent dir).
    /// Doesn't allocate anything on the heap.
    pub fn file_name_cstr(&self) -> Option<&CStr> {
        match self.component.as_bytes() {
            b"/" => None,
            b".." => None,
            b"." => None,
            _ => Some(self.component.as_c_str()),
        }
    }

    /// Returns the parent directory of this path.
    /// Doesn't allocate anything on the heap.
    pub fn parent(&self) -> Option<&Arc<Path>> {
        self.parent.as_ref()
    }

    /// Returns a path that joined to `base` would give this path.
    /// If base is the same as this path, returns current directory.
    /// If this path doesn't have a `base` prefix, returns `None`.
    pub fn strip_prefix(&self, base: &Path) -> Option<Path> {
        let mut self_components = self.components().into_iter().peekable();
        let mut base_components = base.components().into_iter().peekable();
        while let (Some(a), Some(b)) = (self_components.peek(), base_components.peek()) {
            if a != b {
                return None;
            }
            self_components.next();
            base_components.next();
        }
        Some(Path::make(self_components))
    }

    /// Returns true if self is a prefix of another path
    pub fn is_prefix_of(&self, other: &Path) -> bool {
        let mut self_components = self.components().into_iter().peekable();
        let mut other_components = other.components().into_iter().peekable();
        while let (Some(a), Some(b)) = (self_components.peek(), other_components.peek()) {
            if a != b {
                return false;
            }
            self_components.next();
            other_components.next();
        }
        self_components.peek() == None
    }

    /// Converts this path to a standard library path buffer.
    /// We need this to be able to use this path with other standard library I/O functions.
    pub fn to_path_buf(&self) -> PathBuf {
        let mut result = PathBuf::from(OsString::with_capacity(self.capacity()));
        self.for_each_component(|c| result.push(c_to_os_str(c)));
        result
    }

    /// Converts this path to an UTF encoded string.
    /// Any non-Unicode sequences are replaced with
    /// [`U+FFFD REPLACEMENT CHARACTER`][U+FFFD].
    pub fn to_string_lossy(&self) -> String {
        self.to_path_buf().to_string_lossy().to_string()
    }

    pub fn display(&self) -> &Self {
        self
    }

    /// Returns a hash of the full path. Useful for deduplicating paths without making path clones.
    /// We need 128-bits so that collisions are not a problem.
    /// Thanks to using a long hash we can be sure collisions won't be a problem.
    pub fn hash128(&self) -> u128 {
        let mut hasher = MetroHash128::new();
        self.hash(&mut hasher);
        let (a, b) = hasher.finish128();
        (a as u128) << 64 | (b as u128)
    }

    fn new(component: CString) -> Path {
        Path {
            component,
            parent: None,
        }
    }

    fn push(self: &Arc<Path>, component: CString) -> Path {
        Path {
            component,
            parent: Some(self.clone()),
        }
    }

    /// Flattens this path to a vector of strings
    fn components(&self) -> SmallVec<[&CStr; 16]> {
        let mut result = match &self.parent {
            Some(p) => p.components(),
            None => SmallVec::new(),
        };
        result.push(&self.component);
        result
    }

    /// Returns the number of components in this path
    pub fn component_count(&self) -> usize {
        let mut count = 0;
        self.for_each_component(|_| count += 1);
        count
    }

    /// Executes a function for each component, left to right
    fn for_each_component<F: FnMut(&CStr)>(&self, mut f: F) {
        self.for_each_component_ref(&mut f)
    }

    /// Executes a function for each component, left to right
    fn for_each_component_ref<F: FnMut(&CStr)>(&self, f: &mut F) {
        self.parent.iter().for_each(|p| p.for_each_component_ref(f));
        (f)(self.component.as_c_str())
    }

    /// Estimates size of this path in bytes
    fn capacity(&self) -> usize {
        let mut result: usize = 0;
        self.for_each_component(|c| result += c.to_bytes().len() + 1);
        result
    }

    /// Builds a path from individual string components.
    /// If the component list is empty, returns a path pointing to the current directory (".").
    fn make<'a, I>(components: I) -> Path
    where
        I: IntoIterator<Item = &'a CStr> + 'a,
    {
        let mut iter = components.into_iter();
        let first = iter.next();
        let mut result: Path = match first {
            None => Path::new(CString::new(".").unwrap()),
            Some(c) => Path::new(CString::from(c)),
        };
        for c in iter {
            result = Arc::new(result).push(CString::from(c))
        }
        result
    }

    /// Returns the first component of this path
    pub fn root(&self) -> &Path {
        let mut result = self;
        while let Some(parent) = &result.parent {
            result = parent.as_ref();
        }
        result
    }

    #[cfg(unix)]
    pub fn shell_quote(&self) -> String {
        shell_words::quote(self.to_string().as_str()).into()
    }

    #[cfg(windows)]
    pub fn shell_quote(&self) -> String {
        let s = self.to_string();
        const SPECIAL_CHARS: [char; 14] = [
            ' ', '"', '(', ')', '^', '%', '$', '~', '=', '+', '*', '?', '|', '/',
        ];
        let needs_quote = s.chars().into_iter().any(|c| SPECIAL_CHARS.contains(&c));
        if needs_quote {
            format!("\"{}\"", s)
        } else {
            s
        }
    }
}

impl AsRef<Path> for Path {
    fn as_ref(&self) -> &Path {
        self
    }
}

/// Converts std path Component to a new CString
fn component_to_c_string(c: &Component<'_>) -> CString {
    os_to_c_str(c.as_os_str())
}

impl From<&std::path::Path> for Path {
    fn from(p: &std::path::Path) -> Self {
        let mut components = p.components();
        let mut result = Path::new(component_to_c_string(
            &components.next().expect("Empty path not supported"),
        ));
        for c in components {
            result = Arc::new(result).push(component_to_c_string(&c))
        }
        result
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        Path::from(std::path::PathBuf::from(s).as_path())
    }
}

impl From<String> for Path {
    fn from(s: String) -> Self {
        Path::from(std::path::PathBuf::from(s).as_path())
    }
}

impl From<OsString> for Path {
    fn from(s: OsString) -> Self {
        Path::from(std::path::PathBuf::from(s).as_path())
    }
}

impl From<std::path::PathBuf> for Path {
    fn from(p: std::path::PathBuf) -> Self {
        Path::from(p.as_path())
    }
}

impl From<&std::path::PathBuf> for Path {
    fn from(p: &std::path::PathBuf) -> Self {
        Path::from(p.as_path())
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", self.to_path_buf().display()).as_str())
    }
}

impl Serialize for Path {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path_buf().display())
    }
}

mod string {
    use std::ffi::{CStr, CString, OsStr, OsString};

    #[cfg(unix)]
    pub fn c_to_os_str(str: &CStr) -> OsString {
        use std::os::unix::ffi::OsStrExt;
        OsStr::from_bytes(str.to_bytes()).to_os_string()
    }

    #[cfg(unix)]
    pub fn os_to_c_str(str: &OsStr) -> CString {
        use std::os::unix::ffi::OsStrExt;
        CString::new(str.as_bytes()).unwrap()
    }

    #[cfg(windows)]
    pub fn c_to_os_str(str: &CStr) -> OsString {
        OsString::from(str.to_str().unwrap())
    }

    #[cfg(windows)]
    pub fn os_to_c_str(str: &OsStr) -> CString {
        CString::new(str.to_str().unwrap().as_bytes()).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_convert(s: &str) {
        assert_eq!(PathBuf::from(s), Path::from(s).to_path_buf());
    }

    #[test]
    fn convert() {
        test_convert("/");
        test_convert("/bar");
        test_convert("/foo/bar");
        test_convert(".");
        test_convert("./foo/bar");
        test_convert("../foo/bar");
        test_convert("..");
        test_convert("foo");
        test_convert("foo/bar/baz");
        test_convert("foo/bar/baz");
    }

    #[test]
    fn file_name() {
        assert_eq!(
            Path::from("foo").file_name_cstr(),
            Some(CString::new("foo").unwrap().as_c_str())
        );
        assert_eq!(
            Path::from("foo/bar").file_name_cstr(),
            Some(CString::new("bar").unwrap().as_c_str())
        );
        assert_eq!(
            Path::from("/foo").file_name_cstr(),
            Some(CString::new("foo").unwrap().as_c_str())
        );
        assert_eq!(
            Path::from("/foo/bar").file_name_cstr(),
            Some(CString::new("bar").unwrap().as_c_str())
        );
        assert_eq!(Path::from("/").file_name_cstr(), None);
        assert_eq!(Path::from(".").file_name_cstr(), None);
        assert_eq!(Path::from("..").file_name_cstr(), None);
    }

    #[test]
    fn parent() {
        assert_eq!(
            Path::from("foo/bar").parent(),
            Some(&Arc::new(Path::from("foo")))
        );
        assert_eq!(
            Path::from("/foo").parent(),
            Some(&Arc::new(Path::from("/")))
        );
        assert_eq!(Path::from("/").parent(), None);
    }

    #[test]
    fn share_parents() {
        let parent = Path::from("/parent").share();
        let child1 = parent.join(Path::from("c1"));
        let child2 = parent.join(Path::from("c2"));
        assert_eq!(PathBuf::from("/parent/c1"), child1.to_path_buf());
        assert_eq!(PathBuf::from("/parent/c2"), child2.to_path_buf());
    }

    #[test]
    fn is_absolute() {
        assert!(Path::from("/foo/bar").is_absolute());
        assert!(!Path::from("foo/bar").is_absolute());
        assert!(!Path::from("./foo/bar").is_absolute());
        assert!(!Path::from("../foo/bar").is_absolute());
    }

    #[test]
    fn strip_prefix() {
        assert_eq!(
            Path::from("/foo/bar").strip_prefix(&Path::from("/foo")),
            Some(Path::from("bar"))
        );
        assert_eq!(
            Path::from("/foo/bar").strip_prefix(&Path::from("/foo/bar")),
            Some(Path::from("."))
        );
        assert_eq!(
            Path::from("/foo/bar").strip_prefix(&Path::from("/bar")),
            None
        );
    }

    #[test]
    fn is_prefix_of() {
        assert!(Path::from("/foo/bar").is_prefix_of(&Path::from("/foo/bar")));
        assert!(Path::from("/foo/bar").is_prefix_of(&Path::from("/foo/bar/baz")));
        assert!(!Path::from("/foo/bar").is_prefix_of(&Path::from("/foo")))
    }
}
