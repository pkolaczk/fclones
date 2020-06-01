use std::{fmt, io};
use std::ffi::OsString;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use nom::lib::std::fmt::Formatter;
use serde::{Serialize, Serializer};
use smallvec::SmallVec;

/// Memory-efficient path representation.
/// When storing multiple paths with common parent, the standard PathBuf would keep
/// the parent duplicated in memory, wasting a lot of memory.
/// This shares the common parent between many paths.
/// The price is a tiny cost of managing Arc references.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Path {
    component: OsString,
    parent: Option<Arc<Path>>
}

impl Path {

    pub fn canonicalize(&self) -> io::Result<Path> {
        self.to_std_path().canonicalize().map(|p| Path::from(&p))
    }

    pub fn is_absolute(&self) -> bool {
        PathBuf::from(&self.root().component).is_absolute()
    }

    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    pub fn join<P: AsRef<Path>>(self: &Arc<Path>, path: P) -> Path {
        let path = path.as_ref();
        assert!(path.is_relative());
        let components = path.components();
        let mut iter = components.iter();
        let mut result = self.push((*iter.next().unwrap()).clone());

        while let Some(&c) = iter.next() {
            result = Arc::new(result).push(c.clone());
        }
        result
    }

    pub fn file_name(&self) -> Option<OsString> {
        PathBuf::from(&self.component).file_name().map(|s| OsString::from(s))
    }

    pub fn parent(&self) -> Option<&Arc<Path>> {
        self.parent.as_ref()
    }

    pub fn strip_prefix(&self, base: &Path) -> Option<Path> {
        unimplemented!()
    }

    pub fn to_std_path(&self) -> PathBuf {
        match &self.parent {
            Some(p) => p.to_std_path().join(self.component.as_os_str()),
            None => PathBuf::from(self.component.as_os_str())
        }
    }

    pub fn to_string_lossy(&self) -> String {
        self.to_std_path().to_string_lossy().to_string()
    }

    pub fn display(&self) -> &Self {
        &self
    }

    fn new(component: OsString) -> Path {
        Path { component, parent: None }
    }

    fn push(self: &Arc<Path>, mut component: OsString) -> Path {
        component.shrink_to_fit();
        Path { component, parent: Some(self.clone()) }
    }

    /// Flattens this path to a vector of strings
    pub fn components(self: &Path) -> SmallVec<[&OsString;16]> {
        let mut result = match &self.parent {
            Some(p) => p.components(),
            None => SmallVec::new()
        };
        result.push(&self.component);
        result
    }

    pub fn root(&self) -> &Path {
        let mut result = self;
        while let Some(parent) = &result.parent {
            result = parent.as_ref();
        }
        result
    }
}

impl AsRef<Path> for Path {
    fn as_ref(&self) -> &Path {
        &self
    }
}

impl From<&std::path::Path> for Path {
    fn from(p: &std::path::Path) -> Self {
        let mut components = p.components();
        let mut result = Path::new(OsString::from(
            &components.next().expect("Empty path not supported")));
        for c in components {
            result = Arc::new(result).push(OsString::from(&c))
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

impl Hash for Path {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.component.hash(state);
        self.parent.hash(state);
    }
}


impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", self.to_std_path().display()).as_str())
    }
}

impl Serialize for Path {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
        where S: Serializer {
        serializer.collect_str(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_convert(s: &str) {
        assert_eq!(PathBuf::from(s), Path::from(s).to_std_path());
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
        assert_eq!(Path::from("foo").file_name(), Some(OsStr::new("foo")));
        assert_eq!(Path::from("foo/bar").file_name(), Some(OsStr::new("bar")));
        assert_eq!(Path::from("/foo").file_name(), Some(OsStr::new("foo")));
        assert_eq!(Path::from("/foo/bar").file_name(), Some(OsStr::new("bar")));
        assert_eq!(Path::from("/").file_name(), None);
        assert_eq!(Path::from(".").file_name(), None);
        assert_eq!(Path::from("..").file_name(), None);
    }

    #[test]
    fn parent() {
        assert_eq!(Path::from("foo/bar").parent(), Some(&Arc::new(Path::from("foo"))));
        assert_eq!(Path::from("/foo").parent(), Some(&Arc::new(Path::from("/"))));
        assert_eq!(Path::from("/").parent(), None);
    }

    #[test]
    fn share_parents() {
        let parent: Arc<Path> = Arc::new(Path::from("/parent"));
        let child1 = parent.join("c1");
        let child2 = parent.join("c2");
        assert_eq!(PathBuf::from("/parent/c1"), child1.to_std_path());
        assert_eq!(PathBuf::from("/parent/c2"), child2.to_std_path());
    }

}