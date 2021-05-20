use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::path::{Path, MAIN_SEPARATOR};

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{anychar, none_of};
use nom::combinator::{cond, map};
use nom::multi::{many0, separated_list};
use nom::sequence::tuple;
use nom::IResult;
use regex::escape;

use crate::path::PATH_ESCAPE_CHAR;
use crate::regex::Regex;
use std::str::FromStr;

#[derive(Debug)]
pub struct PatternError {
    pub cause: String,
    pub input: String,
}

impl Display for PatternError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to compile pattern '{}': {}",
            self.input, self.cause
        )
    }
}

/// Pattern for matching paths and file names.
/// Can be constructed from a glob pattern or a raw regular expression.
#[derive(Clone, Debug)]
pub struct Pattern {
    src: String,
    anchored_regex: Regex,
    prefix_regex: Regex,
}

impl FromStr for Pattern {
    type Err = PatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Pattern::glob(s)
    }
}

pub struct PatternOpts {
    case_insensitive: bool,
}

impl PatternOpts {
    pub fn case_insensitive() -> PatternOpts {
        PatternOpts {
            case_insensitive: true,
        }
    }
}

impl Default for PatternOpts {
    fn default() -> PatternOpts {
        PatternOpts {
            case_insensitive: false,
        }
    }
}

#[derive(PartialEq, Debug)]
enum Scope {
    TopLevel,
    CurlyBrackets,
    RoundBrackets,
}

impl Pattern {
    /// Creates `Pattern` instance from raw regular expression. Supports PCRE syntax.
    pub fn regex(pattern: &str) -> Result<Pattern, PatternError> {
        Self::regex_with(pattern, &PatternOpts::default())
    }

    /// Creates `Pattern` instance from raw regular expression. Supports PCRE syntax.
    /// Allows to specify case sensitivity
    pub fn regex_with(pattern: &str, opts: &PatternOpts) -> Result<Pattern, PatternError> {
        let pattern = pattern.trim_start_matches('^');
        let pattern = pattern.trim_end_matches('$');
        let pattern = pattern.to_string();

        let anchored_regex = "^".to_string() + &pattern + "$";
        let anchored_regex = Regex::new(anchored_regex.as_str(), opts.case_insensitive);
        let prefix_regex = "^".to_string() + &pattern;
        let prefix_regex = Regex::new(prefix_regex.as_str(), opts.case_insensitive);

        match anchored_regex {
            Ok(anchored_regex) => Ok(Pattern {
                src: pattern,
                anchored_regex,
                prefix_regex: prefix_regex.unwrap(),
            }),
            Err(e) => Err(PatternError {
                input: pattern,
                cause: e.to_string(),
            }),
        }
    }

    /// Creates a `Pattern` that matches literal string. Case insensitive.
    /// Special characters in the string are escaped before creating the underlying regex.
    pub fn literal(s: &str) -> Pattern {
        Self::regex(escape(s).as_str()).unwrap()
    }

    /// Creates `Pattern` instance from a Unix extended glob.
    /// Case insensitive. For syntax reference see [glob_with](glob_with).
    pub fn glob(pattern: &str) -> Result<Pattern, PatternError> {
        Self::glob_with(pattern, &PatternOpts::default())
    }

    /// Creates `Pattern` instance from a Unix extended glob.
    ///
    /// Glob patterns handle the following wildcards:
    /// - `?`: matches any character
    /// - `*`: matches any sequence of characters except the directory separator
    /// - `**`: matches any sequence of characters
    /// - `[a-z]`: matches one of the characters or character ranges given in the square brackets
    /// - `[!a-z]`: matches any character that is not given in the square brackets
    /// - `{a,b}`: matches exactly one pattern from the comma-separated patterns given inside the curly brackets
    /// - `@(a|b)`: same as `{a,b}`
    /// - `?(a|b)`: matches at most one occurrence of the pattern inside the brackets
    /// - `+(a|b)`: matches at least occurrence of the patterns given inside the brackets
    /// - `*(a|b)`: matches any number of occurrences of the patterns given inside the brackets
    /// - `!(a|b)`: matches anything that doesn't match any of the patterns given inside the brackets
    ///
    /// Use `\` to escape the special symbols that need to be matched literally. E.g. `\*` matches
    /// a single `*` character.
    ///
    pub fn glob_with(glob: &str, opts: &PatternOpts) -> Result<Pattern, PatternError> {
        let result: IResult<&str, String> = Self::glob_to_regex(Scope::TopLevel, glob);
        match result {
            Ok((remaining, regex)) if remaining.is_empty() => {
                Self::regex_with(regex.as_str(), opts)
            }
            Ok((remaining, _)) => Err(PatternError {
                input: glob.to_string(),
                cause: format!(
                    "Unexpected '{}' at end of input",
                    remaining.chars().next().unwrap()
                ),
            }),
            Err(e) => Err(PatternError {
                input: glob.to_string(),
                cause: e.to_string(),
            }),
        }
    }

    /// Returns true if this pattern fully matches the given path
    pub fn matches(&self, path: &str) -> bool {
        self.anchored_regex.is_match(path)
    }

    /// Returns true if a prefix of this pattern fully matches the given path
    pub fn matches_partially(&self, path: &str) -> bool {
        self.anchored_regex.is_partial_match(path)
    }

    /// Returns true if this pattern fully matches a prefix of the given path
    pub fn matches_prefix(&self, path: &str) -> bool {
        self.prefix_regex.is_match(path)
    }

    /// Returns true if this pattern fully matches given file path
    pub fn matches_path(&self, path: &Path) -> bool {
        self.anchored_regex
            .is_match(path.to_string_lossy().as_ref())
    }

    /// Parses a UNIX glob and converts it to a regular expression
    fn glob_to_regex(scope: Scope, glob: &str) -> IResult<&str, String> {
        // pass escaped characters as-is:
        let p_escaped = map(tuple((tag(PATH_ESCAPE_CHAR), anychar)), |(_, c)| {
            escape(c.to_string().as_str())
        });

        fn mk_string(contents: Vec<String>, prefix: &str, sep: &str, suffix: &str) -> String {
            format!("{}{}{}", prefix, contents.join(sep), suffix)
        }

        // { glob1, glob2, ..., globN } -> ( regex1, regex2, ..., regexN )
        let p_alt = map(
            tuple((
                tag("{"),
                separated_list(tag(","), |g| Self::glob_to_regex(Scope::CurlyBrackets, g)),
                tag("}"),
            )),
            |(_, list, _)| mk_string(list, "(", "|", ")"),
        );

        let p_ext_glob = map(
            tuple((
                tag("("),
                separated_list(tag("|"), |g| Self::glob_to_regex(Scope::RoundBrackets, g)),
                tag(")"),
            )),
            |(_, list, _)| list,
        );

        let p_ext_optional = map(tuple((tag("?"), |i| p_ext_glob(i))), |(_, g)| {
            mk_string(g, "(", "|", ")?")
        });
        let p_ext_many = map(tuple((tag("*"), |i| p_ext_glob(i))), |(_, g)| {
            mk_string(g, "(", "|", ")*")
        });
        let p_ext_at_least_once = map(tuple((tag("+"), |i| p_ext_glob(i))), |(_, g)| {
            mk_string(g, "(", "|", ")+")
        });
        let p_ext_exactly_once = map(tuple((tag("@"), |i| p_ext_glob(i))), |(_, g)| {
            mk_string(g, "(", "|", ")")
        });
        let p_ext_never = map(tuple((tag("!"), |i| p_ext_glob(i))), |(_, g)| {
            mk_string(g, "(?!", "|", ")")
        });

        // ** -> .*
        let p_double_star = map(tag("**"), |_| ".*".to_string());

        let escaped_sep = escape(MAIN_SEPARATOR.to_string().as_str());

        // * -> [^/]*
        let p_single_star = map(tag("*"), |_| "[^".to_string() + &escaped_sep + "]*");

        // ? -> .
        let p_question_mark = map(tag("?"), |_| "[^".to_string() + &escaped_sep + "]");

        // [ characters ] -> [ characters ]
        let p_neg_character_set = map(
            tuple((tag("[!"), many0(none_of("]")), tag("]"))),
            |(_, characters, _)| {
                "[^".to_string() + &characters.into_iter().collect::<String>() + "]"
            },
        );

        // [ characters ] -> [ characters ]
        let p_character_set = map(
            tuple((tag("["), many0(none_of("]")), tag("]"))),
            |(_, characters, _)| {
                "[".to_string() + &characters.into_iter().collect::<String>() + "]"
            },
        );

        let p_separator = map(tag("/"), |_| escaped_sep.clone());

        // if we are nested, we can't just pass these through without interpretation
        let p_any_char = map(
            tuple((
                cond(scope == Scope::TopLevel, anychar),
                cond(scope == Scope::CurlyBrackets, none_of("{,}")),
                cond(scope == Scope::RoundBrackets, none_of("(|)")),
            )),
            |(a, b, c)| escape(a.or(b).or(c).unwrap().to_string().as_str()),
        );

        let p_token = alt((
            p_escaped,
            p_alt,
            p_ext_optional,
            p_ext_many,
            p_ext_at_least_once,
            p_ext_exactly_once,
            p_ext_never,
            p_double_star,
            p_single_star,
            p_question_mark,
            p_neg_character_set,
            p_character_set,
            p_separator,
            p_any_char,
        ));

        let parse_all = map(many0(p_token), |s| s.join(""));
        (parse_all)(glob)
    }
}

impl Add<Pattern> for Pattern {
    type Output = Pattern;

    fn add(self, rhs: Pattern) -> Self::Output {
        Pattern::regex((self.to_string() + &rhs.to_string()).as_str()).unwrap()
    }
}

impl ToString for Pattern {
    fn to_string(&self) -> String {
        self.src.clone()
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;

    fn glob_to_regex_str(glob: &str) -> String {
        Pattern::glob(glob).unwrap().to_string()
    }

    fn native_dir_sep(str: &str) -> String {
        str.replace("/", MAIN_SEPARATOR.to_string().as_str())
    }

    #[test]
    fn empty() {
        assert_eq!(glob_to_regex_str(""), "");
    }

    #[test]
    fn output_escaping() {
        assert_eq!(glob_to_regex_str("foo.jpg"), "foo\\.jpg");
        assert_eq!(glob_to_regex_str("foo(bar)"), "foo\\(bar\\)");
    }

    #[test]
    fn input_escaping() {
        assert_eq!(glob_to_regex_str("foo\\*"), "foo\\*");
        assert_eq!(glob_to_regex_str("foo\\?"), "foo\\?");
        assert_eq!(glob_to_regex_str("foo\\{"), "foo\\{");
        assert_eq!(glob_to_regex_str("foo\\}"), "foo\\}");
    }

    #[test]
    fn question_mark() {
        let p = Pattern::glob("foo???").unwrap();
        assert!(p.matches("foo123"));
        assert!(!p.matches_path(&PathBuf::from("foo").join("23")));
    }

    #[test]
    fn single_star() {
        let p = Pattern::glob("foo*").unwrap();
        assert!(p.matches("foo123"));
        assert!(!p.matches(native_dir_sep("foo/bar").as_str()));
    }

    #[test]
    fn double_star() {
        let p = Pattern::glob("foo/**/bar").unwrap();
        assert!(p.matches(native_dir_sep("foo/1/2/bar").as_str()));
    }

    #[test]
    fn character_set() {
        assert_eq!(glob_to_regex_str("[a-b.*?-]"), "[a-b.*?-]");
        assert_eq!(glob_to_regex_str("[!a-b.*?-]"), "[^a-b.*?-]");
    }

    #[test]
    fn alternatives() {
        assert_eq!(glob_to_regex_str("{a,b,c}"), "(a|b|c)");
        let p = Pattern::glob("{*.jpg,*.JPG}").unwrap();
        assert!(p.matches("foo.jpg"));
        assert!(p.matches("foo.JPG"));
    }

    #[test]
    fn nested_alternatives() {
        assert_eq!(glob_to_regex_str("{a,{b,c}}"), "(a|(b|c))");
    }

    #[test]
    fn naked_comma() {
        assert_eq!(glob_to_regex_str("a,b,c"), "a,b,c");
    }

    #[test]
    fn naked_bar() {
        assert_eq!(glob_to_regex_str("a|b|c"), "a\\|b\\|c");
    }

    #[test]
    fn unbalanced_paren() {
        // this is how bash interprets unbalanced paren
        assert_eq!(glob_to_regex_str("{a,b,c"), "\\{a,b,c");
        assert_eq!(glob_to_regex_str("a,b,c}"), "a,b,c\\}");
        assert_eq!(glob_to_regex_str("{{a,b}"), "\\{(a|b)");
        assert_eq!(glob_to_regex_str("{a,b}}"), "(a|b)\\}");
        assert_eq!(glob_to_regex_str("{{{a,b}"), "\\{\\{(a|b)");
        assert_eq!(glob_to_regex_str("{{{a,b}}"), "\\{((a|b))");
    }

    #[test]
    fn literal() {
        assert_eq!(
            Pattern::literal("test*?{}\\").to_string(),
            "test\\*\\?\\{\\}\\\\"
        )
    }

    #[test]
    fn case_insensitive() {
        let p = Pattern::glob_with("foo", &PatternOpts::case_insensitive()).unwrap();
        assert!(p.matches("foo"));
        assert!(p.matches("Foo"));
        assert!(p.matches("FOO"));
    }

    #[test]
    fn add() {
        assert_eq!(
            (Pattern::literal("/foo/bar/") + Pattern::glob("*").unwrap()).to_string(),
            Pattern::glob("/foo/bar/*").unwrap().to_string()
        )
    }

    #[test]
    fn matches_double_star_prefix() {
        let g = Pattern::glob("**/b").unwrap();
        assert!(g.matches(native_dir_sep("/b").as_str()));
        assert!(g.matches(native_dir_sep("/a/b").as_str()));
    }

    #[test]
    fn matches_double_star_infix() {
        let g1 = Pattern::glob("/a/**/c").unwrap();
        assert!(g1.matches(native_dir_sep("/a/b1/c").as_str()));
        assert!(g1.matches(native_dir_sep("/a/b1/b2/c").as_str()));
        assert!(g1.matches(native_dir_sep("/a/b1/b2/b3/c").as_str()));
    }

    #[test]
    fn ext_glob_optional() {
        let g = Pattern::glob("/a-?(foo|bar)").unwrap();
        assert!(g.matches(native_dir_sep("/a-foo").as_str()));
        assert!(g.matches(native_dir_sep("/a-bar").as_str()));
    }

    #[test]
    fn ext_glob_many() {
        let g = Pattern::glob("/a-*(foo|bar)").unwrap();
        assert!(g.matches(native_dir_sep("/a-").as_str()));
        assert!(g.matches(native_dir_sep("/a-foo").as_str()));
        assert!(g.matches(native_dir_sep("/a-foofoo").as_str()));
        assert!(g.matches(native_dir_sep("/a-foobar").as_str()));
    }

    #[test]
    fn ext_glob_at_least_one() {
        let g = Pattern::glob("/a-+(foo|bar)").unwrap();
        assert!(!g.matches(native_dir_sep("/a-").as_str()));
        assert!(g.matches(native_dir_sep("/a-foo").as_str()));
        assert!(g.matches(native_dir_sep("/a-foofoo").as_str()));
        assert!(g.matches(native_dir_sep("/a-foobar").as_str()));
    }

    #[test]
    fn ext_glob_nested() {
        let g = Pattern::glob("/a-@(foo|bar?(baz))").unwrap();
        assert!(g.matches(native_dir_sep("/a-foo").as_str()));
        assert!(g.matches(native_dir_sep("/a-bar").as_str()));
        assert!(g.matches(native_dir_sep("/a-barbaz").as_str()));
        assert!(!g.matches(native_dir_sep("/a-foobaz").as_str()));
    }

    #[test]
    fn ext_glob_exactly_one() {
        let g = Pattern::glob("/a-@(foo|bar)").unwrap();
        assert!(!g.matches(native_dir_sep("/a-").as_str()));
        assert!(g.matches(native_dir_sep("/a-foo").as_str()));
        assert!(!g.matches(native_dir_sep("/a-foofoo").as_str()));
        assert!(!g.matches(native_dir_sep("/a-foobar").as_str()));
    }

    #[test]
    fn matches_fully() {
        let g1 = Pattern::glob("/a/b?/*").unwrap();
        assert!(g1.matches(native_dir_sep("/a/b1/c").as_str()));
        assert!(g1.matches(native_dir_sep("/a/b1/").as_str()));
        assert!(!g1.matches(native_dir_sep("/a/b1").as_str()));
        assert!(!g1.matches(native_dir_sep("/a/b/c").as_str()));
    }

    #[test]
    fn matches_partially() {
        let g1 = Pattern::glob("/a/b/*").unwrap();
        assert!(g1.matches_partially(native_dir_sep("/a").as_str()));
        assert!(g1.matches_partially(native_dir_sep("/a/b").as_str()));
        assert!(g1.matches_partially(native_dir_sep("/a/b/foo").as_str()));
        assert!(!g1.matches_partially(native_dir_sep("/b/foo").as_str()));

        let g2 = Pattern::glob("/a/{b1,b2}/c/*").unwrap();
        assert!(g2.matches_partially(native_dir_sep("/a/b1").as_str()));
        assert!(g2.matches_partially(native_dir_sep("/a/b2").as_str()));
        assert!(g2.matches_partially(native_dir_sep("/a/b2/c").as_str()));
        assert!(!g2.matches_partially(native_dir_sep("/b2/c").as_str()));

        let g3 = Pattern::glob("/a/{b11,b21/b22}/c/*").unwrap();
        assert!(g3.matches_partially(native_dir_sep("/a/b11").as_str()));
        assert!(g3.matches_partially(native_dir_sep("/a/b11/c").as_str()));
        assert!(g3.matches_partially(native_dir_sep("/a/b21").as_str()));
        assert!(g3.matches_partially(native_dir_sep("/a/b21/b22").as_str()));
        assert!(g3.matches_partially(native_dir_sep("/a/b21/b22/c").as_str()));
    }

    #[test]
    fn matches_prefix() {
        let g1 = Pattern::glob("/a/b/*").unwrap();
        assert!(g1.matches_prefix(native_dir_sep("/a/b/c").as_str()));
        assert!(g1.matches_prefix(native_dir_sep("/a/b/z/foo").as_str()));
        assert!(!g1.matches_prefix(native_dir_sep("/a/c/z/foo").as_str()));
    }
}
