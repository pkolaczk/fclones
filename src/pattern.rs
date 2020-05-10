use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::path::PathBuf;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{anychar, none_of};
use nom::combinator::{cond, map};
use nom::IResult;
use nom::multi::{many0, separated_list};
use nom::sequence::tuple;
use pcre2::bytes::{Regex, RegexBuilder};
use regex::escape;

#[derive(Debug)]
pub struct PatternError {
    cause: String,
    input: String
}

impl Display for PatternError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid pattern '{}': {}", self.input, self.cause)
    }
}

/// Pattern for matching paths and file names.
/// Can be constructed from a glob pattern or a raw regular expression.
#[derive(Clone, Debug)]
pub struct Pattern {
    src: String,
    anchored_regex: Regex,
    prefix_regex: Regex
}

#[derive(PartialEq, Debug)]
enum Scope {
    TopLevel,
    CurlyBrackets,
    RoundBrackets
}

impl Pattern {

    /// Creates `Pattern` instance from raw regular expression
    pub fn regex(pattern: &str) -> Result<Pattern, PatternError> {
        let pattern = pattern.trim_start_matches("^");
        let pattern = pattern.trim_end_matches("$");
        let mut builder = RegexBuilder::new();
        builder.jit_if_available(true);

        let anchored_regex = "^".to_string() + pattern + "$";
        let anchored_regex = builder.build(anchored_regex.as_str());

        let prefix_regex = "^".to_string() + pattern;
        let prefix_regex = builder.build(prefix_regex.as_str());

        match anchored_regex {
            Ok(anchored_regex) => Ok(Pattern {
                src: pattern.to_owned(),
                anchored_regex,
                prefix_regex: prefix_regex.unwrap(),
            }),
            Err(e) => Err(PatternError {
                input: pattern.to_string(),
                cause: e.to_string()
            })
        }
    }

    /// Creates a `Pattern` that matches literal string.
    /// Special characters in the string are escaped before creating the underlying regex.
    pub fn literal(s: &str) -> Pattern {
        Self::regex(escape(s).as_str()).unwrap()
    }

    /// Creates `Pattern` instance from Unix glob.
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
    pub fn glob(glob: &str) -> Result<Pattern, PatternError> {
        let result: IResult<&str, String> = Self::glob_to_regex(Scope::TopLevel, glob);
        match result {
            Ok((remaining, regex)) if remaining.is_empty() =>
                Self::regex(regex.as_str()),
            Ok((remaining, _)) => Err(PatternError {
                input: glob.to_string(),
                cause: format!("Unexpected '{}' at end of input", remaining.chars().next().unwrap())
            }),
            Err(e) => Err(PatternError {
                input: glob.to_string(),
                cause: e.to_string()
            })
        }
    }

    /// Returns true if this pattern fully matches the given path
    pub fn matches(&self, path: &str) -> bool {
        self.anchored_regex.is_match(path.as_bytes()).unwrap_or(false)
    }

    /// Returns true if a prefix of this pattern fully matches the given path
    pub fn matches_partially(&self, path: &str) -> bool {
        self.anchored_regex.is_partial_match(path.as_bytes()).unwrap_or(false)
    }

    /// Returns true if this pattern fully matches a prefix of the given path
    pub fn matches_prefix(&self, path: &str) -> bool {
        self.prefix_regex.is_match(path.as_bytes()).unwrap_or(false)
    }

    /// Returns true if this pattern fully matches given file path
    pub fn matches_path(&self, path: &PathBuf) -> bool {
        self.anchored_regex.is_match(path.to_string_lossy().as_bytes()).unwrap_or(false)
    }

    /// Parses a UNIX glob and converts it to a regular expression
    fn glob_to_regex(scope: Scope, glob: &str) -> IResult<&str, String> {
        // pass escaped characters as-is:
        let p_escaped =
            map(tuple((tag("\\"), anychar)), |(_, c)|
                escape(c.to_string().as_str()));

        fn mk_string(contents: Vec<String>, prefix: &str, sep: &str, suffix: &str) -> String {
            format!("{}{}{}", prefix, contents.join(sep), suffix)
        }

        // { glob1, glob2, ..., globN } -> ( regex1, regex2, ..., regexN )
        let p_alt =
            map(tuple((
                tag("{"),
                separated_list(tag(","), |g| Self::glob_to_regex(Scope::CurlyBrackets, g)),
                tag("}"))),
                |(_, list, _)| mk_string(list, "(", "|", ")"));

        let p_ext_glob =
            map(tuple((
                tag("("),
                separated_list(tag("|"), |g| Self::glob_to_regex(Scope::RoundBrackets, g)),
                tag(")"))), |(_, list, _)| list);

        let p_ext_optional =
            map(tuple((tag("?"), |i| p_ext_glob(i))),
                |(_, g)| mk_string(g, "(", "|", ")?"));
        let p_ext_many =
            map(tuple((tag("*"), |i| p_ext_glob(i))),
                |(_, g)| mk_string(g, "(", "|", ")*"));
        let p_ext_at_least_once =
            map(tuple((tag("+"), |i| p_ext_glob(i))),
                |(_, g)| mk_string(g, "(", "|", ")+"));
        let p_ext_exactly_once =
            map(tuple((tag("@"), |i| p_ext_glob(i))),
                |(_, g)| mk_string(g, "(", "|", ")"));
        let p_ext_never =
            map(tuple((tag("!"), |i| p_ext_glob(i))),
                |(_, g)| mk_string(g, "(?!", "|", ")"));

        // ** -> .*
        let p_double_star =
            map(tag("**"), |_| ".*".to_string());

        // * -> [^/]*
        let p_single_star =
            map(tag("*"), |_| "[^/]*".to_string());

        // ? -> .
        let p_question_mark =
            map(tag("?"), |_| ".".to_string());

        // [ characters ] -> [ characters ]
        let p_neg_character_set =
            map(tuple((
                tag("[!"),
                many0(none_of("]")),
                tag("]")
            )), |(_, characters, _)|
                    "[^".to_string() + &characters.into_iter().collect::<String>() + "]");

        // [ characters ] -> [ characters ]
        let p_character_set =
            map(tuple((
                tag("["),
                many0(none_of("]")),
                tag("]")
            )), |(_, characters, _)|
                "[".to_string() + &characters.into_iter().collect::<String>() + "]");

        // if we are nested, we can't just pass these through without interpretation
        let p_any_char =
            map(tuple((
                cond(scope == Scope::TopLevel, anychar),
                cond(scope == Scope::CurlyBrackets, none_of("{,}")),
                cond(scope == Scope::RoundBrackets, none_of("(|)"))
            )), |(a, b, c)|
                escape(a.or(b).or(c).unwrap().to_string().as_str()));

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
            p_any_char));

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
    use super::*;

    fn glob_to_regex_str(glob: &str) -> String {
        Pattern::glob(glob).unwrap().to_string()
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
        assert_eq!(glob_to_regex_str("foo???"), "foo...");
    }

    #[test]
    fn single_star() {
        assert_eq!(glob_to_regex_str("*bar*"), "[^/]*bar[^/]*");
    }

    #[test]
    fn double_star() {
        assert_eq!(glob_to_regex_str("foo/**/bar"), "foo/.*/bar");
    }

    #[test]
    fn character_set() {
        assert_eq!(glob_to_regex_str("[a-b.*?-]"), "[a-b.*?-]");
        assert_eq!(glob_to_regex_str("[!a-b.*?-]"), "[^a-b.*?-]");
    }

    #[test]
    fn alternatives() {
        assert_eq!(glob_to_regex_str("{a,b,c}"), "(a|b|c)");
        assert_eq!(glob_to_regex_str("{?.jpg,*.JPG}"), "(.\\.jpg|[^/]*\\.JPG)");
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
        assert_eq!(Pattern::literal("test*?{}\\").to_string(),
                   "test\\*\\?\\{\\}\\\\")
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
        assert!(g.matches("/b"));
        assert!(g.matches("/a/b"));
    }

    #[test]
    fn matches_double_star_infix() {
        let g1 = Pattern::glob("/a/**/c").unwrap();
        assert!(g1.matches("/a/b1/c"));
        assert!(g1.matches("/a/b1/b2/c"));
        assert!(g1.matches("/a/b1/b2/b3/c"));
    }

    #[test]
    fn ext_glob_optional() {
        let g = Pattern::glob("/a-?(foo|bar)").unwrap();
        assert!(g.matches("/a-foo"));
        assert!(g.matches("/a-bar"));
    }

    #[test]
    fn ext_glob_many() {
        let g = Pattern::glob("/a-*(foo|bar)").unwrap();
        assert!(g.matches("/a-"));
        assert!(g.matches("/a-foo"));
        assert!(g.matches("/a-foofoo"));
        assert!(g.matches("/a-foobar"));
    }

    #[test]
    fn ext_glob_at_least_one() {
        let g = Pattern::glob("/a-+(foo|bar)").unwrap();
        assert!(!g.matches("/a-"));
        assert!(g.matches("/a-foo"));
        assert!(g.matches("/a-foofoo"));
        assert!(g.matches("/a-foobar"));
    }

    #[test]
    fn ext_glob_at_least_never() {
        let g = Pattern::glob("/a-!(foo)*").unwrap();
        assert!(!g.matches("/a-foo-bar"));
        assert!(!g.matches("/a-foo"));
        assert!(g.matches("/a-bar"));
        assert!(g.matches("/a-"));
    }

    #[test]
    fn ext_glob_nested() {
        let g = Pattern::glob("/a-@(foo|bar?(baz))").unwrap();
        assert!(g.matches("/a-foo"));
        assert!(g.matches("/a-bar"));
        assert!(g.matches("/a-barbaz"));
        assert!(!g.matches("/a-foobaz"));
    }

    #[test]
    fn ext_glob_exactly_one() {
        let g = Pattern::glob("/a-@(foo|bar)").unwrap();
        assert!(!g.matches("/a-"));
        assert!(g.matches("/a-foo"));
        assert!(!g.matches("/a-foofoo"));
        assert!(!g.matches("/a-foobar"));
    }

    #[test]
    fn matches_fully() {
        let g1 = Pattern::glob("/a/b?/*").unwrap();
        assert!(g1.matches("/a/b1/c"));
        assert!(g1.matches("/a/b1/"));
        assert!(!g1.matches("/a/b1"));
        assert!(!g1.matches("/a/b/c"));
    }

    #[test]
    fn matches_partially() {
        let g1 = Pattern::glob("/a/b/*").unwrap();
        assert!(g1.matches_partially("/a"));
        assert!(g1.matches_partially("/a/b"));
        assert!(g1.matches_partially("/a/b/foo"));
        assert!(!g1.matches_partially("/b/foo"));

        let g2 = Pattern::glob("/a/{b1,b2}/c/*").unwrap();
        assert!(g2.matches_partially("/a/b1"));
        assert!(g2.matches_partially("/a/b2"));
        assert!(g2.matches_partially("/a/b2/c"));
        assert!(!g2.matches_partially("/b2/c"));

        let g3 = Pattern::glob("/a/{b11,b21/b22}/c/*").unwrap();
        assert!(g3.matches_partially("/a/b11"));
        assert!(g3.matches_partially("/a/b11/c"));
        assert!(g3.matches_partially("/a/b21"));
        assert!(g3.matches_partially("/a/b21/b22"));
        assert!(g3.matches_partially("/a/b21/b22/c"));
        assert!(!g3.matches_partially("/a/b11/b21/c"));
        assert!(!g3.matches_partially("/a/b21/c"));
    }

    #[test]
    fn matches_prefix() {
        let g1 = Pattern::glob("/a/b/*").unwrap();
        assert!(g1.matches_prefix("/a/b/c"));
        assert!(g1.matches_prefix("/a/b/z/foo"));
        assert!(!g1.matches_prefix("/a/c/z/foo"));
    }
}
