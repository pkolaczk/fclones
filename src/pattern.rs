use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::path::PathBuf;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{anychar, none_of};
use nom::combinator::{cond, map};
use nom::IResult;
use nom::multi::{many0, separated_list0};
use nom::sequence::tuple;
use pcre2::bytes::Regex;
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
    regex: Regex
}

impl Pattern {

    /// Creates `Pattern` instance from raw regular expression
    pub fn regex(regex: &str) -> Result<Pattern, PatternError> {
        let regex = regex.trim_start_matches("^");
        let regex = regex.trim_end_matches("$");
        match Regex::new(("^".to_string() + regex + "$").as_str()) {
            Ok(compiled) => Ok(Pattern { regex: compiled, src: regex.to_owned() }),
            Err(e) => Err(PatternError {
                input: regex.to_string(),
                cause: e.to_string()
            })
        }
    }

    /// Creates a `Pattern` that matches literal string.
    /// Special characters in the string are escaped before creating the underlying regex.
    pub fn literal(s: &str) -> Result<Pattern, PatternError> {
        Self::regex(escape(s).as_str())
    }

    /// Creates `Pattern` instance from Unix glob.
    ///
    /// Glob patterns handle the following wildcards:
    /// - `?`: matches any character
    /// - `*`: matches any sequence of characters except the directory separator
    /// - `**`: matches any sequence of characters including the directory separator
    /// - `[a-z]`: matches one of the characters or character ranges given in the square brackets
    /// - `[!a-z]`: matches any character that is not given in the square brackets
    /// - `{a,b}`: matches any pattern from the comma-separated patterns in the curly brackets
    ///
    /// Use `\` to escape the special symbols that need to be matched literally. E.g. `\*` matches
    /// a single `*` character.
    ///
    pub fn glob(glob: &str) -> Result<Pattern, PatternError> {
        let result: IResult<&str, String> = Self::glob_to_regex(true, glob);
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

    /// Returns true if this pattern fully matches given string
    pub fn matches(&self, path: &str) -> bool {
        self.regex.is_match(path.as_bytes()).unwrap_or(false)
    }

    /// Returns true if this pattern fully matches given file path
    pub fn matches_path(&self, path: &PathBuf) -> bool {
        self.regex.is_match(path.to_string_lossy().as_bytes()).unwrap_or(false)
    }

    /// Parses a UNIX glob and converts it to a regular expression
    fn glob_to_regex(top_level: bool, glob: &str) -> IResult<&str, String> {
        // pass escaped characters as-is:
        let p_escaped =
            map(tuple((tag("\\"), anychar)), |(_, c)|
                escape(c.to_string().as_str()));

        // { glob1, glob2, ..., globN } -> ( regex1, regex2, ..., regexN )
        let p_alt = map(tuple((
            tag("{"),
            separated_list0(tag(","), |g| Self::glob_to_regex(false, g)),
            tag("}"))), |(_, list, _)| "(".to_string() + &list.join("|") + ")");

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
                cond(top_level, anychar),
                cond(!top_level, none_of("{,}"))
            )), |(left, right)|
                escape(left.or(right).unwrap().to_string().as_str()));

        let p_token = alt((
            p_escaped,
            p_alt,
            p_double_star,
            p_single_star,
            p_question_mark,
            p_neg_character_set,
            p_character_set,
            p_any_char));

        let mut parse_all = map(many0(p_token), |s| s.join(""));
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
        assert_eq!(Pattern::literal("test*?{}\\").unwrap().to_string(),
                   "test\\*\\?\\{\\}\\\\")
    }

    #[test]
    fn add() {
        assert_eq!(
            (Pattern::literal("/foo/bar/").unwrap() + Pattern::glob("*").unwrap()).to_string(),
            Pattern::glob("/foo/bar/*").unwrap().to_string()
        )
    }
}
