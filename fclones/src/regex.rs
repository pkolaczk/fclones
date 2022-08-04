use std::cmp::min;

/// Adds poor-man's partial matching support to the standard regex::Regex
/// Note this is very limited and slightly broken stub for partial matching.
/// False positives for partial matching are allowed.
#[derive(Clone, Debug)]
pub struct Regex {
    regex: regex::Regex,
    fixed_prefix: String,
    case_insensitive: bool,
}

impl Regex {
    pub fn new(re: &str, case_insensitive: bool) -> Result<Regex, regex::Error> {
        assert!(re.starts_with('^'));
        let regex = regex::RegexBuilder::new(re)
            .case_insensitive(case_insensitive)
            .build()?;
        let fixed_prefix = if case_insensitive {
            Self::get_fixed_prefix(re).to_lowercase()
        } else {
            Self::get_fixed_prefix(re)
        };
        Ok(Regex {
            regex,
            fixed_prefix,
            case_insensitive,
        })
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.regex.is_match(s)
    }

    /// Returns true if given string `s` could match the pattern if extended
    /// by more characters.
    ///
    /// Technically it checks if the string s matches the initial characters
    /// in the fixed prefix of the regex, where fixed prefix are all characters up to the
    /// first regex wildcard.
    pub fn is_partial_match(&self, s: &str) -> bool {
        let len = min(s.len(), self.fixed_prefix.len());
        let truncated: String = s.chars().take(len).collect();
        let pattern = if self.case_insensitive {
            truncated.to_lowercase()
        } else {
            truncated
        };
        self.fixed_prefix.starts_with(&pattern)
    }

    /// Returns the initial fragment of the regex string that always matches
    /// a fixed string. That fragment does not contain any wildcard characters (or all are escaped).
    fn get_fixed_prefix(s: &str) -> String {
        let mut escape = false;
        let mut result = String::new();
        let magic_chars = ['.', '^', '$', '(', ')', '{', '}', '[', ']', '|', '.', '+'];

        for (i, c) in s.chars().enumerate() {
            if c == '^' && i == 0 {
                continue;
            }
            if magic_chars.contains(&c) && !escape {
                break;
            }
            // these may make the previous character optional,
            // so we erase the last added one
            if ['?', '*'].contains(&c) && !escape {
                result = result.chars().take(result.len() - 1).collect();
                break;
            }
            // escaped alphabetic character means a character class,
            // so let's stop here as well
            if c.is_ascii_alphabetic() && escape {
                break;
            }

            // we\re not adding the escape char to the output, because the output is not a regexp
            if c == '\\' {
                escape = true;
                continue;
            }

            result.push(c);
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple_text_is_passed_as_is() {
        let r = Regex::new("^foo/BAR", false).unwrap();
        assert!(r.is_match("foo/BAR"));
        assert!(r.is_partial_match("foo/BAR"));
        assert!(!r.is_match("foo/bar"));
        assert!(!r.is_partial_match("foo/bar"));
    }

    #[test]
    fn test_case_insensitive() {
        let r = Regex::new("^foo/BAR", true).unwrap();
        assert!(r.is_match("foo/BAR"));
        assert!(r.is_partial_match("foo/BAR"));
        assert!(r.is_match("foo/bar"));
        assert!(r.is_partial_match("foo/bar"));
        assert!(!r.is_partial_match("foo/baz"));
    }

    #[test]
    fn test_partial_match_stops_on_wildcard() {
        let r = Regex::new("^abcd*ef", true).unwrap();
        assert!(r.is_match("abcdddddef"));
        assert!(r.is_match("abcef"));
        assert!(!r.is_match("abef"));

        assert!(r.is_partial_match("a"));
        assert!(r.is_partial_match("ab"));
        assert!(r.is_partial_match("ab"));
        assert!(r.is_partial_match("abc"));
        assert!(r.is_partial_match("abcef"));

        assert!(!r.is_partial_match("-a"));
        assert!(!r.is_partial_match("a-b"));
        assert!(!r.is_partial_match("ab-"));
    }

    #[test]
    fn test_unicode() {
        let r = Regex::new("^ąęść?", true).unwrap();
        assert!(r.is_partial_match("ą"));
        assert!(r.is_partial_match("ąę"));
        assert!(r.is_partial_match("ąęś"));
        assert!(r.is_partial_match("ąęść"));
        assert!(!r.is_partial_match("ąęść---"));
    }

    #[test]
    fn test_can_partial_match_escaped_chars() {
        let r = Regex::new("^\\.\\*", true).unwrap();
        assert!(r.is_partial_match("."));
        assert!(r.is_partial_match(".*"));
        assert!(!r.is_partial_match("foo"));
    }
}
