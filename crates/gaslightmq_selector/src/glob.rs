use std::fmt;
use std::str::FromStr;

use crate::SelectorError;
use crate::text::text_form;

/// A glob pattern, such as `orders.*`.
///
/// The `matches` operator tests the text form of a value against a pattern.
///
/// | In a pattern | Matches |
/// |---|---|
/// | `*` | Any run of characters, also an empty one. |
/// | `?` | Exactly one character. |
/// | Any other character | Itself. |
///
/// A pattern matches the whole text, and upper and lower case differ.
/// It has no escape character: `is` tests a value that holds a `*` or a `?` itself.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let orders: Glob = "orders.*".parse()?;
/// assert!(orders.matches("orders.eu"));
/// assert!(orders.matches("orders."));
/// assert!(!orders.matches("payments.settled"));
/// assert!(!orders.matches("eu.orders.eu"));
///
/// // A text with a wildcard is a pattern, and any other text is a value.
/// assert!(Glob::is_pattern("orders.*"));
/// assert!(!Glob::is_pattern("orders.eu"));
/// # Ok::<(), SelectorError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Glob(String);

impl Glob {
    /// The characters that stand for other characters.
    pub const WILDCARDS: [char; 2] = ['*', '?'];

    /// Reads a pattern.
    ///
    /// # Errors
    ///
    /// [`SelectorError::EmptyPattern`] when the pattern has no character.
    pub fn new(pattern: impl Into<String>) -> Result<Self, SelectorError> {
        let pattern = pattern.into();
        if pattern.is_empty() {
            return Err(SelectorError::EmptyPattern);
        }
        Ok(Self(pattern))
    }

    /// Whether a selector reads `text` as a pattern instead of as a value: the text holds a wildcard.
    ///
    /// `topic: "orders.*"` in a file is the condition `topic matches orders.*`,
    /// and `topic: orders.eu` is `topic is orders.eu`.
    pub fn is_pattern(text: &str) -> bool {
        text.contains(Self::WILDCARDS)
    }

    /// The pattern as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the pattern holds a wildcard.
    /// A pattern without one matches its own text only.
    pub fn has_wildcard(&self) -> bool {
        Self::is_pattern(&self.0)
    }

    /// Whether the pattern matches the whole of `text`.
    pub fn matches(&self, text: &str) -> bool {
        let pattern = self.0.as_str();
        // Byte offsets of the next character to compare, in the pattern and in the text.
        let (mut at, mut text_at) = (0, 0);
        // Where to go back to when the characters differ: behind the last `*`, with one more character under it.
        let mut retry: Option<(usize, usize)> = None;

        while let Some(character) = text[text_at..].chars().next() {
            match pattern[at..].chars().next() {
                Some('*') => {
                    at += 1;
                    retry = Some((at, text_at));
                }
                Some(expected) if expected == '?' || expected == character => {
                    at += expected.len_utf8();
                    text_at += character.len_utf8();
                }
                _ => {
                    let Some((behind_star, covered_from)) = retry else {
                        return false;
                    };
                    let covered = text[covered_from..].chars().next().map_or(0, char::len_utf8);
                    retry = Some((behind_star, covered_from + covered));
                    at = behind_star;
                    text_at = covered_from + covered;
                }
            }
        }
        pattern[at..].chars().all(|character| character == '*')
    }
}

impl FromStr for Glob {
    type Err = SelectorError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for Glob {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

text_form!(Glob);

#[cfg(test)]
mod tests {
    use super::*;

    fn glob(pattern: &str) -> Glob {
        Glob::new(pattern).unwrap()
    }

    #[test]
    fn a_star_matches_any_run_of_characters() {
        let cases = [
            ("orders.*", "orders.eu", true),
            ("orders.*", "orders.", true),
            ("orders.*", "orders", false),
            ("orders.*", "payments.settled", false),
            ("orders.*", "eu.orders.eu", false),
            ("*.eu", "orders.eu", true),
            ("*.eu", "orders.eu.dlq", false),
            ("*", "anything at all", true),
            ("*", "", true),
            ("**", "orders", true),
            ("*Group*", "JoinGroup", true),
            ("*Group*", "ConsumerGroupHeartbeat", true),
            ("*Group*", "Heartbeat", false),
            ("a*b*c", "abc", true),
            ("a*b*c", "a-b-b-c", true),
            ("a*b*c", "a-b-b", false),
            ("a*a", "a", false),
            ("a*a", "aa", true),
            ("*ab", "aab", true),
            ("*aab", "aaab", true),
        ];

        for (pattern, text, expected) in cases {
            assert_eq!(glob(pattern).matches(text), expected, "{pattern:?} on {text:?}");
        }
    }

    #[test]
    fn a_question_mark_matches_exactly_one_character() {
        let cases = [
            ("orders.??", "orders.eu", true),
            ("orders.??", "orders.e", false),
            ("orders.??", "orders.eur", false),
            ("?", "é", true),
            ("?", "", false),
            ("?*", "x", true),
            ("?*", "", false),
            ("etl-?", "etl-0", true),
            ("etl-?", "etl-10", false),
        ];

        for (pattern, text, expected) in cases {
            assert_eq!(glob(pattern).matches(text), expected, "{pattern:?} on {text:?}");
        }
    }

    #[test]
    fn every_other_character_matches_itself() {
        assert!(glob("orders.eu").matches("orders.eu"));
        assert!(!glob("orders.eu").matches("orders_eu"));
        assert!(!glob("orders.eu").matches("Orders.eu"));
        assert!(!glob("orders.eu").matches("orders.eu "));
        assert!(glob("[ab]").matches("[ab]"));
        assert!(!glob("[ab]").matches("a"));
        assert!(glob(r"a\*").matches(r"a\b"));
    }

    #[test]
    fn a_pattern_compares_characters_and_not_bytes() {
        assert!(glob("größe-*").matches("größe-1"));
        assert!(glob("*ß*").matches("größe"));
        assert!(glob("gr??e").matches("größe"));
        assert!(!glob("gr?e").matches("größe"));
        assert!(glob("✓*✓").matches("✓ ok ✓"));
    }

    #[test]
    fn an_empty_pattern_is_an_error() {
        assert_eq!(Glob::new(""), Err(SelectorError::EmptyPattern));
        assert_eq!("".parse::<Glob>(), Err(SelectorError::EmptyPattern));
    }

    #[test]
    fn a_text_with_a_wildcard_is_a_pattern() {
        for text in ["orders.*", "*", "etl-?", "a*b?c"] {
            assert!(Glob::is_pattern(text), "{text:?}");
            assert!(glob(text).has_wildcard());
        }
        for text in ["orders.eu", "", "[ab]", "#7", "librdkafka 2.6.1"] {
            assert!(!Glob::is_pattern(text), "{text:?}");
        }
        assert!(!glob("orders.eu").has_wildcard());
    }

    #[test]
    fn a_pattern_keeps_its_text() {
        let pattern = glob("orders.*");

        assert_eq!(pattern.as_str(), "orders.*");
        assert_eq!(pattern.to_string(), "orders.*");
        assert_eq!(pattern.as_ref(), "orders.*");
        assert_eq!(Glob::try_from("orders.*"), Ok(pattern.clone()));
        assert_eq!(String::from(pattern), "orders.*");
    }
}
