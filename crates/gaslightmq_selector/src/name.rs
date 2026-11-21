use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::SelectorError;
use crate::text::text_form;

/// The name of a selector, such as `Order producers`.
///
/// Things reference a selector by its name: a rule, a pad layout, and a selector that includes another.
/// A name is unique among selectors, and it is exact: `Order producers` and `order producers` are two names.
///
/// A name has at least one character.
/// It does not start or end with white space, and it holds no control character such as a line break.
///
/// ```
/// use gaslightmq_selector::prelude::*;
///
/// let name: SelectorName = "Order producers".parse()?;
/// assert_eq!(name, "Order producers");
/// assert_eq!(name.to_string(), "Order producers");
///
/// assert!(SelectorName::new("").is_err());
/// assert!(SelectorName::new("Order producers ").is_err());
/// # Ok::<(), SelectorError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectorName(Arc<str>);

impl SelectorName {
    /// Checks the rule for names.
    ///
    /// # Errors
    ///
    /// [`SelectorError::InvalidName`] when the text is empty, starts or ends with white space,
    /// or holds a control character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, SelectorError> {
        let name = name.as_ref();
        let reason = if name.is_empty() {
            "is empty"
        } else if name.trim() != name {
            "starts or ends with white space"
        } else if name.chars().any(char::is_control) {
            "holds a control character"
        } else {
            return Ok(Self(name.into()));
        };
        Err(SelectorError::InvalidName {
            input: name.to_owned(),
            reason,
        })
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SelectorName {
    type Err = SelectorError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for SelectorName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for SelectorName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for SelectorName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for SelectorName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SelectorName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

text_form!(SelectorName);

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["Order producers", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let name = SelectorName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(text.parse::<SelectorName>(), Ok(name.clone()));
            assert_eq!(SelectorName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        let cases = [
            ("", "is empty"),
            (" ", "starts or ends with white space"),
            (" Order producers", "starts or ends with white space"),
            ("Order producers\n", "starts or ends with white space"),
            ("Order\nproducers", "holds a control character"),
            ("Order\tproducers", "holds a control character"),
        ];

        for (text, reason) in cases {
            assert_eq!(
                SelectorName::new(text),
                Err(SelectorError::InvalidName {
                    input: text.to_owned(),
                    reason
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_name_is_exact() {
        assert_ne!(
            SelectorName::new("Order producers").unwrap(),
            SelectorName::new("order producers").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(SelectorName::new("Order producers").unwrap(), 1);

        assert_eq!(by_name.get("Order producers"), Some(&1));
        assert_eq!(by_name.get("Order consumers"), None);
    }
}
