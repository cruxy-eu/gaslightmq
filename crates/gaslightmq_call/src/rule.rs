//! The name a call record knows a rule by.

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// The name a rule had when a call met it, as the record of that call keeps it.
///
/// An explanation names every rule the call touched, and the rule that won a conflict.
/// A record outlives the rules it names: someone can rename or delete a rule after the call.
/// A recorded name is therefore only text that names a rule, and nobody looks it up.
/// It has at least one character, and it is exact: `Stale metadata` and `stale metadata` are two names.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let name: RecordedRuleName = "Slow down order producers".parse()?;
/// assert_eq!(name, "Slow down order producers");
/// assert_eq!(RecordedRuleName::new(""), Err(RecordedRuleNameError::Empty));
/// # Ok::<(), RecordedRuleNameError>(())
/// ```
///
/// With the `serde` feature, a recorded name serializes as its text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordedRuleName(Arc<str>);

impl RecordedRuleName {
    /// Check that `name` has at least one character, and keep it.
    ///
    /// # Errors
    ///
    /// [`RecordedRuleNameError::Empty`] when `name` has no character.
    pub fn new(name: impl AsRef<str>) -> Result<Self, RecordedRuleNameError> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(RecordedRuleNameError::Empty);
        }
        Ok(Self(name.into()))
    }

    /// The name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for RecordedRuleName {
    type Err = RecordedRuleNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl TryFrom<&str> for RecordedRuleName {
    type Error = RecordedRuleNameError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl TryFrom<String> for RecordedRuleName {
    type Error = RecordedRuleNameError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<RecordedRuleName> for String {
    fn from(name: RecordedRuleName) -> Self {
        name.as_str().to_owned()
    }
}

impl fmt::Display for RecordedRuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}

impl AsRef<str> for RecordedRuleName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A map by name finds its entry with plain text.
impl Borrow<str> for RecordedRuleName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for RecordedRuleName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for RecordedRuleName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Why a text is not a [`RecordedRuleName`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordedRuleNameError {
    /// The text has no character, and every rule has a name.
    #[error("the recorded name of a rule is empty")]
    Empty,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_recorded_name_keeps_its_text() {
        for text in ["Slow down order producers", "x", "Ünïcode ✓", " kept as it was "] {
            let name = RecordedRuleName::new(text).unwrap();

            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(text.parse(), Ok(name.clone()));
            assert_eq!(RecordedRuleName::try_from(text), Ok(name.clone()));
            assert_eq!(RecordedRuleName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_recorded_name_has_at_least_one_character() {
        let error = RecordedRuleName::new("").unwrap_err();

        assert_eq!(error, RecordedRuleNameError::Empty);
        assert_eq!(error.to_string(), "the recorded name of a rule is empty");
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(RecordedRuleName::new("Leader flap").unwrap(), 1);

        assert_eq!(by_name.get("Leader flap"), Some(&1));
        assert_eq!(by_name.get("leader flap"), None);
    }
}
