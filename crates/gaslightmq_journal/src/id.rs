//! Which entry of a journal this is: one number among the entries of its environment.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

/// Which entry this is among all the entries of the journal of an environment: one number.
///
/// An environment counts the entries of its journal from one, in the order in which the changes came.
/// A higher ID is therefore a later entry.
/// No two entries of one run of an environment share an ID, and no two entries of a recording do.
///
/// The text form is the number alone, such as `42`.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let first = JournalEntryId::FIRST;
/// let second = first.next();
/// let third = second.next();
///
/// assert!(first < second && second < third);
/// assert_eq!(third, JournalEntryId::new(3)?);
/// assert_eq!(third.to_string(), "3");
/// assert_eq!("3".parse::<JournalEntryId>()?, third);
/// assert_eq!(JournalEntryId::new(0), Err(JournalEntryIdError::Zero));
/// # Ok::<(), JournalEntryIdError>(())
/// ```
///
/// With the `serde` feature, an ID serializes as a number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalEntryId(NonZeroU64);

impl JournalEntryId {
    /// The first entry of the journal of an environment.
    pub const FIRST: JournalEntryId = JournalEntryId(NonZeroU64::MIN);

    /// Check that `value` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`JournalEntryIdError::Zero`] when `value` is zero.
    pub fn new(value: u64) -> Result<JournalEntryId, JournalEntryIdError> {
        NonZeroU64::new(value)
            .map(JournalEntryId)
            .ok_or(JournalEntryIdError::Zero)
    }

    /// The ID as a plain number.
    pub const fn value(self) -> u64 {
        self.0.get()
    }

    /// The ID of the entry after this one in the same journal.
    #[must_use]
    pub const fn next(self) -> JournalEntryId {
        JournalEntryId(self.0.saturating_add(1))
    }
}

/// A number that is at least one is the ID of an entry.
impl From<NonZeroU64> for JournalEntryId {
    fn from(value: NonZeroU64) -> JournalEntryId {
        JournalEntryId(value)
    }
}

impl TryFrom<u64> for JournalEntryId {
    type Error = JournalEntryIdError;

    fn try_from(value: u64) -> Result<JournalEntryId, JournalEntryIdError> {
        JournalEntryId::new(value)
    }
}

impl From<JournalEntryId> for u64 {
    fn from(id: JournalEntryId) -> u64 {
        id.value()
    }
}

/// Reads a whole number that is at least one, such as `42`.
impl FromStr for JournalEntryId {
    type Err = JournalEntryIdError;

    fn from_str(text: &str) -> Result<JournalEntryId, JournalEntryIdError> {
        let malformed = || JournalEntryIdError::Malformed { input: text.to_owned() };
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(malformed());
        }
        JournalEntryId::new(text.parse().map_err(|_| malformed())?)
    }
}

/// Writes the number alone, such as `42`.
impl fmt::Display for JournalEntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for JournalEntryId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.value())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for JournalEntryId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = <u64 as serde::Deserialize>::deserialize(deserializer)?;
        JournalEntryId::new(value).map_err(serde::de::Error::custom)
    }
}

/// Why a number or a text is not a [`JournalEntryId`].
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let error = "#7".parse::<JournalEntryId>().unwrap_err();
/// assert_eq!(error, JournalEntryIdError::Malformed { input: "#7".into() });
/// assert_eq!(error.to_string(), "journal entry ID \"#7\" is not a whole number");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JournalEntryIdError {
    /// The number is zero, and the first entry of a journal has the ID one.
    #[error("a journal entry ID is at least one")]
    Zero,
    /// The text is not a whole number that an ID can hold.
    #[error("journal entry ID {input:?} is not a whole number")]
    Malformed { input: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_entry_has_the_id_one() {
        assert_eq!(JournalEntryId::new(1), Ok(JournalEntryId::FIRST));
        assert_eq!(JournalEntryId::new(0), Err(JournalEntryIdError::Zero));
        assert_eq!(JournalEntryId::try_from(0), Err(JournalEntryIdError::Zero));
        assert_eq!(JournalEntryId::from(NonZeroU64::MIN), JournalEntryId::FIRST);
        assert_eq!(u64::from(JournalEntryId::FIRST.next()), 2);
    }

    #[test]
    fn a_later_entry_has_a_higher_id() {
        let first = JournalEntryId::FIRST;
        let second = first.next();
        let third = second.next();

        assert!(first < second);
        assert!(second < third);
        assert_eq!(third.value(), 3);
    }

    #[test]
    fn an_id_reads_and_writes_a_whole_number() {
        let twelfth = JournalEntryId::new(12).unwrap();

        assert_eq!(twelfth.to_string(), "12");
        assert_eq!("12".parse(), Ok(twelfth));
        assert_eq!("0".parse::<JournalEntryId>(), Err(JournalEntryIdError::Zero));
        for text in ["", "+3", "-3", "3.0", " 3", "#3", "three", "99999999999999999999999"] {
            assert_eq!(
                text.parse::<JournalEntryId>(),
                Err(JournalEntryIdError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn an_id_error_says_what_is_wrong_once() {
        let cases = [
            (JournalEntryIdError::Zero, "a journal entry ID is at least one"),
            (
                JournalEntryIdError::Malformed {
                    input: "7.3".to_owned(),
                },
                "journal entry ID \"7.3\" is not a whole number",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
