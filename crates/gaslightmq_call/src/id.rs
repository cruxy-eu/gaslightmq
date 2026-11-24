//! Which call a record is of: one number among the calls of its environment.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

/// Which call this is among all the calls of an environment: one number.
///
/// An environment counts its calls from one, across all its connections, in the order their requests arrive.
/// A higher ID is therefore a later call, on whichever connection it came.
/// No two calls of one run of an environment share an ID, and no two calls of a recording do.
///
/// The connection a call came on is no part of its ID: a [`Call`](crate::Call) keeps it next to the ID.
///
/// The text form is the number alone, such as `42`.
/// The form `#7` already is a connection number, and a call ID never looks like one.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let first = CallId::FIRST;
/// let second = first.next();
/// let third = second.next();
///
/// assert!(first < second && second < third);
/// assert_eq!(third, CallId::new(3)?);
/// assert_eq!(third.to_string(), "3");
/// assert_eq!("3".parse::<CallId>()?, third);
/// assert_eq!(CallId::new(0), Err(CallIdError::Zero));
/// # Ok::<(), CallIdError>(())
/// ```
///
/// With the `serde` feature, a call ID serializes as a number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallId(NonZeroU64);

impl CallId {
    /// The first call of an environment.
    pub const FIRST: CallId = CallId(NonZeroU64::MIN);

    /// Check that `value` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`CallIdError::Zero`] when `value` is zero.
    pub fn new(value: u64) -> Result<CallId, CallIdError> {
        NonZeroU64::new(value).map(CallId).ok_or(CallIdError::Zero)
    }

    /// The call ID as a plain number.
    pub const fn value(self) -> u64 {
        self.0.get()
    }

    /// The ID of the call after this one in the same environment.
    #[must_use]
    pub const fn next(self) -> CallId {
        CallId(self.0.saturating_add(1))
    }
}

/// A number that is at least one is a call ID.
impl From<NonZeroU64> for CallId {
    fn from(value: NonZeroU64) -> CallId {
        CallId(value)
    }
}

impl TryFrom<u64> for CallId {
    type Error = CallIdError;

    fn try_from(value: u64) -> Result<CallId, CallIdError> {
        CallId::new(value)
    }
}

impl From<CallId> for u64 {
    fn from(id: CallId) -> u64 {
        id.value()
    }
}

/// Reads a whole number that is at least one, such as `42`.
impl FromStr for CallId {
    type Err = CallIdError;

    fn from_str(text: &str) -> Result<CallId, CallIdError> {
        let malformed = || CallIdError::Malformed { input: text.to_owned() };
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(malformed());
        }
        CallId::new(text.parse().map_err(|_| malformed())?)
    }
}

/// Writes the number alone, such as `42`.
impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Why a number or a text is not a [`CallId`].
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let error = "#7".parse::<CallId>().unwrap_err();
/// assert_eq!(error, CallIdError::Malformed { input: "#7".into() });
/// assert_eq!(error.to_string(), "call ID \"#7\" is not a whole number");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallIdError {
    /// The number is zero, and the first call of an environment has the ID one.
    #[error("a call ID is at least one")]
    Zero,
    /// The text is not a whole number that a call ID can hold.
    #[error("call ID {input:?} is not a whole number")]
    Malformed { input: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_call_has_the_id_one() {
        assert_eq!(CallId::new(1), Ok(CallId::FIRST));
        assert_eq!(CallId::new(0), Err(CallIdError::Zero));
        assert_eq!(CallId::try_from(0), Err(CallIdError::Zero));
        assert_eq!(CallId::from(NonZeroU64::MIN), CallId::FIRST);
        assert_eq!(u64::from(CallId::FIRST.next()), 2);
        assert_eq!(CallId::new(u64::MAX).unwrap().next().value(), u64::MAX);
    }

    #[test]
    fn a_later_call_has_a_higher_id() {
        let first = CallId::FIRST;
        let second = first.next();
        let third = second.next();

        assert!(first < second);
        assert!(second < third);
        assert_eq!(third.value(), 3);
    }

    #[test]
    fn a_call_id_reads_and_writes_a_whole_number() {
        let twelfth = CallId::new(12).unwrap();

        assert_eq!(twelfth.to_string(), "12");
        assert_eq!("12".parse(), Ok(twelfth));
        assert_eq!("0".parse::<CallId>(), Err(CallIdError::Zero));
        for text in ["", "+3", "-3", "3.0", " 3", "#3", "three", "99999999999999999999999"] {
            assert_eq!(
                text.parse::<CallId>(),
                Err(CallIdError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_call_id_error_says_what_is_wrong_once() {
        let cases = [
            (CallIdError::Zero, "a call ID is at least one"),
            (
                CallIdError::Malformed {
                    input: "7.3".to_owned(),
                },
                "call ID \"7.3\" is not a whole number",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
