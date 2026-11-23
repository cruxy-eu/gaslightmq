//! Which call a record is of: its number on its connection, and the two together.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use gaslightmq_field::{ConnectionNumber, ConnectionNumberError};

/// Which call of its session this is: the 1st, the 2nd, the 3rd.
///
/// A session numbers its calls in the order their requests arrive, and the first call has number one.
/// The number needs no shared counter, so a run on a virtual clock gives every call the same number again.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let third = CallNumber::new(3)?;
/// assert!(CallNumber::FIRST < third);
/// assert_eq!(CallNumber::FIRST.next().next(), third);
/// assert_eq!("3".parse::<CallNumber>()?, third);
/// assert_eq!(CallNumber::new(0), Err(CallNumberError::Zero));
/// # Ok::<(), CallNumberError>(())
/// ```
///
/// With the `serde` feature, a call number serializes as a number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallNumber(NonZeroU64);

impl CallNumber {
    /// The first call of a session.
    pub const FIRST: CallNumber = CallNumber(NonZeroU64::MIN);

    /// Check that `value` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`CallNumberError::Zero`] when `value` is zero.
    pub fn new(value: u64) -> Result<CallNumber, CallNumberError> {
        NonZeroU64::new(value).map(CallNumber).ok_or(CallNumberError::Zero)
    }

    /// The call number as a plain number.
    pub const fn value(self) -> u64 {
        self.0.get()
    }

    /// The number of the call after this one on the same session.
    #[must_use]
    pub const fn next(self) -> CallNumber {
        CallNumber(self.0.saturating_add(1))
    }
}

impl TryFrom<u64> for CallNumber {
    type Error = CallNumberError;

    fn try_from(value: u64) -> Result<CallNumber, CallNumberError> {
        CallNumber::new(value)
    }
}

impl From<CallNumber> for u64 {
    fn from(number: CallNumber) -> u64 {
        number.value()
    }
}

/// Reads a whole number that is at least one, such as `3`.
impl FromStr for CallNumber {
    type Err = CallNumberError;

    fn from_str(text: &str) -> Result<CallNumber, CallNumberError> {
        let malformed = || CallNumberError::Malformed { input: text.to_owned() };
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(malformed());
        }
        CallNumber::new(text.parse().map_err(|_| malformed())?)
    }
}

impl fmt::Display for CallNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Why a number or a text is not a [`CallNumber`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CallNumberError {
    /// The number is zero, and the first call of a session has number one.
    #[error("a call number is at least one")]
    Zero,
    /// The text is not a whole number that a call number can hold.
    #[error("call number {input:?} is not a whole number")]
    Malformed { input: String },
}

/// Which call this is among all the calls of an engine: the connection, and the number of the call on it.
///
/// A connection number only lives as long as its connection, so no two calls of a run share an ID.
///
/// The text form is the connection and the call number with a dot between them: `#7.3`.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let id = CallId::new(ConnectionNumber::new(7), CallNumber::new(3).expect("a call number"));
/// assert_eq!(id.to_string(), "#7.3");
/// assert_eq!("#7.3".parse::<CallId>()?, id);
/// assert_eq!(id.connection(), ConnectionNumber::new(7));
/// # Ok::<(), CallIdError>(())
/// ```
///
/// With the `serde` feature, a call ID serializes as its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallId {
    connection: ConnectionNumber,
    number: CallNumber,
}

impl CallId {
    /// The call with that number on that connection.
    pub const fn new(connection: ConnectionNumber, number: CallNumber) -> CallId {
        CallId { connection, number }
    }

    /// The connection the call ran on.
    pub const fn connection(self) -> ConnectionNumber {
        self.connection
    }

    /// Which call of that connection it is.
    pub const fn number(self) -> CallNumber {
        self.number
    }
}

/// Reads a connection and a call number with a dot between them, such as `#7.3`.
impl FromStr for CallId {
    type Err = CallIdError;

    fn from_str(text: &str) -> Result<CallId, CallIdError> {
        let input = || text.to_owned();
        let (connection, number) = text
            .split_once('.')
            .ok_or_else(|| CallIdError::Malformed { input: input() })?;
        let connection = connection
            .parse()
            .map_err(|error| CallIdError::InvalidConnection { input: input(), error })?;
        let number = number
            .parse()
            .map_err(|error| CallIdError::InvalidNumber { input: input(), error })?;
        Ok(CallId { connection, number })
    }
}

impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.connection, self.number)
    }
}

/// Why a text is not a [`CallId`].
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let error = "#7.0".parse::<CallId>().unwrap_err();
/// assert_eq!(error, CallIdError::InvalidNumber { input: "#7.0".into(), error: CallNumberError::Zero });
/// assert_eq!(error.to_string(), "call ID `#7.0`: a call number is at least one");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CallIdError {
    /// The text has no dot between a connection and a call number.
    #[error("call ID `{input}` is not a connection and a call number, such as `#7.3`")]
    Malformed { input: String },
    /// The part before the dot is not a connection number.
    #[error("call ID `{input}`: {error}")]
    InvalidConnection {
        input: String,
        error: ConnectionNumberError,
    },
    /// The part after the dot is not a call number.
    #[error("call ID `{input}`: {error}")]
    InvalidNumber { input: String, error: CallNumberError },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_call_has_number_one() {
        assert_eq!(CallNumber::new(1), Ok(CallNumber::FIRST));
        assert_eq!(CallNumber::new(0), Err(CallNumberError::Zero));
        assert_eq!(CallNumber::try_from(0), Err(CallNumberError::Zero));
        assert_eq!(u64::from(CallNumber::FIRST.next()), 2);
        assert_eq!(CallNumber::new(u64::MAX).unwrap().next().value(), u64::MAX);
    }

    #[test]
    fn a_call_number_reads_and_writes_a_whole_number() {
        assert_eq!("12".parse::<CallNumber>().unwrap().to_string(), "12");
        assert_eq!("0".parse::<CallNumber>(), Err(CallNumberError::Zero));
        for text in ["", "+3", "-3", "3.0", " 3", "three", "99999999999999999999999"] {
            assert_eq!(
                text.parse::<CallNumber>(),
                Err(CallNumberError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_call_id_reads_back_what_it_writes() {
        let id = CallId::new(ConnectionNumber::new(7), CallNumber::new(3).unwrap());

        assert_eq!(id.to_string(), "#7.3");
        assert_eq!("#7.3".parse(), Ok(id));
        assert_eq!("7.3".parse(), Ok(id));
        assert_eq!(id.number().value(), 3);
    }

    #[test]
    fn a_call_id_orders_by_connection_and_then_by_number() {
        let id = |text: &str| text.parse::<CallId>().unwrap();

        assert!(id("#7.3") < id("#7.4"));
        assert!(id("#7.4") < id("#8.1"));
    }

    #[test]
    fn a_call_id_error_names_the_text_and_says_what_is_wrong_once() {
        let cases = [
            (
                "#7",
                "call ID `#7` is not a connection and a call number, such as `#7.3`",
            ),
            (
                "x.3",
                "call ID `x.3`: connection number \"x\" does not have the form #N",
            ),
            ("#7.x", "call ID `#7.x`: call number \"x\" is not a whole number"),
            ("#7.0", "call ID `#7.0`: a call number is at least one"),
        ];

        for (text, message) in cases {
            let error = text.parse::<CallId>().unwrap_err();
            assert_eq!(error.to_string(), message);
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
