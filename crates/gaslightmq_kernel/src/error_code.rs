use std::fmt;
use std::num::NonZeroI16;
use std::str::FromStr;

use crate::KernelError;
use crate::code::{self, code_serde};
use crate::text::text_conversions;

/// The code of a Kafka error: the number a broker answers when a request failed.
///
/// Kafka names its errors, such as `NOT_LEADER_OR_FOLLOWER` for code 6.
/// Every name is a constant of this type, such as [`ErrorCode::NOT_LEADER_OR_FOLLOWER`].
///
/// An error code holds every number a response can carry, except zero.
/// A response can carry a code that Kafka gives no name, from a newer broker.
/// Such a code has no [`name`](Self::name).
///
/// Kafka writes "no error" as `0`, and calls it `NONE`.
/// That number is not an `ErrorCode`: [`from_wire`](Self::from_wire) reads it as `None`.
///
/// The text form is the name, such as `NOT_LEADER_OR_FOLLOWER`.
/// A code without a name prints as its number, such as `999`.
/// Reading takes a name as Kafka spells it, and the number of any code.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let error: ErrorCode = "NOT_LEADER_OR_FOLLOWER".parse()?;
/// assert_eq!(error, ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// assert_eq!(ErrorCode::from_wire(6), Some(error));
/// assert_eq!(error.to_string(), "NOT_LEADER_OR_FOLLOWER");
///
/// assert_eq!(ErrorCode::from_wire(0), None);
/// assert_eq!(ErrorCode::to_wire(None), 0);
/// assert!("NONE".parse::<ErrorCode>().is_err());
///
/// let unknown = ErrorCode::try_from(999)?;
/// assert_eq!(unknown.to_string(), "999");
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(NonZeroI16);

const WHAT: &str = "error code";

/// The name Kafka gives the code `0`, which means "no error".
const NONE: &str = "NONE";

/// Define a constant for every error that Kafka names, and the table of those names.
///
/// `names.rs` calls it with one line per error: the code, and the name of the error.
macro_rules! error_codes {
    ($($code:literal $name:ident)*) => {
        impl $crate::ErrorCode {
            $(
                #[doc = concat!("`", stringify!($name), "`, the error with code ", stringify!($code), ".")]
                pub const $name: Self = Self::named($code);
            )*

            /// Every code that Kafka names, with its name, in the order of the codes.
            pub(crate) const NAMES: $crate::code::Names = &[$(($code, stringify!($name))),*];
        }
    };
}

pub(crate) use error_codes;

impl ErrorCode {
    /// Keep `code` as an error code, unless it is zero.
    ///
    /// # Errors
    ///
    /// [`KernelError::Reserved`] when `code` is zero.
    pub fn new(code: i16) -> Result<Self, KernelError> {
        Self::from_wire(code).ok_or_else(|| KernelError::Reserved {
            what: WHAT,
            value: code.to_string(),
        })
    }

    /// A code of the name table, which holds no zero.
    pub(crate) const fn named(code: i16) -> Self {
        match NonZeroI16::new(code) {
            Some(code) => Self(code),
            None => panic!("the name table holds the code 0, which means \"no error\""),
        }
    }

    /// The error code as a plain number, as Kafka writes it.
    pub const fn value(self) -> i16 {
        self.0.get()
    }

    /// Read the number Kafka writes, in which `0` means "no error".
    pub fn from_wire(raw: i16) -> Option<Self> {
        NonZeroI16::new(raw).map(Self)
    }

    /// The number Kafka writes: the error code, or `0` for `None`.
    pub fn to_wire(code: Option<Self>) -> i16 {
        code.map_or(0, Self::value)
    }

    /// The name Kafka gives the error, or `None` for a code without a name.
    pub fn name(self) -> Option<&'static str> {
        code::name(Self::NAMES, self.value())
    }

    /// Whether Kafka gives the code a name.
    pub fn is_known(self) -> bool {
        self.name().is_some()
    }

    /// Every code that Kafka names, in the order of the codes.
    pub fn known() -> impl Iterator<Item = Self> {
        Self::NAMES.iter().map(|(code, _)| Self::named(*code))
    }
}

/// Reads the type Kafka writes an error code as, and rejects zero.
impl TryFrom<i16> for ErrorCode {
    type Error = KernelError;

    fn try_from(code: i16) -> Result<Self, Self::Error> {
        Self::new(code)
    }
}

impl From<ErrorCode> for i16 {
    fn from(code: ErrorCode) -> Self {
        code.value()
    }
}

/// Reads the name of an error as Kafka spells it, or the number of any code.
impl FromStr for ErrorCode {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == NONE {
            return Err(KernelError::Reserved {
                what: WHAT,
                value: NONE.to_owned(),
            });
        }
        Self::new(code::parse(WHAT, Self::NAMES, text)?)
    }
}

/// Writes the name of the error, or the number of a code without a name.
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => fmt::Display::fmt(name, f),
            None => fmt::Display::fmt(&self.value(), f),
        }
    }
}

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErrorCode({self})")
    }
}

text_conversions!(ErrorCode);
code_serde!(ErrorCode, "the name or the number of a Kafka error");

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn code(number: i16) -> ErrorCode {
        ErrorCode::new(number).unwrap()
    }

    #[test]
    fn every_number_but_zero_is_an_error_code() {
        for number in [i16::MIN, -1, 1, 6, 999, i16::MAX] {
            assert_eq!(ErrorCode::new(number).map(ErrorCode::value), Ok(number));
            assert_eq!(ErrorCode::try_from(number).map(ErrorCode::value), Ok(number));
            assert_eq!(i16::from(code(number)), number);
        }
    }

    #[test]
    fn zero_is_no_error_code() {
        let reserved = KernelError::Reserved {
            what: "error code",
            value: "0".into(),
        };

        assert_eq!(ErrorCode::new(0), Err(reserved.clone()));
        assert_eq!(ErrorCode::try_from(0), Err(reserved.clone()));
        assert_eq!("0".parse::<ErrorCode>(), Err(reserved));
        assert_eq!(
            "NONE".parse::<ErrorCode>(),
            Err(KernelError::Reserved {
                what: "error code",
                value: "NONE".into()
            })
        );
        assert!(ErrorCode::known().all(|code| code.value() != 0));
    }

    #[test]
    fn the_wire_writes_no_error_as_zero() {
        assert_eq!(ErrorCode::from_wire(0), None);
        assert_eq!(ErrorCode::from_wire(6), Some(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        assert_eq!(ErrorCode::from_wire(-1), Some(ErrorCode::UNKNOWN_SERVER_ERROR));
        assert_eq!(ErrorCode::from_wire(999), Some(code(999)));
        assert_eq!(ErrorCode::to_wire(None), 0);
        assert_eq!(ErrorCode::to_wire(Some(ErrorCode::NOT_LEADER_OR_FOLLOWER)), 6);
    }

    #[test]
    fn no_error_takes_no_more_room_than_an_error() {
        assert_eq!(size_of::<Option<ErrorCode>>(), size_of::<i16>());
    }

    #[test]
    fn an_error_has_the_name_kafka_gives_it() {
        assert_eq!(ErrorCode::NOT_LEADER_OR_FOLLOWER.value(), 6);
        assert_eq!(ErrorCode::NOT_LEADER_OR_FOLLOWER.name(), Some("NOT_LEADER_OR_FOLLOWER"));
        assert_eq!(ErrorCode::UNKNOWN_SERVER_ERROR.value(), -1);
        assert_eq!(code(1).name(), Some("OFFSET_OUT_OF_RANGE"));
        assert_eq!(ErrorCode::REQUEST_TIMED_OUT.value(), 7);
        assert_eq!(ErrorCode::REBALANCE_IN_PROGRESS.value(), 27);
        assert!(ErrorCode::REQUEST_TIMED_OUT.is_known());
    }

    #[test]
    fn a_code_can_have_no_name() {
        for number in [i16::MIN, -2, 999, i16::MAX] {
            assert_eq!(code(number).name(), None, "{number}");
            assert!(!code(number).is_known(), "{number}");
        }
    }

    #[test]
    fn an_error_code_prints_its_name_or_its_number() {
        assert_eq!(ErrorCode::NOT_LEADER_OR_FOLLOWER.to_string(), "NOT_LEADER_OR_FOLLOWER");
        assert_eq!(code(999).to_string(), "999");
        assert_eq!(code(-2).to_string(), "-2");
        assert_eq!(
            format!("{:>19}|{:>5}|", ErrorCode::REQUEST_TIMED_OUT, code(999)),
            "  REQUEST_TIMED_OUT|  999|"
        );
        assert_eq!(
            format!("{:?} {:?}", ErrorCode::REQUEST_TIMED_OUT, code(999)),
            "ErrorCode(REQUEST_TIMED_OUT) ErrorCode(999)"
        );
    }

    #[test]
    fn an_error_code_reads_a_name_or_a_number() {
        assert_eq!(
            "NOT_LEADER_OR_FOLLOWER".parse::<ErrorCode>(),
            Ok(ErrorCode::NOT_LEADER_OR_FOLLOWER)
        );
        assert_eq!("6".parse::<ErrorCode>(), Ok(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        assert_eq!("-1".parse::<ErrorCode>(), Ok(ErrorCode::UNKNOWN_SERVER_ERROR));
        assert_eq!("999".parse::<ErrorCode>(), Ok(code(999)));
        assert_eq!(
            ErrorCode::try_from("REQUEST_TIMED_OUT"),
            Ok(ErrorCode::REQUEST_TIMED_OUT)
        );
        assert_eq!(
            ErrorCode::try_from(String::from("REQUEST_TIMED_OUT")),
            Ok(ErrorCode::REQUEST_TIMED_OUT)
        );
        assert_eq!(String::from(ErrorCode::REQUEST_TIMED_OUT), "REQUEST_TIMED_OUT");
    }

    #[test]
    fn an_error_code_does_not_read_a_name_kafka_does_not_have() {
        for text in [
            "NOT_LEADER",
            "not_leader_or_follower",
            "NotLeaderOrFollower",
            "none",
            "None",
        ] {
            assert_eq!(
                text.parse::<ErrorCode>(),
                Err(KernelError::UnknownName {
                    what: "error code",
                    input: text.to_owned()
                }),
                "{text:?}"
            );
        }
        assert_eq!("".parse::<ErrorCode>(), Err(KernelError::Empty { what: "error code" }));
        assert_eq!(
            "32768".parse::<ErrorCode>(),
            Err(KernelError::TooLarge {
                what: "error code",
                value: 32_768,
                max: 32_767
            })
        );
    }

    #[test]
    fn every_known_code_reads_what_it_prints() {
        assert!(ErrorCode::known().count() > 120);
        for code in ErrorCode::known() {
            assert!(code.is_known());
            assert_eq!(code.to_string().parse::<ErrorCode>(), Ok(code));
            assert_eq!(code.value().to_string().parse::<ErrorCode>(), Ok(code));
        }
    }

    #[test]
    fn every_code_without_a_name_reads_what_it_prints() {
        for code in (i16::MIN..=i16::MAX)
            .filter_map(ErrorCode::from_wire)
            .filter(|code| !code.is_known())
        {
            assert_eq!(code.to_string(), code.value().to_string());
            assert_eq!(code.to_string().parse::<ErrorCode>(), Ok(code));
        }
    }

    #[test]
    fn the_names_are_in_the_order_of_the_codes_and_differ() {
        let codes: Vec<i16> = ErrorCode::known().map(ErrorCode::value).collect();
        let names: BTreeSet<&str> = ErrorCode::known().filter_map(ErrorCode::name).collect();

        assert!(codes.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(names.len(), codes.len());
    }

    #[test]
    fn error_codes_order_as_numbers() {
        assert!(ErrorCode::UNKNOWN_SERVER_ERROR < ErrorCode::OFFSET_OUT_OF_RANGE);
        assert!(code(6) < code(7));
    }
}
