//! The values of the request fields that the kernel has no type for.

use std::fmt;
use std::str::FromStr;

use crate::text::{self, Flaw, number_form};

/// The version of the API a request uses, which is the value of the field `api.version`.
///
/// An API version is between zero and [`i16::MAX`].
/// Kafka writes it as an `i16`.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let version = ApiVersion::from(11);
/// assert_eq!(version.to_string(), "11");
/// assert_eq!(ApiVersion::try_from(11_i16)?, version);
/// assert!(ApiVersion::try_from(-1_i16).is_err());
/// assert!(ApiVersion::from(9) < version);
/// # Ok::<(), ApiVersionError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiVersion(u16);

impl ApiVersion {
    const MAX_VALUE: u16 = i16::MAX as u16;

    /// Check that `version` is at most [`i16::MAX`], and keep it.
    ///
    /// # Errors
    ///
    /// [`ApiVersionError::OutOfRange`] when `version` is above that.
    pub fn new(version: u16) -> Result<Self, ApiVersionError> {
        if version > Self::MAX_VALUE {
            return Err(ApiVersionError::OutOfRange {
                input: version.to_string(),
            });
        }
        Ok(Self(version))
    }

    /// The API version as a plain number.
    pub const fn value(self) -> u16 {
        self.0
    }
}

impl From<u8> for ApiVersion {
    fn from(version: u8) -> Self {
        Self(version.into())
    }
}

impl TryFrom<u16> for ApiVersion {
    type Error = ApiVersionError;

    fn try_from(version: u16) -> Result<Self, Self::Error> {
        Self::new(version)
    }
}

/// Reads the type Kafka writes an API version as, and rejects every negative number.
impl TryFrom<i16> for ApiVersion {
    type Error = ApiVersionError;

    fn try_from(raw: i16) -> Result<Self, Self::Error> {
        u16::try_from(raw)
            .map(Self)
            .map_err(|_| ApiVersionError::OutOfRange { input: raw.to_string() })
    }
}

impl From<ApiVersion> for u16 {
    fn from(version: ApiVersion) -> Self {
        version.0
    }
}

/// Gives the type Kafka writes an API version as.
impl From<ApiVersion> for i16 {
    fn from(version: ApiVersion) -> Self {
        // The version is at most `i16::MAX`, so the sign bit is clear and the cast keeps it.
        version.0.cast_signed()
    }
}

/// Reads an API version from decimal digits, as [`Display`](fmt::Display) writes it.
impl FromStr for ApiVersion {
    type Err = ApiVersionError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text::number(text, 0, Self::MAX_VALUE.into())
            .map(Self)
            .map_err(|flaw| match flaw {
                Flaw::Malformed => ApiVersionError::Malformed { input: text.to_owned() },
                Flaw::OutOfRange => ApiVersionError::OutOfRange { input: text.to_owned() },
            })
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

number_form!(ApiVersion, ApiVersionError, u16, "an API version such as 11");

/// Why a number or a text is not an [`ApiVersion`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ApiVersionError {
    /// The text is not a whole number.
    #[error("API version {input:?} does not have the form a whole number")]
    Malformed { input: String },
    /// The number is below zero or above [`i16::MAX`].
    #[error("API version {input} is not between 0 and {}", i16::MAX)]
    OutOfRange { input: String },
}

/// The acknowledgements a Produce request asks for, which is the value of the field `acks`.
///
/// Kafka knows three values, and each has a constant:
/// [`ALL`](Self::ALL) is `-1`, [`NONE`](Self::NONE) is `0` and [`LEADER`](Self::LEADER) is `1`.
/// A client can send any other number, and a broker refuses it.
/// `Acks` holds whatever the client sent, so a filter can find such a request.
///
/// The text form is the number.
/// Reading also takes `all` for `-1`, as the configuration of a producer does.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// assert_eq!(Acks::ALL.to_string(), "-1");
/// assert_eq!("-1".parse::<Acks>()?, Acks::ALL);
/// assert_eq!("all".parse::<Acks>()?, Acks::ALL);
/// assert_eq!(Acks::from(1), Acks::LEADER);
/// # Ok::<(), AcksError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Acks(i16);

impl Acks {
    /// `-1`: every in-sync replica has the records before the broker answers.
    pub const ALL: Acks = Acks(-1);
    /// `0`: the broker does not answer at all.
    pub const NONE: Acks = Acks(0);
    /// `1`: the leader has the records before the broker answers.
    pub const LEADER: Acks = Acks(1);

    /// The acks with that number, as a Produce request carries it.
    pub const fn new(acks: i16) -> Self {
        Self(acks)
    }

    /// The acks as the number a Produce request carries.
    pub const fn value(self) -> i16 {
        self.0
    }
}

impl From<i16> for Acks {
    fn from(acks: i16) -> Self {
        Self::new(acks)
    }
}

impl From<Acks> for i16 {
    fn from(acks: Acks) -> Self {
        acks.0
    }
}

/// Reads the number, or `all` for `-1`.
impl FromStr for Acks {
    type Err = AcksError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == "all" {
            return Ok(Acks::ALL);
        }
        text::number(text, i16::MIN.into(), i16::MAX.unsigned_abs().into())
            .map(Self)
            .map_err(|flaw| match flaw {
                Flaw::Malformed => AcksError::Malformed { input: text.to_owned() },
                Flaw::OutOfRange => AcksError::OutOfRange { input: text.to_owned() },
            })
    }
}

impl fmt::Display for Acks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

number_form!(
    Acks,
    AcksError,
    i16,
    "the acks of a Produce request, such as -1 or \"all\""
);

/// Why a text is not an [`Acks`].
///
/// Every `i16` is a value of `acks`, so only a text can fail.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AcksError {
    /// The text is not `all`, and not a whole number.
    #[error("acks {input:?} does not have the form a whole number")]
    Malformed { input: String },
    /// The number does not fit an `i16`.
    #[error("acks {input} is not between {} and {}", i16::MIN, i16::MAX)]
    OutOfRange { input: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_api_version_is_between_zero_and_the_largest_i16() {
        assert_eq!(ApiVersion::new(0).map(ApiVersion::value), Ok(0));
        assert_eq!(ApiVersion::new(32_767).map(ApiVersion::value), Ok(32_767));
        assert_eq!(
            ApiVersion::new(32_768),
            Err(ApiVersionError::OutOfRange {
                input: "32768".to_owned()
            })
        );
        assert_eq!(
            ApiVersion::try_from(40_000_u16),
            Err(ApiVersionError::OutOfRange {
                input: "40000".to_owned()
            })
        );
        assert_eq!(
            ApiVersion::try_from(-1_i16),
            Err(ApiVersionError::OutOfRange { input: "-1".to_owned() })
        );
    }

    #[test]
    fn an_api_version_converts_from_and_to_the_number_kafka_writes() {
        let version = ApiVersion::try_from(11_i16).unwrap();

        assert_eq!(version, ApiVersion::from(11));
        assert_eq!(i16::from(version), 11);
        assert_eq!(u16::from(version), 11);
        assert_eq!(i16::from(ApiVersion::new(32_767).unwrap()), i16::MAX);
    }

    #[test]
    fn an_api_version_reads_and_writes_decimal_digits() {
        assert_eq!(ApiVersion::from(11).to_string(), "11");
        assert_eq!("11".parse(), Ok(ApiVersion::from(11)));
        assert_eq!(
            "v11".parse::<ApiVersion>(),
            Err(ApiVersionError::Malformed {
                input: "v11".to_owned()
            })
        );
        assert_eq!(
            "-1".parse::<ApiVersion>(),
            Err(ApiVersionError::OutOfRange { input: "-1".to_owned() })
        );
        assert_eq!(
            "32768".parse::<ApiVersion>(),
            Err(ApiVersionError::OutOfRange {
                input: "32768".to_owned()
            })
        );
    }

    #[test]
    fn acks_holds_the_number_a_request_carries() {
        assert_eq!(Acks::ALL.value(), -1);
        assert_eq!(Acks::NONE.value(), 0);
        assert_eq!(Acks::LEADER.value(), 1);
        assert_eq!(Acks::new(7).value(), 7);
        assert_eq!(i16::from(Acks::from(-1)), -1);
    }

    #[test]
    fn acks_reads_the_number_or_all() {
        assert_eq!("-1".parse(), Ok(Acks::ALL));
        assert_eq!("all".parse(), Ok(Acks::ALL));
        assert_eq!("0".parse(), Ok(Acks::NONE));
        assert_eq!("1".parse(), Ok(Acks::LEADER));
        assert_eq!("7".parse(), Ok(Acks::new(7)));
        assert_eq!(Acks::ALL.to_string(), "-1");
        assert_eq!(Acks::LEADER.to_string(), "1");
    }

    #[test]
    fn acks_is_a_number_that_fits_i16() {
        for text in ["", "ALL", "none", "leader", "1.0"] {
            assert_eq!(
                text.parse::<Acks>(),
                Err(AcksError::Malformed { input: text.to_owned() }),
                "{text:?}"
            );
        }
        assert_eq!(
            "40000".parse::<Acks>(),
            Err(AcksError::OutOfRange {
                input: "40000".to_owned()
            })
        );
        assert_eq!(
            "-40000".parse::<Acks>(),
            Err(AcksError::OutOfRange {
                input: "-40000".to_owned()
            })
        );
    }
}
