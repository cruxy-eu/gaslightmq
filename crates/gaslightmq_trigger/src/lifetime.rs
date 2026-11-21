//! How long a rule stays armed.

use std::fmt;
use std::str::FromStr;

use gaslightmq_clock::{Instant, TimeSpan};

use crate::{Count, CountError};

/// When an armed rule disarms itself.
///
/// | Lifetime | Disarms | Text |
/// |---|---|---|
/// | [`Lifetime::UntilDisarmed`] | Never by itself. | `until disarmed` |
/// | [`Lifetime::Duration`] | That long after arming. | `30s` |
/// | [`Lifetime::Matches`] | After that many matching calls. | `100 matches` |
/// | [`Lifetime::Firings`] | After the rule's action applied that many times. | `3 firings` |
///
/// Matches and firings are budgets.
/// An [`Arming`](crate::Arming) keeps one [`Budget`](crate::Budget) for such a lifetime, shared by all sessions.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_trigger::prelude::*;
///
/// assert_eq!("until disarmed".parse(), Ok(Lifetime::UntilDisarmed));
/// assert_eq!("30 s".parse(), Ok(Lifetime::Duration(TimeSpan::from_secs(30))));
/// assert_eq!("100 matches".parse(), Ok(Lifetime::Matches(Count::new(100)?)));
/// assert_eq!("1 firing".parse(), Ok(Lifetime::Firings(Count::ONE)));
/// assert_eq!(Lifetime::Firings(Count::new(3)?).to_string(), "3 firings");
/// # Ok::<(), CountError>(())
/// ```
///
/// With the `serde` feature, a lifetime serializes as its text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Lifetime {
    /// The rule stays armed until someone or something disarms it.
    #[default]
    UntilDisarmed,
    /// The rule disarms that long after it was armed.
    Duration(TimeSpan),
    /// The rule disarms after that many calls matched it.
    Matches(Count),
    /// The rule disarms after its action applied that many times.
    Firings(Count),
}

impl Lifetime {
    /// The instant a [`Lifetime::Duration`] ends, for a rule armed at `armed_at`.
    /// Every other lifetime has no such instant.
    pub fn ends_at(self, armed_at: Instant) -> Option<Instant> {
        match self {
            Lifetime::Duration(span) => Some(armed_at + span),
            _ => None,
        }
    }
}

/// Reads `until disarmed`, a time span such as `30s`, or a count such as `100 matches` or `3 firings`.
///
/// A count of one also reads as `1 match` and `1 firing`.
impl FromStr for Lifetime {
    type Err = LifetimeError;

    fn from_str(text: &str) -> Result<Lifetime, LifetimeError> {
        if text == "until disarmed" {
            return Ok(Lifetime::UntilDisarmed);
        }
        let counted = |plural: &str, singular: &str| {
            let number = text.strip_suffix(plural).or_else(|| text.strip_suffix(singular))?;
            let number = number.trim_end_matches(' ');
            let digits = !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit());
            // A text of digits that does not fit `u64` is not a count anyone can reach.
            digits.then(|| number.parse::<u64>().ok()).flatten()
        };
        if let Some(matches) = counted("matches", "match") {
            return Ok(Lifetime::Matches(Count::new(matches)?));
        }
        if let Some(firings) = counted("firings", "firing") {
            return Ok(Lifetime::Firings(Count::new(firings)?));
        }
        text.parse::<TimeSpan>()
            .map(Lifetime::Duration)
            .map_err(|_| LifetimeError::Malformed { input: text.to_owned() })
    }
}

/// Writes `until disarmed`, the time span, or the count with its unit, such as `1 match` or `3 firings`.
///
/// [`FromStr`] reads the text back to the same lifetime.
impl fmt::Display for Lifetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lifetime::UntilDisarmed => f.write_str("until disarmed"),
            Lifetime::Duration(span) => write!(f, "{span}"),
            Lifetime::Matches(Count::ONE) => f.write_str("1 match"),
            Lifetime::Matches(count) => write!(f, "{count} matches"),
            Lifetime::Firings(Count::ONE) => f.write_str("1 firing"),
            Lifetime::Firings(count) => write!(f, "{count} firings"),
        }
    }
}

/// Why a text is not a [`Lifetime`].
///
/// A text that counts matches or firings has a count, and the error keeps what is wrong with that count.
/// Its message is the message of the count's error.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let error = "0 firings".parse::<Lifetime>().unwrap_err();
/// assert_eq!(error, LifetimeError::InvalidCount { error: CountError::Zero });
/// assert_eq!(error.to_string(), "a count is at least one");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifetimeError {
    /// The text is not `until disarmed`, not a time span, and not a count of matches or firings.
    Malformed { input: String },
    /// The number of matches or firings is not a count.
    InvalidCount { error: CountError },
}

impl fmt::Display for LifetimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { input } => write!(
                f,
                "`{input}` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
                 or a count such as `100 matches` or `3 firings`"
            ),
            Self::InvalidCount { error } => fmt::Display::fmt(error, f),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for LifetimeError {}

impl From<CountError> for LifetimeError {
    fn from(error: CountError) -> Self {
        Self::InvalidCount { error }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::Duration;

    use super::*;

    fn count(value: u64) -> Count {
        Count::new(value).unwrap()
    }

    #[test]
    fn a_rule_stays_armed_until_disarmed_unless_it_says_otherwise() {
        assert_eq!(Lifetime::default(), Lifetime::UntilDisarmed);
    }

    #[test]
    fn every_lifetime_reads_from_its_text() {
        for (text, lifetime) in [
            ("until disarmed", Lifetime::UntilDisarmed),
            ("30s", Lifetime::Duration(TimeSpan::from_secs(30))),
            ("30 s", Lifetime::Duration(TimeSpan::from_secs(30))),
            ("200ms", Lifetime::Duration(TimeSpan::from_millis(200))),
            ("100 matches", Lifetime::Matches(count(100))),
            ("1 match", Lifetime::Matches(Count::ONE)),
            ("1 matches", Lifetime::Matches(Count::ONE)),
            ("100matches", Lifetime::Matches(count(100))),
            ("3 firings", Lifetime::Firings(count(3))),
            ("1 firing", Lifetime::Firings(Count::ONE)),
            ("3  firings", Lifetime::Firings(count(3))),
        ] {
            assert_eq!(text.parse(), Ok(lifetime), "`{text}`");
        }
    }

    #[test]
    fn every_lifetime_writes_text_that_reads_back() {
        for (lifetime, text) in [
            (Lifetime::UntilDisarmed, "until disarmed"),
            (Lifetime::Duration(TimeSpan::from_secs(30)), "30s"),
            (Lifetime::Matches(count(100)), "100 matches"),
            (Lifetime::Matches(Count::ONE), "1 match"),
            (Lifetime::Firings(count(3)), "3 firings"),
            (Lifetime::Firings(Count::ONE), "1 firing"),
        ] {
            assert_eq!(lifetime.to_string(), text);
            assert_eq!(text.parse(), Ok(lifetime));
        }
    }

    #[test]
    fn a_text_that_is_no_lifetime_is_an_error() {
        for text in [
            "",
            "forever",
            "until",
            "Until Disarmed",
            "30",
            "three firings",
            "-3 firings",
            "3 fires",
            "matches",
            " 3 firings",
            "3 firings ",
            "99999999999999999999999 matches",
        ] {
            assert_eq!(
                text.parse::<Lifetime>(),
                Err(LifetimeError::Malformed { input: text.into() }),
                "`{text}`"
            );
        }
    }

    #[test]
    fn a_budget_of_zero_is_an_error() {
        let zero = LifetimeError::InvalidCount {
            error: CountError::Zero,
        };
        assert_eq!("0 matches".parse::<Lifetime>(), Err(zero.clone()));
        assert_eq!("0 firings".parse::<Lifetime>(), Err(zero));
    }

    #[test]
    fn a_lifetime_error_says_what_is_wrong() {
        let malformed = LifetimeError::Malformed {
            input: "forever".into(),
        };
        assert_eq!(
            malformed.to_string(),
            "`forever` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
             or a count such as `100 matches` or `3 firings`",
        );
        assert_eq!(
            LifetimeError::from(CountError::Zero).to_string(),
            "a count is at least one"
        );
    }

    #[test]
    fn a_lifetime_error_writes_its_cause_once_and_has_no_source() {
        use std::error::Error;

        let error = "0 matches".parse::<Lifetime>().unwrap_err();
        assert_eq!(error.to_string(), CountError::Zero.to_string());
        assert!(error.source().is_none());
    }

    #[test]
    fn only_a_duration_ends_at_an_instant() {
        let armed_at = Instant::ZERO + Duration::from_secs(5);
        assert_eq!(
            Lifetime::Duration(TimeSpan::from_secs(30)).ends_at(armed_at),
            Some(Instant::ZERO + Duration::from_secs(35)),
        );
        assert_eq!(Lifetime::UntilDisarmed.ends_at(armed_at), None);
        assert_eq!(Lifetime::Matches(count(100)).ends_at(armed_at), None);
        assert_eq!(Lifetime::Firings(count(3)).ends_at(armed_at), None);
    }
}
