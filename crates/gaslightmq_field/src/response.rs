//! The values of the response fields that the kernel has no type for.

use std::fmt;
use std::str::FromStr;

use gaslightmq_clock::{Duration, TimeSpan};

use crate::FieldError;
use crate::text::text_form;

/// How the backend answered, which is the value of the field `outcome`.
///
/// The outcome describes the truth, before any lie applies.
///
/// The text form is the name in lower case: `ok`, `error` or `timeout`.
///
/// ```
/// use gaslightmq_field::prelude::*;
///
/// let outcome: Outcome = "timeout".parse()?;
/// assert_eq!(outcome, Outcome::Timeout);
/// assert_eq!(Outcome::Ok.to_string(), "ok");
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Outcome {
    /// The backend answered without an error.
    Ok,
    /// The backend answered with an error.
    Error,
    /// The backend did not answer in time.
    Timeout,
}

impl Outcome {
    /// Every outcome.
    pub const ALL: [Outcome; 3] = [Outcome::Ok, Outcome::Error, Outcome::Timeout];

    /// The name of the outcome in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error => "error",
            Outcome::Timeout => "timeout",
        }
    }
}

/// Reads the name of an outcome, as [`Display`](fmt::Display) writes it.
impl FromStr for Outcome {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Outcome::ALL
            .into_iter()
            .find(|outcome| outcome.name() == text)
            .ok_or_else(|| FieldError::unknown_name("outcome", text))
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(Outcome);

/// How long the backend took to answer, which is the value of the field `latency`.
///
/// A latency is a whole number of milliseconds.
/// A measured [`Duration`] rounds down to it, so a latency equals what its text form shows.
///
/// The text form is that of a [`TimeSpan`], such as `14ms`.
///
/// ```
/// use gaslightmq_clock::Duration;
/// use gaslightmq_field::prelude::*;
///
/// let measured = Latency::from(Duration::from_micros(14_250));
/// assert_eq!(measured.to_string(), "14ms");
/// assert_eq!("14 ms".parse::<Latency>()?, measured);
/// assert!(measured < Latency::from_millis(15));
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Latency(TimeSpan);

impl Latency {
    /// The latency of that time span.
    pub const fn new(span: TimeSpan) -> Self {
        Self(span)
    }

    /// The latency of that many milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(TimeSpan::from_millis(millis))
    }

    /// The latency as a time span.
    pub const fn span(self) -> TimeSpan {
        self.0
    }

    /// The latency as a duration.
    pub const fn duration(self) -> Duration {
        self.0.duration()
    }
}

impl From<TimeSpan> for Latency {
    fn from(span: TimeSpan) -> Self {
        Self::new(span)
    }
}

/// Rounds a measured duration down to whole milliseconds.
impl From<Duration> for Latency {
    fn from(duration: Duration) -> Self {
        Self(TimeSpan::from_duration(duration))
    }
}

impl From<Latency> for TimeSpan {
    fn from(latency: Latency) -> Self {
        latency.0
    }
}

impl From<Latency> for Duration {
    fn from(latency: Latency) -> Self {
        latency.duration()
    }
}

/// Reads a whole number and a unit, as a [`TimeSpan`] does: `14ms`, `2s`, `1m` or `1h`.
impl FromStr for Latency {
    type Err = FieldError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse::<TimeSpan>()
            .map(Self)
            .map_err(|_| FieldError::malformed("latency", text, "a whole number and a unit, such as 14ms"))
    }
}

impl fmt::Display for Latency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

text_form!(Latency);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outcome_has_one_of_three_names() {
        assert_eq!(Outcome::ALL.map(Outcome::name), ["ok", "error", "timeout"]);
        for outcome in Outcome::ALL {
            assert_eq!(outcome.to_string().parse(), Ok(outcome));
        }
        for text in ["", "OK", "Ok", "success", "delayed", "throttled", "ok "] {
            assert_eq!(
                text.parse::<Outcome>(),
                Err(FieldError::unknown_name("outcome", text)),
                "{text:?}"
            );
        }
    }

    #[test]
    fn a_latency_is_whole_milliseconds() {
        assert_eq!(Latency::from(Duration::from_micros(14_999)), Latency::from_millis(14));
        assert_eq!(Latency::from(Duration::from_nanos(999_999)), Latency::from_millis(0));
        assert_eq!(Latency::from_millis(14).duration(), Duration::from_millis(14));
        assert_eq!(Duration::from(Latency::from_millis(14)), Duration::from_millis(14));
        assert_eq!(
            Latency::new(TimeSpan::from_secs(2)).span(),
            TimeSpan::from_millis(2_000)
        );
        assert_eq!(
            TimeSpan::from(Latency::from(TimeSpan::from_millis(14))),
            TimeSpan::from_millis(14)
        );
    }

    #[test]
    fn a_latency_reads_and_writes_like_a_time_span() {
        assert_eq!(Latency::from_millis(14).to_string(), "14ms");
        assert_eq!(Latency::from_millis(2_000).to_string(), "2s");
        assert_eq!("14ms".parse(), Ok(Latency::from_millis(14)));
        assert_eq!("14 ms".parse(), Ok(Latency::from_millis(14)));
        assert_eq!("2s".parse(), Ok(Latency::from_millis(2_000)));
        for text in ["", "14", "ms", "1.5s", "14 parsecs", "-1ms"] {
            assert_eq!(
                text.parse::<Latency>(),
                Err(FieldError::malformed(
                    "latency",
                    text,
                    "a whole number and a unit, such as 14ms"
                )),
                "{text:?}"
            );
        }
    }

    #[test]
    fn latencies_order_by_length() {
        assert!(Latency::from_millis(999) < Latency::from_millis(1_000));
        assert!("2s".parse::<Latency>().unwrap() > "1500ms".parse().unwrap());
    }
}
