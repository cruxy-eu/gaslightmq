use core::fmt;
use core::str::FromStr;
use core::time::Duration;

const MILLIS_PER_SECOND: u64 = 1_000;
const MILLIS_PER_MINUTE: u64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: u64 = 60 * MILLIS_PER_MINUTE;

/// A length of time as a definition writes it, such as `10s` or `200ms`.
///
/// A time span is a whole number of milliseconds.
/// Its text is a whole number and a unit, with an optional space between them.
///
/// | Unit | Is |
/// |---|---|
/// | `ms` | Milliseconds |
/// | `s` | Seconds |
/// | `m` | Minutes |
/// | `h` | Hours |
///
/// A time span is what someone wrote.
/// A [`Duration`] is what someone measured or computed.
/// An [`Instant`](crate::Instant) plus a time span gives the instant that much later.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant, TimeSpan};
///
/// let after: TimeSpan = "10s".parse()?;
/// assert_eq!(after, TimeSpan::from_secs(10));
/// assert_eq!(after.duration(), Duration::from_secs(10));
/// assert_eq!((Instant::ZERO + after).as_nanos(), 10_000_000_000);
///
/// // The text names the largest unit that fits exactly.
/// assert_eq!("120 s".parse::<TimeSpan>()?.to_string(), "2m");
/// assert_eq!(TimeSpan::from_millis(1500).to_string(), "1500ms");
/// # Ok::<(), gaslightmq_clock::TimeSpanError>(())
/// ```
///
/// With the `serde` feature, a time span serializes as its text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "String", into = "String")
)]
pub struct TimeSpan(u64);

impl TimeSpan {
    /// No time at all.
    pub const ZERO: TimeSpan = TimeSpan(0);

    /// Returns the time span of that many milliseconds.
    pub const fn from_millis(millis: u64) -> TimeSpan {
        TimeSpan(millis)
    }

    /// Returns the time span of that many seconds.
    /// It stops at the longest time span.
    pub const fn from_secs(secs: u64) -> TimeSpan {
        TimeSpan(secs.saturating_mul(MILLIS_PER_SECOND))
    }

    /// Returns the time span of that duration, rounded down to whole milliseconds.
    /// It stops at the longest time span.
    pub fn from_duration(duration: Duration) -> TimeSpan {
        TimeSpan(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
    }

    /// Returns the length in milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the length as a [`Duration`].
    pub const fn duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}

impl From<TimeSpan> for Duration {
    fn from(span: TimeSpan) -> Duration {
        span.duration()
    }
}

/// Reads a whole number and a unit: `ms`, `s`, `m` or `h`.
///
/// A space between the number and the unit is optional.
/// A fraction such as `1.5s` and a compound such as `1m30s` are errors.
impl FromStr for TimeSpan {
    type Err = TimeSpanError;

    fn from_str(text: &str) -> Result<TimeSpan, TimeSpanError> {
        let input = || text.to_owned();

        let digits = text.bytes().take_while(u8::is_ascii_digit).count();
        let (number, rest) = text.split_at(digits);
        if number.is_empty() {
            return Err(TimeSpanError::NoNumber { input: input() });
        }
        let millis_per_unit = match rest.trim_start_matches(' ') {
            "ms" => 1,
            "s" => MILLIS_PER_SECOND,
            "m" => MILLIS_PER_MINUTE,
            "h" => MILLIS_PER_HOUR,
            "" => return Err(TimeSpanError::NoUnit { input: input() }),
            unit => {
                return Err(TimeSpanError::UnknownUnit {
                    input: input(),
                    unit: unit.to_owned(),
                });
            }
        };
        number
            .parse::<u64>()
            .ok()
            .and_then(|count| count.checked_mul(millis_per_unit))
            .map(TimeSpan)
            .ok_or_else(|| TimeSpanError::TooLong { input: input() })
    }
}

/// Writes the number and the largest unit that fits exactly, such as `90s` or `2m`.
///
/// [`FromStr`] reads the text back to the same time span.
impl fmt::Display for TimeSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.0;
        if millis == 0 {
            write!(f, "0s")
        } else if millis.is_multiple_of(MILLIS_PER_HOUR) {
            write!(f, "{}h", millis / MILLIS_PER_HOUR)
        } else if millis.is_multiple_of(MILLIS_PER_MINUTE) {
            write!(f, "{}m", millis / MILLIS_PER_MINUTE)
        } else if millis.is_multiple_of(MILLIS_PER_SECOND) {
            write!(f, "{}s", millis / MILLIS_PER_SECOND)
        } else {
            write!(f, "{millis}ms")
        }
    }
}

#[cfg(feature = "serde")]
impl TryFrom<String> for TimeSpan {
    type Error = TimeSpanError;

    fn try_from(text: String) -> Result<TimeSpan, TimeSpanError> {
        text.parse()
    }
}

#[cfg(feature = "serde")]
impl From<TimeSpan> for String {
    fn from(span: TimeSpan) -> String {
        span.to_string()
    }
}

/// Why a text is not a [`TimeSpan`].
///
/// ```
/// use gaslightmq_clock::TimeSpan;
///
/// let error = "1.5s".parse::<TimeSpan>().unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "invalid time span `1.5s`: unknown unit `.5s`, expected ms, s, m or h",
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeSpanError {
    /// The text does not start with a whole number.
    NoNumber { input: String },
    /// The text is a number without a unit.
    NoUnit { input: String },
    /// The text after the number is not `ms`, `s`, `m` or `h`.
    UnknownUnit { input: String, unit: String },
    /// The time span is more milliseconds than a `u64` holds.
    TooLong { input: String },
}

impl fmt::Display for TimeSpanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::NoNumber { input }
        | Self::NoUnit { input }
        | Self::UnknownUnit { input, .. }
        | Self::TooLong { input }) = self;
        write!(f, "invalid time span `{input}`: ")?;
        match self {
            Self::NoNumber { .. } => write!(f, "expected a whole number and a unit, such as `10s`"),
            Self::NoUnit { .. } => write!(f, "missing unit, expected ms, s, m or h"),
            Self::UnknownUnit { unit, .. } => {
                write!(f, "unknown unit `{unit}`, expected ms, s, m or h")
            }
            Self::TooLong { .. } => write!(f, "too long"),
        }
    }
}

impl core::error::Error for TimeSpanError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> TimeSpan {
        text.parse()
            .unwrap_or_else(|error| panic!("`{text}` should parse: {error}"))
    }

    fn error(text: &str) -> TimeSpanError {
        text.parse::<TimeSpan>().expect_err("the text should not parse")
    }

    fn no_number(text: &str) -> TimeSpanError {
        TimeSpanError::NoNumber { input: text.to_owned() }
    }

    fn no_unit(text: &str) -> TimeSpanError {
        TimeSpanError::NoUnit { input: text.to_owned() }
    }

    fn unknown_unit(text: &str, unit: &str) -> TimeSpanError {
        TimeSpanError::UnknownUnit {
            input: text.to_owned(),
            unit: unit.to_owned(),
        }
    }

    fn too_long(text: &str) -> TimeSpanError {
        TimeSpanError::TooLong { input: text.to_owned() }
    }

    #[test]
    fn constructors_count_milliseconds() {
        assert_eq!(TimeSpan::ZERO.as_millis(), 0);
        assert_eq!(TimeSpan::default(), TimeSpan::ZERO);
        assert_eq!(TimeSpan::from_millis(200).as_millis(), 200);
        assert_eq!(TimeSpan::from_secs(10).as_millis(), 10_000);
        assert_eq!(TimeSpan::from_secs(u64::MAX).as_millis(), u64::MAX);
    }

    #[test]
    fn a_time_span_converts_to_a_duration() {
        let span = TimeSpan::from_millis(1_500);
        assert_eq!(span.duration(), Duration::from_millis(1_500));
        assert_eq!(Duration::from(span), Duration::from_millis(1_500));
        assert_eq!(
            TimeSpan::from_millis(u64::MAX).duration(),
            Duration::from_millis(u64::MAX)
        );
    }

    #[test]
    fn from_duration_rounds_down_to_whole_milliseconds() {
        assert_eq!(
            TimeSpan::from_duration(Duration::from_micros(20_999)),
            TimeSpan::from_millis(20)
        );
        assert_eq!(TimeSpan::from_duration(Duration::from_nanos(999_999)), TimeSpan::ZERO);
        assert_eq!(
            TimeSpan::from_duration(Duration::from_secs(20)),
            TimeSpan::from_secs(20)
        );
        assert_eq!(TimeSpan::from_duration(Duration::MAX), TimeSpan::from_millis(u64::MAX));
    }

    #[test]
    fn time_spans_order_by_length() {
        assert!(parse("999ms") < parse("1s"));
        assert!(parse("59m") < parse("1h"));
        assert_eq!(parse("60s"), parse("1m"));
    }

    #[test]
    fn parses_every_unit() {
        assert_eq!(parse("200ms"), TimeSpan::from_millis(200));
        assert_eq!(parse("10s"), TimeSpan::from_secs(10));
        assert_eq!(parse("5m"), TimeSpan::from_secs(300));
        assert_eq!(parse("2h"), TimeSpan::from_secs(7_200));
    }

    #[test]
    fn parses_the_durations_of_the_specs() {
        for (text, millis) in [
            ("10s", 10_000),
            ("40s", 40_000),
            ("20s", 20_000),
            ("1s", 1_000),
            ("200ms", 200),
            ("750ms", 750),
            ("30 s", 30_000),
            ("14 ms", 14),
        ] {
            assert_eq!(parse(text).as_millis(), millis, "{text}");
        }
    }

    #[test]
    fn parses_zero_and_leading_zeros() {
        assert_eq!(parse("0s"), TimeSpan::ZERO);
        assert_eq!(parse("0ms"), TimeSpan::ZERO);
        assert_eq!(parse("0h"), TimeSpan::ZERO);
        assert_eq!(parse("007s"), TimeSpan::from_secs(7));
    }

    #[test]
    fn accepts_spaces_between_the_number_and_the_unit() {
        assert_eq!(parse("30 s"), TimeSpan::from_secs(30));
        assert_eq!(parse("30  ms"), TimeSpan::from_millis(30));
    }

    #[test]
    fn rejects_a_text_without_a_number() {
        for text in ["", " ", "s", "ms", "-5s", "+5s", " 5s", ".5s", "ten s"] {
            assert_eq!(error(text), no_number(text), "`{text}`");
        }
    }

    #[test]
    fn rejects_a_number_without_a_unit() {
        for text in ["10", "0", "10 ", "10   "] {
            assert_eq!(error(text), no_unit(text), "`{text}`");
        }
    }

    #[test]
    fn rejects_an_unknown_unit() {
        for (text, unit) in [
            ("10sec", "sec"),
            ("10S", "S"),
            ("10MS", "MS"),
            ("10us", "us"),
            ("10ns", "ns"),
            ("10d", "d"),
            ("10 minutes", "minutes"),
            ("10s ", "s "),
            ("10\ts", "\ts"),
            ("10 m s", "m s"),
        ] {
            assert_eq!(error(text), unknown_unit(text, unit), "`{text}`");
        }
    }

    #[test]
    fn rejects_fractions_and_compounds() {
        assert_eq!(error("1.5s"), unknown_unit("1.5s", ".5s"));
        assert_eq!(error("1,5s"), unknown_unit("1,5s", ",5s"));
        assert_eq!(error("1m30s"), unknown_unit("1m30s", "m30s"));
        assert_eq!(error("1m 30s"), unknown_unit("1m 30s", "m 30s"));
    }

    #[test]
    fn rejects_a_time_span_that_does_not_fit() {
        assert_eq!(parse("18446744073709551615ms").as_millis(), u64::MAX);
        assert_eq!(error("18446744073709551616ms"), too_long("18446744073709551616ms"));
        assert_eq!(error("18446744073709551615s"), too_long("18446744073709551615s"));
        assert_eq!(
            error("99999999999999999999999999h"),
            too_long("99999999999999999999999999h")
        );

        let most_hours = u64::MAX / MILLIS_PER_HOUR;
        assert_eq!(
            parse(&format!("{most_hours}h")).as_millis(),
            most_hours * MILLIS_PER_HOUR
        );
        let one_more = format!("{}h", most_hours + 1);
        assert_eq!(error(&one_more), too_long(&one_more));
    }

    #[test]
    fn displays_the_largest_unit_that_fits_exactly() {
        for (millis, text) in [
            (0, "0s"),
            (1, "1ms"),
            (200, "200ms"),
            (999, "999ms"),
            (1_000, "1s"),
            (1_500, "1500ms"),
            (10_000, "10s"),
            (59_000, "59s"),
            (60_000, "1m"),
            (90_000, "90s"),
            (3_600_000, "1h"),
            (5_400_000, "90m"),
            (86_400_000, "24h"),
            (u64::MAX, "18446744073709551615ms"),
        ] {
            assert_eq!(TimeSpan::from_millis(millis).to_string(), text);
        }
    }

    #[test]
    fn the_displayed_text_parses_back_to_the_same_time_span() {
        for millis in [
            0,
            1,
            999,
            1_000,
            1_001,
            60_000,
            61_000,
            3_600_000,
            3_660_000,
            3_600_001,
            u64::MAX,
            u64::MAX - 615,
        ] {
            let span = TimeSpan::from_millis(millis);
            assert_eq!(parse(&span.to_string()), span, "{millis} ms");
        }
    }

    #[test]
    fn the_error_names_the_text_and_the_reason() {
        let message = |text: &str| text.parse::<TimeSpan>().unwrap_err().to_string();
        assert_eq!(
            message(""),
            "invalid time span ``: expected a whole number and a unit, such as `10s`"
        );
        assert_eq!(
            message("10"),
            "invalid time span `10`: missing unit, expected ms, s, m or h"
        );
        assert_eq!(
            message("10sec"),
            "invalid time span `10sec`: unknown unit `sec`, expected ms, s, m or h"
        );
        assert_eq!(
            message("18446744073709551616ms"),
            "invalid time span `18446744073709551616ms`: too long"
        );
    }

    #[test]
    fn the_error_is_a_standard_error() {
        fn message_of<E: core::error::Error + Send + Sync + 'static>(error: E) -> String {
            assert!(error.source().is_none(), "the message is complete: {error}");
            error.to_string()
        }
        assert!("10s".parse::<TimeSpan>().is_ok());
        assert_eq!(
            message_of("ten".parse::<TimeSpan>().unwrap_err()),
            "invalid time span `ten`: expected a whole number and a unit, such as `10s`"
        );
    }

    #[cfg(feature = "serde")]
    mod serde_mapping {
        use super::*;

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Window {
            after: TimeSpan,
            between: [TimeSpan; 2],
            jitter: Option<TimeSpan>,
        }

        #[test]
        fn a_time_span_serializes_as_its_text() {
            assert_eq!(serde_json::to_string(&TimeSpan::from_secs(10)).unwrap(), "\"10s\"");
            assert_eq!(serde_json::to_string(&TimeSpan::from_millis(200)).unwrap(), "\"200ms\"");
        }

        #[test]
        fn a_time_span_deserializes_from_its_text() {
            assert_eq!(
                serde_json::from_str::<TimeSpan>("\"30 s\"").unwrap(),
                TimeSpan::from_secs(30)
            );
        }

        #[test]
        fn a_definition_keeps_its_time_spans() {
            let json = r#"{"after":"10s","between":["10s","40s"],"jitter":"200ms"}"#;
            let window: Window = serde_json::from_str(json).unwrap();
            assert_eq!(
                window,
                Window {
                    after: TimeSpan::from_secs(10),
                    between: [TimeSpan::from_secs(10), TimeSpan::from_secs(40)],
                    jitter: Some(TimeSpan::from_millis(200)),
                }
            );
            assert_eq!(serde_json::to_string(&window).unwrap(), json);
        }

        #[test]
        fn deserializing_reports_why_a_text_is_not_a_time_span() {
            let error = serde_json::from_str::<TimeSpan>("\"1.5s\"").unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("invalid time span `1.5s`: unknown unit `.5s`"),
                "{error}"
            );
        }

        #[test]
        fn deserializing_rejects_a_bare_number() {
            assert!(serde_json::from_str::<TimeSpan>("10").is_err());
        }
    }
}
