//! An instant in UTC, to the millisecond: when something was on the calendar, such as the start of a recording.

use std::fmt;
use std::str::FromStr;

/// An instant in UTC, to the millisecond: when a recording started, on the wall clock.
///
/// A clock of an environment starts at zero, and an [`Instant`](crate::Instant) tells how far it has run.
/// An instant in UTC says when something was on the calendar: the description of a recording holds both, the instant
/// on the clock of its environment and the instant in UTC at which it started, so they tie together.
///
/// The crate never reads the wall clock.
/// Whoever reads it hands in the milliseconds since the Unix epoch, or asks a [`UtcClock`](crate::UtcClock).
///
/// The text form is ISO 8601 with the milliseconds and a `Z`, from the year 1970 up to the year 9999.
/// With the `serde` feature, an instant is that text.
///
/// ```
/// use gaslightmq_clock::{UtcInstant, UtcInstantError};
///
/// let started = UtcInstant::new(1_795_269_731_527)?;
/// assert_eq!(started.to_string(), "2026-11-21T14:02:11.527Z");
/// assert_eq!("2026-11-21T14:02:11.527Z".parse(), Ok(started));
/// assert_eq!(started.unix_millis(), 1_795_269_731_527);
/// # Ok::<(), UtcInstantError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcInstant(u64);

/// How a text writes an instant: `:` between the hours, the minutes and the seconds, or `-` in the name of a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeSeparator {
    Colon,
    Hyphen,
}

impl TimeSeparator {
    const fn character(self) -> char {
        match self {
            TimeSeparator::Colon => ':',
            TimeSeparator::Hyphen => '-',
        }
    }
}

const MILLIS_PER_DAY: u64 = 24 * 60 * 60 * 1000;
/// The days from 0000-03-01 to 1970-01-01 in the calendar that the algorithm below counts in.
const DAYS_TO_THE_EPOCH: u64 = 719_468;
const DAYS_PER_ERA: u64 = 146_097;
/// The first instant after the year 9999: 10000-01-01T00:00:00.000Z.
const END_OF_YEAR_9999: u64 = 253_402_300_800_000;

impl UtcInstant {
    /// The instant `unix_millis` milliseconds after 1970-01-01T00:00:00.000Z.
    ///
    /// # Errors
    ///
    /// [`UtcInstantError::AfterYear9999`] when the instant is after the year 9999, which its text cannot write.
    pub fn new(unix_millis: u64) -> Result<UtcInstant, UtcInstantError> {
        if unix_millis >= END_OF_YEAR_9999 {
            return Err(UtcInstantError::AfterYear9999 { unix_millis });
        }
        Ok(UtcInstant(unix_millis))
    }

    /// The milliseconds since 1970-01-01T00:00:00.000Z.
    pub const fn unix_millis(self) -> u64 {
        self.0
    }

    /// The instant as the name of a directory writes it: the colons of the time as hyphens,
    /// `2026-11-21T14-02-11.527Z`, which every system takes in a name.
    pub const fn in_a_name(self) -> InAName {
        InAName(self)
    }

    /// Reads an instant as the name of a directory writes it: `2026-11-21T14-02-11.527Z`.
    ///
    /// # Errors
    ///
    /// The [`UtcInstantError`] of the text, as [`from_str`](std::str::FromStr::from_str) has it.
    pub fn from_a_name(text: &str) -> Result<UtcInstant, UtcInstantError> {
        UtcInstant::parse(text, TimeSeparator::Hyphen)
    }

    /// Writes the instant, with `separator` between the hours, the minutes and the seconds.
    fn write(self, f: &mut fmt::Formatter<'_>, separator: TimeSeparator) -> fmt::Result {
        let (year, month, day) = civil_from_days(self.0 / MILLIS_PER_DAY);
        let of_day = self.0 % MILLIS_PER_DAY;
        let (hours, minutes) = (of_day / 3_600_000, of_day / 60_000 % 60);
        let (seconds, millis) = (of_day / 1000 % 60, of_day % 1000);
        let s = separator.character();
        write!(
            f,
            "{year:04}-{month:02}-{day:02}T{hours:02}{s}{minutes:02}{s}{seconds:02}.{millis:03}Z"
        )
    }

    /// Reads an instant that `separator` writes.
    fn parse(text: &str, separator: TimeSeparator) -> Result<UtcInstant, UtcInstantError> {
        let not_an_instant = || UtcInstantError::NotAnInstant { input: text.to_owned() };
        let bytes = text.as_bytes();
        let s = separator.character() as u8;
        let shape = [b'-', b'-', b'T', s, s, b'.', b'Z'];
        let at = [4, 7, 10, 13, 16, 19, 23];
        if !text.is_ascii() || bytes.len() != 24 || at.iter().zip(shape).any(|(at, expected)| bytes[*at] != expected) {
            return Err(not_an_instant());
        }
        let number = |from: usize, to: usize| -> Result<u64, UtcInstantError> {
            let digits = &text[from..to];
            if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(not_an_instant());
            }
            digits.parse().map_err(|_| not_an_instant())
        };
        let (year, month, day) = (number(0, 4)?, number(5, 7)?, number(8, 10)?);
        let (hours, minutes, seconds, millis) = (number(11, 13)?, number(14, 16)?, number(17, 19)?, number(20, 23)?);
        if year < 1970 {
            return Err(UtcInstantError::BeforeTheEpoch { input: text.to_owned() });
        }
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hours > 23 || minutes > 59 || seconds > 59 {
            return Err(not_an_instant());
        }
        let days = days_from_civil(year, month, day);
        // A day that the month does not have, such as the 30th of February, is a day of the next month.
        if civil_from_days(days) != (year, month, day) {
            return Err(not_an_instant());
        }
        let of_day = ((hours * 60 + minutes) * 60 + seconds) * 1000 + millis;
        Ok(UtcInstant(days * MILLIS_PER_DAY + of_day))
    }
}

/// The year, the month and the day of the day that is `days` days after 1970-01-01.
///
/// This is the algorithm `civil_from_days` of Howard Hinnant's "chrono-Compatible Low-Level Date Algorithms",
/// for days on or after the epoch, so every number is positive.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let days = days + DAYS_TO_THE_EPOCH;
    let era = days / DAYS_PER_ERA;
    let day_of_era = days - era * DAYS_PER_ERA;
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// The days from 1970-01-01 to that day, which is on or after it: the algorithm `days_from_civil` of the same paper.
fn days_from_civil(year: u64, month: u64, day: u64) -> u64 {
    let year = year - u64::from(month <= 2);
    let era = year / 400;
    let year_of_era = year - era * 400;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era * DAYS_PER_ERA + day_of_era).saturating_sub(DAYS_TO_THE_EPOCH)
}

/// Writes the instant in ISO 8601: `2026-11-21T14:02:11.527Z`.
impl fmt::Display for UtcInstant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write(f, TimeSeparator::Colon)
    }
}

impl FromStr for UtcInstant {
    type Err = UtcInstantError;

    fn from_str(text: &str) -> Result<UtcInstant, UtcInstantError> {
        UtcInstant::parse(text, TimeSeparator::Colon)
    }
}

/// A source of the current instant in UTC: the wall clock of the system, or an instant that stands still.
///
/// Whoever starts a recording asks one for the instant that names it. The wall clock is no part of this crate, which
/// never reads the system: it is at the IO edge, in `gaslightmq_clock_waitable`. An instant is a clock that stands
/// still at it, so a test names the instant that it wants.
///
/// ```
/// use gaslightmq_clock::{UtcClock, UtcInstant, UtcInstantError};
///
/// let started: UtcInstant = "2026-11-21T14:02:11.527Z".parse()?;
/// assert_eq!(started.utc_now(), started);
/// # Ok::<(), UtcInstantError>(())
/// ```
pub trait UtcClock {
    /// The instant in UTC at this moment, to the millisecond.
    fn utc_now(&self) -> UtcInstant;
}

/// An instant is a clock that stands still at it.
impl UtcClock for UtcInstant {
    fn utc_now(&self) -> UtcInstant {
        *self
    }
}

/// An instant as the name of a directory writes it: see [`UtcInstant::in_a_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InAName(UtcInstant);

/// Writes the instant with the colons of its time as hyphens: `2026-11-21T14-02-11.527Z`.
impl fmt::Display for InAName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.write(f, TimeSeparator::Hyphen)
    }
}

/// Why a number or a text is no [`UtcInstant`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UtcInstantError {
    /// The text is not an instant written as `2026-11-21T14:02:11.527Z`, or it names a day or a time that does not exist.
    #[error("{input:?} is no instant in UTC to the millisecond")]
    NotAnInstant { input: String },
    /// The text names an instant before 1970.
    #[error("{input:?} is before 1970")]
    BeforeTheEpoch { input: String },
    /// The number is an instant after the year 9999, which the text of an instant cannot write.
    #[error("{unix_millis} milliseconds after 1970 is after the year 9999")]
    AfterYear9999 { unix_millis: u64 },
}

/// Writes the text of the instant.
#[cfg(feature = "serde")]
impl serde::Serialize for UtcInstant {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Reads the text of an instant, through its checks.
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for UtcInstant {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instant_writes_and_reads_its_text() {
        let cases = [
            (0, "1970-01-01T00:00:00.000Z"),
            (951_782_400_000, "2000-02-29T00:00:00.000Z"),
            (1_795_269_731_527, "2026-11-21T14:02:11.527Z"),
            (END_OF_YEAR_9999 - 1, "9999-12-31T23:59:59.999Z"),
        ];

        for (millis, text) in cases {
            let instant = UtcInstant::new(millis).unwrap();
            assert_eq!(instant.to_string(), text);
            assert_eq!(text.parse(), Ok(instant), "{text}");
        }
    }

    #[test]
    fn every_day_of_four_centuries_reads_back_as_itself() {
        let days_of_four_centuries = 146_097;
        for day in 0..days_of_four_centuries {
            let (year, month, date) = civil_from_days(day);
            assert_eq!(days_from_civil(year, month, date), day, "{year}-{month}-{date}");
        }
    }

    #[test]
    fn a_text_that_is_no_instant_is_an_error() {
        for text in [
            "2026-11-21T14:02:11Z",
            "2026-11-21 14:02:11.527Z",
            "2026-11-21T14:02:11.527",
            "2026-11-21T14-02-11.527Z",
            "2026-02-30T00:00:00.000Z",
            "2026-13-01T00:00:00.000Z",
            "2026-11-21T24:00:00.000Z",
            "2026-11-21T14:60:00.000Z",
            "+026-11-21T14:02:11.527Z",
        ] {
            assert_eq!(
                text.parse::<UtcInstant>(),
                Err(UtcInstantError::NotAnInstant { input: text.to_owned() }),
                "{text}"
            );
        }
    }

    #[test]
    fn an_instant_lies_between_1970_and_the_end_of_9999() {
        assert_eq!(
            "1969-12-31T23:59:59.999Z".parse::<UtcInstant>(),
            Err(UtcInstantError::BeforeTheEpoch {
                input: "1969-12-31T23:59:59.999Z".to_owned()
            })
        );
        assert_eq!(
            UtcInstant::new(END_OF_YEAR_9999),
            Err(UtcInstantError::AfterYear9999 {
                unix_millis: END_OF_YEAR_9999
            })
        );
        assert_eq!(
            UtcInstantError::AfterYear9999 {
                unix_millis: END_OF_YEAR_9999
            }
            .to_string(),
            "253402300800000 milliseconds after 1970 is after the year 9999"
        );
    }

    #[test]
    fn the_name_of_a_directory_writes_the_time_with_hyphens() {
        let instant: UtcInstant = "2026-11-21T14:02:11.527Z".parse().unwrap();

        assert_eq!(instant.in_a_name().to_string(), "2026-11-21T14-02-11.527Z");
        assert_eq!(UtcInstant::from_a_name("2026-11-21T14-02-11.527Z"), Ok(instant));
        assert!(UtcInstant::from_a_name("2026-11-21T14:02:11.527Z").is_err());
    }
}
