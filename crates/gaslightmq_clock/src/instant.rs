use core::fmt;
use core::ops::{Add, AddAssign, Sub};
use core::time::Duration;

use crate::TimeSpan;

/// A point on a clock's timeline.
///
/// An instant counts the nanoseconds since its clock started.
/// It comes from a [`Clock`](crate::Clock) and never from the system,
/// so a run on a virtual clock gives the same instants on every machine.
/// Only instants of the same clock compare.
///
/// A deadline is an instant too.
/// A delay becomes a deadline at the moment someone decides it: `now + delay`.
///
/// Arithmetic saturates and never panics.
/// An instant plus a duration stops at [`Instant::MAX`],
/// and the time between two instants stops at zero.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
///
/// let arrived = Instant::ZERO + Duration::from_millis(250);
/// let answered = arrived + Duration::from_millis(14);
/// assert!(arrived < answered);
/// assert_eq!(answered - arrived, Duration::from_millis(14));
/// assert_eq!(arrived - answered, Duration::ZERO);
/// ```
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(transparent))]
pub struct Instant(u64);

impl Instant {
    /// The moment the clock started.
    pub const ZERO: Instant = Instant(0);

    /// The last instant of a timeline.
    /// A deadline at this instant never comes.
    pub const MAX: Instant = Instant(u64::MAX);

    /// Returns the instant that many nanoseconds after the clock started.
    pub const fn from_nanos(nanos: u64) -> Instant {
        Instant(nanos)
    }

    /// Returns the nanoseconds between the start of the clock and this instant.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns the time from `earlier` to this instant.
    /// The result is zero when `earlier` is the later one.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Returns the time from `earlier` to this instant,
    /// or `None` when `earlier` is the later one.
    pub fn checked_duration_since(self, earlier: Instant) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }

    /// Returns the instant that much later,
    /// or `None` when it lies beyond [`Instant::MAX`].
    pub fn checked_add(self, duration: Duration) -> Option<Instant> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Instant)
    }
}

/// Moves the instant later, and stops at [`Instant::MAX`].
impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, duration: Duration) -> Instant {
        self.checked_add(duration).unwrap_or(Instant::MAX)
    }
}

/// Moves the instant later, and stops at [`Instant::MAX`].
impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, duration: Duration) {
        *self = *self + duration;
    }
}

/// Moves the instant later by what a definition says, and stops at [`Instant::MAX`].
impl Add<TimeSpan> for Instant {
    type Output = Instant;

    fn add(self, span: TimeSpan) -> Instant {
        self + span.duration()
    }
}

/// Measures the time from `earlier` to this instant, and stops at zero.
impl Sub for Instant {
    type Output = Duration;

    fn sub(self, earlier: Instant) -> Duration {
        self.duration_since(earlier)
    }
}

/// Shows the instant as the time since the clock started, such as `Instant(12.5s)`.
impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instant({:?})", Duration::from_nanos(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instant_counts_nanoseconds_from_zero() {
        assert_eq!(Instant::ZERO.as_nanos(), 0);
        assert_eq!(Instant::default(), Instant::ZERO);
        assert_eq!(Instant::from_nanos(1_500).as_nanos(), 1_500);
        assert_eq!(Instant::MAX.as_nanos(), u64::MAX);
    }

    #[test]
    fn instants_order_by_time() {
        let early = Instant::from_nanos(10);
        let late = Instant::from_nanos(11);
        assert!(early < late);
        assert!(Instant::ZERO < early);
        assert!(late < Instant::MAX);
    }

    #[test]
    fn adding_a_duration_moves_the_instant_later() {
        let at = Instant::ZERO + Duration::from_secs(2);
        assert_eq!(at.as_nanos(), 2_000_000_000);

        let mut at = at;
        at += Duration::from_nanos(7);
        assert_eq!(at.as_nanos(), 2_000_000_007);
    }

    #[test]
    fn adding_a_time_span_moves_the_instant_later() {
        let armed_at = Instant::from_nanos(5);
        let opens_at = armed_at + TimeSpan::from_secs(10);
        assert_eq!(opens_at.as_nanos(), 10_000_000_005);
    }

    #[test]
    fn adding_stops_at_the_last_instant() {
        let late = Instant::from_nanos(u64::MAX - 1);
        assert_eq!(late + Duration::from_nanos(1), Instant::MAX);
        assert_eq!(late + Duration::from_nanos(2), Instant::MAX);
        assert_eq!(Instant::ZERO + Duration::MAX, Instant::MAX);
        assert_eq!(Instant::ZERO + TimeSpan::from_millis(u64::MAX), Instant::MAX);
        assert_eq!(Instant::MAX + Duration::ZERO, Instant::MAX);

        let mut at = Instant::MAX;
        at += Duration::from_secs(1);
        assert_eq!(at, Instant::MAX);
    }

    #[test]
    fn checked_add_reports_an_instant_beyond_the_last() {
        let late = Instant::from_nanos(u64::MAX - 1);
        assert_eq!(late.checked_add(Duration::from_nanos(1)), Some(Instant::MAX));
        assert_eq!(late.checked_add(Duration::from_nanos(2)), None);
        assert_eq!(Instant::ZERO.checked_add(Duration::MAX), None);
        assert_eq!(
            Instant::ZERO.checked_add(Duration::from_millis(3)),
            Some(Instant::from_nanos(3_000_000))
        );
    }

    #[test]
    fn subtracting_measures_the_time_between_two_instants() {
        let arrived = Instant::from_nanos(1_000);
        let answered = Instant::from_nanos(1_750);
        assert_eq!(answered - arrived, Duration::from_nanos(750));
        assert_eq!(answered.duration_since(arrived), Duration::from_nanos(750));
        assert_eq!(answered - answered, Duration::ZERO);
        assert_eq!(Instant::MAX - Instant::ZERO, Duration::from_nanos(u64::MAX));
    }

    #[test]
    fn the_time_since_a_later_instant_is_zero() {
        let arrived = Instant::from_nanos(1_000);
        let answered = Instant::from_nanos(1_750);
        assert_eq!(arrived - answered, Duration::ZERO);
        assert_eq!(arrived.duration_since(answered), Duration::ZERO);
    }

    #[test]
    fn checked_duration_since_reports_a_later_instant() {
        let arrived = Instant::from_nanos(1_000);
        let answered = Instant::from_nanos(1_750);
        assert_eq!(
            answered.checked_duration_since(arrived),
            Some(Duration::from_nanos(750))
        );
        assert_eq!(arrived.checked_duration_since(arrived), Some(Duration::ZERO));
        assert_eq!(arrived.checked_duration_since(answered), None);
    }

    #[test]
    fn debug_shows_the_time_since_the_clock_started() {
        let at = Instant::ZERO + Duration::from_millis(12_500);
        assert_eq!(format!("{at:?}"), "Instant(12.5s)");
        assert_eq!(format!("{:?}", Instant::ZERO), "Instant(0ns)");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_maps_an_instant_to_its_nanoseconds() {
        let at = Instant::from_nanos(1_500);
        assert_eq!(serde_json::to_string(&at).unwrap(), "1500");
        assert_eq!(serde_json::from_str::<Instant>("1500").unwrap(), at);
        assert_eq!(
            serde_json::from_str::<Instant>(&u64::MAX.to_string()).unwrap(),
            Instant::MAX
        );
        assert!(serde_json::from_str::<Instant>("-1").is_err());
        assert!(serde_json::from_str::<Instant>("\"1500\"").is_err());
    }
}
