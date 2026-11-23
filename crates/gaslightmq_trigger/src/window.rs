//! The time window of a time trigger.

use std::fmt;

use gaslightmq_clock::{Duration, Instant, TimeSpan};

/// When a time trigger agrees, measured from the moment the rule was armed.
///
/// | Definition | The window is open |
/// |---|---|
/// | `after: 10s` | From ten seconds after arming, for as long as the rule stays armed. |
/// | `between: [10s, 40s]` | From ten seconds after arming, up to and not including forty seconds. |
///
/// A window only opens and closes.
/// It never disarms a rule: the [`Lifetime`](crate::Lifetime) does that.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant, TimeSpan};
/// use gaslightmq_trigger::prelude::*;
///
/// let window = TimeWindow::between(TimeSpan::from_secs(10), TimeSpan::from_secs(40))?;
/// assert!(!window.is_open(Duration::from_secs(9)));
/// assert!(window.is_open(Duration::from_secs(10)));
/// assert!(!window.is_open(Duration::from_secs(40)));
///
/// // The engine sets a timer for the moment the window opens.
/// let armed_at = Instant::ZERO + Duration::from_secs(5);
/// assert_eq!(window.opens_at(armed_at), Instant::ZERO + Duration::from_secs(15));
/// # Ok::<(), TimeWindowError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    opens: TimeSpan,
    closes: Option<TimeSpan>,
}

impl TimeWindow {
    /// The window that opens that long after arming and never closes: `after: 10s`.
    pub const fn after(opens: TimeSpan) -> TimeWindow {
        TimeWindow { opens, closes: None }
    }

    /// The window that is open from `from` after arming up to, and not including, `until`: `between: [10s, 40s]`.
    ///
    /// # Errors
    ///
    /// [`TimeWindowError::Empty`] when `until` is not later than `from`.
    pub fn between(from: TimeSpan, until: TimeSpan) -> Result<TimeWindow, TimeWindowError> {
        if until <= from {
            return Err(TimeWindowError::Empty { from, until });
        }
        Ok(TimeWindow {
            opens: from,
            closes: Some(until),
        })
    }

    /// How long after arming the window opens.
    pub const fn opens_after(self) -> TimeSpan {
        self.opens
    }

    /// How long after arming the window closes, or `None` when it stays open.
    pub const fn closes_after(self) -> Option<TimeSpan> {
        self.closes
    }

    /// Whether the window is open that long after arming.
    pub fn is_open(self, since_armed: Duration) -> bool {
        since_armed >= self.opens.duration() && self.closes.is_none_or(|closes| since_armed < closes.duration())
    }

    /// The instant the window opens, for a rule armed at `armed_at`.
    pub fn opens_at(self, armed_at: Instant) -> Instant {
        armed_at + self.opens
    }

    /// The instant the window closes, for a rule armed at `armed_at`, or `None` when it stays open.
    pub fn closes_at(self, armed_at: Instant) -> Option<Instant> {
        self.closes.map(|closes| armed_at + closes)
    }
}

/// Writes the window as a sentence part, such as `after 10s` or `between 10s and 40s`.
impl fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.closes {
            None => write!(f, "after {}", self.opens),
            Some(closes) => write!(f, "between {} and {closes}", self.opens),
        }
    }
}

/// Why two time spans are not a [`TimeWindow`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TimeWindowError {
    /// The window closes before it opens, or at the moment it opens.
    #[error("a window between {from} and {until} never opens")]
    Empty { from: TimeSpan, until: TimeSpan },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(secs: u64) -> TimeSpan {
        TimeSpan::from_secs(secs)
    }

    #[test]
    fn after_opens_and_stays_open() {
        let window = TimeWindow::after(secs(10));
        assert!(!window.is_open(Duration::ZERO));
        assert!(!window.is_open(Duration::from_millis(9_999)));
        assert!(window.is_open(Duration::from_secs(10)));
        assert!(window.is_open(Duration::MAX));
        assert_eq!(window.opens_after(), secs(10));
        assert_eq!(window.closes_after(), None);
    }

    #[test]
    fn between_is_open_up_to_and_not_including_its_end() {
        let window = TimeWindow::between(secs(10), secs(40)).unwrap();
        assert!(!window.is_open(Duration::from_millis(9_999)));
        assert!(window.is_open(Duration::from_secs(10)));
        assert!(window.is_open(Duration::from_millis(39_999)));
        assert!(!window.is_open(Duration::from_secs(40)));
        assert!(!window.is_open(Duration::MAX));
        assert_eq!(window.opens_after(), secs(10));
        assert_eq!(window.closes_after(), Some(secs(40)));
    }

    #[test]
    fn a_window_from_zero_is_open_at_arming() {
        assert!(TimeWindow::after(TimeSpan::ZERO).is_open(Duration::ZERO));
        assert!(
            TimeWindow::between(TimeSpan::ZERO, secs(1))
                .unwrap()
                .is_open(Duration::ZERO)
        );
    }

    #[test]
    fn a_window_that_never_opens_is_an_error() {
        assert_eq!(
            TimeWindow::between(secs(40), secs(10)),
            Err(TimeWindowError::Empty {
                from: secs(40),
                until: secs(10)
            }),
        );
        assert_eq!(
            TimeWindow::between(secs(10), secs(10)),
            Err(TimeWindowError::Empty {
                from: secs(10),
                until: secs(10)
            }),
        );
    }

    #[test]
    fn a_window_error_says_what_is_wrong() {
        let error = TimeWindowError::Empty {
            from: secs(40),
            until: secs(10),
        };
        assert_eq!(error.to_string(), "a window between 40s and 10s never opens");
    }

    #[test]
    fn a_window_names_its_instants_from_the_moment_of_arming() {
        let armed_at = Instant::ZERO + Duration::from_secs(5);
        let window = TimeWindow::between(secs(10), secs(40)).unwrap();
        assert_eq!(window.opens_at(armed_at), Instant::ZERO + Duration::from_secs(15));
        assert_eq!(
            window.closes_at(armed_at),
            Some(Instant::ZERO + Duration::from_secs(45))
        );
        assert_eq!(TimeWindow::after(secs(10)).closes_at(armed_at), None);
    }

    #[test]
    fn a_window_writes_as_a_sentence_part() {
        assert_eq!(TimeWindow::after(secs(10)).to_string(), "after 10s");
        assert_eq!(
            TimeWindow::between(secs(10), secs(40)).unwrap().to_string(),
            "between 10s and 40s"
        );
    }
}
