//! What the traffic of an engine is configured with.
//!
//! A property is a public field, and its type only holds values that the property allows.
//! No property is one of Kafka: all of them are ours.

use std::fmt;
use std::num::NonZeroUsize;

/// What one [`Traffic`](crate::Traffic) is configured with.
///
/// ```
/// use gaslightmq_traffic::prelude::*;
///
/// let mut configuration = TrafficConfiguration::new();
/// assert_eq!(configuration.live_window.calls(), 1000);
///
/// configuration.live_window = LiveWindowSize::new(250)?;
/// # Ok::<(), LiveWindowSizeError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TrafficConfiguration {
    /// Ours. Default: 1000 calls.
    ///
    /// How many calls the [live window](crate::LiveWindow) keeps.
    /// A call that arrives at a full window pushes the oldest one out.
    /// The window is what someone sees who starts to watch, so it is as large as the stretch they want to look back on.
    /// Every call it keeps is memory that the traffic holds.
    pub live_window: LiveWindowSize,
}

impl TrafficConfiguration {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }
}

/// How many calls a [live window](crate::LiveWindow) keeps.
///
/// A window keeps at least one call.
/// It takes the room for a call when the call arrives, so a large window costs nothing until the calls are there.
///
/// ```
/// use gaslightmq_traffic::prelude::*;
///
/// let size = LiveWindowSize::new(250)?;
/// assert_eq!(size.calls(), 250);
/// assert_eq!(size.to_string(), "250 calls");
/// assert_eq!(LiveWindowSize::new(0), Err(LiveWindowSizeError::Zero));
/// # Ok::<(), LiveWindowSizeError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LiveWindowSize(NonZeroUsize);

impl LiveWindowSize {
    /// Check that `calls` is at least one, and keep it.
    ///
    /// # Errors
    ///
    /// [`LiveWindowSizeError::Zero`] when `calls` is zero.
    pub const fn new(calls: usize) -> Result<Self, LiveWindowSizeError> {
        match NonZeroUsize::new(calls) {
            Some(calls) => Ok(Self(calls)),
            None => Err(LiveWindowSizeError::Zero),
        }
    }

    /// The size as a plain number of calls.
    pub const fn calls(self) -> usize {
        self.0.get()
    }
}

/// 1000 calls.
impl Default for LiveWindowSize {
    fn default() -> Self {
        const DEFAULT: NonZeroUsize = NonZeroUsize::new(1000).unwrap();
        Self(DEFAULT)
    }
}

impl TryFrom<usize> for LiveWindowSize {
    type Error = LiveWindowSizeError;

    fn try_from(calls: usize) -> Result<Self, Self::Error> {
        Self::new(calls)
    }
}

impl From<LiveWindowSize> for usize {
    fn from(size: LiveWindowSize) -> Self {
        size.calls()
    }
}

/// Writes the number of calls: `1000 calls`, and `1 call`.
impl fmt::Display for LiveWindowSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.calls() {
            1 => f.write_str("1 call"),
            calls => write!(f, "{calls} calls"),
        }
    }
}

/// Why a number of calls is not a [`LiveWindowSize`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveWindowSizeError {
    /// The size is zero, and a window that keeps no call shows nothing.
    #[error("a live window keeps at least one call")]
    Zero,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_starts_with_the_defaults() {
        let configuration = TrafficConfiguration::new();

        assert_eq!(configuration.live_window.calls(), 1000);
        assert_eq!(configuration, TrafficConfiguration::default());
    }

    #[test]
    fn a_live_window_keeps_at_least_one_call() {
        assert_eq!(LiveWindowSize::new(1).map(LiveWindowSize::calls), Ok(1));
        assert_eq!(LiveWindowSize::new(0), Err(LiveWindowSizeError::Zero));
        assert_eq!(LiveWindowSize::try_from(0), Err(LiveWindowSizeError::Zero));
        assert_eq!(LiveWindowSize::try_from(250).map(usize::from), Ok(250));
        assert_eq!(
            LiveWindowSizeError::Zero.to_string(),
            "a live window keeps at least one call"
        );
    }

    #[test]
    fn a_size_writes_its_number_of_calls() {
        let size = |calls| LiveWindowSize::new(calls).map(|size| size.to_string());

        assert_eq!(size(1).as_deref(), Ok("1 call"));
        assert_eq!(size(250).as_deref(), Ok("250 calls"));
        assert_eq!(LiveWindowSize::default().to_string(), "1000 calls");
    }
}
