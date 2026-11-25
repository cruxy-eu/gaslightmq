//! What the UI is configured with.

use std::num::NonZeroUsize;

use gaslightmq_control_api::clock::TimeSpan;

/// What the UI is configured with.
///
/// A property is a public field with a default. The UI is no Kafka client, so each property is ours.
///
/// ```
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_ui::UiConfiguration;
///
/// let configuration = UiConfiguration::default();
/// assert_eq!(configuration.calls_kept.get(), 500);
/// assert_eq!(configuration.reconnect_after, TimeSpan::from_secs(1));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UiConfiguration {
    /// Ours. Default: 500 calls.
    ///
    /// How many calls the live traffic keeps, the newest first. An older call leaves the window.
    pub calls_kept: NonZeroUsize,
    /// Ours. Default: 1 second.
    ///
    /// How long the UI waits to open a new live stream, after one ended or did not open.
    pub reconnect_after: TimeSpan,
    /// Ours. Default: 100 milliseconds.
    ///
    /// How often the UI shows what the live stream sent since it showed last.
    /// The messages in between apply together, so a busy environment does not draw every call on its own.
    pub refresh_interval: TimeSpan,
    /// Ours. Default: 5 seconds.
    ///
    /// How long a notice stays on the screen, such as the error of an operation that someone started.
    pub notice_for: TimeSpan,
}

impl Default for UiConfiguration {
    fn default() -> Self {
        Self {
            calls_kept: NonZeroUsize::new(500).expect("500 is no zero"),
            reconnect_after: TimeSpan::from_secs(1),
            refresh_interval: TimeSpan::from_millis(100),
            notice_for: TimeSpan::from_secs(5),
        }
    }
}
