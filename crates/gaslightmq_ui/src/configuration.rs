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
/// assert_eq!(configuration.undo_depth.get(), 100);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UiConfiguration {
    /// Ours. Default: 500 calls.
    ///
    /// How many calls the live traffic keeps, the newest first. An older call leaves the window.
    pub calls_kept: NonZeroUsize,
    /// Ours. Default: 60 statistics.
    ///
    /// How many statistics of each environment the UI keeps, the newest last, for the activity of a rule.
    /// The control plane sends them at an interval, so 60 of them at the default interval are the last minute.
    pub stats_kept: NonZeroUsize,
    /// Ours. Default: 1 second.
    ///
    /// How long the UI waits before it reaches for the control plane again: to open a new live stream, after one
    /// ended or did not open, and to send the release of a pad again that did not arrive.
    pub reconnect_after: TimeSpan,
    /// Ours. Default: 100 milliseconds.
    ///
    /// How often the UI shows what the live stream sent since it showed last.
    /// The messages in between apply together, so a busy environment does not draw every call on its own.
    pub refresh_interval: TimeSpan,
    /// Ours. Default: 150 milliseconds.
    ///
    /// How long ⌘K waits after the last key before it asks the control plane to read the sentence,
    /// so a fast typist sends one question and not one per key.
    pub parse_after: TimeSpan,
    /// Ours. Default: 5 seconds.
    ///
    /// How long a notice stays on the screen, such as the error of an operation that someone started.
    pub notice_for: TimeSpan,
    /// Ours. Default: 100 steps.
    ///
    /// How many steps the [undo history](crate::history::History) of a tab holds.
    /// The oldest step goes when a new one would be one too many.
    pub undo_depth: NonZeroUsize,
}

impl Default for UiConfiguration {
    fn default() -> Self {
        Self {
            calls_kept: NonZeroUsize::new(500).expect("500 is no zero"),
            stats_kept: NonZeroUsize::new(60).expect("60 is no zero"),
            reconnect_after: TimeSpan::from_secs(1),
            refresh_interval: TimeSpan::from_millis(100),
            notice_for: TimeSpan::from_secs(5),
            parse_after: TimeSpan::from_millis(150),
            undo_depth: NonZeroUsize::new(100).expect("100 is no zero"),
        }
    }
}
