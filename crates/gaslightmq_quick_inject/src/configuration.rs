//! What a quick inject is configured with.
//!
//! A property is a public field with a default, and its type only holds values that the property allows.

use gaslightmq_clock::TimeSpan;

/// What a [`QuickInject`](crate::QuickInject) is configured with: what a sentence means where it leaves a value out,
/// and how much it offers.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_quick_inject::prelude::*;
///
/// let mut configuration = QuickInjectConfiguration::default();
/// assert_eq!(configuration.default_delay, TimeSpan::from_millis(500));
///
/// configuration.completion_limit = 8;
/// let quick_inject = QuickInject::new(configuration);
/// assert_eq!(quick_inject.configuration().completion_limit, 8);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct QuickInjectConfiguration {
    /// Default: 500 milliseconds.
    ///
    /// How long a `delay` holds a call back when the sentence names no time span.
    pub default_delay: TimeSpan,
    /// Default: 2 seconds.
    ///
    /// How long a `throttle` tells a client to back off when the sentence names no time span.
    pub default_throttle: TimeSpan,
    /// Default: 5.
    ///
    /// The most completions a sentence offers for the word that is being typed.
    /// With zero, a sentence offers none.
    pub completion_limit: usize,
}

/// The defaults that each property names.
impl Default for QuickInjectConfiguration {
    fn default() -> Self {
        Self {
            default_delay: TimeSpan::from_millis(500),
            default_throttle: TimeSpan::from_secs(2),
            completion_limit: 5,
        }
    }
}
