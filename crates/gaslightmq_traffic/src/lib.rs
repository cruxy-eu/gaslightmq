//! Live traffic: the newest calls of an engine, subscriptions by selector, statistics per rule and per API, and the
//! names that the calls named.
//!
//! An engine hands over the record of every call that ended.
//! This crate is what a traffic view keeps of them, for someone who watches what happens now.
//! It keeps no history: a recording keeps what happened, and the index counts it.
//! What it keeps of the calls is bounded: a window of the newest, and the names that were seen last.
//!
//! | Type | Is |
//! |---|---|
//! | [`Traffic`] | The traffic of one engine: it takes the record of every call, and holds the four parts below. |
//! | [`LiveWindow`] | The newest calls, for someone who starts to watch. |
//! | [`SubscriptionId`] | One subscription to the calls that a selector matches. |
//! | [`Statistics`] | The number of calls of each API, and what each rule did. |
//! | [`RuleStatistics`] | How far one rule got, on how many calls: its matches and its firings. |
//! | [`SeenNames`] | The topics, consumer groups and client IDs that the calls named, for a ⌘K sentence to say. |
//! | [`TrafficConfiguration`] | What a traffic is configured with. |
//! | [`LiveWindowSize`] | How many calls the live window keeps. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! # Whoever calls does the waiting
//!
//! A traffic waits for nothing, and it sends nothing.
//! The engine waits for the next record, and hands it over with [`Traffic::record`].
//! The traffic answers with the subscriptions that the record goes to, and the engine passes it on.
//! The engine also decides when to send the statistics: it reads [`Traffic::statistics`] whenever its timer says so.
//!
//! A record arrives as an `Arc`, as the engine hands it out, and it is never copied.
//! The live window, a subscriber and a recorder all hold the same record.
//!
//! # A traffic can miss calls
//!
//! A traffic shows what happens now, so no session of the engine waits for it.
//! When it falls behind, the engine drops the oldest events for it, and says how many.
//! [`Traffic::fell_behind`] takes that number, and [`Statistics::missed_events`] gives the sum.
//! Whoever needs every call keeps a recording.
//!
//! # Errors
//!
//! Nothing that a traffic does can fail.
//! A subscription that nobody knows is no error: ending it changes nothing.
//! The one type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`LiveWindowSize`] | [`LiveWindowSizeError`] | The window keeps no call. |
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_call` for the record of a call, on `gaslightmq_selector` for the selector
//! of a subscription, and on `gaslightmq_rule` for the name that someone asks the statistics of a rule by.
//! It reads the API and the names of a call with `gaslightmq_field`, and keeps them as the types of `gaslightmq_kernel`.
//! It does not depend on the Kafka codec.
//! It has no async code, no IO, no clock and no randomness: the order of the records is all it knows of time.

mod configuration;
mod names;
mod statistics;
mod subscription;
mod traffic;
mod window;

pub use configuration::{LiveWindowSize, LiveWindowSizeError, TrafficConfiguration};
pub use names::SeenNames;
pub use statistics::{RuleStatistics, Statistics};
pub use subscription::SubscriptionId;
pub use traffic::Traffic;
pub use window::LiveWindow;

/// Every type of this crate, with the error of the window size.
///
/// Import it whole wherever code speaks of live traffic,
/// next to the preludes of `gaslightmq_call`, `gaslightmq_selector` and `gaslightmq_rule`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_traffic::prelude::*;
///
/// let size: Result<LiveWindowSize, LiveWindowSizeError> = LiveWindowSize::new(64);
/// let mut configuration = TrafficConfiguration::new();
/// configuration.live_window = size?;
///
/// let traffic = Traffic::new(configuration);
/// assert!(traffic.live_window().is_empty());
/// # Ok::<(), LiveWindowSizeError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        LiveWindow, LiveWindowSize, LiveWindowSizeError, RuleStatistics, SeenNames, Statistics, SubscriptionId,
        Traffic, TrafficConfiguration,
    };
}
