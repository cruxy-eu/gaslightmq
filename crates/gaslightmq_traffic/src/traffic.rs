//! The traffic of one engine: its newest calls, who watches which of them, and what it counted.

use std::sync::Arc;

use gaslightmq_call::Call;
use gaslightmq_selector::EffectiveSelector;

use crate::subscription::Subscriptions;
use crate::{LiveWindow, Statistics, SubscriptionId, TrafficConfiguration};

/// The traffic of one engine, as someone watches it: the newest calls, the subscriptions and the statistics.
///
/// Whoever runs the engine hands it the record of every call that ended, with [`record`](Self::record).
/// The traffic does three things with a record, and none of them waits:
///
/// | It | So that |
/// |---|---|
/// | Keeps it in the [live window](Self::live_window). | Someone who starts to watch sees what just happened. |
/// | Says which [subscriptions](Self::subscribe) it goes to. | A UI that only shows `orders.*` does not receive every call. |
/// | Counts it in the [statistics](Self::statistics). | A rule shows its matches and its firings, and an API its calls. |
///
/// A traffic sends nothing itself.
/// [`record`](Self::record) returns the subscriptions of a call, and whoever holds the connections passes the call on.
///
/// ```
/// use std::sync::Arc;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_traffic::prelude::*;
///
/// // The second call of connection #7: a Heartbeat that no rule touched.
/// let fields = Fields::new()
///     .with(ConnectionNumber::new(7))
///     .with(ApiKey::HEARTBEAT)
///     .with(Outcome::Ok);
/// let heartbeat = Call::new(
///     CallNumber::FIRST.next(),
///     fields,
///     ResponseSummary::new(Answer::Ok).into(),
///     Explanation::none(),
///     CallTiming::arrived(Instant::ZERO),
/// )?;
///
/// let mut traffic = Traffic::new(TrafficConfiguration::new());
/// let group_calls = traffic.subscribe(Condition::is(ApiKey::HEARTBEAT).into());
/// let produce_calls = traffic.subscribe(Condition::is(ApiKey::PRODUCE).into());
///
/// // The record goes to whoever asked for the calls of its API, and to nobody else.
/// assert_eq!(traffic.record(Arc::new(heartbeat)), [group_calls]);
/// assert!(traffic.selector_of(produce_calls).is_some());
///
/// assert_eq!(traffic.statistics().calls_of(ApiKey::HEARTBEAT), 1);
/// let newest = traffic.live_window().calls().next_back().expect("the window keeps the call");
/// assert_eq!(newest.id().to_string(), "#7.2");
/// # Ok::<(), CallError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Traffic {
    configuration: TrafficConfiguration,
    live_window: LiveWindow,
    subscriptions: Subscriptions,
    statistics: Statistics,
}

impl Traffic {
    /// A traffic without a call and without a subscription.
    pub fn new(configuration: TrafficConfiguration) -> Self {
        Self {
            live_window: LiveWindow::new(configuration.live_window),
            configuration,
            subscriptions: Subscriptions::new(),
            statistics: Statistics::default(),
        }
    }

    /// What the traffic is configured with.
    pub const fn configuration(&self) -> &TrafficConfiguration {
        &self.configuration
    }

    /// A call ended, and `call` is its record.
    ///
    /// The live window keeps the record, and the statistics count it.
    /// The result names the subscriptions whose selector matches the call, in the order in which they were made.
    /// Whoever calls passes the record on to each of them.
    pub fn record(&mut self, call: Arc<Call>) -> Vec<SubscriptionId> {
        let subscriptions = self.subscriptions.of(&call);
        self.statistics.count(&call);
        self.live_window.push(call);
        subscriptions
    }

    /// Whoever feeds the traffic fell behind, and `events` of the engine never reached it.
    ///
    /// The traffic cannot make up what it never saw.
    /// It counts the events, and the [statistics](Statistics::missed_events) say that their numbers are too low.
    pub const fn fell_behind(&mut self, events: u64) {
        self.statistics.miss(events);
    }

    /// The newest calls.
    pub const fn live_window(&self) -> &LiveWindow {
        &self.live_window
    }

    /// What the traffic counted since it began.
    pub const fn statistics(&self) -> &Statistics {
        &self.statistics
    }

    /// Subscribes to the calls that `selector` matches, from the next call on.
    ///
    /// A selector without a condition matches every call.
    /// What the subscription missed is in the live window: [`LiveWindow::matching`] gives the calls so far.
    ///
    /// A selector is evaluated against the record of a call that ended.
    /// The truth of such a call is settled, so a selector that tests a response field decides at once.
    pub fn subscribe(&mut self, selector: EffectiveSelector) -> SubscriptionId {
        self.subscriptions.add(selector)
    }

    /// Ends a subscription, and gives its selector back.
    ///
    /// An ID that names no subscription changes nothing, and gives `None`:
    /// whoever watched can be gone before the last call for it is passed on.
    pub fn unsubscribe(&mut self, subscription: SubscriptionId) -> Option<EffectiveSelector> {
        self.subscriptions.remove(subscription)
    }

    /// The selector of a subscription, or `None` for an ID that names no subscription.
    pub fn selector_of(&self, subscription: SubscriptionId) -> Option<&EffectiveSelector> {
        self.subscriptions.selector(subscription)
    }

    /// Every subscription with its selector, in the order in which they were made.
    pub fn subscriptions(&self) -> impl Iterator<Item = (SubscriptionId, &EffectiveSelector)> {
        self.subscriptions.iter()
    }
}
