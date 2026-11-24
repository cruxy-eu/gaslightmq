//! Subscriptions: who receives which calls.

use std::fmt;

use gaslightmq_call::Call;
use gaslightmq_selector::{ApiIndex, EffectiveSelector};

/// Which subscription of a [`Traffic`](crate::Traffic) this is.
///
/// [`Traffic::subscribe`](crate::Traffic::subscribe) hands one out, and no two subscriptions of a traffic share one.
/// An ID stays with its subscription until someone [ends](crate::Traffic::unsubscribe) it, and it is never used again.
/// Whoever subscribed keeps it next to the place the calls go to, such as the connection of a UI.
///
/// The text form is the word and the number: `subscription 3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// The first subscription of a traffic.
    const FIRST: Self = Self(1);

    /// The ID of the subscription after this one.
    const fn next(self) -> Self {
        // A traffic that hands out a billion IDs a second runs out of them after five hundred years.
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "subscription {}", self.0)
    }
}

/// The subscriptions of a traffic, each with its selector, in the order in which they were made.
#[derive(Debug, Clone)]
pub(crate) struct Subscriptions {
    next: SubscriptionId,
    /// By API, so a call only tests the selectors that can match its API.
    by_api: ApiIndex<SubscriptionId>,
}

impl Subscriptions {
    pub fn new() -> Self {
        Self {
            next: SubscriptionId::FIRST,
            by_api: ApiIndex::default(),
        }
    }

    /// A new subscription to the calls that `selector` matches.
    pub fn add(&mut self, selector: EffectiveSelector) -> SubscriptionId {
        let id = self.next;
        self.next = id.next();
        self.by_api = self.entries().chain([(id, selector)]).collect();
        id
    }

    /// Ends the subscription, and gives its selector back. `None` for an ID that names no subscription.
    pub fn remove(&mut self, id: SubscriptionId) -> Option<EffectiveSelector> {
        let selector = self.selector(id)?.clone();
        self.by_api = self.entries().filter(|(other, _)| *other != id).collect();
        Some(selector)
    }

    /// The selector of the subscription. `None` for an ID that names no subscription.
    pub fn selector(&self, id: SubscriptionId) -> Option<&EffectiveSelector> {
        let mut subscriptions = self.by_api.iter();
        subscriptions.find_map(|(other, selector)| (*other == id).then_some(selector))
    }

    /// Every subscription with its selector, in the order in which they were made.
    pub fn iter(&self) -> impl Iterator<Item = (SubscriptionId, &EffectiveSelector)> {
        self.by_api.iter().map(|(id, selector)| (*id, selector))
    }

    /// The subscriptions whose selector matches `call`, in the order in which they were made.
    pub fn of(&self, call: &Call) -> Vec<SubscriptionId> {
        self.by_api.matches(call).map(|(id, _)| *id).collect()
    }

    /// Every subscription with its selector, to build the index again.
    fn entries(&self) -> impl Iterator<Item = (SubscriptionId, EffectiveSelector)> {
        self.iter().map(|(id, selector)| (id, selector.clone()))
    }
}
