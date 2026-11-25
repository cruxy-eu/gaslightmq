//! Which calls the live stream follows: what the UI asked, what the control plane follows, and what it refused.

use std::collections::VecDeque;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;

/// What the UI asked of the live stream, and what the control plane answered.
///
/// Every subscription gets one answer, in the order the subscriptions went, so the answers that are still to come
/// belong to the oldest subscriptions that wait.
/// A new stream follows nothing until it asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Following {
    /// The subscriptions that wait for their answer, the oldest first.
    asked: VecDeque<Subscription>,
    /// The subscription the control plane follows.
    in_force: Subscription,
    /// The last subscription the control plane refused, and why.
    refused: Option<(Subscription, String)>,
}

impl Default for Following {
    fn default() -> Self {
        Self {
            asked: VecDeque::new(),
            in_force: Subscription::Nothing,
            refused: None,
        }
    }
}

impl Following {
    /// The subscription that the control plane follows now.
    pub fn in_force(&self) -> &Subscription {
        &self.in_force
    }

    /// Why the control plane refused the last subscription, while it follows nothing instead.
    pub fn refusal(&self) -> Option<&str> {
        self.refused.as_ref().map(|(_, reason)| reason.as_str())
    }

    /// Whether the control plane follows `wanted`, so the calls that come are calls of it.
    pub fn follows(&self, wanted: &Subscription) -> bool {
        self.asked.is_empty() && &self.in_force == wanted
    }

    /// What to ask so that the stream follows `wanted`, or `None` when it follows it already, waits for it,
    /// or the control plane refused it and nothing changed since.
    pub fn next(&self, wanted: &Subscription) -> Option<Subscription> {
        let settled = match self.asked.back() {
            Some(last) => last == wanted,
            None => {
                &self.in_force == wanted
                    || self
                        .refused
                        .as_ref()
                        .is_some_and(|(subscription, _)| subscription == wanted)
            }
        };
        (!settled).then(|| wanted.clone())
    }

    /// The UI sent `subscription`, and waits for its answer.
    pub fn asked(&mut self, subscription: Subscription) {
        self.asked.push_back(subscription);
    }

    /// The control plane follows `subscription`: it answered the oldest one that waits.
    pub fn followed(&mut self, subscription: Subscription) {
        self.asked.pop_front();
        self.in_force = subscription;
        self.refused = None;
    }

    /// The control plane refused the oldest subscription that waits, and follows nothing.
    pub fn refused(&mut self, reason: String) {
        let subscription = self.asked.pop_front().unwrap_or(Subscription::Nothing);
        self.in_force = Subscription::Nothing;
        self.refused = Some((subscription, reason));
    }

    /// The environment stopped, and the stream follows none of its calls.
    pub fn stopped(&mut self, environment: &EnvironmentName) {
        if names(&self.in_force, environment) {
            self.in_force = Subscription::Nothing;
        }
    }

    /// The environment started, so a subscription to it that was refused may be followed now.
    pub fn started(&mut self, environment: &EnvironmentName) {
        if self
            .refused
            .as_ref()
            .is_some_and(|(subscription, _)| names(subscription, environment))
        {
            self.refused = None;
        }
    }
}

/// Whether `subscription` follows calls of `environment`.
fn names(subscription: &Subscription, environment: &EnvironmentName) -> bool {
    matches!(subscription, Subscription::Calls { environment: followed, .. } if followed == environment)
}
