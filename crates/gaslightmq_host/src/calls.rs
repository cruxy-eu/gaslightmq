//! The calls of an environment, as its traffic counts them and hands them to whoever follows them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_call::Call;
use gaslightmq_selector::EffectiveSelector;
use gaslightmq_traffic::{SubscriptionId, Traffic};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::CallBacklog;

/// The traffic of one environment, and the followers of its subscriptions.
///
/// The traffic says which subscriptions a call goes to, and this hands the call to their followers.
#[derive(Debug)]
pub(crate) struct Watched {
    traffic: Traffic,
    followers: BTreeMap<SubscriptionId, broadcast::Sender<Arc<Call>>>,
}

impl Watched {
    pub(crate) fn new(traffic: Traffic) -> Self {
        Self {
            traffic,
            followers: BTreeMap::new(),
        }
    }

    /// The traffic, to read its statistics.
    pub(crate) const fn traffic(&self) -> &Traffic {
        &self.traffic
    }

    /// Counts a call, and hands it to the follower of every subscription whose selector matches it.
    pub(crate) fn record(&mut self, call: &Arc<Call>) {
        for subscription in self.traffic.record(Arc::clone(call)) {
            if let Some(follower) = self.followers.get(&subscription) {
                // A follower that let go of its subscription a moment ago takes no call.
                let _ = follower.send(Arc::clone(call));
            }
        }
    }

    /// The environment stopped: every follower gets the calls it did not take yet, and then no more.
    pub(crate) fn end(&mut self) {
        self.followers.clear();
    }
}

/// The calls of one environment that one selector matches, from the moment someone followed them.
///
/// [`Host::follow_calls`](crate::Host::follow_calls) makes one, and the subscription ends when it is let go of.
/// No session waits for it: the host keeps a [backlog](crate::HostConfiguration::call_backlog) of calls for it,
/// and a follower that falls further behind misses the oldest of them.
#[derive(Debug)]
pub struct CallSubscription {
    calls: broadcast::Receiver<Arc<Call>>,
    watched: Arc<Mutex<Watched>>,
    subscription: SubscriptionId,
}

impl CallSubscription {
    /// Subscribes to the calls of `watched` that `selector` matches.
    pub(crate) fn new(watched: Arc<Mutex<Watched>>, selector: EffectiveSelector, backlog: CallBacklog) -> Self {
        let (follower, calls) = broadcast::channel(backlog.calls());
        let subscription = {
            let mut locked = lock(&watched);
            let subscription = locked.traffic.subscribe(selector);
            locked.followers.insert(subscription, follower);
            subscription
        };
        Self {
            calls,
            watched,
            subscription,
        }
    }

    /// Waits for the next call that the selector matches, or returns `None` once the environment stopped
    /// and the follower took every call it was handed.
    ///
    /// A follower that fell behind goes on with the oldest call that the host still keeps for it.
    pub async fn next(&mut self) -> Option<Arc<Call>> {
        loop {
            match self.calls.recv().await {
                Ok(call) => return Some(call),
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return None,
            }
        }
    }
}

/// Ends the subscription, so the traffic matches no call for it any more.
impl Drop for CallSubscription {
    fn drop(&mut self) {
        let mut locked = lock(&self.watched);
        locked.traffic.unsubscribe(self.subscription);
        locked.followers.remove(&self.subscription);
    }
}

/// The traffic and its followers. They are whole whenever the lock is free, also after a panic of the task that held
/// it: every change is one call counted, or one subscription added or removed.
pub(crate) fn lock(watched: &Mutex<Watched>) -> MutexGuard<'_, Watched> {
    watched.lock().unwrap_or_else(PoisonError::into_inner)
}
