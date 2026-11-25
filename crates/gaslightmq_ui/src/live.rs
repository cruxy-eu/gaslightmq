//! Everything the UI knows from the live stream, and what it shows of it.

use gaslightmq_control_api::call::{Call, CallId};
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::field::{Field, FieldValue};
use gaslightmq_control_api::prelude::*;

use crate::configuration::UiConfiguration;
use crate::filter::TrafficFilter;
use crate::following::Following;
use crate::process::Process;
use crate::traffic::Traffic;

/// Where the live stream stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// The UI opens a live stream, and knows nothing yet.
    Opening,
    /// The stream is open: the UI holds what the process holds, from the first snapshot on.
    Open,
    /// The stream ended, or did not open, and the UI opens a new one soon.
    /// What it shows is what it knew last. `reason` says what happened.
    Lost { reason: String },
}

/// What the UI knows of the process from the live stream, and what it shows of it.
///
/// It applies every message of the stream: the snapshot and each change to the [`Process`],
/// the calls to the [`Traffic`] of the environment it shows, and the answers to its subscriptions to its
/// [`Following`]. It shows one environment at a time, and follows the calls of that environment that
/// its [`TrafficFilter`] lets through.
///
/// Whoever holds the stream asks it for the [next subscription](Self::next_subscription) after every change,
/// and sends what it returns.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_ui::{Live, UiConfiguration};
///
/// let mut live = Live::new(&UiConfiguration::default());
/// live.opened();
///
/// // Nothing runs yet, so the UI follows no calls.
/// live.apply(StreamMessage::Snapshot(ProcessState::default()));
/// assert_eq!(live.next_subscription(), None);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Live {
    link: Link,
    process: Option<Process>,
    shown: Option<EnvironmentName>,
    filter: TrafficFilter,
    following: Following,
    traffic: Traffic,
    inspected: Option<Call>,
}

impl Live {
    /// The UI before its first live stream.
    pub fn new(configuration: &UiConfiguration) -> Self {
        Self {
            link: Link::Opening,
            process: None,
            shown: None,
            filter: TrafficFilter::default(),
            following: Following::default(),
            traffic: Traffic::new(configuration.calls_kept),
            inspected: None,
        }
    }

    /// Where the live stream stands.
    pub fn link(&self) -> &Link {
        &self.link
    }

    /// The process, once the first snapshot came.
    pub fn process(&self) -> Option<&Process> {
        self.process.as_ref()
    }

    /// The environment the UI shows, whether it runs or not.
    pub fn shown(&self) -> Option<&EnvironmentName> {
        self.shown.as_ref()
    }

    /// The environment the UI shows, while it runs.
    pub fn shown_running(&self) -> Option<&RunningEnvironment> {
        self.process.as_ref()?.environment(self.shown.as_ref()?)
    }

    /// The filter of the live traffic.
    pub fn filter(&self) -> &TrafficFilter {
        &self.filter
    }

    /// What the stream follows, and what it was refused.
    pub fn following(&self) -> &Following {
        &self.following
    }

    /// The calls of the environment shown that the filter lets through.
    pub fn traffic(&self) -> &Traffic {
        &self.traffic
    }

    /// The call someone looks at, which stays while the window lets it go.
    pub fn inspected(&self) -> Option<&Call> {
        self.inspected.as_ref()
    }

    /// The UI opens a new live stream.
    pub fn opening(&mut self) {
        self.link = Link::Opening;
    }

    /// A new live stream is open. It follows no calls until it asks, and its snapshot comes first.
    pub fn opened(&mut self) {
        self.link = Link::Open;
        self.following = Following::default();
    }

    /// The live stream ended, or did not open. `reason` says why.
    pub fn lost(&mut self, reason: String) {
        self.link = Link::Lost { reason };
        self.following = Following::default();
    }

    /// Applies one message of the live stream.
    pub fn apply(&mut self, message: StreamMessage) {
        match message {
            StreamMessage::Snapshot(state) => {
                let process = Process::new(state);
                if self.shown.is_none() {
                    self.shown = process
                        .environments()
                        .first()
                        .map(|running| running.environment.clone());
                }
                self.process = Some(process);
            }
            StreamMessage::Call { environment, call } => {
                if self.shown.as_ref() == Some(&environment) && self.following.follows(&self.wanted()) {
                    self.traffic.arrived(call);
                }
            }
            StreamMessage::Following { subscription } => self.following.followed(subscription),
            StreamMessage::SubscriptionRefused { reason } => self.following.refused(reason),
            change => {
                match &change {
                    StreamMessage::EnvironmentStarted { environment } => {
                        self.following.started(&environment.environment);
                        if self.shown.is_none() {
                            self.shown = Some(environment.environment.clone());
                        }
                    }
                    StreamMessage::EnvironmentStopped { environment } => self.following.stopped(environment),
                    _ => {}
                }
                if let Some(process) = &mut self.process {
                    process.apply(&change);
                }
            }
        }
    }

    /// The UI shows another environment, and its traffic from here on.
    pub fn show(&mut self, environment: EnvironmentName) {
        if self.shown.as_ref() != Some(&environment) {
            self.shown = Some(environment);
            self.traffic.clear();
            self.inspected = None;
        }
    }

    /// Adds `value` to the filter, or takes it away, and the traffic shows the calls of the new filter from here on.
    pub fn toggle(&mut self, value: FieldValue) {
        self.filter.toggle(value);
        self.traffic.clear();
    }

    /// Takes the condition of `field` away from the filter.
    pub fn clear_filter(&mut self, field: Field) {
        self.filter.clear(field);
        self.traffic.clear();
    }

    /// The rows of the traffic stand still, or go on.
    pub fn pause(&mut self, paused: bool) {
        if paused {
            self.traffic.pause();
        } else {
            self.traffic.resume();
        }
    }

    /// Someone looks at the call with that ID, or at none.
    pub fn inspect(&mut self, id: Option<CallId>) {
        self.inspected = id.and_then(|id| self.traffic.call(id)).cloned();
    }

    /// What the UI sends next so that the stream follows the calls it shows, or `None` when nothing needs to change.
    ///
    /// The UI follows the calls of the environment it shows that the filter lets through, while that environment runs,
    /// and no calls otherwise. It asks nothing before the first snapshot.
    pub fn next_subscription(&self) -> Option<Subscription> {
        if self.process.is_none() || self.link != Link::Open {
            return None;
        }
        self.following.next(&self.wanted())
    }

    /// The UI sent `subscription` on the live stream.
    pub fn asked(&mut self, subscription: Subscription) {
        self.following.asked(subscription);
    }

    /// The subscription that follows what the UI shows.
    fn wanted(&self) -> Subscription {
        match (&self.shown, &self.process) {
            (Some(shown), Some(process)) if process.runs(shown) => Subscription::Calls {
                environment: shown.clone(),
                selector: self.filter.selector(),
            },
            _ => Subscription::Nothing,
        }
    }
}
