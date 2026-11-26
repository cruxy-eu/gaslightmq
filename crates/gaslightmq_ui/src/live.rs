//! Everything the UI knows from the live stream, and what it shows of it.

use gaslightmq_control_api::call::{Call, CallId};
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::field::{Field, FieldValue};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::surface::PresenceId;

use crate::configuration::UiConfiguration;
use crate::filter::TrafficFilter;
use crate::following::Following;
use crate::history::{History, Subject};
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
/// It keeps the undo [history](Self::history) of the tab, which ends with the run of the process.
///
/// The UI is one [presence](Self::presence) of the control plane, which the live stream names.
/// Every request of the UI [says so](Self::source), so the pads that it holds end with the presence
/// when the tab is gone. A stream that ends keeps the presence: the next one asks to come back as it.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_ui::{Live, UiConfiguration};
///
/// let mut live = Live::new(&UiConfiguration::default());
/// live.opened();
///
/// // Nothing runs yet, so the UI follows no calls.
/// let snapshot = ProcessState {
///     run: "4be0643f1d98573b97cdca98a65347dd".parse().expect("a run ID"),
///     definitions: Definitions::default(),
///     workspace: WorkspaceStatus::default(),
///     environments: Vec::new(),
/// };
/// live.apply(StreamMessage::Snapshot(snapshot));
/// assert_eq!(live.next_subscription(), None);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Live {
    configuration: UiConfiguration,
    link: Link,
    presence: Option<PresenceId>,
    process: Option<Process>,
    shown: Option<EnvironmentName>,
    filter: TrafficFilter,
    following: Following,
    traffic: Traffic,
    inspected: Option<Call>,
    history: History,
}

impl Live {
    /// The UI before its first live stream.
    pub fn new(configuration: &UiConfiguration) -> Self {
        Self {
            configuration: configuration.clone(),
            link: Link::Opening,
            presence: None,
            process: None,
            shown: None,
            filter: TrafficFilter::default(),
            following: Following::default(),
            traffic: Traffic::new(configuration.calls_kept),
            inspected: None,
            history: History::new(configuration.undo_depth),
        }
    }

    /// Where the live stream stands.
    pub fn link(&self) -> &Link {
        &self.link
    }

    /// The presence of this tab: the one that its live stream named last.
    ///
    /// The next stream asks to come back as it. `None` before the first stream named one,
    /// and after the UI [let go](Self::let_go_of_presence) of the one it had.
    pub fn presence(&self) -> Option<PresenceId> {
        self.presence
    }

    /// Where a request of `surface` comes from: the surface, and the presence of this tab once it has one.
    pub fn source(&self, surface: RequestSurface) -> RequestSource {
        let source = RequestSource::new(surface);
        match self.presence {
            Some(presence) => source.with_presence(presence),
            None => source,
        }
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

    /// The live stream that opened named `presence` as the presence of this tab.
    ///
    /// Returns whether the presence that the tab had before has ended: the stream asked to come back as it,
    /// and the control plane no longer held it. The pads that the tab held as that presence are held no more.
    pub fn present_as(&mut self, presence: PresenceId) -> bool {
        let ended = self.presence.is_some_and(|before| before != presence);
        self.presence = Some(presence);
        if ended {
            self.history.holds_ended();
        }
        ended
    }

    /// The UI lets go of its presence: the next stream asks for none, and gets a new one.
    ///
    /// The control plane ends a presence that nobody comes back as, with every hold of it.
    /// The UI does this when a press in hold got no answer: the control plane may hold the pad for it,
    /// and the UI does not know which hold to release.
    pub fn let_go_of_presence(&mut self) {
        self.presence = None;
        self.history.holds_ended();
    }

    /// The live stream ended, or did not open. `reason` says why.
    pub fn lost(&mut self, reason: String) {
        self.link = Link::Lost { reason };
        self.following = Following::default();
    }

    /// The undo history of this tab.
    pub fn history(&self) -> &History {
        &self.history
    }

    /// The undo history of this tab, to record a change, or to undo or redo one.
    pub fn history_mut(&mut self) -> &mut History {
        &mut self.history
    }

    /// What a change of `subject` expects: the run of the snapshot, and the revision that the tab shows.
    /// `None` before the first snapshot, and while the process holds no such subject.
    ///
    /// The UI sends it with every change it makes, so a change never overwrites what the tab has not shown.
    pub fn expects(&self, subject: &Subject) -> Option<Expects> {
        let process = self.process.as_ref()?;
        Some(Expects {
            run: process.run(),
            revision: process.revision_of(subject)?,
        })
    }

    /// Applies one message of the live stream. The [history](Self::history) follows it too.
    pub fn apply(&mut self, message: StreamMessage) {
        self.history.apply(&message);
        match message {
            StreamMessage::Snapshot(state) => {
                let process = Process::new(state, self.configuration.stats_kept);
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
