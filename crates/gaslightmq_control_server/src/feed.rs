//! The changes of the host as messages of the live stream, and the statistics at their interval.

use std::collections::BTreeMap;
use std::sync::Arc;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::RuleName;
use gaslightmq_control_api::trigger::Budget;
use gaslightmq_engine::SessionEvent;
use gaslightmq_host::{HostChange, HostChanges, HostChangesError};
use gaslightmq_session::ClosedBy;
use gaslightmq_workspace::WorkspaceContext;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::StatsInterval;
use crate::definitions::{loses_comment, named, reference};
use crate::plane::{Change, ControlPlane, session_status, status};
use crate::revisions::revision;
use crate::server::stopped;

/// The task that hands every change of the host to the clients of the live stream, and the statistics of every
/// environment that runs at each interval, until the server stops.
pub(crate) struct Feed<C: WorkspaceContext> {
    plane: Arc<ControlPlane<C>>,
    changes: HostChanges,
    stats_interval: StatsInterval,
    stopping: watch::Receiver<bool>,
}

impl<C: WorkspaceContext + 'static> Feed<C> {
    /// The feed of `plane`, which follows the host from this moment on.
    pub(crate) fn new(
        plane: Arc<ControlPlane<C>>,
        stats_interval: StatsInterval,
        stopping: watch::Receiver<bool>,
    ) -> Self {
        let changes = plane.host().changes();
        Self {
            plane,
            changes,
            stats_interval,
            stopping,
        }
    }

    /// Hands changes and statistics on until the server stops, or the host is gone.
    pub(crate) async fn run(mut self) {
        let mut ticks = tokio::time::interval(self.stats_interval.span().duration());
        // A server that was busy sends the statistics once, and not once for every interval it missed.
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stopped(&mut self.stopping) => return,
                change = self.changes.next() => match change {
                    Ok(change) => {
                        if let Some(message) = self.message(change).await {
                            self.plane.publish(Change::Message(Arc::new(message)));
                        }
                    }
                    // The clients hold what the server told them, and the server missed changes since.
                    Err(HostChangesError::Missed { .. }) => self.plane.publish(Change::Resync),
                    Err(HostChangesError::HostGone) => return,
                },
                _ = ticks.tick() => {
                    for environment in self.plane.host().running() {
                        if let Some(stats) = self.stats(&environment) {
                            self.plane.publish(Change::Message(Arc::new(stats)));
                        }
                    }
                }
            }
        }
    }

    /// The message of the live stream that tells a change of the host, with the state that it names now.
    ///
    /// A change whose subject is gone again by now tells nothing: a later change tells that it went.
    async fn message(&self, change: HostChange) -> Option<StreamMessage> {
        match change {
            HostChange::EnvironmentStarted { environment } => {
                let environment = self.plane.running_environment(&environment).await.ok()??;
                Some(StreamMessage::EnvironmentStarted { environment })
            }
            HostChange::EnvironmentStopped { environment } => Some(StreamMessage::EnvironmentStopped { environment }),
            HostChange::WorkspaceChanged => Some(StreamMessage::WorkspaceChanged {
                workspace: self.plane.workspace_status().await,
            }),
            HostChange::DefinitionChanged { name } => {
                let workspace = self.plane.host().workspace().await;
                let definition = named(&*workspace, &name).await.ok()??;
                // The definition and what is kept beside it are read as one: nobody changes the workspace in between.
                let loses_comment = loses_comment(&*workspace, &name).await.ok()?;
                let revision = revision(workspace.revision(name));
                Some(StreamMessage::DefinitionChanged {
                    definition,
                    revision,
                    loses_comment,
                })
            }
            HostChange::DefinitionDeleted { name } => Some(StreamMessage::DefinitionDeleted { name: reference(name) }),
            HostChange::RuleChanged { environment, rule } => {
                let rule = self.plane.host().rule_of(&environment, &rule).await.ok()??;
                Some(StreamMessage::RuleChanged {
                    environment,
                    rule: status(rule),
                })
            }
            HostChange::RuleRemoved { environment, rule } => Some(StreamMessage::RuleRemoved { environment, rule }),
            HostChange::ScenarioChanged { environment } => {
                let scenario = self.plane.scenario_status(&environment)?;
                Some(StreamMessage::ScenarioChanged { environment, scenario })
            }
            HostChange::Session { environment, event } => Some(session_message(environment, event)),
            HostChange::Journaled { environment, entry } => Some(StreamMessage::JournalEntry { environment, entry }),
            HostChange::RecordingStarted { environment, recording } => {
                Some(StreamMessage::RecordingStarted { environment, recording })
            }
            HostChange::RecordingStopped {
                environment,
                recording,
                why,
            } => Some(StreamMessage::RecordingStopped {
                environment,
                recording,
                why,
            }),
        }
    }

    /// What an environment counted so far, or `None` once it stopped.
    fn stats(&self, environment: &EnvironmentName) -> Option<StreamMessage> {
        let host = self.plane.host();
        let statistics = host.statistics(environment).ok()?;
        let armed = host.armed_rules(environment).ok()?;
        // Every rule that a call met, and every rule that is armed, which has a budget to tell even before a call.
        let counted = statistics
            .rules()
            .filter_map(|(name, _)| name.as_str().parse::<RuleName>().ok());
        let names: Vec<RuleName> = counted.chain(armed.iter().map(|rule| rule.name().clone())).collect();
        let rules: BTreeMap<RuleName, RuleStats> = names
            .into_iter()
            .map(|name| {
                let counts = statistics.rule(&name);
                let left = armed
                    .get(&name)
                    .and_then(|rule| rule.arming().budget().map(Budget::left));
                let stats = RuleStats {
                    matches: counts.matches(),
                    firings: counts.firings(),
                    left,
                };
                (name, stats)
            })
            .collect();
        Some(StreamMessage::Stats {
            environment: environment.clone(),
            stats: Stats {
                calls: statistics.calls(),
                missed_events: statistics.missed_events(),
                apis: statistics.apis().collect(),
                rules,
            },
        })
    }
}

/// The message of the live stream that tells what happened to a session.
fn session_message(environment: EnvironmentName, event: SessionEvent) -> StreamMessage {
    match event {
        SessionEvent::Opened(session) => StreamMessage::SessionOpened {
            environment,
            session: session_status(&session),
        },
        SessionEvent::Identified {
            connection,
            client,
            software,
        } => StreamMessage::SessionIdentified {
            environment,
            connection,
            identity: SessionIdentity { client, software },
        },
        SessionEvent::SoftwareNamed { connection, software } => StreamMessage::SessionSoftwareNamed {
            environment,
            connection,
            software,
        },
        SessionEvent::Closed { connection, by } => StreamMessage::SessionClosed {
            environment,
            connection,
            by: closer(by),
        },
    }
}

/// Who closed a session, as the control plane says it.
const fn closer(by: ClosedBy) -> SessionCloser {
    match by {
        ClosedBy::Client => SessionCloser::Client,
        ClosedBy::Backend => SessionCloser::Backend,
        ClosedBy::Lie => SessionCloser::Lie,
        ClosedBy::Proxy => SessionCloser::Proxy,
    }
}
