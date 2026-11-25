//! The process as the live stream tells it: the snapshot, with every change applied to it.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::field::{ClientSoftware, ConnectionNumber};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName, RuleState};

/// What the process holds, as far as the live stream told it: every definition of the workspace,
/// every environment that runs with its backend, its rules and its sessions, and what each one counted.
///
/// It starts from a [snapshot](StreamMessage::Snapshot), and every change applies to it.
/// A change says what its subject is now, so applying it again changes nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Process {
    state: ProcessState,
    /// The statistics of each environment, the newest last, as many as `stats_kept` says.
    stats: BTreeMap<EnvironmentName, VecDeque<Stats>>,
    stats_kept: NonZeroUsize,
    workspace_invalid: Option<String>,
}

impl Process {
    /// The process as a snapshot says it is. It keeps the last `stats_kept` statistics of each environment.
    pub fn new(state: ProcessState, stats_kept: NonZeroUsize) -> Self {
        Self {
            state,
            stats: BTreeMap::new(),
            stats_kept,
            workspace_invalid: None,
        }
    }

    /// Every definition of the workspace.
    pub fn definitions(&self) -> &Definitions {
        &self.state.definitions
    }

    /// Every environment that runs, in the order the process told them.
    pub fn environments(&self) -> &[RunningEnvironment] {
        &self.state.environments
    }

    /// The environment of that name, when it runs.
    pub fn environment(&self, name: &EnvironmentName) -> Option<&RunningEnvironment> {
        self.state
            .environments
            .iter()
            .find(|running| &running.environment == name)
    }

    /// Whether the environment of that name runs.
    pub fn runs(&self, name: &EnvironmentName) -> bool {
        self.environment(name).is_some()
    }

    /// What the environment counted, once its first statistics came.
    pub fn stats(&self, name: &EnvironmentName) -> Option<&Stats> {
        self.stats.get(name)?.back()
    }

    /// The last statistics of the environment, the oldest first: one for each interval of the control plane.
    pub fn stats_history(&self, name: &EnvironmentName) -> impl Iterator<Item = &Stats> {
        self.stats.get(name).into_iter().flatten()
    }

    /// Why the definitions of the workspace do not fit each other, when the process said so last.
    pub fn workspace_invalid(&self) -> Option<&str> {
        self.workspace_invalid.as_deref()
    }

    /// Applies one change of the live stream. A call, the answer to a subscription and a snapshot are no change of
    /// the process, and change nothing here.
    pub fn apply(&mut self, message: &StreamMessage) {
        match message {
            StreamMessage::Stats { environment, stats } => {
                let history = self.stats.entry(environment.clone()).or_default();
                history.push_back(stats.clone());
                while history.len() > self.stats_kept.get() {
                    history.pop_front();
                }
            }
            StreamMessage::RuleChanged { environment, rule } => {
                if let Some(running) = self.running(environment) {
                    put_rule(&mut running.rules, rule.clone());
                }
            }
            StreamMessage::RuleRemoved { environment, rule } => {
                if let Some(running) = self.running(environment) {
                    running.rules.retain(|status| status.rule.name() != rule);
                }
            }
            StreamMessage::DefinitionChanged { definition } => self.definition_changed(definition),
            StreamMessage::DefinitionDeleted { name } => self.definition_deleted(name),
            StreamMessage::SessionOpened { environment, session } => {
                if let Some(running) = self.running(environment) {
                    running.sessions.retain(|open| open.connection != session.connection);
                    running.sessions.push(session.clone());
                }
            }
            StreamMessage::SessionIdentified {
                environment,
                connection,
                identity,
            } => {
                if let Some(session) = self.session(environment, *connection) {
                    session.identity = Some(identity.clone());
                }
            }
            StreamMessage::SessionSoftwareNamed {
                environment,
                connection,
                software,
            } => {
                if let Some(session) = self.session(environment, *connection) {
                    named(session, software.clone());
                }
            }
            StreamMessage::SessionClosed {
                environment,
                connection,
                ..
            } => {
                if let Some(running) = self.running(environment) {
                    running.sessions.retain(|open| open.connection != *connection);
                }
            }
            StreamMessage::EnvironmentStarted { environment } => {
                let environments = &mut self.state.environments;
                match environments
                    .iter_mut()
                    .find(|running| running.environment == environment.environment)
                {
                    Some(running) => *running = environment.clone(),
                    None => environments.push(environment.clone()),
                }
            }
            StreamMessage::EnvironmentStopped { environment } => {
                self.state
                    .environments
                    .retain(|running| &running.environment != environment);
                self.stats.remove(environment);
            }
            StreamMessage::WorkspaceInvalid { reason } => self.workspace_invalid = Some(reason.clone()),
            StreamMessage::Snapshot(_)
            | StreamMessage::Call { .. }
            | StreamMessage::Following { .. }
            | StreamMessage::SubscriptionRefused { .. } => {}
        }
    }

    fn running(&mut self, name: &EnvironmentName) -> Option<&mut RunningEnvironment> {
        self.state
            .environments
            .iter_mut()
            .find(|running| &running.environment == name)
    }

    fn session(&mut self, environment: &EnvironmentName, connection: ConnectionNumber) -> Option<&mut SessionStatus> {
        self.running(environment)?
            .sessions
            .iter_mut()
            .find(|session| session.connection == connection)
    }

    /// A definition of the workspace is what the message says. A rule of the workspace is a rule of every environment
    /// too: it keeps its state where an environment holds it, and every other environment holds it disarmed.
    fn definition_changed(&mut self, definition: &AnyDefinition) {
        let definitions = &mut self.state.definitions;
        match definition {
            AnyDefinition::Selector(selector) => put(&mut definitions.selectors, selector.clone(), |held| {
                held.name() == selector.name()
            }),
            AnyDefinition::Preset(preset) => {
                put(&mut definitions.presets, preset.clone(), |held| {
                    held.name() == preset.name()
                });
            }
            AnyDefinition::Layout(layout) => {
                put(&mut definitions.layouts, layout.clone(), |held| {
                    held.name() == layout.name()
                });
            }
            AnyDefinition::Environment(environment) => {
                put(&mut definitions.environments, environment.clone(), |held| {
                    held.name() == environment.name()
                });
            }
            AnyDefinition::Rule(rule) => {
                put(&mut definitions.rules, rule.clone(), |held| held.name() == rule.name());
                for running in &mut self.state.environments {
                    let state = running
                        .rules
                        .iter()
                        .find(|status| status.rule.name() == rule.name())
                        .map_or(RuleState::Disarmed, |status| status.state);
                    put_rule(
                        &mut running.rules,
                        RuleStatus {
                            rule: rule.clone(),
                            live: false,
                            state,
                        },
                    );
                }
            }
        }
    }

    /// A definition of the workspace went, and a rule of the workspace leaves every environment with it.
    fn definition_deleted(&mut self, name: &DefinitionRef) {
        let definitions = &mut self.state.definitions;
        match name {
            DefinitionRef::Selector(name) => definitions.selectors.retain(|held| held.name() != name),
            DefinitionRef::Preset(name) => definitions.presets.retain(|held| held.name() != name),
            DefinitionRef::Layout(name) => definitions.layouts.retain(|held| held.name() != name),
            DefinitionRef::Environment(name) => definitions.environments.retain(|held| held.name() != name),
            DefinitionRef::Rule(name) => {
                definitions.rules.retain(|held| held.name() != name);
                for running in &mut self.state.environments {
                    running.rules.retain(|status| status.live || status.rule.name() != name);
                }
            }
        }
    }
}

/// The rule of an environment with that name, or `None`.
pub fn rule_status<'a>(environment: &'a RunningEnvironment, name: &RuleName) -> Option<&'a RuleStatus> {
    environment.rules.iter().find(|status| status.rule.name() == name)
}

/// Replaces the item that `same` finds with `item`, or adds it at the end.
fn put<T>(items: &mut Vec<T>, item: T, same: impl Fn(&T) -> bool) {
    match items.iter_mut().find(|held| same(held)) {
        Some(held) => *held = item,
        None => items.push(item),
    }
}

/// Replaces the rule of the same name with `status`, or adds it at the end.
fn put_rule(rules: &mut Vec<RuleStatus>, status: RuleStatus) {
    let name: &RuleName = status.rule.name();
    let name = name.clone();
    put(rules, status, |held: &RuleStatus| held.rule.name() == &name);
}

/// The session names its client software, and keeps the client ID that identified it.
fn named(session: &mut SessionStatus, software: ClientSoftware) {
    let identity = session.identity.get_or_insert_with(SessionIdentity::default);
    identity.software = Some(software);
}

/// A rule's title where it has one, and its name where it has none.
pub fn rule_label(rule: &Rule) -> String {
    rule.title()
        .map_or_else(|| rule.name().to_string(), ToString::to_string)
}
