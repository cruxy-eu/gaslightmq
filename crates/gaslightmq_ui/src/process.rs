//! The process as the live stream tells it: the snapshot, with every change applied to it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;

use gaslightmq_control_api::environment::{Environment, EnvironmentName};
use gaslightmq_control_api::field::{ClientSoftware, ConnectionNumber};
use gaslightmq_control_api::journal::RecordingId;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule, RuleName, RuleState};
use gaslightmq_control_api::scenario::Scenario;
use gaslightmq_control_api::selector::Selector;
use gaslightmq_control_api::surface::PadLayout;

use crate::history::Subject;

/// Every definition of the workspace, by kind, each as its file holds it.
///
/// A definition holds what a person writes. What the process keeps beside it is no part of it:
/// see [`Process::revision`] and [`Process::loses_comment`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkspaceDefinitions {
    /// Every selector.
    pub selectors: Vec<Selector>,
    /// Every preset.
    pub presets: Vec<Preset>,
    /// Every rule of the workspace. Live rules belong to an environment, and are not here.
    pub rules: Vec<Rule>,
    /// Every scenario.
    pub scenarios: Vec<Scenario>,
    /// Every pad layout.
    pub layouts: Vec<PadLayout>,
    /// Every environment, whether it runs or not.
    pub environments: Vec<Environment>,
}

/// What the process holds, as far as the live stream told it: every definition of the workspace,
/// every environment that runs with its backend, its rules and its sessions, and what each one counted.
///
/// It starts from a [snapshot](StreamMessage::Snapshot), and every change applies to it.
/// A change says what its subject is now, so applying it again changes nothing.
///
/// The snapshot names the [run](Self::run) of the process, and every revision means something in that run alone:
/// the [revision](Self::revision) of a definition, and the one of a rule in an environment, which its
/// [`RuleStatus`] holds.
#[derive(Debug, Clone, PartialEq)]
pub struct Process {
    run: RunId,
    definitions: WorkspaceDefinitions,
    /// The revision of every definition, beside it.
    revisions: BTreeMap<DefinitionRef, Revision>,
    /// The definitions whose write would lose a comment of their file.
    losing_comments: BTreeSet<DefinitionRef>,
    /// The workspace as a whole: when it writes its changes, whether it takes any, and what nobody saved yet.
    workspace: WorkspaceStatus,
    environments: Vec<RunningEnvironment>,
    /// The statistics of each environment, the newest last, as many as `stats_kept` says.
    stats: BTreeMap<EnvironmentName, VecDeque<Stats>>,
    stats_kept: NonZeroUsize,
    workspace_invalid: Option<String>,
}

impl Process {
    /// The process as a snapshot says it is. It keeps the last `stats_kept` statistics of each environment.
    pub fn new(state: ProcessState, stats_kept: NonZeroUsize) -> Self {
        let ProcessState {
            run,
            definitions,
            workspace,
            environments,
        } = state;
        let mut revisions = BTreeMap::new();
        let mut losing_comments = BTreeSet::new();
        let definitions = WorkspaceDefinitions {
            selectors: held(
                definitions.selectors,
                &mut revisions,
                &mut losing_comments,
                |selector| selector.name().clone().into(),
            ),
            presets: held(definitions.presets, &mut revisions, &mut losing_comments, |preset| {
                preset.name().clone().into()
            }),
            rules: held(definitions.rules, &mut revisions, &mut losing_comments, |rule| {
                rule.name().clone().into()
            }),
            scenarios: held(
                definitions.scenarios,
                &mut revisions,
                &mut losing_comments,
                |scenario| scenario.name().clone().into(),
            ),
            layouts: held(definitions.layouts, &mut revisions, &mut losing_comments, |layout| {
                layout.name().clone().into()
            }),
            environments: held(
                definitions.environments,
                &mut revisions,
                &mut losing_comments,
                |environment| environment.name().clone().into(),
            ),
        };
        Self {
            run,
            definitions,
            revisions,
            losing_comments,
            workspace,
            environments,
            stats: BTreeMap::new(),
            stats_kept,
            workspace_invalid: None,
        }
    }

    /// The run of the process, as its snapshot named it. A snapshot that names another run is another process.
    pub const fn run(&self) -> RunId {
        self.run
    }

    /// Every definition of the workspace.
    pub const fn definitions(&self) -> &WorkspaceDefinitions {
        &self.definitions
    }

    /// The revision of the definition of that kind and name, when the workspace has one.
    pub fn revision(&self, name: &DefinitionRef) -> Option<Revision> {
        self.revisions.get(name).copied()
    }

    /// Whether a write of the definition of that kind and name would lose a comment that someone wrote inside it, in
    /// its file. The UI asks before it saves an edit of such a definition.
    pub fn loses_comment(&self, name: &DefinitionRef) -> bool {
        self.losing_comments.contains(name)
    }

    /// The workspace as a whole: when it writes its changes, whether it is read-only, and what nobody saved yet.
    pub const fn workspace(&self) -> &WorkspaceStatus {
        &self.workspace
    }

    /// The revision that `subject` has, as far as the live stream told it, or `None` when the process holds no such
    /// thing: an environment that does not run, a rule that it does not hold, or a definition that is gone.
    pub fn revision_of(&self, subject: &Subject) -> Option<Revision> {
        match subject {
            Subject::Rule { environment, rule } => Some(rule_status(self.environment(environment)?, rule)?.revision),
            Subject::Definition(definition) => self.revision(definition),
        }
    }

    /// Every environment that runs, in the order the process told them.
    pub fn environments(&self) -> &[RunningEnvironment] {
        &self.environments
    }

    /// The environment of that name, when it runs.
    pub fn environment(&self, name: &EnvironmentName) -> Option<&RunningEnvironment> {
        self.environments.iter().find(|running| &running.environment == name)
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

    /// Applies one change of the live stream. A call, the answer to a subscription, a snapshot and the presence of
    /// the client are no change of the process, and change nothing here.
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
            StreamMessage::DefinitionChanged {
                definition,
                revision,
                loses_comment,
            } => {
                if *loses_comment {
                    self.losing_comments.insert(definition.name());
                } else {
                    self.losing_comments.remove(&definition.name());
                }
                self.definition_changed(definition, *revision);
            }
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
                let environments = &mut self.environments;
                match environments
                    .iter_mut()
                    .find(|running| running.environment == environment.environment)
                {
                    Some(running) => *running = environment.clone(),
                    None => environments.push(environment.clone()),
                }
            }
            StreamMessage::EnvironmentStopped { environment } => {
                self.environments.retain(|running| &running.environment != environment);
                self.stats.remove(environment);
            }
            StreamMessage::RecordingStarted { environment, recording } => {
                self.recording_started(environment, recording);
            }
            StreamMessage::RecordingStopped {
                environment, recording, ..
            } => self.recording_stopped(environment, recording),
            StreamMessage::WorkspaceChanged { workspace } => self.workspace = workspace.clone(),
            StreamMessage::WorkspaceInvalid { reason } => self.workspace_invalid = Some(reason.clone()),
            // No view shows the journal yet, so the state keeps no entry of it.
            StreamMessage::JournalEntry { .. }
            | StreamMessage::Present { .. }
            | StreamMessage::Snapshot(_)
            | StreamMessage::Call { .. }
            | StreamMessage::Following { .. }
            | StreamMessage::SubscriptionRefused { .. } => {}
        }
    }

    /// The environment writes `recording` from now on.
    fn recording_started(&mut self, environment: &EnvironmentName, recording: &RecordingId) {
        if let Some(running) = self.running(environment) {
            running.recording = Some(recording.clone());
        }
    }

    /// The environment stopped writing `recording`. A message that comes late leaves a later recording alone.
    fn recording_stopped(&mut self, environment: &EnvironmentName, recording: &RecordingId) {
        if let Some(running) = self.running(environment)
            && running.recording.as_ref() == Some(recording)
        {
            running.recording = None;
        }
    }

    fn running(&mut self, name: &EnvironmentName) -> Option<&mut RunningEnvironment> {
        self.environments
            .iter_mut()
            .find(|running| &running.environment == name)
    }

    fn session(&mut self, environment: &EnvironmentName, connection: ConnectionNumber) -> Option<&mut SessionStatus> {
        self.running(environment)?
            .sessions
            .iter_mut()
            .find(|session| session.connection == connection)
    }

    /// A definition of the workspace is what the message says, at the revision it says.
    /// A rule of the workspace is a rule of every environment too: it keeps its state where an environment holds it,
    /// and every other environment holds it disarmed. Its revision in an environment is the later of the change of
    /// its definition and the last change of its state there.
    fn definition_changed(&mut self, definition: &AnyDefinition, revision: Revision) {
        self.revisions.insert(definition.name(), revision);
        let definitions = &mut self.definitions;
        match definition {
            AnyDefinition::Selector(selector) => put(&mut definitions.selectors, selector.clone(), |held| {
                held.name() == selector.name()
            }),
            AnyDefinition::Preset(preset) => {
                put(&mut definitions.presets, preset.clone(), |held| {
                    held.name() == preset.name()
                });
            }
            AnyDefinition::Scenario(scenario) => {
                put(&mut definitions.scenarios, scenario.clone(), |held| {
                    held.name() == scenario.name()
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
                for running in &mut self.environments {
                    let held = running.rules.iter().find(|status| status.rule.name() == rule.name());
                    let state = held.map_or(RuleState::Disarmed, |status| status.state);
                    let revision = held.map_or(revision, |status| status.revision.max(revision));
                    put_rule(
                        &mut running.rules,
                        RuleStatus {
                            rule: rule.clone(),
                            live: false,
                            state,
                            revision,
                        },
                    );
                }
            }
        }
    }

    /// A definition of the workspace went, and a rule of the workspace leaves every environment with it.
    fn definition_deleted(&mut self, name: &DefinitionRef) {
        self.revisions.remove(name);
        self.losing_comments.remove(name);
        let definitions = &mut self.definitions;
        match name {
            DefinitionRef::Selector(name) => definitions.selectors.retain(|held| held.name() != name),
            DefinitionRef::Preset(name) => definitions.presets.retain(|held| held.name() != name),
            DefinitionRef::Scenario(name) => definitions.scenarios.retain(|held| held.name() != name),
            DefinitionRef::Layout(name) => definitions.layouts.retain(|held| held.name() != name),
            DefinitionRef::Environment(name) => definitions.environments.retain(|held| held.name() != name),
            DefinitionRef::Rule(name) => {
                definitions.rules.retain(|held| held.name() != name);
                for running in &mut self.environments {
                    running.rules.retain(|status| status.live || status.rule.name() != name);
                }
            }
        }
    }
}

/// The definitions of one kind as their files hold them. The revision of each goes to `revisions`, and the name of
/// each whose write would lose a comment to `losing_comments`, under the name that `named` gives the definition.
fn held<D>(
    definitions: Vec<DefinitionStatus<D>>,
    revisions: &mut BTreeMap<DefinitionRef, Revision>,
    losing_comments: &mut BTreeSet<DefinitionRef>,
    named: impl Fn(&D) -> DefinitionRef,
) -> Vec<D> {
    definitions
        .into_iter()
        .map(
            |DefinitionStatus {
                 definition,
                 revision,
                 loses_comment,
             }| {
                let name = named(&definition);
                if loses_comment {
                    losing_comments.insert(name.clone());
                }
                revisions.insert(name, revision);
                definition
            },
        )
        .collect()
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
