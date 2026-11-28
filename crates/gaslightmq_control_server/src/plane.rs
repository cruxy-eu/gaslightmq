//! The control plane over one host: the answer to every operation, and the state of the process.

use std::collections::BTreeMap;
use std::sync::Arc;

use gaslightmq_control_api::environment::{Environment, EnvironmentName, ListenAddress};
use gaslightmq_control_api::field::ConnectionNumber;
use gaslightmq_control_api::incident::Requirement;
use gaslightmq_control_api::journal::{ChangeSource, JournalEntry, JournalEntryId, RecordingId};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule, RuleName};
use gaslightmq_control_api::scenario::Scenario;
use gaslightmq_control_api::selector::Selector;
use gaslightmq_control_api::surface::{Intent, PadLayout, PresenceId};
use gaslightmq_definition_api::WriteBack as StoreWriteBack;
use gaslightmq_engine::{OpenSession, StoredRecording};
use gaslightmq_host::{
    ArmRuleError, CallSubscription, DisarmRuleError, EnvironmentConfiguration, EnvironmentConfigurationError,
    EnvironmentRule, EnvironmentRulesError, EnvironmentStartError, Host, ReadSentenceError, Unsaved,
};
use gaslightmq_quick_inject::{CompletionKind, Sentence};
use gaslightmq_workspace::{CreateError, DefinitionChange as Changed, ReloadError, SaveError, WorkspaceContext};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::StreamBacklog;
use crate::definitions::{self, DefinitionOperations, Editable, each, reference, rule_holder};
use crate::revisions::{expected, revised, revision, run};

/// What the server answers a request with, before it travels.
#[derive(Debug)]
pub(crate) enum Reply {
    /// The operation succeeded, and this is the form of its answer.
    Answered(String),
    /// The operation failed, and this is the form of its error.
    Failed(String),
    /// The request got neither: the server read no operation in it.
    Refused(RequestError),
}

impl Reply {
    /// The reply of an operation of the type `O`, which only takes the answer and the error of that operation.
    fn of<O: Operation>(result: Result<O::Answer, O::Error>) -> Self {
        match result {
            Ok(answer) => Reply::Answered(form(&answer)),
            Err(error) => Reply::Failed(form(&error)),
        }
    }
}

/// The JSON of a value of the control plane.
///
/// Every type of the control plane has a JSON form: every key of a map is text, and no number is out of range.
/// A value that had none would be a bug of `gaslightmq_control_api`, and the server answers `null` for it.
pub(crate) fn form(value: &impl Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

/// What reaches every client of the live stream.
#[derive(Debug, Clone)]
pub(crate) enum Change {
    /// A message of the live stream, which every client sends as it is.
    Message(Arc<StreamMessage>),
    /// The server missed changes of the host, so no client holds what the process holds:
    /// each one gets the whole state again.
    Resync,
}

/// The control plane of one host: it answers every operation and makes the state of the process.
/// The changes of the host reach the clients of the live stream through it.
pub(crate) struct ControlPlane<C: WorkspaceContext> {
    host: Arc<Host<C>>,
    changes: broadcast::Sender<Change>,
}

impl<C: WorkspaceContext + 'static> ControlPlane<C> {
    /// The control plane of `host`, which keeps `backlog` changes for a client that falls behind.
    pub(crate) fn new(host: Arc<Host<C>>, backlog: StreamBacklog) -> Self {
        let (changes, _) = broadcast::channel(backlog.messages());
        Self { host, changes }
    }

    /// The changes from now on, for one client of the live stream.
    pub(crate) fn changes(&self) -> broadcast::Receiver<Change> {
        self.changes.subscribe()
    }

    /// Hands a change to every client of the live stream. A change that no client listens to is gone.
    pub(crate) fn publish(&self, change: Change) {
        let _ = self.changes.send(change);
    }

    /// The host behind the control plane.
    pub(crate) fn host(&self) -> &Host<C> {
        &self.host
    }

    /// Answers one request.
    ///
    /// The request says where it comes from. Its surface and its cause go to the host with every change,
    /// which records them in the journal of the environment. The presence of its sender goes to the host with an
    /// intent, so a hold ends with the presence.
    #[expect(
        clippy::too_many_lines,
        reason = "one arm for each operation of the control plane, each a line or three"
    )]
    pub(crate) async fn answer(&self, request: OperationRequest) -> Reply {
        let OperationRequest { from, operation } = request;
        let presence = from.presence;
        let from = ChangeSource::from(from);
        match operation {
            AnyOperation::ListSelectors(_) => Reply::of::<ListDefinitions<Selector>>(self.list().await),
            AnyOperation::GetSelector(get) => Reply::of::<GetDefinition<Selector>>(self.get(get.name).await),
            AnyOperation::CreateSelector(create) => {
                Reply::of::<CreateDefinition<Selector>>(self.create(create.definition, from).await)
            }
            AnyOperation::UpdateSelector(update) => {
                Reply::of::<UpdateDefinition<Selector>>(self.update(update, from).await)
            }
            AnyOperation::DeleteSelector(delete) => {
                Reply::of::<DeleteDefinition<Selector>>(self.delete(delete, from).await)
            }
            AnyOperation::ListPresets(_) => Reply::of::<ListDefinitions<Preset>>(self.list().await),
            AnyOperation::GetPreset(get) => Reply::of::<GetDefinition<Preset>>(self.get(get.name).await),
            AnyOperation::CreatePreset(create) => {
                Reply::of::<CreateDefinition<Preset>>(self.create(create.definition, from).await)
            }
            AnyOperation::UpdatePreset(update) => {
                Reply::of::<UpdateDefinition<Preset>>(self.update(update, from).await)
            }
            AnyOperation::DeletePreset(delete) => {
                Reply::of::<DeleteDefinition<Preset>>(self.delete(delete, from).await)
            }
            AnyOperation::ListRules(_) => Reply::of::<ListDefinitions<Rule>>(self.list().await),
            AnyOperation::GetRule(get) => Reply::of::<GetDefinition<Rule>>(self.get(get.name).await),
            AnyOperation::CreateRule(create) => {
                Reply::of::<CreateDefinition<Rule>>(self.create(create.definition, from).await)
            }
            AnyOperation::UpdateRule(update) => Reply::of::<UpdateDefinition<Rule>>(self.update(update, from).await),
            AnyOperation::DeleteRule(delete) => Reply::of::<DeleteDefinition<Rule>>(self.delete(delete, from).await),
            AnyOperation::ListScenarios(_) => Reply::of::<ListDefinitions<Scenario>>(self.list().await),
            AnyOperation::GetScenario(get) => Reply::of::<GetDefinition<Scenario>>(self.get(get.name).await),
            AnyOperation::CreateScenario(create) => {
                Reply::of::<CreateDefinition<Scenario>>(self.create(create.definition, from).await)
            }
            AnyOperation::UpdateScenario(update) => {
                Reply::of::<UpdateDefinition<Scenario>>(self.update(update, from).await)
            }
            AnyOperation::DeleteScenario(delete) => {
                Reply::of::<DeleteDefinition<Scenario>>(self.delete(delete, from).await)
            }
            AnyOperation::ListLayouts(_) => Reply::of::<ListDefinitions<PadLayout>>(self.list().await),
            AnyOperation::GetLayout(get) => Reply::of::<GetDefinition<PadLayout>>(self.get(get.name).await),
            AnyOperation::CreateLayout(create) => {
                Reply::of::<CreateDefinition<PadLayout>>(self.create(create.definition, from).await)
            }
            AnyOperation::UpdateLayout(update) => {
                Reply::of::<UpdateDefinition<PadLayout>>(self.update(update, from).await)
            }
            AnyOperation::DeleteLayout(delete) => {
                Reply::of::<DeleteDefinition<PadLayout>>(self.delete(delete, from).await)
            }
            AnyOperation::ListEnvironments(_) => Reply::of::<ListEnvironments>(self.list_environments().await),
            AnyOperation::GetEnvironment(get) => {
                Reply::of::<GetEnvironment>(self.get_environment(get.environment).await)
            }
            AnyOperation::StartEnvironment(start) => Reply::of::<StartEnvironment>(self.start(start.environment).await),
            AnyOperation::StopEnvironment(stop) => Reply::of::<StopEnvironment>(self.stop(stop.environment).await),
            AnyOperation::CheckHealth(_) => Reply::of::<CheckHealth>(self.health()),
            AnyOperation::ListEnvironmentRules(list) => {
                Reply::of::<ListEnvironmentRules>(self.environment_rules(list.environment).await)
            }
            AnyOperation::CreateLiveRule(create) => {
                Reply::of::<CreateLiveRule>(self.create_live_rule(create, from).await)
            }
            AnyOperation::UpdateLiveRule(update) => {
                Reply::of::<UpdateLiveRule>(self.update_live_rule(update, from).await)
            }
            AnyOperation::DeleteLiveRule(delete) => Reply::of::<DeleteLiveRule>(self.delete_live_rule(delete, from)),
            AnyOperation::KeepRule(keep) => Reply::of::<KeepRule>(self.keep(keep, from).await),
            AnyOperation::ArmRule(arm) => Reply::of::<ArmRule>(self.arm(arm, from).await),
            AnyOperation::DisarmRule(disarm) => Reply::of::<DisarmRule>(self.disarm(disarm, from).await),
            AnyOperation::FireRule(fire) => Reply::of::<FireRule>(self.fire(fire.environment, fire.rule, from).await),
            AnyOperation::ActivateScenario(activate) => {
                Reply::of::<ActivateScenario>(self.activate_scenario(activate, from).await)
            }
            AnyOperation::DeactivateScenario(deactivate) => {
                Reply::of::<DeactivateScenario>(self.deactivate_scenario(deactivate, from))
            }
            AnyOperation::StepScenario(step) => Reply::of::<StepScenario>(self.step_scenario(step, from)),
            AnyOperation::SendIntent(send) => {
                Reply::of::<SendIntent>(self.intent(presence, send.environment, &send.intent, from).await)
            }
            AnyOperation::ListSessions(list) => Reply::of::<ListSessions>(self.sessions(list.environment)),
            AnyOperation::CloseSession(close) => {
                Reply::of::<CloseSession>(self.close_session(&close.environment, close.connection))
            }
            AnyOperation::GetBackend(get) => Reply::of::<GetBackend>(self.backend(&get.environment)),
            AnyOperation::ParseSentence(parse) => {
                Reply::of::<ParseSentence>(self.parse(parse.environment, &parse.text).await)
            }
            AnyOperation::QueryJournal(query) => {
                Reply::of::<QueryJournal>(self.journal(query.environment, query.after))
            }
            AnyOperation::ReloadWorkspace(reload) => {
                Reply::of::<ReloadWorkspace>(self.reload(reload.discard_unsaved).await)
            }
            AnyOperation::SaveWorkspace(_) => Reply::of::<SaveWorkspace>(self.save().await),
            AnyOperation::StartRecording(start) => {
                Reply::of::<StartRecording>(self.start_recording(start.environment, from).await)
            }
            AnyOperation::StopRecording(stop) => {
                Reply::of::<StopRecording>(self.stop_recording(stop.environment).await)
            }
            AnyOperation::ListRecordings(_) => Reply::of::<ListRecordings>(self.list_recordings().await),
            AnyOperation::OpenRecording(open) => Reply::of::<OpenRecording>(self.open_recording(&open.recording).await),
        }
    }

    // The definitions of the workspace

    async fn list<D: Editable>(&self) -> Result<Vec<DefinitionStatus<D>>, ListDefinitionsError> {
        DefinitionOperations::list(&*self.host.workspace().await).await
    }

    async fn get<D: Editable>(
        &self,
        name: <D as EditableDefinition>::Name,
    ) -> Result<DefinitionStatus<D>, GetDefinitionError> {
        DefinitionOperations::get(&*self.host.workspace().await, name).await
    }

    async fn create<D: Editable>(&self, definition: D, from: ChangeSource) -> Result<Revised, CreateDefinitionError> {
        DefinitionOperations::create(&self.host, definition, from).await
    }

    async fn update<D: Editable>(
        &self,
        update: UpdateDefinition<D>,
        from: ChangeSource,
    ) -> Result<Revised, UpdateDefinitionError> {
        DefinitionOperations::update(&self.host, update, from).await
    }

    async fn delete<D: Editable>(
        &self,
        delete: DeleteDefinition<D>,
        from: ChangeSource,
    ) -> Result<Revised, DeleteDefinitionError> {
        DefinitionOperations::delete(&self.host, delete, from).await
    }

    /// Reads the files of the workspace again, through the host, which records what changed and tells whoever
    /// follows it. Whoever asks, the changes come from the files.
    /// It lets go of what nobody saved when `discard_unsaved` says so, and refuses while there is any otherwise.
    async fn reload(&self, discard_unsaved: bool) -> Result<Reloaded, ReloadWorkspaceError> {
        use gaslightmq_host::ReloadWorkspaceError as Host;
        let unsaved = if discard_unsaved {
            Unsaved::Discard
        } else {
            Unsaved::Refuse
        };
        match self.host.reload(unsaved).await {
            Ok(reloaded) => Ok(Reloaded {
                revision: revision(reloaded.revision),
                changes: reloaded.changes.into_iter().map(definition_change).collect(),
            }),
            Err(Host::NameTaken { rule, environment }) => Err(ReloadWorkspaceError::NameTaken {
                rule,
                holder: RuleHolder::Environment(environment),
            }),
            Err(Host::Unresolved {
                environment,
                rule,
                reason,
            }) => Err(ReloadWorkspaceError::LiveRuleDoesNotFit {
                environment,
                rule,
                reason,
            }),
            Err(Host::Unsaved { definitions }) => Err(ReloadWorkspaceError::Unsaved {
                definitions: definitions.into_iter().map(reference).collect(),
            }),
            Err(Host::NotReloaded {
                error: ReloadError::Invalid { errors },
            }) => Err(ReloadWorkspaceError::DoesNotFit { reason: each(&errors) }),
            Err(Host::NotReloaded {
                error: ReloadError::StoreFailed { reason },
            }) => Err(ReloadWorkspaceError::StoreFailed { reason }),
        }
    }

    /// Writes every change that nobody saved yet, through the host, which tells whoever follows it.
    async fn save(&self) -> Result<Saved, SaveWorkspaceError> {
        match self.host.save().await {
            Ok(saved) => Ok(Saved {
                saved: saved.into_iter().map(reference).collect(),
            }),
            Err(SaveError::ChangedOutside { reason }) => Err(SaveWorkspaceError::ChangedOutside { reason }),
            Err(SaveError::StoreFailed { reason }) => Err(SaveWorkspaceError::StoreFailed { reason }),
            Err(SaveError::PartlySaved { saved, reason }) => Err(SaveWorkspaceError::PartlySaved {
                saved: saved.into_iter().map(reference).collect(),
                reason,
            }),
        }
    }

    /// The workspace as a whole: when it writes its changes, whether it takes any, and what nobody saved yet.
    pub(crate) async fn workspace_status(&self) -> WorkspaceStatus {
        let workspace = self.host.workspace().await;
        WorkspaceStatus {
            write_back: match workspace.write_back() {
                StoreWriteBack::EachChange => WriteBack::EachChange,
                StoreWriteBack::OnSave => WriteBack::OnSave,
            },
            read_only: workspace.read_only(),
            unsaved: workspace.unsaved().await.into_iter().map(reference).collect(),
        }
    }

    // The environments

    /// Every environment that the process started with, and whether it runs: healthy when each of them does.
    pub(crate) fn health(&self) -> Result<Health, Unhealthy> {
        let running = self.host.running();
        let health = Health {
            environments: self
                .host
                .started_with()
                .into_iter()
                .map(|environment| EnvironmentHealth {
                    running: running.contains(&environment),
                    environment,
                })
                .collect(),
        };
        if health.is_healthy() {
            Ok(health)
        } else {
            Err(Unhealthy { health })
        }
    }

    async fn list_environments(&self) -> Result<Vec<EnvironmentStatus>, ListDefinitionsError> {
        let environments = definitions::list_with_revisions::<Environment, C>(&*self.host.workspace().await).await;
        let environments = environments.map_err(|reason| ListDefinitionsError::StoreFailed { reason })?;
        let running = self.host.running();
        Ok(environments
            .into_iter()
            .map(
                |DefinitionStatus {
                     definition,
                     revision,
                     loses_comment,
                 }| EnvironmentStatus {
                    running: running.contains(definition.name()),
                    environment: definition,
                    revision,
                    loses_comment,
                },
            )
            .collect())
    }

    async fn get_environment(&self, name: EnvironmentName) -> Result<EnvironmentStatus, GetDefinitionError> {
        let found = {
            let workspace = self.host.workspace().await;
            match definitions::get::<Environment, C>(&*workspace, &name).await {
                Ok(Some(environment)) => definitions::with_revision(&*workspace, environment).await.map(Some),
                Ok(None) => Ok(None),
                Err(reason) => Err(reason),
            }
        };
        match found {
            Ok(Some(DefinitionStatus {
                definition,
                revision,
                loses_comment,
            })) => Ok(EnvironmentStatus {
                running: self.host.running().contains(&name),
                environment: definition,
                revision,
                loses_comment,
            }),
            Ok(None) => Err(GetDefinitionError::NotFound { name: name.into() }),
            Err(reason) => Err(GetDefinitionError::StoreFailed {
                name: name.into(),
                reason,
            }),
        }
    }

    /// Starts the environment of the workspace with that name, as it describes itself.
    async fn start(&self, name: EnvironmentName) -> Result<(), StartEnvironmentError> {
        let failed = |reason: String| StartEnvironmentError::Failed {
            environment: name.clone(),
            reason,
        };
        let environment = match self.environment(&name).await {
            Ok(Some(environment)) => environment,
            Ok(None) => return Err(StartEnvironmentError::NotFound { environment: name }),
            Err(reason) => return Err(failed(reason)),
        };
        let configuration = EnvironmentConfiguration::new(&environment).map_err(
            |EnvironmentConfigurationError::UnsupportedBroker { .. }| {
                failed("it runs a broker, and the host cannot run a broker yet".to_owned())
            },
        )?;
        match self.host.start(configuration).await {
            Ok(()) => Ok(()),
            Err(EnvironmentStartError::AlreadyRunning { .. }) => {
                Err(StartEnvironmentError::Running { environment: name })
            }
            Err(EnvironmentStartError::SharedListener { running, address, .. }) => Err(failed(format!(
                "{address} is taken by environment {:?}",
                running.as_str()
            ))),
            Err(EnvironmentStartError::BackendNotStarted { error, .. }) => Err(failed(error.to_string())),
            Err(EnvironmentStartError::ProxyNotStarted { error, .. }) => Err(failed(error.to_string())),
        }
    }

    async fn stop(&self, name: EnvironmentName) -> Result<(), EnvironmentNotRunning> {
        self.host
            .stop(&name)
            .await
            .map_err(|_| EnvironmentNotRunning { environment: name })
    }

    /// The environment of the workspace with that name.
    async fn environment(&self, name: &EnvironmentName) -> Result<Option<Environment>, String> {
        definitions::get::<Environment, C>(&*self.host.workspace().await, name).await
    }

    // The rules of an environment

    async fn environment_rules(&self, environment: EnvironmentName) -> Result<Vec<RuleStatus>, EnvironmentReadError> {
        match self.host.rules_of(&environment).await {
            Ok(rules) => Ok(rules.into_iter().map(status).collect()),
            Err(EnvironmentRulesError::NotRunning { environment }) => {
                Err(EnvironmentReadError::NotRunning { environment })
            }
            Err(EnvironmentRulesError::StoreFailed { environment, reason }) => {
                Err(EnvironmentReadError::StoreFailed { environment, reason })
            }
        }
    }

    /// Adds the live rule, armed when the operation says so: the host arms and adds it as one step.
    async fn create_live_rule(
        &self,
        create: CreateLiveRule,
        from: ChangeSource,
    ) -> Result<Revised, CreateLiveRuleError> {
        use gaslightmq_host::CreateLiveRuleError as Host;
        let CreateLiveRule {
            environment,
            rule,
            armed,
        } = create;
        let name = rule.name().clone();
        let created = if armed {
            self.host.create_live_rule_armed(&environment, rule, from).await
        } else {
            self.host.create_live_rule(&environment, rule, from).await
        };
        match created {
            Ok(created) => Ok(revised(created)),
            Err(Host::NotRunning { environment }) => Err(CreateLiveRuleError::NotRunning { environment }),
            Err(Host::NameTaken {
                environment,
                rule,
                holder,
            }) => Err(CreateLiveRuleError::NameTaken {
                environment,
                rule,
                holder: holder.map(rule_holder),
            }),
            Err(Host::StoreFailed { rule, reason }) => Err(CreateLiveRuleError::StoreFailed { rule, reason }),
            Err(Host::WorkspaceInvalid { rule, reason }) => Err(CreateLiveRuleError::DoesNotFit {
                rule,
                reason: format!("the workspace is not valid: {reason}"),
            }),
            Err(Host::Unresolved { error }) => Err(CreateLiveRuleError::DoesNotFit {
                rule: name,
                reason: error.to_string(),
            }),
        }
    }

    async fn update_live_rule(
        &self,
        update: UpdateLiveRule,
        from: ChangeSource,
    ) -> Result<Revised, UpdateLiveRuleError> {
        use gaslightmq_host::UpdateLiveRuleError as Host;
        let UpdateLiveRule {
            environment,
            rule,
            expects,
        } = update;
        let name = rule.name().clone();
        let updated = self.host.update_live_rule(&environment, rule, expected(expects), from);
        match updated.await {
            Ok(updated) => Ok(revised(updated)),
            Err(Host::OtherRun { run: of_the_host }) => Err(UpdateLiveRuleError::OtherRun { run: run(of_the_host) }),
            Err(Host::NotRunning { environment }) => Err(UpdateLiveRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(UpdateLiveRuleError::NotFound { environment, rule }),
            Err(Host::Changed {
                environment,
                rule,
                revision: now,
            }) => Err(UpdateLiveRuleError::Changed {
                environment,
                rule,
                revision: revision(now),
            }),
            Err(Host::StoreFailed { rule, reason }) => Err(UpdateLiveRuleError::StoreFailed { rule, reason }),
            Err(Host::WorkspaceInvalid { rule, reason }) => Err(UpdateLiveRuleError::DoesNotFit {
                rule,
                reason: format!("the workspace is not valid: {reason}"),
            }),
            Err(Host::Unresolved { error }) => Err(UpdateLiveRuleError::DoesNotFit {
                rule: name,
                reason: error.to_string(),
            }),
        }
    }

    fn delete_live_rule(&self, delete: DeleteLiveRule, from: ChangeSource) -> Result<Revised, DeleteLiveRuleError> {
        use gaslightmq_host::DeleteLiveRuleError as Host;
        let DeleteLiveRule {
            environment,
            rule,
            expects,
        } = delete;
        match self.host.delete_live_rule(&environment, &rule, expected(expects), from) {
            Ok(deleted) => Ok(revised(deleted)),
            Err(Host::OtherRun { run: of_the_host }) => Err(DeleteLiveRuleError::OtherRun { run: run(of_the_host) }),
            Err(Host::NotRunning { environment }) => Err(DeleteLiveRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(DeleteLiveRuleError::NotFound { environment, rule }),
            Err(Host::Changed {
                environment,
                rule,
                revision: now,
            }) => Err(DeleteLiveRuleError::Changed {
                environment,
                rule,
                revision: revision(now),
            }),
        }
    }

    async fn keep(&self, keep: KeepRule, from: ChangeSource) -> Result<Revised, KeepRuleError> {
        use gaslightmq_host::KeepRuleError as Host;
        let KeepRule {
            environment,
            rule,
            expects,
        } = keep;
        match self.host.keep(&environment, &rule, expected(expects), from).await {
            Ok(kept) => Ok(revised(kept)),
            Err(Host::OtherRun { run: of_the_host }) => Err(KeepRuleError::OtherRun { run: run(of_the_host) }),
            Err(Host::NotRunning { environment }) => Err(KeepRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(KeepRuleError::NotFound { environment, rule }),
            Err(Host::Changed {
                environment,
                rule,
                revision: now,
            }) => Err(KeepRuleError::Changed {
                environment,
                rule,
                revision: revision(now),
            }),
            Err(Host::NameTaken { rule, holder }) => Err(KeepRuleError::NameTaken {
                rule,
                holder: Some(rule_holder(holder)),
            }),
            Err(Host::NotKept { rule, error }) => Err(match error {
                CreateError::NameTaken { .. } => KeepRuleError::NameTaken { rule, holder: None },
                CreateError::DoesNotFit { errors, .. } => KeepRuleError::DoesNotFit {
                    rule,
                    reason: each(&errors),
                },
                CreateError::ChangedOutside { reason, .. } => KeepRuleError::ChangedOutside { rule, reason },
                CreateError::StoreFailed { reason, .. } => KeepRuleError::StoreFailed { rule, reason },
            }),
        }
    }

    /// The rule of the environment that a command names: a rule of the workspace, or a live rule.
    async fn commanded_rule(&self, environment: &EnvironmentName, name: &RuleName) -> Result<EnvironmentRule, NoRule> {
        match self.host.rule_of(environment, name).await {
            Ok(Some(rule)) => Ok(rule),
            Ok(None) => Err(NoRule::NotFound),
            Err(EnvironmentRulesError::NotRunning { .. }) => Err(NoRule::NotRunning),
            Err(EnvironmentRulesError::StoreFailed { reason, .. }) => Err(NoRule::StoreFailed { reason }),
        }
    }

    /// The run of the process, when a change expects a revision of another run.
    ///
    /// A client that read from an earlier run learns that first, whatever else is wrong with its change:
    /// nothing that it holds is certain any more.
    fn other_run(&self, expects: Option<Expects>) -> Option<RunId> {
        let of_the_process = run(self.host.run());
        expects
            .is_some_and(|expects| expects.run != of_the_process)
            .then_some(of_the_process)
    }

    // The scenario of an environment

    async fn activate_scenario(
        &self,
        activate: ActivateScenario,
        from: ChangeSource,
    ) -> Result<Revised, ActivateScenarioError> {
        use gaslightmq_host::ScenarioActivationError as Host;
        let ActivateScenario {
            environment,
            scenario,
            expects,
        } = activate;
        if let Some(run) = self.other_run(expects) {
            return Err(ActivateScenarioError::OtherRun { run });
        }
        let activated = self
            .host
            .activate_scenario(&environment, &scenario, expected(expects), from)
            .await;
        activated.map(revised).map_err(|error| match error {
            Host::OtherRun { run: of_the_host } => ActivateScenarioError::OtherRun { run: run(of_the_host) },
            Host::NotRunning { environment } => ActivateScenarioError::NotRunning { environment },
            Host::NotFound { scenario } => ActivateScenarioError::NotFound { scenario },
            Host::StoreFailed { scenario, reason } => ActivateScenarioError::StoreFailed { scenario, reason },
            Host::Unresolved { scenario, reason } => ActivateScenarioError::Unresolved { scenario, reason },
            Host::Changed {
                environment,
                revision: now,
            } => ActivateScenarioError::Changed {
                environment,
                revision: revision(now),
            },
            Host::AlreadyActive { environment, scenario } => {
                ActivateScenarioError::AlreadyActive { environment, scenario }
            }
            Host::Unsupported {
                environment,
                scenario,
                error,
            } => ActivateScenarioError::Unsupported {
                environment,
                scenario,
                incidents: error.incidents().iter().map(Requirement::kind).collect(),
            },
        })
    }

    fn deactivate_scenario(
        &self,
        deactivate: DeactivateScenario,
        from: ChangeSource,
    ) -> Result<Revised, DeactivateScenarioError> {
        use gaslightmq_host::ScenarioDeactivationError as Host;
        let DeactivateScenario { environment, expects } = deactivate;
        if let Some(run) = self.other_run(expects) {
            return Err(DeactivateScenarioError::OtherRun { run });
        }
        let deactivated = self.host.deactivate_scenario(&environment, expected(expects), from);
        deactivated.map(revised).map_err(|error| match error {
            Host::OtherRun { run: of_the_host } => DeactivateScenarioError::OtherRun { run: run(of_the_host) },
            Host::NotRunning { environment } => DeactivateScenarioError::NotRunning { environment },
            Host::Changed {
                environment,
                revision: now,
            } => DeactivateScenarioError::Changed {
                environment,
                revision: revision(now),
            },
            Host::NotActive { environment } => DeactivateScenarioError::NotActive { environment },
        })
    }

    fn step_scenario(&self, step: StepScenario, from: ChangeSource) -> Result<Revised, StepScenarioError> {
        use gaslightmq_host::ScenarioStepError as Host;
        let StepScenario { environment, expects } = step;
        if let Some(run) = self.other_run(expects) {
            return Err(StepScenarioError::OtherRun { run });
        }
        let stepped = self.host.step_scenario(&environment, expected(expects), from);
        stepped.map(revised).map_err(|error| match error {
            Host::OtherRun { run: of_the_host } => StepScenarioError::OtherRun { run: run(of_the_host) },
            Host::NotRunning { environment } => StepScenarioError::NotRunning { environment },
            Host::Changed {
                environment,
                revision: now,
            } => StepScenarioError::Changed {
                environment,
                revision: revision(now),
            },
            Host::NotActive { environment } => StepScenarioError::NotActive { environment },
            Host::NoStep { environment, scenario } => StepScenarioError::NoStep { environment, scenario },
        })
    }

    /// The scenario of an environment as the live stream tells it, or `None` once the environment stopped.
    pub(crate) fn scenario_status(&self, environment: &EnvironmentName) -> Option<ScenarioStatus> {
        let scenario = self.host.scenario_of(environment).ok()?;
        Some(ScenarioStatus {
            active: scenario.active,
            revision: revision(scenario.revision),
        })
    }

    async fn arm(&self, arm: ArmRule, from: ChangeSource) -> Result<Revised, RuleCommandError> {
        let ArmRule {
            environment,
            rule: name,
            expects,
        } = arm;
        if let Some(run) = self.other_run(expects) {
            return Err(RuleCommandError::OtherRun { run });
        }
        let found = self.commanded_rule(&environment, &name).await;
        let rule = found.map_err(|missing| missing.of_a_command(&environment, &name))?;
        let refused = |reason: String| RuleCommandError::Refused {
            environment: environment.clone(),
            rule: name.clone(),
            reason,
        };
        match self.host.arm(&environment, &rule.rule, expected(expects), from).await {
            Ok(armed) => Ok(revised(armed)),
            Err(ArmRuleError::OtherRun { run: of_the_host }) => {
                Err(RuleCommandError::OtherRun { run: run(of_the_host) })
            }
            Err(ArmRuleError::NotRunning { .. }) => Err(RuleCommandError::NotRunning {
                environment: environment.clone(),
            }),
            Err(ArmRuleError::Changed {
                environment,
                rule,
                revision: now,
            }) => Err(RuleCommandError::Changed {
                environment,
                rule,
                revision: revision(now),
            }),
            Err(ArmRuleError::StoreFailed { reason, .. }) => Err(RuleCommandError::StoreFailed { rule: name, reason }),
            Err(ArmRuleError::WorkspaceInvalid { reason, .. }) => {
                Err(refused(format!("the workspace is not valid: {reason}")))
            }
            Err(ArmRuleError::Unresolved { error }) => Err(refused(error.to_string())),
            Err(ArmRuleError::AlreadyArmed { .. }) => Err(refused("it is armed already".to_owned())),
        }
    }

    async fn disarm(&self, disarm: DisarmRule, from: ChangeSource) -> Result<Revised, RuleCommandError> {
        let DisarmRule {
            environment,
            rule: name,
            expects,
        } = disarm;
        if let Some(run) = self.other_run(expects) {
            return Err(RuleCommandError::OtherRun { run });
        }
        let found = self.commanded_rule(&environment, &name).await;
        found.map_err(|missing| missing.of_a_command(&environment, &name))?;
        match self.host.disarm(&environment, &name, expected(expects), from).await {
            Ok(disarmed) => Ok(revised(disarmed)),
            Err(DisarmRuleError::OtherRun { run: of_the_host }) => {
                Err(RuleCommandError::OtherRun { run: run(of_the_host) })
            }
            Err(DisarmRuleError::NotRunning { environment }) => Err(RuleCommandError::NotRunning { environment }),
            Err(DisarmRuleError::Changed {
                environment,
                rule,
                revision: now,
            }) => Err(RuleCommandError::Changed {
                environment,
                rule,
                revision: revision(now),
            }),
            Err(DisarmRuleError::NotArmed { environment, rule }) => Err(RuleCommandError::Refused {
                environment,
                rule,
                reason: "it is not armed".to_owned(),
            }),
        }
    }

    async fn fire(
        &self,
        environment: EnvironmentName,
        name: RuleName,
        from: ChangeSource,
    ) -> Result<(), FireRuleError> {
        use gaslightmq_host::FireRuleError as Host;
        let found = self.commanded_rule(&environment, &name).await;
        found.map_err(|missing| missing.of_a_firing(&environment, &name))?;
        let refused = |reason: String| FireRuleError::Refused {
            environment: environment.clone(),
            rule: name.clone(),
            reason,
        };
        match self.host.fire(&environment, &name, from).await {
            Ok(()) => Ok(()),
            Err(Host::NotRunning { .. }) => Err(FireRuleError::NotRunning {
                environment: environment.clone(),
            }),
            Err(Host::NotArmed { .. }) => Err(refused("it is not armed".to_owned())),
            Err(Host::NotFired { .. }) => Err(refused(
                "it fires no more: its lifetime has ended, or its budget has no firing free".to_owned(),
            )),
            Err(Host::NeedsCall { error }) => Err(refused(error.to_string())),
            Err(Host::IncidentRejected { error, .. }) => Err(refused(error.to_string())),
        }
    }

    // Pads

    /// Hands an intent to the host, as one of `presence` when the request names the presence of its sender.
    async fn intent(
        &self,
        presence: Option<PresenceId>,
        environment: EnvironmentName,
        intent: &Intent,
        from: ChangeSource,
    ) -> Result<IntentEffect, IntentError> {
        use gaslightmq_host::IntentError as Host;
        let acted = match presence {
            Some(presence) => {
                let acted = self.host.send_intent_from(presence, &environment, intent, from);
                acted.await
            }
            None => self.host.send_intent(&environment, intent, from).await,
        };
        match acted {
            Ok(effect) => Ok(IntentEffect {
                rule: effect.rule.map(status),
                found: effect.found.map(revision),
                made: effect.made,
                changed: effect.changed,
                hold: effect.hold,
            }),
            Err(Host::NotRunning { environment }) => Err(IntentError::NotRunning { environment }),
            Err(Host::UnknownLayout { layout }) => Err(IntentError::UnknownLayout { layout }),
            Err(Host::StoreFailed { reason }) => Err(IntentError::StoreFailed { reason }),
            Err(Host::Refused { reason }) => Err(IntentError::Refused { reason }),
        }
    }

    // Sessions

    fn sessions(&self, environment: EnvironmentName) -> Result<Vec<SessionStatus>, EnvironmentNotRunning> {
        match self.host.sessions(&environment) {
            Ok(sessions) => Ok(sessions.iter().map(session_status).collect()),
            Err(_) => Err(EnvironmentNotRunning { environment }),
        }
    }

    fn close_session(
        &self,
        environment: &EnvironmentName,
        connection: ConnectionNumber,
    ) -> Result<(), CloseSessionError> {
        use gaslightmq_host::CloseSessionError as Host;
        match self.host.close_session(environment, connection) {
            Ok(()) => Ok(()),
            Err(Host::NotRunning { environment }) => Err(CloseSessionError::NotRunning { environment }),
            Err(Host::NotOpen {
                environment,
                connection,
            }) => Err(CloseSessionError::NotFound {
                environment,
                connection,
            }),
        }
    }

    // The journal

    /// The entries that the journal of the environment keeps, or those after the entry that the client read last.
    fn journal(
        &self,
        environment: EnvironmentName,
        after: Option<JournalEntryId>,
    ) -> Result<Vec<JournalEntry>, EnvironmentNotRunning> {
        self.host
            .journal(&environment, after)
            .map_err(|_| EnvironmentNotRunning { environment })
    }

    // Recordings

    async fn start_recording(
        &self,
        environment: EnvironmentName,
        from: ChangeSource,
    ) -> Result<RecordingId, StartRecordingError> {
        use gaslightmq_host::RecordingStartError as Host;
        self.host
            .start_recording(&environment, from)
            .await
            .map_err(|error| match error {
                Host::NotRunning { environment } => StartRecordingError::NotRunning { environment },
                Host::AlreadyRecording { environment, recording } => {
                    StartRecordingError::AlreadyRecording { environment, recording }
                }
                Host::StorageFailed { environment, reason } => {
                    StartRecordingError::StorageFailed { environment, reason }
                }
            })
    }

    async fn stop_recording(&self, environment: EnvironmentName) -> Result<RecordingEnd, StopRecordingError> {
        use gaslightmq_host::RecordingStopError as Host;
        match self.host.stop_recording(&environment).await {
            Ok(end) => Ok(recording_end(end)),
            Err(Host::NotRunning { environment }) => Err(StopRecordingError::NotRunning { environment }),
            Err(Host::NotRecording { environment }) => Err(StopRecordingError::NotRecording { environment }),
            Err(Host::CutShort { recording, reason }) => Err(StopRecordingError::CutShort { recording, reason }),
        }
    }

    async fn list_recordings(&self) -> Result<Vec<RecordingId>, ListRecordingsError> {
        use gaslightmq_engine::ListRecordingsError as Engine;
        self.host.recordings().await.map_err(|error| match error {
            Engine::StorageUnavailable { reason } => ListRecordingsError::StorageUnavailable { reason },
        })
    }

    async fn open_recording(&self, recording: &RecordingId) -> Result<RecordingStatus, OpenRecordingError> {
        use gaslightmq_engine::OpenRecordingError as Engine;
        let stored = self.host.open_recording(recording).await.map_err(|error| match error {
            Engine::NotFound { recording } => OpenRecordingError::NotFound { recording },
            Engine::Unreadable { recording, reason } => OpenRecordingError::Unreadable { recording, reason },
            Engine::StorageUnavailable { recording, reason } => {
                OpenRecordingError::StorageUnavailable { recording, reason }
            }
        })?;
        Ok(recording_status(stored))
    }

    // What runs in an environment

    fn backend(&self, environment: &EnvironmentName) -> Result<BackendStatus, EnvironmentNotRunning> {
        let not_running = |_| EnvironmentNotRunning {
            environment: environment.clone(),
        };
        let endpoints = self.host.endpoints(environment).map_err(not_running)?;
        let incidents = self.host.incident_support(environment).map_err(not_running)?;
        let mut addresses = BTreeMap::new();
        for endpoint in endpoints {
            let address = self.host.address_of(environment, endpoint).map_err(not_running)?;
            addresses.insert(endpoint, address.map(ListenAddress::from));
        }
        Ok(BackendStatus {
            kind: BackendKind::Upstream,
            endpoints: addresses,
            incidents,
        })
    }

    async fn parse(&self, environment: EnvironmentName, text: &str) -> Result<ParsedSentence, EnvironmentReadError> {
        match self.host.read_sentence(&environment, text).await {
            Ok(sentence) => Ok(parsed(&sentence)),
            Err(ReadSentenceError::NotRunning { environment }) => Err(EnvironmentReadError::NotRunning { environment }),
            Err(ReadSentenceError::StoreFailed { reason }) => {
                Err(EnvironmentReadError::StoreFailed { environment, reason })
            }
        }
    }

    // The state of the process

    /// An environment that runs, with what is live in it, or `None` when it does not run.
    pub(crate) async fn running_environment(
        &self,
        name: &EnvironmentName,
    ) -> Result<Option<RunningEnvironment>, String> {
        let rules = match self.host.rules_of(name).await {
            Ok(rules) => rules,
            Err(EnvironmentRulesError::NotRunning { .. }) => return Ok(None),
            Err(EnvironmentRulesError::StoreFailed { reason, .. }) => return Err(reason),
        };
        let (Ok(backend), Ok(sessions), Ok(recording), Some(scenario)) = (
            self.backend(name),
            self.host.sessions(name),
            self.host.recording(name),
            self.scenario_status(name),
        ) else {
            return Ok(None);
        };
        Ok(Some(RunningEnvironment {
            environment: name.clone(),
            backend,
            rules: rules.into_iter().map(status).collect(),
            sessions: sessions.iter().map(session_status).collect(),
            scenario,
            recording,
        }))
    }

    /// The whole state of the process: its run, every definition, and every environment that runs.
    ///
    /// # Errors
    ///
    /// What a store of the workspace said when it could not read.
    pub(crate) async fn state(&self) -> Result<ProcessState, String> {
        let definitions = definitions::every(&*self.host.workspace().await).await?;
        let mut environments = Vec::new();
        for name in self.host.running() {
            // An environment that stopped since the host named it is not part of the state.
            if let Some(environment) = self.running_environment(&name).await? {
                environments.push(environment);
            }
        }
        let workspace = self.workspace_status().await;
        Ok(ProcessState {
            run: run(self.host.run()),
            definitions,
            workspace,
            environments,
        })
    }

    /// The calls that a subscription follows, or `None` for a subscription to nothing.
    ///
    /// # Errors
    ///
    /// Why the subscription cannot be followed: the environment does not run, the selectors could not be read,
    /// or the selector does not resolve.
    pub(crate) async fn follow(
        &self,
        subscription: Subscription,
    ) -> Result<Option<(EnvironmentName, CallSubscription)>, String> {
        let Subscription::Calls { environment, selector } = subscription else {
            return Ok(None);
        };
        match self.host.follow_calls(&environment, &selector).await {
            Ok(calls) => Ok(Some((environment, calls))),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// A rule of an environment as the control plane hands it out.
pub(crate) fn status(rule: EnvironmentRule) -> RuleStatus {
    RuleStatus {
        rule: rule.rule,
        live: rule.live,
        state: rule.state,
        revision: revision(rule.revision),
    }
}

/// What a reload changed in one definition, as the control plane writes it.
fn definition_change(change: Changed) -> DefinitionChange {
    match change {
        Changed::Created(name) => DefinitionChange::Created(reference(name)),
        Changed::Updated(name) => DefinitionChange::Updated(reference(name)),
        Changed::Deleted(name) => DefinitionChange::Deleted(reference(name)),
    }
}

/// Why a command on a rule of an environment found no rule to act on.
enum NoRule {
    /// The environment does not run.
    NotRunning,
    /// Neither the workspace nor the environment has a rule of that name.
    NotFound,
    /// The store of the rules could not be read. `reason` says what it said.
    StoreFailed { reason: String },
}

impl NoRule {
    /// The error of an arm or a disarm that found no rule.
    fn of_a_command(self, environment: &EnvironmentName, rule: &RuleName) -> RuleCommandError {
        let (environment, rule) = (environment.clone(), rule.clone());
        match self {
            NoRule::NotRunning => RuleCommandError::NotRunning { environment },
            NoRule::NotFound => RuleCommandError::NotFound { environment, rule },
            NoRule::StoreFailed { reason } => RuleCommandError::StoreFailed { rule, reason },
        }
    }

    /// The error of a firing that found no rule.
    fn of_a_firing(self, environment: &EnvironmentName, rule: &RuleName) -> FireRuleError {
        let (environment, rule) = (environment.clone(), rule.clone());
        match self {
            NoRule::NotRunning => FireRuleError::NotRunning { environment },
            NoRule::NotFound => FireRuleError::NotFound { environment, rule },
            NoRule::StoreFailed { reason } => FireRuleError::StoreFailed { rule, reason },
        }
    }
}

/// An open session as the control plane hands it out.
pub(crate) fn session_status(session: &OpenSession) -> SessionStatus {
    SessionStatus {
        connection: session.connection(),
        address: session.address(),
        endpoint: session.endpoint(),
        opened: session.opened(),
        identity: session.is_identified().then(|| SessionIdentity {
            client: session.client().cloned(),
            software: session.software().cloned(),
        }),
    }
}

/// The sentence as the control plane hands it out.
fn parsed(sentence: &Sentence) -> ParsedSentence {
    ParsedSentence {
        rule: sentence.rule().cloned(),
        completions: sentence
            .completions()
            .iter()
            .map(|completion| WordCompletion {
                kind: word_kind(completion.kind()),
                word: completion.word().to_owned(),
                title: completion.title().map(ToOwned::to_owned),
                sentence: completion.sentence().to_owned(),
            })
            .collect(),
        unread: sentence
            .unread()
            .iter()
            .map(|unread| UnreadWord {
                word: unread.word().to_owned(),
                start: unread.span().start(),
                reason: unread.to_string(),
            })
            .collect(),
    }
}

/// What a completion is, as the control plane says it.
const fn word_kind(kind: CompletionKind) -> WordKind {
    match kind {
        CompletionKind::Lie => WordKind::Lie,
        CompletionKind::Preset => WordKind::Preset,
        CompletionKind::Api => WordKind::Api,
        CompletionKind::Selector => WordKind::Selector,
        CompletionKind::Topic => WordKind::Topic,
        CompletionKind::Group => WordKind::Group,
        CompletionKind::Client => WordKind::Client,
        CompletionKind::ErrorCode => WordKind::ErrorCode,
    }
}

/// The end of a recording, as the control plane says it.
const fn recording_end(end: gaslightmq_recording::End) -> RecordingEnd {
    use gaslightmq_recording::Ending;
    let ending = match end.ending {
        Ending::Stopped => RecordingEnding::Stopped,
        Ending::EnvironmentStopped => RecordingEnding::EnvironmentStopped,
        Ending::FellBehind => RecordingEnding::FellBehind,
    };
    RecordingEnd {
        ending,
        at: end.at,
        entries: end.entries,
        calls: end.calls,
    }
}

/// A recording as its storage holds it, as the control plane says it.
fn recording_status(stored: StoredRecording) -> RecordingStatus {
    use gaslightmq_recording::RecordedBackend;
    let StoredRecording { description, extent } = stored;
    let recording = description.id();
    let backend = match description.backend {
        RecordedBackend::Upstream => BackendKind::Upstream,
        RecordedBackend::Simulated => BackendKind::Simulated,
    };
    let scenario = description.scenario.map(|active| RecordedScenario {
        scenario: active.scenario,
        step: active.step,
    });
    RecordingStatus {
        description: RecordingDescription {
            recording,
            clock: description.clock,
            mode: description.mode,
            backend,
            written_by: description.written_by,
            armed: description.armed,
            scenario,
            journal_after: description.journal_after,
        },
        entries: extent.entries,
        end: extent.end.map(recording_end),
    }
}
