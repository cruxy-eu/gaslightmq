//! The control plane over one host: the answer to every operation, and the state of the process.

use std::collections::BTreeMap;
use std::sync::Arc;

use gaslightmq_control_api::environment::{Environment, EnvironmentName, ListenAddress};
use gaslightmq_control_api::field::ConnectionNumber;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule, RuleName};
use gaslightmq_control_api::selector::Selector;
use gaslightmq_control_api::surface::{Intent, PadLayout};
use gaslightmq_engine::OpenSession;
use gaslightmq_host::{
    ArmRuleError, CallSubscription, EnvironmentConfiguration, EnvironmentConfigurationError, EnvironmentRule,
    EnvironmentRulesError, EnvironmentStartError, FireRuleError, Host, ReadSentenceError,
};
use gaslightmq_quick_inject::{CompletionKind, Sentence};
use gaslightmq_workspace::{CreateError, WorkspaceContext};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::StreamBacklog;
use crate::definitions::{self, DefinitionOperations, Editable, each};

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
    /// The request says where it comes from, and nothing records that yet:
    /// the journal of an environment does, once the engine has one.
    pub(crate) async fn answer(&self, request: OperationRequest) -> Reply {
        let OperationRequest { from: _, operation } = request;
        match operation {
            AnyOperation::ListSelectors(_) => Reply::of::<ListDefinitions<Selector>>(self.list().await),
            AnyOperation::GetSelector(get) => Reply::of::<GetDefinition<Selector>>(self.get(get.name).await),
            AnyOperation::CreateSelector(create) => {
                Reply::of::<CreateDefinition<Selector>>(self.create(create.definition).await)
            }
            AnyOperation::UpdateSelector(update) => {
                Reply::of::<UpdateDefinition<Selector>>(self.update(update.definition).await)
            }
            AnyOperation::DeleteSelector(delete) => {
                Reply::of::<DeleteDefinition<Selector>>(self.delete::<Selector>(delete.name).await)
            }
            AnyOperation::ListPresets(_) => Reply::of::<ListDefinitions<Preset>>(self.list().await),
            AnyOperation::GetPreset(get) => Reply::of::<GetDefinition<Preset>>(self.get(get.name).await),
            AnyOperation::CreatePreset(create) => {
                Reply::of::<CreateDefinition<Preset>>(self.create(create.definition).await)
            }
            AnyOperation::UpdatePreset(update) => {
                Reply::of::<UpdateDefinition<Preset>>(self.update(update.definition).await)
            }
            AnyOperation::DeletePreset(delete) => {
                Reply::of::<DeleteDefinition<Preset>>(self.delete::<Preset>(delete.name).await)
            }
            AnyOperation::ListRules(_) => Reply::of::<ListDefinitions<Rule>>(self.list().await),
            AnyOperation::GetRule(get) => Reply::of::<GetDefinition<Rule>>(self.get(get.name).await),
            AnyOperation::CreateRule(create) => {
                Reply::of::<CreateDefinition<Rule>>(self.create(create.definition).await)
            }
            AnyOperation::UpdateRule(update) => {
                Reply::of::<UpdateDefinition<Rule>>(self.update(update.definition).await)
            }
            AnyOperation::DeleteRule(delete) => {
                Reply::of::<DeleteDefinition<Rule>>(self.delete::<Rule>(delete.name).await)
            }
            AnyOperation::ListLayouts(_) => Reply::of::<ListDefinitions<PadLayout>>(self.list().await),
            AnyOperation::GetLayout(get) => Reply::of::<GetDefinition<PadLayout>>(self.get(get.name).await),
            AnyOperation::CreateLayout(create) => {
                Reply::of::<CreateDefinition<PadLayout>>(self.create(create.definition).await)
            }
            AnyOperation::UpdateLayout(update) => {
                Reply::of::<UpdateDefinition<PadLayout>>(self.update(update.definition).await)
            }
            AnyOperation::DeleteLayout(delete) => {
                Reply::of::<DeleteDefinition<PadLayout>>(self.delete::<PadLayout>(delete.name).await)
            }
            AnyOperation::ListEnvironments(_) => Reply::of::<ListEnvironments>(self.list_environments().await),
            AnyOperation::GetEnvironment(get) => {
                Reply::of::<GetEnvironment>(self.get_environment(get.environment).await)
            }
            AnyOperation::StartEnvironment(start) => Reply::of::<StartEnvironment>(self.start(start.environment).await),
            AnyOperation::StopEnvironment(stop) => Reply::of::<StopEnvironment>(self.stop(stop.environment).await),
            AnyOperation::ListEnvironmentRules(list) => {
                Reply::of::<ListEnvironmentRules>(self.environment_rules(list.environment).await)
            }
            AnyOperation::CreateLiveRule(create) => Reply::of::<CreateLiveRule>(self.create_live_rule(create).await),
            AnyOperation::UpdateLiveRule(update) => {
                Reply::of::<UpdateLiveRule>(self.update_live_rule(update.environment, update.rule).await)
            }
            AnyOperation::DeleteLiveRule(delete) => {
                Reply::of::<DeleteLiveRule>(self.delete_live_rule(&delete.environment, &delete.rule))
            }
            AnyOperation::KeepRule(keep) => Reply::of::<KeepRule>(self.keep(keep.environment, keep.rule).await),
            AnyOperation::ArmRule(arm) => Reply::of::<ArmRule>(self.arm(arm.environment, arm.rule).await),
            AnyOperation::DisarmRule(disarm) => {
                Reply::of::<DisarmRule>(self.disarm(disarm.environment, disarm.rule).await)
            }
            AnyOperation::FireRule(fire) => Reply::of::<FireRule>(self.fire(fire.environment, fire.rule).await),
            AnyOperation::SendIntent(send) => {
                Reply::of::<SendIntent>(self.intent(send.environment, &send.intent).await)
            }
            AnyOperation::ListSessions(list) => Reply::of::<ListSessions>(self.sessions(list.environment)),
            AnyOperation::CloseSession(close) => {
                Reply::of::<CloseSession>(self.close_session(&close.environment, close.connection))
            }
            AnyOperation::GetBackend(get) => Reply::of::<GetBackend>(self.backend(&get.environment)),
            AnyOperation::ParseSentence(parse) => {
                Reply::of::<ParseSentence>(self.parse(parse.environment, &parse.text).await)
            }
        }
    }

    // The definitions of the workspace

    async fn list<D: Editable>(&self) -> Result<Vec<D>, ListDefinitionsError> {
        DefinitionOperations::list(&*self.host.workspace().await).await
    }

    async fn get<D: Editable>(&self, name: <D as EditableDefinition>::Name) -> Result<D, GetDefinitionError> {
        DefinitionOperations::get(&*self.host.workspace().await, name).await
    }

    async fn create<D: Editable>(&self, definition: D) -> Result<(), CreateDefinitionError> {
        DefinitionOperations::create(&self.host, definition).await
    }

    async fn update<D: Editable>(&self, definition: D) -> Result<(), UpdateDefinitionError> {
        DefinitionOperations::update(&self.host, definition).await
    }

    async fn delete<D: Editable>(&self, name: <D as EditableDefinition>::Name) -> Result<(), DeleteDefinitionError> {
        DefinitionOperations::delete::<D, C>(&self.host, name).await
    }

    // The environments

    async fn list_environments(&self) -> Result<Vec<EnvironmentStatus>, ListDefinitionsError> {
        let environments = definitions::list::<Environment, C>(&*self.host.workspace().await).await;
        let environments = environments.map_err(|reason| ListDefinitionsError::StoreFailed { reason })?;
        let running = self.host.running();
        Ok(environments
            .into_iter()
            .map(|environment| EnvironmentStatus {
                running: running.contains(environment.name()),
                environment,
            })
            .collect())
    }

    async fn get_environment(&self, name: EnvironmentName) -> Result<EnvironmentStatus, GetDefinitionError> {
        match self.environment(&name).await {
            Ok(Some(environment)) => Ok(EnvironmentStatus {
                running: self.host.running().contains(&name),
                environment,
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
    async fn create_live_rule(&self, create: CreateLiveRule) -> Result<(), CreateLiveRuleError> {
        use gaslightmq_host::CreateLiveRuleError as Host;
        let CreateLiveRule {
            environment,
            rule,
            armed,
        } = create;
        let name = rule.name().clone();
        let created = if armed {
            self.host.create_live_rule_armed(&environment, rule).await.map(|_| ())
        } else {
            self.host.create_live_rule(&environment, rule).await.map(|_| ())
        };
        match created {
            Ok(()) => Ok(()),
            Err(Host::NotRunning { environment }) => Err(CreateLiveRuleError::NotRunning { environment }),
            Err(Host::NameTaken { environment, rule }) => Err(CreateLiveRuleError::NameTaken { environment, rule }),
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

    async fn update_live_rule(&self, environment: EnvironmentName, rule: Rule) -> Result<(), UpdateLiveRuleError> {
        use gaslightmq_host::UpdateLiveRuleError as Host;
        let name = rule.name().clone();
        match self.host.update_live_rule(&environment, rule).await {
            Ok(_) => Ok(()),
            Err(Host::NotRunning { environment }) => Err(UpdateLiveRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(UpdateLiveRuleError::NotFound { environment, rule }),
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

    fn delete_live_rule(&self, environment: &EnvironmentName, rule: &RuleName) -> Result<(), DeleteLiveRuleError> {
        use gaslightmq_host::DeleteLiveRuleError as Host;
        match self.host.delete_live_rule(environment, rule) {
            Ok(()) => Ok(()),
            Err(Host::NotRunning { environment }) => Err(DeleteLiveRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(DeleteLiveRuleError::NotFound { environment, rule }),
        }
    }

    async fn keep(&self, environment: EnvironmentName, name: RuleName) -> Result<(), KeepRuleError> {
        use gaslightmq_host::KeepRuleError as Host;
        match self.host.keep(&environment, &name).await {
            Ok(_) => Ok(()),
            Err(Host::NotRunning { environment }) => Err(KeepRuleError::NotRunning { environment }),
            Err(Host::NotFound { environment, rule }) => Err(KeepRuleError::NotFound { environment, rule }),
            Err(Host::NameTaken { rule, environment }) => Err(KeepRuleError::NameTaken {
                rule,
                environment: Some(environment),
            }),
            Err(Host::NotKept { rule, error }) => Err(match error {
                CreateError::NameTaken { .. } => KeepRuleError::NameTaken {
                    rule,
                    environment: None,
                },
                CreateError::DoesNotFit { errors, .. } => KeepRuleError::DoesNotFit {
                    rule,
                    reason: each(&errors),
                },
                CreateError::StoreFailed { reason, .. } => KeepRuleError::StoreFailed { rule, reason },
            }),
        }
    }

    /// The rule of the environment that a command names: a rule of the workspace, or a live rule.
    async fn commanded_rule(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
    ) -> Result<EnvironmentRule, RuleCommandError> {
        match self.host.rule_of(environment, name).await {
            Ok(Some(rule)) => Ok(rule),
            Ok(None) => Err(RuleCommandError::NotFound {
                environment: environment.clone(),
                rule: name.clone(),
            }),
            Err(EnvironmentRulesError::NotRunning { environment }) => Err(RuleCommandError::NotRunning { environment }),
            Err(EnvironmentRulesError::StoreFailed { reason, .. }) => Err(RuleCommandError::StoreFailed {
                rule: name.clone(),
                reason,
            }),
        }
    }

    async fn arm(&self, environment: EnvironmentName, name: RuleName) -> Result<(), RuleCommandError> {
        let rule = self.commanded_rule(&environment, &name).await?;
        let refused = |reason: String| RuleCommandError::Refused {
            environment: environment.clone(),
            rule: name.clone(),
            reason,
        };
        match self.host.arm(&environment, &rule.rule).await {
            Ok(_) => Ok(()),
            Err(ArmRuleError::NotRunning { .. }) => Err(RuleCommandError::NotRunning {
                environment: environment.clone(),
            }),
            Err(ArmRuleError::StoreFailed { reason, .. }) => Err(RuleCommandError::StoreFailed { rule: name, reason }),
            Err(ArmRuleError::WorkspaceInvalid { reason, .. }) => {
                Err(refused(format!("the workspace is not valid: {reason}")))
            }
            Err(ArmRuleError::Unresolved { error }) => Err(refused(error.to_string())),
            Err(ArmRuleError::AlreadyArmed { .. }) => Err(refused("it is armed already".to_owned())),
        }
    }

    async fn disarm(&self, environment: EnvironmentName, name: RuleName) -> Result<(), RuleCommandError> {
        self.commanded_rule(&environment, &name).await?;
        let armed = self
            .host
            .rules(&environment)
            .map_err(|_| RuleCommandError::NotRunning {
                environment: environment.clone(),
            })?;
        armed.disarm(&name).map_err(|_| RuleCommandError::Refused {
            environment,
            rule: name,
            reason: "it is not armed".to_owned(),
        })?;
        Ok(())
    }

    async fn fire(&self, environment: EnvironmentName, name: RuleName) -> Result<(), RuleCommandError> {
        self.commanded_rule(&environment, &name).await?;
        let refused = |reason: String| RuleCommandError::Refused {
            environment: environment.clone(),
            rule: name.clone(),
            reason,
        };
        match self.host.fire(&environment, &name).await {
            Ok(()) => Ok(()),
            Err(FireRuleError::NotRunning { .. }) => Err(RuleCommandError::NotRunning {
                environment: environment.clone(),
            }),
            Err(FireRuleError::NotArmed { .. }) => Err(refused("it is not armed".to_owned())),
            Err(FireRuleError::NotFired { .. }) => Err(refused(
                "it fires no more: its lifetime has ended, or its budget has no firing free".to_owned(),
            )),
            Err(FireRuleError::NeedsCall { error }) => Err(refused(error.to_string())),
            Err(FireRuleError::IncidentRejected { error, .. }) => Err(refused(error.to_string())),
        }
    }

    // Pads

    async fn intent(&self, environment: EnvironmentName, intent: &Intent) -> Result<IntentEffect, IntentError> {
        use gaslightmq_host::IntentError as Host;
        match self.host.send_intent(&environment, intent).await {
            Ok(effect) => Ok(IntentEffect {
                rule: effect.rule.map(status),
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
        let (Ok(backend), Ok(sessions)) = (self.backend(name), self.host.sessions(name)) else {
            return Ok(None);
        };
        Ok(Some(RunningEnvironment {
            environment: name.clone(),
            backend,
            rules: rules.into_iter().map(status).collect(),
            sessions: sessions.iter().map(session_status).collect(),
        }))
    }

    /// The whole state of the process: every definition, and every environment that runs.
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
        Ok(ProcessState {
            definitions,
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
