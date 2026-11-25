//! The control plane over one host: the answer to every operation, the state of the process, and the changes.

use std::sync::Arc;

use gaslightmq_control_api::environment::{Environment, EnvironmentName, ListenAddress};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Preset, Rule, RuleName, RuleState};
use gaslightmq_control_api::selector::{EffectiveSelector, Selector, SelectorSet};
use gaslightmq_control_api::surface::PadLayout;
use gaslightmq_host::{
    ArmRuleError, EnvironmentConfiguration, EnvironmentConfigurationError, EnvironmentStartError, FireRuleError, Host,
    ReadSentenceError,
};
use gaslightmq_quick_inject::{CompletionKind, Sentence};
use gaslightmq_workspace::WorkspaceContext;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::StreamBacklog;
use crate::definitions::{self, DefinitionOperations, Editable};

/// What the server answers a request with, before it travels.
#[derive(Debug)]
pub(crate) enum Reply {
    /// The operation succeeded, and this is the form of its answer.
    Answered(String),
    /// The operation failed, and this is the form of its error.
    Failed(String),
    /// The request got neither: the server does not serve the operation, or read none.
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

/// The control plane of one host: it answers every operation and makes the state of the process, and every change it
/// makes reaches the clients of the live stream.
pub(crate) struct ControlPlane<C: WorkspaceContext> {
    host: Arc<Host<C>>,
    changes: broadcast::Sender<StreamMessage>,
}

impl<C: WorkspaceContext + 'static> ControlPlane<C> {
    /// The control plane of `host`, which keeps `backlog` changes for a client that falls behind.
    pub(crate) fn new(host: Arc<Host<C>>, backlog: StreamBacklog) -> Self {
        let (changes, _) = broadcast::channel(backlog.messages());
        Self { host, changes }
    }

    /// The changes from now on, for one client of the live stream.
    pub(crate) fn changes(&self) -> broadcast::Receiver<StreamMessage> {
        self.changes.subscribe()
    }

    /// Hands a change to every client of the live stream. A change that no client listens to is gone.
    fn publish(&self, change: StreamMessage) {
        let _ = self.changes.send(change);
    }

    /// The host behind the control plane.
    pub(crate) fn host(&self) -> &Host<C> {
        &self.host
    }

    /// Answers one operation.
    pub(crate) async fn answer(&self, operation: AnyOperation) -> Reply {
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
            AnyOperation::ArmRule(arm) => Reply::of::<ArmRule>(self.arm(arm.environment, arm.rule).await),
            AnyOperation::DisarmRule(disarm) => {
                Reply::of::<DisarmRule>(self.disarm(disarm.environment, disarm.rule).await)
            }
            AnyOperation::FireRule(fire) => Reply::of::<FireRule>(self.fire(fire.environment, fire.rule).await),
            AnyOperation::GetBackend(get) => Reply::of::<GetBackend>(self.backend(&get.environment)),
            AnyOperation::ParseSentence(parse) => {
                Reply::of::<ParseSentence>(self.parse(parse.environment, &parse.text).await)
            }
            // The host has no live rules, no sessions and no pads yet.
            unserved @ (AnyOperation::CreateLiveRule(_)
            | AnyOperation::UpdateLiveRule(_)
            | AnyOperation::DeleteLiveRule(_)
            | AnyOperation::KeepRule(_)
            | AnyOperation::SendIntent(_)
            | AnyOperation::ListSessions(_)
            | AnyOperation::CloseSession(_)) => Reply::Refused(RequestError::NotServed {
                operation: unserved.name().to_owned(),
            }),
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
        let created = DefinitionOperations::create(&mut *self.host.workspace().await, definition).await?;
        self.publish(StreamMessage::DefinitionChanged {
            definition: EditableDefinition::into_any(created),
        });
        Ok(())
    }

    async fn update<D: Editable>(&self, definition: D) -> Result<(), UpdateDefinitionError> {
        let updated = DefinitionOperations::update(&mut *self.host.workspace().await, definition).await?;
        self.publish(StreamMessage::DefinitionChanged {
            definition: EditableDefinition::into_any(updated),
        });
        Ok(())
    }

    async fn delete<D: Editable>(&self, name: <D as EditableDefinition>::Name) -> Result<(), DeleteDefinitionError> {
        DefinitionOperations::delete::<D, C>(&mut *self.host.workspace().await, name.clone()).await?;
        self.publish(StreamMessage::DefinitionDeleted {
            name: Into::<DefinitionRef>::into(name),
        });
        Ok(())
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
            Ok(()) => {}
            Err(EnvironmentStartError::AlreadyRunning { .. }) => {
                return Err(StartEnvironmentError::Running { environment: name });
            }
            Err(EnvironmentStartError::SharedListener { running, address, .. }) => {
                let reason = format!("{address} is taken by environment {:?}", running.as_str());
                return Err(failed(reason));
            }
            Err(EnvironmentStartError::BackendNotStarted { error, .. }) => return Err(failed(error.to_string())),
            Err(EnvironmentStartError::ProxyNotStarted { error, .. }) => return Err(failed(error.to_string())),
        }
        // A client that cannot be told what runs now gets it with its next snapshot.
        if let Ok(Some(environment)) = self.running_environment(&name).await {
            self.publish(StreamMessage::EnvironmentStarted { environment });
        }
        Ok(())
    }

    async fn stop(&self, name: EnvironmentName) -> Result<(), EnvironmentNotRunning> {
        match self.host.stop(&name).await {
            Ok(()) => {
                self.publish(StreamMessage::EnvironmentStopped {
                    environment: name.clone(),
                });
                Ok(())
            }
            Err(_) => Err(EnvironmentNotRunning { environment: name }),
        }
    }

    /// The environment of the workspace with that name, without its UUID.
    async fn environment(&self, name: &EnvironmentName) -> Result<Option<Environment>, String> {
        definitions::get::<Environment, C>(&*self.host.workspace().await, name).await
    }

    // The rules of an environment

    async fn environment_rules(&self, environment: EnvironmentName) -> Result<Vec<RuleStatus>, EnvironmentReadError> {
        match self.rule_statuses(&environment).await {
            Ok(Some(rules)) => Ok(rules),
            Ok(None) => Err(EnvironmentReadError::NotRunning { environment }),
            Err(reason) => Err(EnvironmentReadError::StoreFailed { environment, reason }),
        }
    }

    /// The rules of the workspace, each with its state in the environment, or `None` when it does not run.
    ///
    /// A rule of the workspace is armed where someone armed it, and disarmed everywhere else.
    async fn rule_statuses(&self, environment: &EnvironmentName) -> Result<Option<Vec<RuleStatus>>, String> {
        let rules = definitions::list::<Rule, C>(&*self.host.workspace().await).await?;
        let Ok(armed) = self.host.rules(environment) else {
            return Ok(None);
        };
        let armed = armed.snapshot();
        let statuses = rules.into_iter().map(|rule| {
            let state = if armed.get(rule.name()).is_some() {
                RuleState::Armed
            } else {
                RuleState::Disarmed
            };
            RuleStatus {
                rule,
                live: false,
                state,
            }
        });
        Ok(Some(statuses.collect()))
    }

    /// The rule of the workspace that a command on an environment names.
    async fn commanded_rule(&self, environment: &EnvironmentName, name: &RuleName) -> Result<Rule, RuleCommandError> {
        if !self.host.running().contains(environment) {
            return Err(RuleCommandError::NotRunning {
                environment: environment.clone(),
            });
        }
        match definitions::get::<Rule, C>(&*self.host.workspace().await, name).await {
            Ok(Some(rule)) => Ok(rule),
            Ok(None) => Err(RuleCommandError::NotFound {
                environment: environment.clone(),
                rule: name.clone(),
            }),
            Err(reason) => Err(RuleCommandError::StoreFailed {
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
        match self.host.arm(&environment, &rule).await {
            Ok(_) => {}
            Err(ArmRuleError::NotRunning { .. }) => {
                return Err(RuleCommandError::NotRunning {
                    environment: environment.clone(),
                });
            }
            Err(ArmRuleError::StoreFailed { reason, .. }) => {
                return Err(RuleCommandError::StoreFailed { rule: name, reason });
            }
            Err(ArmRuleError::WorkspaceInvalid { reason, .. }) => {
                return Err(refused(format!("the workspace is not valid: {reason}")));
            }
            Err(ArmRuleError::Unresolved { error }) => return Err(refused(error.to_string())),
            Err(ArmRuleError::AlreadyArmed { .. }) => return Err(refused("it is armed already".to_owned())),
        }
        self.publish(StreamMessage::RuleChanged {
            environment,
            rule: RuleStatus {
                rule,
                live: false,
                state: RuleState::Armed,
            },
        });
        Ok(())
    }

    async fn disarm(&self, environment: EnvironmentName, name: RuleName) -> Result<(), RuleCommandError> {
        let rule = self.commanded_rule(&environment, &name).await?;
        let armed = self
            .host
            .rules(&environment)
            .map_err(|_| RuleCommandError::NotRunning {
                environment: environment.clone(),
            })?;
        if armed.disarm(&name).is_err() {
            return Err(RuleCommandError::Refused {
                environment,
                rule: name,
                reason: "it is not armed".to_owned(),
            });
        }
        self.publish(StreamMessage::RuleChanged {
            environment,
            rule: RuleStatus {
                rule,
                live: false,
                state: RuleState::Disarmed,
            },
        });
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

    // What runs in an environment

    fn backend(&self, environment: &EnvironmentName) -> Result<BackendStatus, EnvironmentNotRunning> {
        let not_running = |_| EnvironmentNotRunning {
            environment: environment.clone(),
        };
        let endpoints = self.host.endpoints(environment).map_err(not_running)?;
        let incidents = self.host.incident_support(environment).map_err(not_running)?;
        let mut addresses = std::collections::BTreeMap::new();
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
        if !self.host.running().contains(&environment) {
            return Err(EnvironmentReadError::NotRunning { environment });
        }
        match self.host.read_sentence(text).await {
            Ok(sentence) => Ok(parsed(&sentence)),
            Err(ReadSentenceError::StoreFailed { reason }) => {
                Err(EnvironmentReadError::StoreFailed { environment, reason })
            }
        }
    }

    // The state of the process

    /// An environment that runs, with what is live in it, or `None` when it does not run.
    async fn running_environment(&self, name: &EnvironmentName) -> Result<Option<RunningEnvironment>, String> {
        let Some(rules) = self.rule_statuses(name).await? else {
            return Ok(None);
        };
        let Ok(backend) = self.backend(name) else {
            return Ok(None);
        };
        Ok(Some(RunningEnvironment {
            environment: name.clone(),
            backend,
            rules,
            sessions: Vec::new(),
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

    /// The calls that a subscription follows: the environment, and the selector that its calls have to match.
    ///
    /// # Errors
    ///
    /// Why the subscription cannot be followed: the environment does not run, or the selector does not resolve.
    pub(crate) async fn follow(
        &self,
        subscription: Subscription,
    ) -> Result<Option<(EnvironmentName, EffectiveSelector)>, String> {
        let Subscription::Calls { environment, selector } = subscription else {
            return Ok(None);
        };
        if !self.host.running().contains(&environment) {
            return Err(EnvironmentNotRunning { environment }.to_string());
        }
        let selectors = definitions::list::<Selector, C>(&*self.host.workspace().await).await?;
        let selectors = SelectorSet::new(selectors).map_err(|error| error.to_string())?;
        let selector = selectors.resolve(&selector).map_err(|error| error.to_string())?;
        Ok(Some((environment, selector)))
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
