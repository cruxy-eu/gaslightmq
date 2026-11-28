//! One environment that runs, in the words of whoever works with it: its rules, its scenario, its recordings, the
//! calls it follows and its journal.

use gaslightmq_control_api::call::Call;
use gaslightmq_control_api::environment::{EnvironmentName, RecordingMode};
use gaslightmq_control_api::journal::{JournalEntry, JournalEntryId, RecordingId};
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::{Rule, RuleName};
use gaslightmq_control_api::scenario::{Scenario, ScenarioName};
use gaslightmq_definition_text::DefinitionText;

use crate::client::within;
use crate::written::{Given, Written};
use crate::{
    ActivateError, ArmError, ControlClient, CurrentRecordingError, DefineError, EnvironmentStateError, FollowError,
    LiveStream, OperationError, QueryError, RuleToArm, ScenarioToActivate, SelectorToMatch, StreamError,
    WaitForCallsError,
};

/// The name of a scenario that a caller hands over without one, before it is numbered: `scenario`, `scenario-2`.
const UNNAMED_SCENARIO: &str = "scenario";

impl ControlClient {
    /// The environment of this name, which runs, in the words of whoever works with it.
    ///
    /// A test library hands its test the environment of its instance this way.
    pub fn environment(&self, environment: EnvironmentName) -> ControlledEnvironment {
        ControlledEnvironment {
            client: self.clone(),
            environment,
            recording: None,
        }
    }

    /// The definitions of the workspace, to create some.
    pub fn definitions(&self) -> Definitions {
        Definitions { client: self.clone() }
    }
}

/// One environment that runs, as a client works with it: its rules, its scenario, its recordings, the calls it follows,
/// and its journal.
///
/// Every request names the environment, and comes from the [surface](crate::ControlClientConfiguration::surface) of
/// the client. Each call expects nothing of what it changes, so the last change wins, as in a test that has the
/// process to itself.
///
/// ```no_run
/// use gaslightmq_control_client::{ControlClient, rule, selector};
///
/// # async fn orders(client: ControlClient) -> anyhow::Result<()> {
/// let orders = client.environment("orders-sim".parse()?);
/// let mut produces = orders.follow(selector! { api: Produce }).await?;
/// let refused = orders
///     .rules()
///     .arm(rule! { selector: { api: Produce }, action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER }, trigger: { first: 1 } })
///     .await?;
/// // … the client under test produces, and tries again …
/// let calls = produces.wait_for(2).await?;
/// assert!(calls[0].explanation().applied(&refused));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ControlledEnvironment {
    client: ControlClient,
    environment: EnvironmentName,
    /// How much of each call a recording that the caller starts keeps, or `None` for the mode of the environment.
    recording: Option<RecordingMode>,
}

impl ControlledEnvironment {
    /// The name of the environment.
    pub fn name(&self) -> &EnvironmentName {
        &self.environment
    }

    /// The client of the control plane that the environment is reached through.
    pub fn client(&self) -> &ControlClient {
        &self.client
    }

    /// The same environment, whose recordings that the caller [starts](Recordings::start) keep as much of each call
    /// as `mode` says, whatever the environment says.
    #[must_use]
    pub fn recording_in(mut self, mode: RecordingMode) -> Self {
        self.recording = Some(mode);
        self
    }

    /// The rules of the environment: to arm one.
    pub fn rules(&self) -> Rules {
        Rules {
            environment: self.clone(),
        }
    }

    /// The rule of this name in the environment: a rule of the workspace, or a live rule of the environment.
    pub fn rule(&self, name: RuleName) -> EnvironmentRule {
        EnvironmentRule {
            environment: self.clone(),
            name,
        }
    }

    /// The scenarios of the environment: to activate one.
    pub fn scenarios(&self) -> Scenarios {
        Scenarios {
            environment: self.clone(),
        }
    }

    /// The recordings of the environment: to start one, or to take the one that runs.
    pub fn recordings(&self) -> Recordings {
        Recordings {
            environment: self.clone(),
        }
    }

    /// Follows the calls of the environment that `selector` matches, from now on.
    ///
    /// The call returns once the control plane follows them, so every call that arrives after it is one that the
    /// stream hands over. A call reaches the recording of the environment before it reaches a follower, so a call
    /// that the stream handed over is in the recording.
    ///
    /// # Errors
    ///
    /// - [`FollowError::Unreadable`] when the selector does not read.
    /// - [`FollowError::Unreachable`] when the live stream did not open, or ended before the control plane followed.
    /// - [`FollowError::Refused`] when the control plane cannot follow the calls: the environment does not run, or the
    ///   selector does not exist.
    pub async fn follow(&self, selector: impl Into<SelectorToMatch>) -> Result<FollowedCalls, FollowError> {
        let selector = selector
            .into()
            .read()
            .map_err(|error| FollowError::Unreadable { error })?;
        let unreachable = |error| FollowError::Unreachable { error };
        let mut stream = self.client.live_stream().await.map_err(unreachable)?;
        stream
            .follow(Subscription::Calls {
                environment: self.environment.clone(),
                selector,
            })
            .await;
        loop {
            match stream.next().await.map_err(unreachable)? {
                StreamMessage::Following { .. } => break,
                StreamMessage::SubscriptionRefused { reason } => return Err(FollowError::Refused { reason }),
                _ => {}
            }
        }
        Ok(FollowedCalls {
            stream,
            environment: self.environment.clone(),
            timeout: self.client.configuration().follow_timeout,
        })
    }

    /// Every entry that the journal of the environment keeps.
    ///
    /// # Errors
    ///
    /// The [`EnvironmentNotRunning`] that the control plane refused with, or why no answer came.
    pub async fn journal(&self) -> Result<Vec<JournalEntry>, OperationError<EnvironmentNotRunning>> {
        self.client
            .send(QueryJournal {
                environment: self.environment.clone(),
                after: None,
            })
            .await
    }

    /// The entries of the journal of the environment after `entry`, which the caller read last.
    ///
    /// # Errors
    ///
    /// As for [`journal`](Self::journal).
    pub async fn journal_after(
        &self,
        entry: JournalEntryId,
    ) -> Result<Vec<JournalEntry>, OperationError<EnvironmentNotRunning>> {
        self.client
            .send(QueryJournal {
                environment: self.environment.clone(),
                after: Some(entry),
            })
            .await
    }

    /// Where the scenario of the environment stands: the scenario that is active and the step that runs, and its
    /// revision.
    ///
    /// # Errors
    ///
    /// [`EnvironmentStateError`] when the live stream gave no snapshot, or the environment does not run.
    pub async fn scenario_status(&self) -> Result<ScenarioStatus, EnvironmentStateError> {
        self.state().await.map(|state| state.scenario)
    }

    /// The environment as the snapshot of a new live stream holds it.
    async fn state(&self) -> Result<RunningEnvironment, EnvironmentStateError> {
        let unreachable = |error| EnvironmentStateError::Unreachable { error };
        let mut stream = self.client.live_stream().await.map_err(unreachable)?;
        let state = loop {
            if let StreamMessage::Snapshot(state) = stream.next().await.map_err(unreachable)? {
                break state;
            }
        };
        stream.close().await;
        state
            .environments
            .into_iter()
            .find(|running| running.environment == self.environment)
            .ok_or_else(|| EnvironmentStateError::NotRunning {
                environment: self.environment.clone(),
            })
    }
}

/// The rules of an [environment](ControlledEnvironment): to arm one.
#[derive(Debug, Clone)]
pub struct Rules {
    environment: ControlledEnvironment,
}

impl Rules {
    /// Arms `rule` in the environment: a rule of the workspace by its name, or a new live rule of the environment,
    /// armed as it is created.
    ///
    /// A new rule that nobody named has the name of what it says. When a rule of the environment has that name, the
    /// rule takes it with `-2`, `-3` and so on, until it has one of its own.
    /// The rule that comes back says which name it took, so an explanation says whether it applied to a call.
    ///
    /// # Errors
    ///
    /// - [`ArmError::Unreadable`] when the rule does not read.
    /// - [`ArmError::NotCreated`] when the environment did not create the live rule: it does not run, a rule that
    ///   someone named has the name, or the rule does not fit.
    /// - [`ArmError::NotArmed`] when the environment did not arm the rule of that name.
    pub async fn arm(&self, rule: impl Into<RuleToArm>) -> Result<EnvironmentRule, ArmError> {
        let written = match rule.into().given() {
            Given::Named(name) => {
                let rule = self.environment.rule(name.clone());
                rule.arm().await.map_err(|error| ArmError::NotArmed { error })?;
                return Ok(rule);
            }
            Given::Written(written) => written.clone(),
        };
        let rule = written.read().map_err(|error| ArmError::Unreadable { error })?;
        let numbered = rule.is_named_by_its_phrase();
        let named = rule.name().clone();
        let mut number = 1;
        loop {
            let candidate = match number {
                1 => rule.clone(),
                _ => rule.clone().named(named.numbered(number)),
            };
            let name = candidate.name().clone();
            let created = self
                .environment
                .client
                .send(CreateLiveRule {
                    environment: self.environment.environment.clone(),
                    rule: candidate,
                    armed: true,
                })
                .await;
            match created {
                Ok(_) => return Ok(self.environment.rule(name)),
                Err(OperationError::Refused {
                    error: CreateLiveRuleError::NameTaken { .. },
                }) if numbered => number += 1,
                Err(error) => return Err(ArmError::NotCreated { error }),
            }
        }
    }
}

/// A rule in an [environment](ControlledEnvironment), by its name: one that a caller armed, or any other.
///
/// The name is what an [explanation](gaslightmq_control_api::call::Explanation::applied) names the rule by, so a test
/// asks the explanation of a call whether the rule applied with the rule itself.
#[derive(Debug, Clone)]
pub struct EnvironmentRule {
    environment: ControlledEnvironment,
    name: RuleName,
}

impl EnvironmentRule {
    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        &self.name
    }

    /// Arms the rule again.
    ///
    /// # Errors
    ///
    /// The [`RuleCommandError`] that the control plane refused with, or why no answer came.
    pub async fn arm(&self) -> Result<Revised, OperationError<RuleCommandError>> {
        self.environment
            .client
            .send(ArmRule {
                environment: self.environment.environment.clone(),
                rule: self.name.clone(),
                expects: None,
            })
            .await
    }

    /// Disarms the rule.
    ///
    /// # Errors
    ///
    /// The [`RuleCommandError`] that the control plane refused with, or why no answer came.
    pub async fn disarm(&self) -> Result<Revised, OperationError<RuleCommandError>> {
        self.environment
            .client
            .send(DisarmRule {
                environment: self.environment.environment.clone(),
                rule: self.name.clone(),
                expects: None,
            })
            .await
    }

    /// Fires the rule by hand: it skips its trigger once.
    ///
    /// # Errors
    ///
    /// The [`FireRuleError`] that the control plane refused with, or why no answer came.
    pub async fn fire(&self) -> Result<(), OperationError<FireRuleError>> {
        self.environment
            .client
            .send(FireRule {
                environment: self.environment.environment.clone(),
                rule: self.name.clone(),
            })
            .await
    }
}

/// The name of the rule, which explanations and statistics name it by.
impl AsRef<str> for EnvironmentRule {
    fn as_ref(&self) -> &str {
        self.name.as_str()
    }
}

/// The scenarios of an [environment](ControlledEnvironment): to activate one.
#[derive(Debug, Clone)]
pub struct Scenarios {
    environment: ControlledEnvironment,
}

impl Scenarios {
    /// Activates `scenario` in the environment: a scenario of the workspace by its name, or a scenario that the
    /// workspace gets first.
    ///
    /// A scenario that the caller wrote without a name is `scenario`, or `scenario-2`, `scenario-3` and so on when the
    /// workspace has a scenario of that name.
    ///
    /// # Errors
    ///
    /// - [`ActivateError::Unreadable`] when the scenario does not read.
    /// - [`ActivateError::NotCreated`] when the workspace did not take the scenario.
    /// - [`ActivateError::NotActivated`] when the environment did not activate it.
    pub async fn activate(&self, scenario: impl Into<ScenarioToActivate>) -> Result<ActiveScenario, ActivateError> {
        let name = match scenario.into().given() {
            Given::Named(name) => name.clone(),
            Given::Written(written) => self
                .environment
                .client
                .definitions()
                .scenario(ScenarioText(written.clone()))
                .await
                .map_err(|error| match error {
                    DefineError::Unreadable { error } => ActivateError::Unreadable { error },
                    DefineError::NotCreated { error } => ActivateError::NotCreated { error },
                })?,
        };
        self.environment
            .client
            .send(ActivateScenario {
                environment: self.environment.environment.clone(),
                scenario: name.clone(),
                expects: None,
            })
            .await
            .map_err(|error| ActivateError::NotActivated { error })?;
        Ok(ActiveScenario {
            environment: self.environment.clone(),
            name,
        })
    }
}

/// The scenario that a caller activated in an [environment](ControlledEnvironment).
#[derive(Debug, Clone)]
pub struct ActiveScenario {
    environment: ControlledEnvironment,
    name: ScenarioName,
}

impl ActiveScenario {
    /// The name of the scenario.
    pub fn name(&self) -> &ScenarioName {
        &self.name
    }

    /// Ends the step that runs, whatever its condition says, and starts the next one.
    ///
    /// # Errors
    ///
    /// The [`StepScenarioError`] that the control plane refused with, or why no answer came.
    pub async fn step(&self) -> Result<Revised, OperationError<StepScenarioError>> {
        self.environment
            .client
            .send(StepScenario {
                environment: self.environment.environment.clone(),
                expects: None,
            })
            .await
    }

    /// Deactivates the scenario: every rule that it armed disarms.
    ///
    /// # Errors
    ///
    /// The [`DeactivateScenarioError`] that the control plane refused with, or why no answer came.
    pub async fn deactivate(self) -> Result<Revised, OperationError<DeactivateScenarioError>> {
        self.environment
            .client
            .send(DeactivateScenario {
                environment: self.environment.environment.clone(),
                expects: None,
            })
            .await
    }
}

/// The recordings of an [environment](ControlledEnvironment).
#[derive(Debug, Clone)]
pub struct Recordings {
    environment: ControlledEnvironment,
}

impl Recordings {
    /// Starts a recording that keeps as much of each call as the caller [said](ControlledEnvironment::recording_in),
    /// or else as the environment says.
    ///
    /// # Errors
    ///
    /// The [`StartRecordingError`] that the control plane refused with, or why no answer came.
    pub async fn start(&self) -> Result<Recording, OperationError<StartRecordingError>> {
        self.started(self.environment.recording).await
    }

    /// Starts a recording that keeps as much of each call as `mode` says, whatever the environment says.
    ///
    /// # Errors
    ///
    /// As for [`start`](Self::start).
    pub async fn start_with(&self, mode: RecordingMode) -> Result<Recording, OperationError<StartRecordingError>> {
        self.started(Some(mode)).await
    }

    /// The recording that the environment writes now.
    ///
    /// # Errors
    ///
    /// - [`CurrentRecordingError::NotRecording`] when the environment records nothing.
    /// - [`CurrentRecordingError::NotRunning`] and [`CurrentRecordingError::Unreachable`] when the environment does not
    ///   run, or the live stream gave no snapshot.
    pub async fn current(&self) -> Result<Recording, CurrentRecordingError> {
        let state = self.environment.state().await.map_err(|error| match error {
            EnvironmentStateError::NotRunning { environment } => CurrentRecordingError::NotRunning { environment },
            EnvironmentStateError::Unreachable { error } => CurrentRecordingError::Unreachable { error },
        })?;
        match state.recording {
            Some(id) => Ok(self.recording(id)),
            None => Err(CurrentRecordingError::NotRecording {
                environment: state.environment,
            }),
        }
    }

    /// The recording of this ID: one that runs, or one that ended.
    pub fn recording(&self, id: RecordingId) -> Recording {
        Recording {
            environment: self.environment.clone(),
            id,
        }
    }

    async fn started(&self, mode: Option<RecordingMode>) -> Result<Recording, OperationError<StartRecordingError>> {
        let id = self
            .environment
            .client
            .send(StartRecording {
                environment: self.environment.environment.clone(),
                mode,
            })
            .await?;
        Ok(self.recording(id))
    }
}

/// A recording of an [environment](ControlledEnvironment), by its ID.
#[derive(Debug, Clone)]
pub struct Recording {
    environment: ControlledEnvironment,
    id: RecordingId,
}

impl Recording {
    /// The ID of the recording.
    pub fn id(&self) -> &RecordingId {
        &self.id
    }

    /// Stops the recording of the environment, and returns once the recording holds everything that the environment
    /// handed over before the stop.
    ///
    /// # Errors
    ///
    /// The [`StopRecordingError`] that the control plane refused with, or why no answer came.
    pub async fn stop(&self) -> Result<RecordingEnd, OperationError<StopRecordingError>> {
        self.environment
            .client
            .send(StopRecording {
                environment: self.environment.environment.clone(),
            })
            .await
    }

    /// The calls of the recording that `selector` matches: their count over the whole recording, and the first page
    /// of them, each with its verdict and its explanation.
    ///
    /// # Errors
    ///
    /// - [`QueryError::Unreadable`] when the selector does not read.
    /// - [`QueryError::NotQueried`] with the [`QueryRecordingError`] that the control plane refused with, or why no
    ///   answer came.
    pub async fn query(&self, selector: impl Into<SelectorToMatch>) -> Result<RecordedCalls, QueryError> {
        let selector = selector
            .into()
            .read()
            .map_err(|error| QueryError::Unreadable { error })?;
        self.environment
            .client
            .send(QueryRecording {
                recording: self.id.clone(),
                selector,
                draft: None,
                skip: 0,
            })
            .await
            .map_err(|error| QueryError::NotQueried { error })
    }

    /// How many calls of the whole recording `selector` matches.
    ///
    /// # Errors
    ///
    /// As for [`query`](Self::query).
    pub async fn count(&self, selector: impl Into<SelectorToMatch>) -> Result<u64, QueryError> {
        self.query(selector).await.map(|calls| calls.matched)
    }
}

/// The calls of an environment that a live stream [follows](ControlledEnvironment::follow).
///
/// It holds a live stream of its own. Letting go of it ends the stream.
pub struct FollowedCalls {
    stream: LiveStream,
    environment: EnvironmentName,
    timeout: gaslightmq_control_api::clock::TimeSpan,
}

impl FollowedCalls {
    /// Waits until the stream handed over `count` calls more, and returns them in the order they came.
    ///
    /// Each of them is in the recording of the environment, if one runs: a call reaches the recording before it
    /// reaches a follower. The wait lasts as long as the
    /// [follow timeout](crate::ControlClientConfiguration::follow_timeout) at most.
    ///
    /// # Errors
    ///
    /// - [`WaitForCallsError::TimedOut`] when fewer calls came within the follow timeout.
    /// - [`WaitForCallsError::Ended`] when the stream ended first.
    pub async fn wait_for(&mut self, count: u64) -> Result<Vec<Call>, WaitForCallsError> {
        let mut calls = Vec::new();
        let waiting = async {
            while (calls.len() as u64) < count {
                if let StreamMessage::Call { environment, call } = self.stream.next().await?
                    && environment == self.environment
                {
                    calls.push(call);
                }
            }
            Ok::<(), StreamError>(())
        };
        match within(self.timeout, waiting).await {
            Some(Ok(())) => Ok(calls),
            Some(Err(error)) => Err(WaitForCallsError::Ended { error }),
            None => Err(WaitForCallsError::TimedOut {
                wanted: count,
                seen: calls.len() as u64,
                timeout: self.timeout,
            }),
        }
    }
}

/// The definitions of the workspace, as a client creates them.
#[derive(Debug, Clone)]
pub struct Definitions {
    client: ControlClient,
}

impl Definitions {
    /// Creates `rule` in the workspace, and returns its name: the one it names, or the name of what it says.
    ///
    /// # Errors
    ///
    /// - [`DefineError::Unreadable`] when the rule does not read.
    /// - [`DefineError::NotCreated`] when the workspace did not take it.
    pub async fn rule(&self, rule: impl Into<DefinitionText<Rule>>) -> Result<RuleName, DefineError> {
        let rule: Rule = rule.into().read().map_err(|error| DefineError::Unreadable { error })?;
        let name = rule.name().clone();
        self.client
            .send(CreateDefinition { definition: rule })
            .await
            .map_err(|error| DefineError::NotCreated { error })?;
        Ok(name)
    }

    /// Creates `scenario` in the workspace, and returns its name.
    ///
    /// A scenario that the caller wrote without a name is `scenario`, or `scenario-2`, `scenario-3` and so on when the
    /// workspace has a scenario of that name.
    ///
    /// # Errors
    ///
    /// - [`DefineError::Unreadable`] when the scenario does not read.
    /// - [`DefineError::NotCreated`] when the workspace did not take it.
    pub async fn scenario(&self, scenario: impl Into<ScenarioText>) -> Result<ScenarioName, DefineError> {
        let scenario = scenario.into().0;
        let unreadable = |error| DefineError::Unreadable { error };
        let numbered = match &scenario {
            Written::Text(text) => !text.names_itself().map_err(unreadable)?,
            Written::Value(_) => false,
        };
        let mut number = 1;
        loop {
            let name = match number {
                1 => UNNAMED_SCENARIO.to_owned(),
                _ => format!("{UNNAMED_SCENARIO}-{number}"),
            };
            let definition: Scenario = match &scenario {
                Written::Text(text) => text.read_named(&name).map_err(unreadable)?,
                Written::Value(value) => value.clone(),
            };
            let named = definition.name().clone();
            match self.client.send(CreateDefinition { definition }).await {
                Ok(_) => return Ok(named),
                Err(OperationError::Refused {
                    error: CreateDefinitionError::NameTaken { .. },
                }) if numbered => number += 1,
                Err(error) => return Err(DefineError::NotCreated { error }),
            }
        }
    }
}

/// A scenario to create in the workspace: its text, as [`scenario!`](crate::scenario) or a file writes it, or its
/// value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioText(Written<Scenario>);

impl From<DefinitionText<Scenario>> for ScenarioText {
    fn from(text: DefinitionText<Scenario>) -> Self {
        Self(Written::Text(text))
    }
}

impl From<&str> for ScenarioText {
    fn from(text: &str) -> Self {
        Self(Written::Text(DefinitionText::new(text)))
    }
}

impl From<Scenario> for ScenarioText {
    fn from(scenario: Scenario) -> Self {
        Self(Written::Value(scenario))
    }
}
