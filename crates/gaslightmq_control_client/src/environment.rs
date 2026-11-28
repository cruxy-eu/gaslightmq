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
    ActivateError, ArmError, ControlClient, CurrentRecordingError, DeactivateError, DefineError, DisarmError,
    EnvironmentStateError, FireError, FiringsError, FollowError, JournalError, LiveStream, QueryError,
    RecordingStartError, RecordingStopError, RuleToArm, ScenarioToActivate, SelectorToMatch, SendError, StepError,
    StreamError, WaitForCallsError,
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
    /// [`JournalError`] when the environment does not run, or no answer came.
    pub async fn journal(&self) -> Result<Vec<JournalEntry>, JournalError> {
        let query = QueryJournal {
            environment: self.environment.clone(),
            after: None,
        };
        answered(
            &self.client,
            query,
            |error| JournalError::Refused { error },
            |error| JournalError::Unanswered { error },
        )
        .await
    }

    /// The entries of the journal of the environment after `entry`, which the caller read last.
    ///
    /// # Errors
    ///
    /// As for [`journal`](Self::journal).
    pub async fn journal_after(&self, entry: JournalEntryId) -> Result<Vec<JournalEntry>, JournalError> {
        let query = QueryJournal {
            environment: self.environment.clone(),
            after: Some(entry),
        };
        answered(
            &self.client,
            query,
            |error| JournalError::Refused { error },
            |error| JournalError::Unanswered { error },
        )
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
                rule.arm().await?;
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
                Ok(Ok(_)) => return Ok(self.environment.rule(name)),
                Ok(Err(CreateLiveRuleError::NameTaken { .. })) if numbered => number += 1,
                Ok(Err(error)) => return Err(ArmError::NotCreated { error }),
                Err(error) => return Err(ArmError::Unanswered { error }),
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
    /// [`ArmError::NotArmed`] with the [`RuleCommandError`] that the control plane refused with, or
    /// [`ArmError::Unanswered`].
    pub async fn arm(&self) -> Result<Revised, ArmError> {
        let arm = ArmRule {
            environment: self.environment.environment.clone(),
            rule: self.name.clone(),
            expects: None,
        };
        answered(
            &self.environment.client,
            arm,
            |error| ArmError::NotArmed { error },
            |error| ArmError::Unanswered { error },
        )
        .await
    }

    /// Disarms the rule.
    ///
    /// # Errors
    ///
    /// [`DisarmError`] with the [`RuleCommandError`] that the control plane refused with, or why no answer came.
    pub async fn disarm(&self) -> Result<Revised, DisarmError> {
        let disarm = DisarmRule {
            environment: self.environment.environment.clone(),
            rule: self.name.clone(),
            expects: None,
        };
        answered(
            &self.environment.client,
            disarm,
            |error| DisarmError::Refused { error },
            |error| DisarmError::Unanswered { error },
        )
        .await
    }

    /// Fires the rule by hand: it skips its trigger once.
    ///
    /// # Errors
    ///
    /// [`FireError`] with the [`FireRuleError`] that the control plane refused with, or why no answer came.
    pub async fn fire(&self) -> Result<(), FireError> {
        let fire = FireRule {
            environment: self.environment.environment.clone(),
            rule: self.name.clone(),
        };
        answered(
            &self.environment.client,
            fire,
            |error| FireError::Refused { error },
            |error| FireError::Unanswered { error },
        )
        .await
    }
}

impl EnvironmentRule {
    /// On how many calls the action of the rule took effect, as the next statistics of the environment say: the
    /// firings that count for a lifetime of firings. A rule that did nothing yet has fired on none.
    ///
    /// The environment sends its statistics at an interval, so the count is the one of that moment, and it waits as
    /// long as the [follow timeout](crate::ControlClientConfiguration::follow_timeout) at most.
    ///
    /// # Errors
    ///
    /// - [`FiringsError::Unreachable`] when the live stream did not open, or ended first.
    /// - [`FiringsError::TimedOut`] when no statistics came within the follow timeout.
    pub async fn firings(&self) -> Result<u64, FiringsError> {
        let unreachable = |error| FiringsError::Unreachable { error };
        let mut stream = self.environment.client.live_stream().await.map_err(unreachable)?;
        let timeout = self.environment.client.configuration().follow_timeout;
        let waiting = async {
            loop {
                if let StreamMessage::Stats { environment, stats } = stream.next().await?
                    && environment == self.environment.environment
                {
                    let firings = stats.rules.get(&self.name).map_or(0, |rule| rule.firings);
                    return Ok::<u64, StreamError>(firings);
                }
            }
        };
        let counted = within(timeout, waiting).await;
        stream.close().await;
        match counted {
            Some(Ok(firings)) => Ok(firings),
            Some(Err(error)) => Err(FiringsError::Unreachable { error }),
            None => Err(FiringsError::TimedOut { timeout }),
        }
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
                    DefineError::Unanswered { error } => ActivateError::Unanswered { error },
                    DefineError::NotCreated { error } => ActivateError::NotCreated { error },
                })?,
        };
        let activate = ActivateScenario {
            environment: self.environment.environment.clone(),
            scenario: name.clone(),
            expects: None,
        };
        answered(
            &self.environment.client,
            activate,
            |error| ActivateError::NotActivated { error },
            |error| ActivateError::Unanswered { error },
        )
        .await?;
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
    /// [`StepError`] with the [`StepScenarioError`] that the control plane refused with, or why no answer came.
    pub async fn step(&self) -> Result<Revised, StepError> {
        let step = StepScenario {
            environment: self.environment.environment.clone(),
            expects: None,
        };
        answered(
            &self.environment.client,
            step,
            |error| StepError::Refused { error },
            |error| StepError::Unanswered { error },
        )
        .await
    }

    /// Deactivates the scenario: every rule that it armed disarms.
    ///
    /// # Errors
    ///
    /// [`DeactivateError`] with the [`DeactivateScenarioError`] that the control plane refused with, or why no answer
    /// came.
    pub async fn deactivate(self) -> Result<Revised, DeactivateError> {
        let deactivate = DeactivateScenario {
            environment: self.environment.environment.clone(),
            expects: None,
        };
        answered(
            &self.environment.client,
            deactivate,
            |error| DeactivateError::Refused { error },
            |error| DeactivateError::Unanswered { error },
        )
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
    /// [`RecordingStartError`] with the [`StartRecordingError`] that the control plane refused with, or why no answer
    /// came.
    pub async fn start(&self) -> Result<Recording, RecordingStartError> {
        self.started(self.environment.recording).await
    }

    /// Starts a recording that keeps as much of each call as `mode` says, whatever the environment says.
    ///
    /// # Errors
    ///
    /// As for [`start`](Self::start).
    pub async fn start_with(&self, mode: RecordingMode) -> Result<Recording, RecordingStartError> {
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

    async fn started(&self, mode: Option<RecordingMode>) -> Result<Recording, RecordingStartError> {
        let start = StartRecording {
            environment: self.environment.environment.clone(),
            mode,
        };
        let id = answered(
            &self.environment.client,
            start,
            |error| RecordingStartError::Refused { error },
            |error| RecordingStartError::Unanswered { error },
        )
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
    /// [`RecordingStopError`] with the [`StopRecordingError`] that the control plane refused with, or why no answer
    /// came.
    pub async fn stop(&self) -> Result<RecordingEnd, RecordingStopError> {
        let stop = StopRecording {
            environment: self.environment.environment.clone(),
        };
        answered(
            &self.environment.client,
            stop,
            |error| RecordingStopError::Refused { error },
            |error| RecordingStopError::Unanswered { error },
        )
        .await
    }

    /// The calls of the recording that `selector` matches: their count over the whole recording, and the first page
    /// of them, each with its verdict and its explanation.
    ///
    /// # Errors
    ///
    /// - [`QueryError::Unreadable`] when the selector does not read.
    /// - [`QueryError::Refused`] with the [`QueryRecordingError`] that the control plane refused with.
    /// - [`QueryError::Unanswered`] when no answer came.
    pub async fn query(&self, selector: impl Into<SelectorToMatch>) -> Result<RecordedCalls, QueryError> {
        let selector = selector
            .into()
            .read()
            .map_err(|error| QueryError::Unreadable { error })?;
        let query = QueryRecording {
            recording: self.id.clone(),
            selector,
            draft: None,
            skip: 0,
        };
        answered(
            &self.environment.client,
            query,
            |error| QueryError::Refused { error },
            |error| QueryError::Unanswered { error },
        )
        .await
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
        answered(
            &self.client,
            CreateDefinition { definition: rule },
            |error| DefineError::NotCreated { error },
            |error| DefineError::Unanswered { error },
        )
        .await?;
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
                Ok(Ok(_)) => return Ok(named),
                Ok(Err(CreateDefinitionError::NameTaken { .. })) if numbered => number += 1,
                Ok(Err(error)) => return Err(DefineError::NotCreated { error }),
                Err(error) => return Err(DefineError::Unanswered { error }),
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

/// What `operation` answered, or the error of the call that `refused` makes of its error, and that `unanswered` makes
/// of a [`SendError`].
async fn answered<O: Operation, E>(
    client: &ControlClient,
    operation: O,
    refused: impl FnOnce(O::Error) -> E,
    unanswered: impl FnOnce(SendError) -> E,
) -> Result<O::Answer, E> {
    match client.send(operation).await {
        Ok(Ok(answer)) => Ok(answer),
        Ok(Err(error)) => Err(refused(error)),
        Err(error) => Err(unanswered(error)),
    }
}
