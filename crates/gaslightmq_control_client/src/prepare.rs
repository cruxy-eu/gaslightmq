//! What an instance starts with, declared before it starts, and done through the control plane once it runs.

use gaslightmq_control_api::environment::RecordingMode;
use gaslightmq_control_api::rule::Rule;
use gaslightmq_definition_text::DefinitionText;

use crate::{
    ActiveScenario, ControlledEnvironment, EnvironmentRule, PrepareError, Recording, RuleToArm, ScenarioText,
    ScenarioToActivate,
};

/// What an instance of GaslightMQ starts with, as a test declares it before the instance starts: definitions of the
/// workspace, the rules armed from the start, a recording from the start, and the scenario activated.
///
/// A test library holds one in the configuration of its instance, and [prepares](Self::prepare) the environment of
/// the instance with it once the instance runs, in this order:
///
/// 1. The rules, and then the scenarios, that the workspace gets.
/// 2. The rules that it arms, each as [`Rules::arm`](crate::Rules::arm) arms it.
/// 3. The recording from the start, so that it holds every call of the scenario.
/// 4. The scenario that it activates.
///
/// ```
/// use gaslightmq_control_api::environment::RecordingMode;
/// use gaslightmq_control_client::{Preparation, rule};
///
/// let preparation = Preparation::default()
///     .with_rule(rule! { name: slow-fetches, selector: { api: Fetch }, action: { lie: timing, delay: 1s } })
///     .arming(rule! { selector: { api: Produce }, action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER } })
///     .arming("slow-fetches")
///     .recording_in(RecordingMode::Full)
///     .recording_from_the_start();
/// assert!(preparation.records_from_the_start());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preparation {
    rules: Vec<DefinitionText<Rule>>,
    scenarios: Vec<ScenarioText>,
    arming: Vec<RuleToArm>,
    recording: Option<RecordingMode>,
    from_the_start: bool,
    activating: Option<ScenarioToActivate>,
}

/// What a [preparation](Preparation) did: the rules that it armed, the recording that it started, and the scenario
/// that it activated.
#[derive(Debug, Clone, Default)]
pub struct Prepared {
    /// The rules that it armed, in the order the test declared them.
    pub armed: Vec<EnvironmentRule>,
    /// The recording that runs from the start, when the test asked for one.
    pub recording: Option<Recording>,
    /// The scenario that it activated, when the test named one.
    pub scenario: Option<ActiveScenario>,
}

impl Preparation {
    /// The same preparation, with `rule` among the rules of the workspace, disarmed.
    #[must_use]
    pub fn with_rule(mut self, rule: impl Into<DefinitionText<Rule>>) -> Self {
        self.rules.push(rule.into());
        self
    }

    /// The same preparation, with `scenario` among the scenarios of the workspace.
    #[must_use]
    pub fn with_scenario(mut self, scenario: impl Into<ScenarioText>) -> Self {
        self.scenarios.push(scenario.into());
        self
    }

    /// The same preparation, which arms `rule` once the instance runs: a rule of the workspace by its name, or a new
    /// live rule of the environment.
    #[must_use]
    pub fn arming(mut self, rule: impl Into<RuleToArm>) -> Self {
        self.arming.push(rule.into());
        self
    }

    /// The same preparation, whose recordings keep as much of each call as `mode` says: the one from the start, and
    /// each that the test [starts](crate::Recordings::start) through the environment.
    #[must_use]
    pub fn recording_in(mut self, mode: RecordingMode) -> Self {
        self.recording = Some(mode);
        self
    }

    /// The same preparation, which starts a recording once the instance runs, before it activates a scenario.
    #[must_use]
    pub fn recording_from_the_start(mut self) -> Self {
        self.from_the_start = true;
        self
    }

    /// The same preparation, which activates `scenario` last: a scenario of the workspace by its name, or one that the
    /// workspace gets first.
    #[must_use]
    pub fn activating(mut self, scenario: impl Into<ScenarioToActivate>) -> Self {
        self.activating = Some(scenario.into());
        self
    }

    /// Whether the instance records from the start.
    pub fn records_from_the_start(&self) -> bool {
        self.from_the_start
    }

    /// The mode of the recordings, when the test named one.
    pub fn recording_mode(&self) -> Option<RecordingMode> {
        self.recording
    }

    /// Does what the preparation declares, in `environment`, which runs.
    ///
    /// # Errors
    ///
    /// [`PrepareError`] with the first declaration that failed. What came before it stays done.
    pub async fn prepare(&self, environment: &ControlledEnvironment) -> Result<Prepared, PrepareError> {
        let environment = match self.recording {
            Some(mode) => environment.clone().recording_in(mode),
            None => environment.clone(),
        };
        let definitions = environment.client().definitions();
        for rule in &self.rules {
            definitions
                .rule(rule.clone())
                .await
                .map_err(|error| PrepareError::NotDefined { error })?;
        }
        for scenario in &self.scenarios {
            definitions
                .scenario(scenario.clone())
                .await
                .map_err(|error| PrepareError::NotDefined { error })?;
        }
        let mut prepared = Prepared::default();
        for rule in &self.arming {
            let armed = environment
                .rules()
                .arm(rule.clone())
                .await
                .map_err(|error| PrepareError::NotArmed { error })?;
            prepared.armed.push(armed);
        }
        if self.from_the_start {
            let recording = environment
                .recordings()
                .start()
                .await
                .map_err(|error| PrepareError::NotRecording { error })?;
            prepared.recording = Some(recording);
        }
        if let Some(scenario) = &self.activating {
            let active = environment
                .scenarios()
                .activate(scenario.clone())
                .await
                .map_err(|error| PrepareError::NotActivated { error })?;
            prepared.scenario = Some(active);
        }
        Ok(prepared)
    }
}
