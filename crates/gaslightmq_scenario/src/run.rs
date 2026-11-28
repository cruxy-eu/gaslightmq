//! A scenario that runs: the step of a Sequence where it stands, what ends that step, and the order in which the
//! rules of the scenario win a conflict.

use gaslightmq_call::CallId;
use gaslightmq_clock::Instant;
use gaslightmq_field::FieldSource;
use gaslightmq_rule::{ResolvedRule, RuleName};
use gaslightmq_trigger::Budget;

use crate::{Mode, ResolvedScenario, ResolvedStep, ResolvedUntil, ScenarioName, StepName};

/// The run of one scenario in one environment, as sans-IO state: it takes calls and time, and says when a step ends.
///
/// Whoever runs it holds the armed rules, and arms and disarms what the run says:
/// the rules of the scenario for the whole run, and the rules of the step that runs.
///
/// | The run | Says |
/// |---|---|
/// | [`armed_in_order`](Self::armed_in_order) | The rules that the scenario keeps armed now, the rule that wins a conflict first. |
/// | [`step_budget`](Self::step_budget) | The firings that the step allows the rule it ends on, which every session takes from. |
/// | [`call_recorded`](Self::call_recorded) | Counts a call of the step that the selector of its condition matches. |
/// | [`ends_at`](Self::ends_at) | The instant at which a step that ends after a time ends. |
/// | [`step_is_over`](Self::step_is_over) | Whether the condition of the step holds. |
/// | [`advance`](Self::advance) | Ends the step, and starts the next one. |
///
/// # Steps
///
/// A Sequence starts at its first step. A step ends when its condition holds, or when someone advances the scenario.
/// Then the next step starts. After the last step no step runs, and the rules of the scenario stay armed until
/// someone deactivates it. A Live and a Chain scenario have no step.
///
/// A call belongs to the step that was current when the call arrived: the run knows the calls of the environment
/// by their IDs, which grow with their arrival, so a call with an ID above the calls that had arrived when the step
/// started belongs to the step.
///
/// A counted condition is a budget. A step that ends after the firings of a rule hands out a [`Budget`] of that many
/// firings, which the arming of the rule takes from while the step runs: the rule fires exactly that often,
/// however many connections match it at once. A step that ends after a number of calls counts exactly that many.
///
/// ```
/// use gaslightmq_call::CallId;
/// use gaslightmq_clock::{Instant, TimeSpan};
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// // Warm up for two Produce calls, then wait ten seconds, then run on.
/// let produce = Conditions::from(Condition::is(ApiKey::PRODUCE));
/// let warm_up = Step::new("warm-up".parse().expect("a step name"))
///     .ending_on(Until::Calls { count: Count::new(2).expect("a count"), selector: produce.into() });
/// let pause = Step::new("pause".parse().expect("a step name")).ending_on(Until::After(TimeSpan::from_secs(10)));
/// let scenario = Scenario::new(
///     "warm-up-and-pause".parse().expect("a scenario name"),
///     Mode::Sequence(vec![warm_up, pause]),
///     Seed::new(7),
///     [],
/// )?;
/// let resolved = scenario.resolve(&RuleSet::default(), &SelectorSet::default(), &PresetSet::default());
///
/// let started = Instant::ZERO;
/// let mut run = ScenarioRun::start(resolved.expect("a scenario that names nothing"), started, 0);
/// assert_eq!(run.position().step.expect("a step runs").as_str(), "warm-up");
///
/// let a_produce = Fields::new().with(ApiKey::PRODUCE);
/// run.call_recorded(CallId::FIRST, &a_produce);
/// assert!(!run.step_is_over(started));
/// run.call_recorded(CallId::FIRST.next(), &a_produce);
/// assert!(run.step_is_over(started));
///
/// let advanced = run.advance(started, 2).expect("a step ran");
/// assert_eq!(advanced.started.expect("a next step").as_str(), "pause");
/// assert_eq!(run.ends_at(), Some(started + TimeSpan::from_secs(10).duration()));
/// # Ok::<(), ScenarioError>(())
/// ```
#[derive(Debug, Clone)]
pub struct ScenarioRun {
    scenario: ResolvedScenario,
    /// The step that runs, or `None` when the scenario has no step, or ran its last one.
    step: Option<StepRun>,
}

/// Where a Sequence stands in its current step.
#[derive(Debug, Clone)]
struct StepRun {
    /// The position of the step among the steps of the scenario.
    index: usize,
    /// When the step started, on the clock of the engine.
    started_at: Instant,
    /// How many calls had arrived in the environment when the step started.
    calls_before: u64,
    /// How many calls of the step the selector of its condition matched.
    counted: u64,
    /// The firings that the step allows the rule it ends on.
    budget: Option<Budget>,
}

impl StepRun {
    fn new(scenario: &ResolvedScenario, index: usize, now: Instant, calls_before: u64) -> Option<StepRun> {
        let step = scenario.steps().get(index)?;
        let budget = match step.until() {
            Some(ResolvedUntil::Firings { count, .. }) => Some(Budget::new(*count)),
            _ => None,
        };
        Some(StepRun {
            index,
            started_at: now,
            calls_before,
            counted: 0,
            budget,
        })
    }
}

impl ScenarioRun {
    /// The run of `scenario` from `now`, at its first step when it is a Sequence.
    ///
    /// `calls_before` is how many calls had arrived in the environment, so the calls of the first step are those
    /// whose ID is higher.
    pub fn start(scenario: ResolvedScenario, now: Instant, calls_before: u64) -> ScenarioRun {
        let step = StepRun::new(&scenario, 0, now, calls_before);
        ScenarioRun { scenario, step }
    }

    /// The scenario that runs.
    pub const fn scenario(&self) -> &ResolvedScenario {
        &self.scenario
    }

    /// The scenario, and the step that runs.
    pub fn position(&self) -> ScenarioPosition {
        ScenarioPosition {
            scenario: self.scenario.name().clone(),
            step: self.step().map(|step| step.name().clone()),
        }
    }

    /// The step that runs, or `None` when the scenario has no step, or ran its last one.
    pub fn step(&self) -> Option<&ResolvedStep> {
        let step = self.step.as_ref()?;
        self.scenario.steps().get(step.index)
    }

    /// The rules that the scenario keeps armed now, the rule that wins a conflict first.
    ///
    /// | Mode | Order |
    /// |---|---|
    /// | Live | The rules of the scenario, the last one it lists first: activating arms them in the order the scenario lists them, so a rule further down counts as armed more recently. |
    /// | Chain | The rules of the scenario, in the order it lists them. |
    /// | Sequence | The rules of the step that runs, in the order it lists them, then the rules of the scenario, in theirs. |
    pub fn armed_in_order(&self) -> Vec<&ResolvedRule> {
        let of_the_scenario = self.scenario.rules().iter();
        match self.scenario.mode() {
            Mode::Live => of_the_scenario.rev().collect(),
            Mode::Chain => of_the_scenario.collect(),
            Mode::Sequence(_) => {
                let of_the_step = self.step().map_or(&[][..], ResolvedStep::arm).iter();
                of_the_step.chain(of_the_scenario).collect()
            }
        }
    }

    /// The rule that the step ends on the firings of, with the budget of firings that the step allows it.
    ///
    /// Whoever holds the armed rules hands the budget to the arming of the rule while the step runs:
    /// see [`ArmedRule::with_step_budget`](gaslightmq_rule::ArmedRule::with_step_budget).
    pub fn step_budget(&self) -> Option<(&RuleName, &Budget)> {
        let budget = self.step.as_ref()?.budget.as_ref()?;
        match self.step()?.until()? {
            ResolvedUntil::Firings { rule, .. } => Some((rule, budget)),
            ResolvedUntil::Calls { .. } | ResolvedUntil::After(_) => None,
        }
    }

    /// Counts the call of that ID, whose record is `call`, when it belongs to the step that runs and the step ends
    /// after a number of calls that its selector matches.
    ///
    /// A call that arrived before the step started belongs to the step before, and counts for nothing here.
    /// The count stops at the number of the condition.
    pub fn call_recorded<S: FieldSource + ?Sized>(&mut self, id: CallId, call: &S) {
        let Some(ResolvedUntil::Calls { count, selector }) = self.step().and_then(ResolvedStep::until).cloned() else {
            return;
        };
        let Some(step) = self.step.as_mut() else {
            return;
        };
        if id.value() > step.calls_before && step.counted < count.value() && selector.evaluate(call).is_matched() {
            step.counted += 1;
        }
    }

    /// The instant at which the step that runs ends, when it ends after a time.
    pub fn ends_at(&self) -> Option<Instant> {
        let step = self.step.as_ref()?;
        match self.step()?.until()? {
            ResolvedUntil::After(span) => Some(step.started_at + span.duration()),
            ResolvedUntil::Calls { .. } | ResolvedUntil::Firings { .. } => None,
        }
    }

    /// Whether the condition of the step that runs holds at `now`: enough calls matched, the rule spent the firings
    /// of the step, or the time is up. A step without a condition, and a run without a step, are never over by
    /// themselves.
    pub fn step_is_over(&self, now: Instant) -> bool {
        let Some(step) = self.step.as_ref() else {
            return false;
        };
        match self.step().and_then(ResolvedStep::until) {
            Some(ResolvedUntil::Calls { count, .. }) => step.counted >= count.value(),
            Some(ResolvedUntil::Firings { .. }) => step.budget.as_ref().is_some_and(Budget::is_spent),
            Some(ResolvedUntil::After(_)) => self.ends_at().is_some_and(|ends_at| now >= ends_at),
            None => false,
        }
    }

    /// Ends the step that runs, and starts the next one at `now`, or none after the last step.
    ///
    /// `calls_before` is how many calls had arrived in the environment, so the calls of the next step are those
    /// whose ID is higher. Returns the step that ended and the one that started, or `None` when no step ran.
    pub fn advance(&mut self, now: Instant, calls_before: u64) -> Option<StepAdvance> {
        let ended = self.step()?.name().clone();
        let next = self.step.as_ref()?.index + 1;
        self.step = StepRun::new(&self.scenario, next, now, calls_before);
        Some(StepAdvance {
            ended,
            started: self.step().map(|step| step.name().clone()),
        })
    }
}

/// Where a scenario that runs stands: the scenario, and the step that runs.
///
/// With the `serde` feature, it is a map: `{ "scenario": "leader-churn", "step": "warm-up" }`.
/// A scenario without a step that runs leaves `step` out.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct ScenarioPosition {
    /// The scenario.
    pub scenario: ScenarioName,
    /// The step that runs, or `None` when the scenario has no step, or ran its last one.
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    pub step: Option<StepName>,
}

/// What [advancing](ScenarioRun::advance) a scenario did: the step that ended, and the one that started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepAdvance {
    /// The step that ended.
    pub ended: StepName,
    /// The step that started, or `None` when the step that ended was the last one.
    pub started: Option<StepName>,
}
