//! A scenario resolved against the workspace: every rule it arms, and every condition that ends a step, without a
//! name left to look up.

use gaslightmq_clock::TimeSpan;
use gaslightmq_incident::Requirements;
use gaslightmq_rule::{PresetSet, ResolvedRule, Rule, RuleError, RuleName, RuleSet};
use gaslightmq_selector::{EffectiveSelector, SelectorSet, SelectorSetError};
use gaslightmq_trigger::{Count, Seed};

use crate::{Mode, Scenario, ScenarioName, Step, StepName, Until};

/// A scenario as an environment runs it: its rules and the rules of its steps resolved against the workspace, and
/// the selector of every step that counts calls.
///
/// [`Scenario::resolve`] makes one. It keeps the definition it came from, so whoever runs it can say what runs.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// // The workspace has a rule that refuses Produce, and the scenario names it in its one step.
/// let refuse = Rule::new(
///     "refuse-with-not-leader".parse().expect("a rule name"),
///     Conditions::from(Condition::is(ApiKey::PRODUCE)),
///     RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
/// );
/// let churn = Step::new("leader-churn".parse().expect("a step name"))
///     .arming([refuse.name().clone()])
///     .ending_on(Until::Firings { count: Count::new(3).expect("a count"), rule: refuse.name().clone() });
/// let scenario = Scenario::new("leader-churn".parse().expect("a scenario name"), Mode::Sequence(vec![churn]), Seed::new(7), [])
///     .expect("a scenario whose step arms what it ends on");
///
/// let workspace = RuleSet::new([refuse.clone()]).expect("rules of one name each");
/// let resolved = scenario.resolve(&workspace, &SelectorSet::default(), &PresetSet::default())?;
///
/// assert_eq!(resolved.steps()[0].arm()[0].name(), refuse.name());
/// // The scenario only lies, so it runs on every backend.
/// assert!(resolved.requirements().is_empty());
/// # Ok::<(), ResolveScenarioError>(())
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedScenario {
    definition: Scenario,
    rules: Vec<ResolvedRule>,
    steps: Vec<ResolvedStep>,
}

impl ResolvedScenario {
    /// The scenario as its file holds it.
    pub const fn definition(&self) -> &Scenario {
        &self.definition
    }

    /// The name of the scenario.
    pub const fn name(&self) -> &ScenarioName {
        self.definition.name()
    }

    /// How the rules of the scenario combine.
    pub const fn mode(&self) -> &Mode {
        self.definition.mode()
    }

    /// The root of every die of the run.
    pub const fn seed(&self) -> Seed {
        self.definition.seed()
    }

    /// The rules of the scenario, in its order, which stay armed for the whole run.
    pub fn rules(&self) -> &[ResolvedRule] {
        &self.rules
    }

    /// The steps of a Sequence, in the order they run. A Live and a Chain scenario have none.
    pub fn steps(&self) -> &[ResolvedStep] {
        &self.steps
    }

    /// What the backend of an environment must support to run the scenario: the incidents that its rules and the
    /// rules of its steps fire, with their consequences. A scenario that only lies needs nothing.
    pub fn requirements(&self) -> Requirements {
        let of_the_steps = self.steps.iter().flat_map(|step| step.arm.iter());
        let rules = self.rules.iter().chain(of_the_steps);
        rules.filter_map(|rule| rule.action().incident()).collect()
    }
}

/// One step of a [`ResolvedScenario`]: the rules it arms, resolved, and the condition that ends it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedStep {
    name: StepName,
    arm: Vec<ResolvedRule>,
    until: Option<ResolvedUntil>,
}

impl ResolvedStep {
    /// The name of the step, unique among the steps of its scenario.
    pub const fn name(&self) -> &StepName {
        &self.name
    }

    /// The rules that the step arms, in the order that resolves their conflicts.
    pub fn arm(&self) -> &[ResolvedRule] {
        &self.arm
    }

    /// The condition that ends the step, or `None` when someone advances or deactivates the scenario.
    pub const fn until(&self) -> Option<&ResolvedUntil> {
        self.until.as_ref()
    }
}

/// The condition that ends a [`ResolvedStep`], with the selector that counts calls resolved.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedUntil {
    /// After `count` calls that arrived during the step matched `selector`.
    Calls { count: Count, selector: EffectiveSelector },
    /// After the action of `rule` applied `count` times during the step.
    Firings { count: Count, rule: RuleName },
    /// After that much time on the clock of the engine, from the start of the step.
    After(TimeSpan),
}

impl Scenario {
    /// The scenario as an environment runs it: every rule it owns, every rule of the workspace that it or one of its
    /// steps names, and the selector of every step that counts calls, resolved against the definitions of a
    /// workspace.
    ///
    /// `rules` are the rules of the workspace, and `selectors` and `presets` what the rules and the conditions name.
    ///
    /// # Errors
    ///
    /// - [`ResolveScenarioError::MissingRule`] when the scenario or a step names a rule that `rules` does not hold.
    /// - [`ResolveScenarioError::Unresolved`] when a rule names a selector or a preset that does not exist, or could
    ///   never fire.
    /// - [`ResolveScenarioError::UnresolvedCondition`] when a step counts the calls of a selector that does not exist.
    pub fn resolve(
        &self,
        rules: &RuleSet,
        selectors: &SelectorSet,
        presets: &PresetSet,
    ) -> Result<ResolvedScenario, ResolveScenarioError> {
        let resolve = |name: &RuleName, owned: Option<&Rule>| {
            let rule = owned
                .or_else(|| rules.get(name))
                .ok_or_else(|| ResolveScenarioError::MissingRule {
                    scenario: self.name().clone(),
                    rule: name.clone(),
                })?;
            rule.resolve(selectors, presets)
                .map_err(|error| ResolveScenarioError::Unresolved {
                    scenario: self.name().clone(),
                    error,
                })
        };
        let scenario_rules = self
            .rules()
            .iter()
            .map(|rule| resolve(rule.name(), rule.owned()))
            .collect::<Result<Vec<_>, _>>()?;
        let steps = self
            .steps()
            .iter()
            .map(|step| self.resolve_step(step, &resolve, selectors))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ResolvedScenario {
            definition: self.clone(),
            rules: scenario_rules,
            steps,
        })
    }

    /// The step with its rules and its condition resolved.
    fn resolve_step(
        &self,
        step: &Step,
        resolve: &impl Fn(&RuleName, Option<&Rule>) -> Result<ResolvedRule, ResolveScenarioError>,
        selectors: &SelectorSet,
    ) -> Result<ResolvedStep, ResolveScenarioError> {
        let arm = step
            .arm()
            .iter()
            .map(|rule| resolve(rule.name(), rule.owned()))
            .collect::<Result<Vec<_>, _>>()?;
        let until = match step.until() {
            None => None,
            Some(Until::Calls { count, selector }) => {
                let selector =
                    selectors
                        .resolve(selector)
                        .map_err(|error| ResolveScenarioError::UnresolvedCondition {
                            scenario: self.name().clone(),
                            step: step.name().clone(),
                            error,
                        })?;
                Some(ResolvedUntil::Calls {
                    count: *count,
                    selector,
                })
            }
            Some(Until::Firings { count, rule }) => Some(ResolvedUntil::Firings {
                count: *count,
                rule: rule.clone(),
            }),
            Some(Until::After(span)) => Some(ResolvedUntil::After(*span)),
        };
        Ok(ResolvedStep {
            name: step.name().clone(),
            arm,
            until,
        })
    }
}

/// Why a [`Scenario`] does not [resolve](Scenario::resolve) against the definitions of a workspace.
///
/// The workspace checks every reference of a scenario before it keeps one, so a scenario of the workspace resolves
/// against it. Each variant names the scenario.
///
/// ```
/// use gaslightmq_scenario::prelude::*;
///
/// let error = ResolveScenarioError::MissingRule {
///     scenario: "leader-churn".parse()?,
///     rule: "refuse-with-not-leader".parse().expect("a rule name"),
/// };
/// assert_eq!(
///     error.to_string(),
///     "scenario \"leader-churn\" names rule \"refuse-with-not-leader\", which the workspace does not have"
/// );
/// # Ok::<(), ScenarioNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveScenarioError {
    /// The scenario or one of its steps names a rule that the workspace does not have.
    #[error(
        "scenario {:?} names rule {:?}, which the workspace does not have",
        .scenario.as_str(),
        .rule.as_str()
    )]
    MissingRule { scenario: ScenarioName, rule: RuleName },
    /// A rule of the scenario names a selector or a preset that does not exist, or could never fire.
    #[error("scenario {:?}: {error}", .scenario.as_str())]
    Unresolved { scenario: ScenarioName, error: RuleError },
    /// A step counts the calls of a selector that does not resolve.
    #[error("the calls that end step {:?} of scenario {:?} have no selector: {error}", .step.as_str(), .scenario.as_str())]
    UnresolvedCondition {
        scenario: ScenarioName,
        step: StepName,
        error: SelectorSetError,
    },
}
