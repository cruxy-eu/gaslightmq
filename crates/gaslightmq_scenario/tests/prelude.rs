//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates whose types a scenario speaks in, and of the store contract.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_clock::TimeSpan;
    use gaslightmq_definition_api::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_scenario::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// The scenarios that name a rule, with their store named by its trait.
    #[expect(
        dead_code,
        reason = "no store is at hand here: the trait only has to compile next to the other preludes"
    )]
    async fn naming(scenarios: &impl ScenarioStore, rule: &RuleName) -> Vec<Scenario> {
        scenarios.using_rule(rule).await.unwrap()
    }

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let scenario_name: ScenarioName = ScenarioName::new("producer-recovers-from-leader-churn").unwrap();
        let scenario_title: ScenarioTitle = ScenarioTitle::new("Producer recovers from leader churn").unwrap();
        let step_name: StepName = StepName::new("slow-recovery").unwrap();
        let step_title: StepTitle = StepTitle::new("Slow recovery of the producer").unwrap();
        let rule: ScenarioRule = ScenarioRule::from(RuleName::new("background-jitter").unwrap());
        let until: Until = Until::After(TimeSpan::from_secs(20));
        let step: Step = Step::new(step_name.clone())
            .with_title(step_title.clone())
            .ending_on(until.clone());
        let mode: Mode = Mode::Sequence(vec![step.clone()]);
        let scenario: Scenario = Scenario::new(scenario_name.clone(), mode.clone(), Seed::new(7), [rule.clone()])
            .unwrap()
            .with_title(scenario_title.clone());

        vec![
            scenario_name.to_string(),
            scenario_title.to_string(),
            step_name.to_string(),
            step_title.to_string(),
            rule.name().to_string(),
            format!("{until:?}"),
            step.name().to_string(),
            mode.name().to_owned(),
            scenario.name().to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let scenario: ScenarioName = "churn".parse().unwrap();
        let step: StepName = "leader-churn".parse().unwrap();
        let rule: RuleName = "refuse-with-not-leader".parse().unwrap();
        vec![
            message_of(ScenarioNameError::Empty),
            message_of(ScenarioTitleError::Empty),
            message_of(StepNameError::SurroundingHyphen {
                input: "warm-up-".to_owned(),
            }),
            message_of(StepTitleError::Empty),
            message_of(ScenarioError::DuplicateRule {
                scenario: scenario.clone(),
                rule: rule.clone(),
            }),
            message_of(ScenarioError::NoStep {
                scenario: scenario.clone(),
            }),
            message_of(ScenarioError::DuplicateStep {
                scenario: scenario.clone(),
                step: step.clone(),
            }),
            message_of(ScenarioError::DuplicateArm {
                scenario: scenario.clone(),
                step: step.clone(),
                rule: rule.clone(),
            }),
            message_of(ScenarioError::ArmsScenarioRule {
                scenario: scenario.clone(),
                step: step.clone(),
                rule: rule.clone(),
            }),
            message_of(ScenarioError::EndsOnUnarmedRule { scenario, step, rule }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 9);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "scenario name \"\" is empty",
            "scenario title \"\" is empty",
            "step name \"warm-up-\" starts or ends with a hyphen",
            "step title \"\" is empty",
            "scenario \"churn\" has two rules named \"refuse-with-not-leader\"",
            "scenario \"churn\" is a sequence without a step",
            "scenario \"churn\" has two steps named \"leader-churn\"",
            "step \"leader-churn\" of scenario \"churn\" arms rule \"refuse-with-not-leader\" twice",
            "step \"leader-churn\" of scenario \"churn\" arms rule \"refuse-with-not-leader\", \
             which the scenario keeps armed for the whole run",
            "step \"leader-churn\" of scenario \"churn\" ends on the firings of rule \"refuse-with-not-leader\", \
             which is not armed during the step",
        ]
    );
}
