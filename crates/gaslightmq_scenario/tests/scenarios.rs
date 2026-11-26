//! A scenario as someone writes it: its mode, its rules and its steps, what it names, and what it refuses.
//!
//! The scenarios are those of `docs/specs/domains/scenarios.md`.

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;

#[test]
fn a_sequence_runs_its_steps_on_top_of_the_rules_of_the_scenario() {
    let jitter: RuleName = "background-jitter".parse().unwrap();
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let slow: RuleName = "slow-order-producers".parse().unwrap();
    let order_producers: SelectorName = "order-producers".parse().unwrap();
    let warm_up = Step::new("warm-up".parse().unwrap()).ending_on(Until::Calls {
        count: Count::new(100).unwrap(),
        selector: order_producers.clone().into(),
    });
    let churn = Step::new("leader-churn".parse().unwrap())
        .arming([refuse.clone()])
        .ending_on(Until::Firings {
            count: Count::new(3).unwrap(),
            rule: refuse.clone(),
        });
    let recovery = Step::new("slow-recovery".parse().unwrap())
        .with_title("Slow recovery of the producer".parse().unwrap())
        .arming([slow.clone()])
        .ending_on(Until::After(TimeSpan::from_secs(20)));
    let back_to_normal = Step::new("back-to-normal".parse().unwrap());
    let steps = vec![warm_up, churn, recovery, back_to_normal];

    let scenario = Scenario::new(
        "producer-recovers-from-leader-churn".parse().unwrap(),
        Mode::Sequence(steps.clone()),
        Seed::new(7),
        [ScenarioRule::from(jitter.clone())],
    )
    .unwrap()
    .with_title("Producer recovers from leader churn".parse().unwrap());

    assert_eq!(scenario.name(), "producer-recovers-from-leader-churn");
    assert_eq!(
        scenario.title().map(ScenarioTitle::as_str),
        Some("Producer recovers from leader churn")
    );
    assert_eq!(scenario.mode().name(), "sequence");
    assert_eq!(scenario.seed(), Seed::new(7));
    assert_eq!(scenario.steps(), steps);
    // The rules of the scenario stay armed across all steps, and each step arms its own on top.
    assert_eq!(scenario.rules(), [ScenarioRule::Named(jitter.clone())]);
    assert_eq!(scenario.rule_references(), [&jitter, &refuse, &slow]);
    assert_eq!(scenario.selector_references(), [&order_producers]);
    assert_eq!(scenario.preset_references(), Vec::<&PresetName>::new());
    // The last step has no condition: it ends when someone advances or deactivates the scenario.
    assert_eq!(scenario.steps()[3].until(), None);
}

#[test]
fn a_chain_owns_some_rules_and_names_others_of_the_workspace() {
    let order_producers: SelectorName = "order-producers".parse().unwrap();
    let leader_moves: PresetName = "leader-moves".parse().unwrap();
    let leader_moves_away = Rule::new(
        "leader-moves-away".parse().unwrap(),
        order_producers.clone(),
        leader_moves.clone(),
    )
    .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(1).unwrap()));
    let stale_metadata = Rule::new(
        "stale-metadata".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::METADATA)),
        LieDefinition::Silence,
    );
    let jitter: RuleName = "background-jitter".parse().unwrap();

    let scenario = Scenario::new(
        "leader-election".parse().unwrap(),
        Mode::Chain,
        Seed::new(42),
        [
            leader_moves_away.clone().into(),
            stale_metadata.clone().into(),
            jitter.clone().into(),
        ],
    )
    .unwrap();

    // The order of the rules is the order of the chain: the first rule wins a conflict.
    let names: Vec<&RuleName> = scenario.rules().iter().map(ScenarioRule::name).collect();
    assert_eq!(names, [leader_moves_away.name(), stale_metadata.name(), &jitter]);
    assert_eq!(
        scenario.owned_rules().collect::<Vec<_>>(),
        [&leader_moves_away, &stale_metadata]
    );
    // A rule that the scenario owns is no rule of the workspace, but what it names is in the workspace.
    assert_eq!(scenario.rule_references(), [&jitter]);
    assert_eq!(scenario.selector_references(), [&order_producers]);
    assert_eq!(scenario.preset_references(), [&leader_moves]);
    assert_eq!(scenario.steps(), []);
}

#[test]
fn a_live_scenario_is_a_saved_set_of_rules_to_play_with() {
    let rules: [RuleName; 2] = ["delay-1s-on-order-producers", "blackhole-metadata"].map(|rule| rule.parse().unwrap());

    let scenario = Scenario::new(
        "playground".parse().unwrap(),
        Mode::Live,
        Seed::new(1),
        rules.clone().map(ScenarioRule::from),
    )
    .unwrap();

    assert_eq!(scenario.mode(), &Mode::Live);
    assert_eq!(scenario.title(), None);
    assert_eq!(scenario.rule_references(), [&rules[0], &rules[1]]);
}

#[test]
fn a_title_is_no_part_of_the_identity_of_a_scenario() {
    let untitled = Scenario::new("flaky-orders".parse().unwrap(), Mode::Chain, Seed::new(42), []).unwrap();
    let titled = untitled.clone().with_title("Flaky orders".parse().unwrap());

    assert_eq!(titled.name(), untitled.name());
    assert_ne!(titled, untitled);
    assert_eq!(titled.without_title(), untitled);
}

#[test]
fn a_rule_stands_in_a_scenario_once_whether_it_owns_or_names_it() {
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let owned = Rule::new(refuse.clone(), Conditions::new(), LieDefinition::Silence);

    let error = Scenario::new(
        "flaky-orders".parse().unwrap(),
        Mode::Chain,
        Seed::new(42),
        [owned.into(), refuse.clone().into()],
    )
    .unwrap_err();

    assert_eq!(
        error,
        ScenarioError::DuplicateRule {
            scenario: "flaky-orders".parse().unwrap(),
            rule: refuse
        }
    );
    assert_eq!(
        error.to_string(),
        "scenario \"flaky-orders\" has two rules named \"refuse-with-not-leader\""
    );
}

#[test]
fn a_sequence_has_at_least_one_step_and_each_step_has_a_name_of_its_own() {
    let empty = Scenario::new("churn".parse().unwrap(), Mode::Sequence(Vec::new()), Seed::new(7), []).unwrap_err();
    let twice = Scenario::new(
        "churn".parse().unwrap(),
        Mode::Sequence(vec![
            Step::new("warm-up".parse().unwrap()),
            Step::new("warm-up".parse().unwrap()),
        ]),
        Seed::new(7),
        [],
    )
    .unwrap_err();

    assert_eq!(empty.to_string(), "scenario \"churn\" is a sequence without a step");
    assert_eq!(
        twice,
        ScenarioError::DuplicateStep {
            scenario: "churn".parse().unwrap(),
            step: "warm-up".parse().unwrap()
        }
    );
    assert_eq!(twice.to_string(), "scenario \"churn\" has two steps named \"warm-up\"");
}

#[test]
fn a_step_arms_each_rule_once_and_none_that_the_scenario_keeps_armed() {
    let jitter: RuleName = "background-jitter".parse().unwrap();
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let sequence = |step: Step| {
        Scenario::new(
            "churn".parse().unwrap(),
            Mode::Sequence(vec![step]),
            Seed::new(7),
            [jitter.clone().into()],
        )
    };

    let twice = sequence(Step::new("leader-churn".parse().unwrap()).arming([refuse.clone(), refuse.clone()]));
    let of_the_scenario = sequence(Step::new("leader-churn".parse().unwrap()).arming([jitter.clone()]));

    assert_eq!(
        twice.unwrap_err().to_string(),
        "step \"leader-churn\" of scenario \"churn\" arms rule \"refuse-with-not-leader\" twice"
    );
    assert_eq!(
        of_the_scenario.unwrap_err(),
        ScenarioError::ArmsScenarioRule {
            scenario: "churn".parse().unwrap(),
            step: "leader-churn".parse().unwrap(),
            rule: jitter
        }
    );
}

#[test]
fn a_step_ends_on_the_firings_of_a_rule_that_is_armed_during_it() {
    let jitter: RuleName = "background-jitter".parse().unwrap();
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let three = Count::new(3).unwrap();
    let ending_on = |rule: &RuleName| {
        let step = Step::new("leader-churn".parse().unwrap()).ending_on(Until::Firings {
            count: three,
            rule: rule.clone(),
        });
        Scenario::new(
            "churn".parse().unwrap(),
            Mode::Sequence(vec![step]),
            Seed::new(7),
            [jitter.clone().into()],
        )
    };

    // A rule of the scenario is armed during every step.
    assert!(ending_on(&jitter).is_ok());
    assert_eq!(
        ending_on(&refuse).unwrap_err().to_string(),
        "step \"leader-churn\" of scenario \"churn\" ends on the firings of rule \"refuse-with-not-leader\", \
         which is not armed during the step"
    );
}
