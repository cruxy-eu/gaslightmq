//! The `serde` form of a scenario, of its rules and of its steps, and the rules that reading applies.
//!
//! The form of a scenario is the YAML of `docs/specs/domains/scenarios.md` and of the examples, written here as JSON.

#![cfg(feature = "serde")]

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_scenario::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_trigger::prelude::*;
use serde_json::json;

#[test]
fn a_sequence_writes_its_steps_after_its_rules() {
    // name: consumer-survives-leader-churn
    // title: Consumer survives leader churn
    // mode: sequence
    // seed: 7
    // rules: [background-jitter]
    // steps:
    //   - name: warm-up
    //     title: Warm up
    //     until: { calls: 100, selector: order-fetches }
    //   - name: leader-churn
    //     arm: [refuse-with-not-leader]
    //     until: { firings: 3, rule: refuse-with-not-leader }
    //   - name: slow-recovery
    //     arm: [order-fetches-are-slow-for-analytics]
    //     until: { after: 20s }
    //   - name: back-to-normal
    let refuse: RuleName = "refuse-with-not-leader".parse().unwrap();
    let steps = vec![
        Step::new("warm-up".parse().unwrap())
            .with_title("Warm up".parse().unwrap())
            .ending_on(Until::Calls {
                count: Count::new(100).unwrap(),
                selector: "order-fetches".parse::<SelectorName>().unwrap().into(),
            }),
        Step::new("leader-churn".parse().unwrap())
            .arming([refuse.clone()])
            .ending_on(Until::Firings {
                count: Count::new(3).unwrap(),
                rule: refuse,
            }),
        Step::new("slow-recovery".parse().unwrap())
            .arming(["order-fetches-are-slow-for-analytics".parse::<RuleName>().unwrap()])
            .ending_on(Until::After(TimeSpan::from_secs(20))),
        Step::new("back-to-normal".parse().unwrap()),
    ];
    let scenario = Scenario::new(
        "consumer-survives-leader-churn".parse().unwrap(),
        Mode::Sequence(steps),
        Seed::new(7),
        ["background-jitter".parse::<RuleName>().unwrap().into()],
    )
    .unwrap()
    .with_title("Consumer survives leader churn".parse().unwrap());

    let written = json!({
        "name": "consumer-survives-leader-churn",
        "title": "Consumer survives leader churn",
        "mode": "sequence",
        "seed": 7,
        "rules": ["background-jitter"],
        "steps": [
            { "name": "warm-up", "title": "Warm up", "until": { "calls": 100, "selector": "order-fetches" } },
            {
                "name": "leader-churn",
                "arm": ["refuse-with-not-leader"],
                "until": { "firings": 3, "rule": "refuse-with-not-leader" },
            },
            { "name": "slow-recovery", "arm": ["order-fetches-are-slow-for-analytics"], "until": { "after": "20s" } },
            { "name": "back-to-normal" },
        ],
    });
    assert_eq!(serde_json::to_value(&scenario).unwrap(), written);
    assert_eq!(serde_json::from_value::<Scenario>(written).unwrap(), scenario);
}

#[test]
fn a_chain_writes_a_rule_it_names_as_its_name_and_a_rule_it_owns_as_its_map() {
    // name: leader-election
    // mode: chain
    // seed: 42
    // rules:
    //   - background-jitter
    //   - name: stale-metadata
    //     selector: { api: Metadata }
    //     action: { lie: silence }
    let stale_metadata = Rule::new(
        "stale-metadata".parse().unwrap(),
        Conditions::from(Condition::is(ApiKey::METADATA)),
        LieDefinition::Silence,
    );
    let scenario = Scenario::new(
        "leader-election".parse().unwrap(),
        Mode::Chain,
        Seed::new(42),
        [
            "background-jitter".parse::<RuleName>().unwrap().into(),
            stale_metadata.clone().into(),
        ],
    )
    .unwrap();

    let written = json!({
        "name": "leader-election",
        "mode": "chain",
        "seed": 42,
        "rules": ["background-jitter", serde_json::to_value(&stale_metadata).unwrap()],
    });
    assert_eq!(serde_json::to_value(&scenario).unwrap(), written);
    assert_eq!(serde_json::from_value::<Scenario>(written).unwrap(), scenario);
}

#[test]
fn a_scenario_without_rules_leaves_them_out() {
    let scenario = Scenario::new("playground".parse().unwrap(), Mode::Live, Seed::new(1), []).unwrap();

    let written = json!({ "name": "playground", "mode": "live", "seed": 1 });
    assert_eq!(serde_json::to_value(&scenario).unwrap(), written);
    assert_eq!(serde_json::from_value::<Scenario>(written).unwrap(), scenario);
}

#[test]
fn reading_rejects_a_key_that_a_scenario_does_not_have() {
    let revision = json!({ "name": "flaky-orders", "mode": "chain", "seed": 42, "revision": 7 });
    let active = json!({ "name": "flaky-orders", "mode": "chain", "seed": 42, "active": true });

    for written in [revision, active] {
        let error = serde_json::from_value::<Scenario>(written.clone())
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field"), "{written}: {error}");
    }
}

#[test]
fn only_a_sequence_has_steps() {
    let chain = json!({ "name": "flaky-orders", "mode": "chain", "seed": 42, "steps": [] });
    let sequence = json!({ "name": "churn", "mode": "sequence", "seed": 7 });

    assert_eq!(
        serde_json::from_value::<Scenario>(chain).unwrap_err().to_string(),
        "scenario \"flaky-orders\" has steps, which only a scenario of mode `sequence` has"
    );
    assert_eq!(
        serde_json::from_value::<Scenario>(sequence).unwrap_err().to_string(),
        "scenario \"churn\" is a sequence without a step"
    );
}

#[test]
fn reading_checks_the_scenario_as_its_constructor_does() {
    let twice = json!({
        "name": "churn",
        "mode": "sequence",
        "seed": 7,
        "steps": [{ "name": "warm-up" }, { "name": "warm-up" }],
    });

    assert_eq!(
        serde_json::from_value::<Scenario>(twice).unwrap_err().to_string(),
        "scenario \"churn\" has two steps named \"warm-up\""
    );
}

#[test]
fn a_step_ends_on_one_condition_with_its_own_keys() {
    let both = json!({ "calls": 100, "selector": "order-fetches", "after": "20s" });
    let calls_without_selector = json!({ "calls": 100 });
    let rule_without_firings = json!({ "rule": "refuse-with-not-leader" });
    let unknown = json!({ "before": "20s" });

    for written in [both, calls_without_selector, rule_without_firings] {
        assert_eq!(
            serde_json::from_value::<Until>(written).unwrap_err().to_string(),
            "a step ends on one condition: `calls` with a `selector`, `firings` with a `rule`, or `after` a time"
        );
    }
    assert!(
        serde_json::from_value::<Until>(unknown)
            .unwrap_err()
            .to_string()
            .contains("unknown field `before`")
    );
}

#[test]
fn a_step_ends_on_the_calls_of_conditions_of_its_own() {
    let until = Until::Calls {
        count: Count::new(100).unwrap(),
        selector: Conditions::from(Condition::is(ApiKey::FETCH)).into(),
    };

    let written = serde_json::to_value(&until).unwrap();
    assert_eq!(written, json!({ "calls": 100, "selector": { "api": "Fetch" } }));
    assert_eq!(serde_json::from_value::<Until>(written).unwrap(), until);
}

#[test]
fn a_count_of_zero_ends_no_step() {
    let error = serde_json::from_value::<Until>(json!({ "firings": 0, "rule": "refuse-with-not-leader" }))
        .unwrap_err()
        .to_string();

    assert!(error.contains("a count is at least one"), "{error}");
}

#[test]
fn a_sequence_without_names_names_its_steps_by_their_place_and_its_rules_by_what_they_say() {
    // name: orders-survive-leader-churn
    // mode: sequence
    // seed: 7
    // steps:
    //   - until: { calls: 1, selector: { api: Produce } }
    //   - arm:
    //       - selector: { api: Produce }
    //         action: { lie: refusal, error: NOT_LEADER_OR_FOLLOWER }
    //     until: { firings: 2 }
    //   - title: Recovery
    let form = json!({
        "name": "orders-survive-leader-churn",
        "mode": "sequence",
        "seed": 7,
        "steps": [
            { "until": { "calls": 1, "selector": { "api": "Produce" } } },
            {
                "arm": [{ "selector": { "api": "Produce" }, "action": { "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" } }],
                "until": { "firings": 2 },
            },
            { "title": "Recovery" },
        ],
    });

    let scenario: Scenario = serde_json::from_value(form).unwrap();

    let steps: Vec<&str> = scenario.steps().iter().map(|step| step.name().as_str()).collect();
    assert_eq!(steps, ["step-1", "step-2", "step-3"]);
    let refuse: RuleName = "refuse-with-not-leader-or-follower-on-produce".parse().unwrap();
    assert_eq!(scenario.steps()[1].arm()[0].name(), &refuse);
    assert_eq!(
        scenario.steps()[1].until(),
        Some(&Until::Firings {
            count: Count::new(2).unwrap(),
            rule: refuse.clone()
        })
    );
    // The rule of the step belongs to the scenario, and names no rule of the workspace.
    let owned: Vec<&RuleName> = scenario.owned_rules().map(Rule::name).collect();
    assert_eq!(owned, [&refuse]);
    assert!(scenario.rule_references().is_empty());
}

#[test]
fn a_step_that_arms_several_rules_names_the_rule_whose_firings_end_it() {
    let form = json!({
        "name": "two-at-once",
        "mode": "sequence",
        "seed": 7,
        "steps": [{
            "arm": ["slow-order-producers", "refuse-with-not-leader"],
            "until": { "firings": 2 },
        }],
    });

    let error = serde_json::from_value::<Scenario>(form).unwrap_err().to_string();

    assert_eq!(
        error,
        "a step that ends on `firings` without a `rule` arms one rule, and this one arms 2"
    );
}

#[test]
fn a_rule_that_stands_the_same_in_two_steps_is_one_rule_and_two_that_say_the_same_differently_are_an_error() {
    let refuse =
        json!({ "selector": { "api": "Produce" }, "action": { "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" } });
    let refuse_once = json!({
        "selector": { "api": "Produce" },
        "action": { "lie": "refusal", "error": "NOT_LEADER_OR_FOLLOWER" },
        "trigger": { "first": 1 },
    });
    let twice = json!({
        "name": "churn-twice",
        "mode": "sequence",
        "seed": 7,
        "steps": [{ "arm": [refuse] }, {}, { "arm": [refuse] }],
    });
    let differently = json!({
        "name": "churn-differently",
        "mode": "sequence",
        "seed": 7,
        "steps": [{ "arm": [refuse] }, { "arm": [refuse_once] }],
    });

    let scenario: Scenario = serde_json::from_value(twice).unwrap();
    let error = serde_json::from_value::<Scenario>(differently).unwrap_err().to_string();

    assert_eq!(scenario.owned_rules().count(), 1);
    assert_eq!(
        error,
        "scenario \"churn-differently\" has two rules named \"refuse-with-not-leader-or-follower-on-produce\""
    );
}
