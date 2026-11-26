//! What a store of scenarios does, as a check that the tests of any store of scenarios run.
//!
//! [`ScenarioStore`] says what a store of scenarios is asked.
//! This crate holds that behaviour once: [`assert_scenario_store`] takes a way to make an empty store,
//! and fails when the store does not do what its trait asks.
//! It runs the check of every store, [`assert_store_contract`], with scenarios of its own,
//! and then asks which scenarios name a rule, a selector or a preset.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_memory::MemoryStore;
//! use gaslightmq_scenario_conformance::assert_scenario_store;
//!
//! // The test of a store hands the check a way to make an empty one, and drives it with the executor it has.
//! block_on(assert_scenario_store(MemoryStore::new));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the scenarios, on the check of every store, and on what its scenarios are made of:
//! `gaslightmq_rule`, `gaslightmq_selector` and `gaslightmq_trigger`.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_rule::{PresetName, Rule, RuleName};
use gaslightmq_scenario::{Mode, Scenario, ScenarioName, ScenarioRule, ScenarioStore, Step, StepName, Until};
use gaslightmq_selector::SelectorName;
use gaslightmq_trigger::{Count, Seed};

/// Fails when a store of scenarios does not do what a [`ScenarioStore`] does.
///
/// `empty` makes a store that holds no scenario, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | The store keeps the contract of every store. | [`assert_store_contract`], with scenarios. |
/// | It gives the scenarios that name a rule. | In the order of their names: those that list the rule, and those with a step that arms it. No scenario that owns a rule of that name. |
/// | It gives the scenarios that name a selector. | In the order of their names: those that own a rule with the selector, and those with a step that ends on its calls. |
/// | It gives the scenarios that name a preset. | Those that own a rule with the preset as its action. |
/// | A rule, a selector and a preset of one name are three things. | A scenario that names the one is not given for the others. |
/// | The answers follow every change. | An update and a delete change who names a rule and a selector. |
///
/// # Panics
///
/// When the store does not do one of them. The message says which behaviour it broke.
pub async fn assert_scenario_store<S: ScenarioStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &scenarios()).await;
    it_gives_the_scenarios_that_name_a_rule(&empty()).await;
    it_gives_the_scenarios_that_name_a_selector(&empty()).await;
    it_gives_the_scenarios_that_name_a_preset(&empty()).await;
    a_rule_a_selector_and_a_preset_of_one_name_are_three_things(&empty()).await;
    the_answers_follow_every_change(&empty()).await;
}

async fn it_gives_the_scenarios_that_name_a_rule<S: ScenarioStore>(store: &S) {
    // The scenario that sorts last comes first.
    created(store, lists_the_refusal()).await;
    created(store, owns_the_refusal()).await;
    created(store, arms_the_refusal()).await;
    created(store, ends_on_order_fetches()).await;

    assert_eq!(
        store.using_rule(&rule("refuse-with-not-leader")).await,
        Ok(vec![arms_the_refusal(), lists_the_refusal()]),
        "a store gives the scenarios that list a rule or arm it in a step, in the order of their names, \
         and no scenario that owns a rule of that name"
    );
    assert_eq!(
        store.using_rule(&rule("slow-order-producers")).await,
        Ok(Vec::new()),
        "a store gives no scenario for a rule that no scenario names"
    );
}

async fn it_gives_the_scenarios_that_name_a_selector<S: ScenarioStore>(store: &S) {
    // The scenario that sorts last comes first.
    created(store, owns_the_refusal()).await;
    created(store, lists_the_refusal()).await;
    created(store, ends_on_order_fetches()).await;

    assert_eq!(
        store.using_selector(&selector("order-fetches")).await,
        Ok(vec![ends_on_order_fetches(), owns_the_refusal()]),
        "a store gives the scenarios that own a rule with a selector or end a step on its calls, \
         in the order of their names"
    );
    assert_eq!(
        store.using_selector(&selector("order-producers")).await,
        Ok(Vec::new()),
        "a store gives no scenario for a selector that no scenario names"
    );
}

async fn it_gives_the_scenarios_that_name_a_preset<S: ScenarioStore>(store: &S) {
    created(store, owns_the_refusal()).await;
    created(store, lists_the_refusal()).await;

    assert_eq!(
        store.using_preset(&preset("not-leader")).await,
        Ok(vec![owns_the_refusal()]),
        "a store gives the scenarios that own a rule with a preset as its action"
    );
    assert_eq!(
        store.using_preset(&preset("blackhole")).await,
        Ok(Vec::new()),
        "a store gives no scenario for a preset that no scenario names"
    );
}

async fn a_rule_a_selector_and_a_preset_of_one_name_are_three_things<S: ScenarioStore>(store: &S) {
    created(store, owns_the_refusal()).await;
    created(store, lists_the_refusal()).await;

    assert_eq!(
        store.using_selector(&selector("refuse-with-not-leader")).await,
        Ok(Vec::new()),
        "a scenario that names a rule does not name the selector of the same name"
    );
    assert_eq!(
        store.using_rule(&rule("order-fetches")).await,
        Ok(Vec::new()),
        "a scenario that names a selector does not name the rule of the same name"
    );
    assert_eq!(
        store.using_preset(&preset("order-fetches")).await,
        Ok(Vec::new()),
        "a scenario that names a selector does not name the preset of the same name"
    );
}

async fn the_answers_follow_every_change<S: ScenarioStore>(store: &S) {
    created(store, owns_the_refusal()).await;
    created(store, lists_the_refusal()).await;

    // The scenario names the refusal of the workspace, instead of owning a refusal of its own.
    let naming = chain("owns-the-refusal", [rule("refuse-with-not-leader").into()]);
    assert_eq!(
        store.update(naming.clone()).await,
        Ok(()),
        "update takes a scenario whose name the store holds"
    );
    assert_eq!(
        store.using_selector(&selector("order-fetches")).await,
        Ok(Vec::new()),
        "a scenario that no longer names a selector is not given for it"
    );
    assert_eq!(
        store.using_rule(&rule("refuse-with-not-leader")).await,
        Ok(vec![lists_the_refusal(), naming]),
        "a scenario is given as the store holds it now"
    );

    assert_eq!(
        store.delete(lists_the_refusal().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );
    assert_eq!(
        store.using_rule(&rule("refuse-with-not-leader")).await,
        Ok(vec![chain("owns-the-refusal", [rule("refuse-with-not-leader").into()])]),
        "a scenario that the store let go of is not given"
    );
}

/// Creates a scenario that a behaviour starts from.
async fn created<S: ScenarioStore>(store: &S, scenario: Scenario) {
    assert_eq!(
        store.create(scenario).await,
        Ok(()),
        "create takes a scenario whose name is free"
    );
}

/// The scenarios that the contract of every store is checked with:
/// a Chain as someone wrote it, the same as a Live scenario with a title, and a Sequence.
fn scenarios() -> Samples<Scenario> {
    let flaky = chain("flaky-orders", [rule("orders-sometimes-fail").into()]);
    let played = Scenario::new(
        name("flaky-orders"),
        Mode::Live,
        Seed::new(7),
        [
            rule("orders-sometimes-fail").into(),
            rule("order-fetches-are-slow").into(),
        ],
    )
    .expect("a scenario")
    .with_title("Flaky orders".parse().expect("a scenario title"));
    Samples::new(flaky, played, ends_on_order_fetches())
}

/// The scenario `lists-the-refusal`: a Chain that names the rule `refuse-with-not-leader` among its own.
fn lists_the_refusal() -> Scenario {
    chain("lists-the-refusal", [rule("refuse-with-not-leader").into()])
}

/// The scenario `owns-the-refusal`: a Chain that owns a rule `refuse-with-not-leader`,
/// on the selector `order-fetches` with the preset `not-leader`.
fn owns_the_refusal() -> Scenario {
    let owned = Rule::new(
        rule("refuse-with-not-leader"),
        selector("order-fetches"),
        preset("not-leader"),
    );
    chain("owns-the-refusal", [owned.into()])
}

/// The scenario `arms-the-refusal`: a Sequence with a step that arms the rule `refuse-with-not-leader`.
fn arms_the_refusal() -> Scenario {
    let churn = Step::new(step("leader-churn")).arming([rule("refuse-with-not-leader")]);
    sequence("arms-the-refusal", churn)
}

/// The scenario `ends-on-order-fetches`: a Sequence with a step that ends after 100 calls of the selector
/// `order-fetches`.
fn ends_on_order_fetches() -> Scenario {
    let calls = Until::Calls {
        count: Count::new(100).expect("a count"),
        selector: selector("order-fetches").into(),
    };
    sequence("ends-on-order-fetches", Step::new(step("warm-up")).ending_on(calls))
}

/// A Chain of that name with these rules, and the seed 42.
fn chain<const N: usize>(scenario: &str, rules: [ScenarioRule; N]) -> Scenario {
    Scenario::new(name(scenario), Mode::Chain, Seed::new(42), rules).expect("a scenario")
}

/// A Sequence of that name with one step, and the seed 7.
fn sequence(scenario: &str, step: Step) -> Scenario {
    Scenario::new(name(scenario), Mode::Sequence(vec![step]), Seed::new(7), []).expect("a scenario")
}

fn name(name: &str) -> ScenarioName {
    name.parse().expect("a scenario name")
}

fn step(name: &str) -> StepName {
    name.parse().expect("a step name")
}

fn rule(name: &str) -> RuleName {
    name.parse().expect("a rule name")
}

fn selector(name: &str) -> SelectorName {
    name.parse().expect("a selector name")
}

fn preset(name: &str) -> PresetName {
    name.parse().expect("a preset name")
}
