//! What a store of rules and a store of presets do, as checks that the tests of any such store run.
//!
//! [`RuleStore`] and [`PresetStore`] say what a store of rules and a store of presets are asked.
//! This crate holds that behaviour once: [`assert_rule_store`] and [`assert_preset_store`] take a way to make
//! an empty store, and fail when the store does not do what its trait asks.
//! Each runs the check of every store, [`assert_store_contract`], with rules or presets of its own.
//! The check of a store of rules then asks which rules use a selector or a preset.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_memory::MemoryStore;
//! use gaslightmq_rule_conformance::{assert_preset_store, assert_rule_store};
//!
//! // The test of a store hands the check a way to make an empty one, and drives it with the executor it has.
//! block_on(assert_rule_store(MemoryStore::new));
//! block_on(assert_preset_store(MemoryStore::new));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the rules, on the check of every store, and on what its rules and presets are made of:
//! `gaslightmq_selector`, `gaslightmq_lie` and `gaslightmq_kernel`.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_kernel::ApiKey;
use gaslightmq_lie::{HangUpLie, LieDefinition};
use gaslightmq_rule::{Preset, PresetName, PresetStore, Rule, RuleName, RuleStore};
use gaslightmq_selector::{Condition, Conditions, SelectorName};

/// Fails when a store of presets does not do what a [`PresetStore`] does.
///
/// `empty` makes a store that holds no preset, and the check asks for a new one for each behaviour.
/// A store of presets is asked nothing but the contract of every store,
/// so this is [`assert_store_contract`] with presets.
///
/// # Panics
///
/// When the store breaks the contract. The message says which behaviour it broke.
pub async fn assert_preset_store<S: PresetStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &presets()).await;
}

/// Fails when a store of rules does not do what a [`RuleStore`] does.
///
/// `empty` makes a store that holds no rule, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | The store keeps the contract of every store. | [`assert_store_contract`], with rules. |
/// | It gives the rules that use a selector. | In the order of their names, and no rule with inline conditions. |
/// | It gives the rules that use a preset. | In the order of their names, and no rule with an inline action. |
/// | A selector and a preset of one name are two things. | A rule that uses the one is not given for the other. |
/// | The answers follow every change. | An update and a delete change who uses a selector and a preset. |
///
/// # Panics
///
/// When the store does not do one of them. The message says which behaviour it broke.
pub async fn assert_rule_store<S: RuleStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &rules()).await;
    it_gives_the_rules_that_use_a_selector(&empty()).await;
    it_gives_the_rules_that_use_a_preset(&empty()).await;
    a_selector_and_a_preset_of_one_name_are_two_things(&empty()).await;
    the_answers_follow_every_change(&empty()).await;
}

async fn it_gives_the_rules_that_use_a_selector<S: RuleStore>(store: &S) {
    // The rule that sorts last comes first.
    created(store, slow_order_producers()).await;
    created(store, slow_fetches()).await;
    created(store, blackhole_metadata()).await;
    created(store, blackhole_order_producers()).await;

    assert_eq!(
        store.using_selector(&selector("order-producers")).await,
        Ok(vec![blackhole_order_producers(), slow_order_producers()]),
        "a store gives the rules that use a selector, in the order of their names"
    );
    assert_eq!(
        store.using_selector(&selector("fetches")).await,
        Ok(Vec::new()),
        "a store gives no rule for a selector that no rule names"
    );
}

async fn it_gives_the_rules_that_use_a_preset<S: RuleStore>(store: &S) {
    // The rule that sorts last comes first.
    created(store, slow_order_producers()).await;
    created(store, slow_fetches()).await;
    created(store, blackhole_metadata()).await;
    created(store, blackhole_order_producers()).await;

    assert_eq!(
        store.using_preset(&preset("delay-1s")).await,
        Ok(vec![slow_fetches(), slow_order_producers()]),
        "a store gives the rules that use a preset, in the order of their names"
    );
    assert_eq!(
        store.using_preset(&preset("blackhole")).await,
        Ok(Vec::new()),
        "a store gives no rule for a preset that no rule names"
    );
}

async fn a_selector_and_a_preset_of_one_name_are_two_things<S: RuleStore>(store: &S) {
    created(store, slow_order_producers()).await;

    assert_eq!(
        store.using_selector(&selector("delay-1s")).await,
        Ok(Vec::new()),
        "a rule that uses a preset does not use the selector of the same name"
    );
    assert_eq!(
        store.using_preset(&preset("order-producers")).await,
        Ok(Vec::new()),
        "a rule that uses a selector does not use the preset of the same name"
    );
}

async fn the_answers_follow_every_change<S: RuleStore>(store: &S) {
    created(store, slow_order_producers()).await;
    created(store, blackhole_order_producers()).await;

    // The rule keeps its preset, and takes conditions of its own instead of the selector.
    let inline = slow_order_producers().with_selector(on(ApiKey::PRODUCE));
    assert_eq!(
        store.update(inline.clone()).await,
        Ok(()),
        "update takes a rule whose name the store holds"
    );
    assert_eq!(
        store.using_selector(&selector("order-producers")).await,
        Ok(vec![blackhole_order_producers()]),
        "a rule that no longer names a selector is not given for it"
    );
    assert_eq!(
        store.using_preset(&preset("delay-1s")).await,
        Ok(vec![inline]),
        "a rule is given as the store holds it now"
    );

    assert_eq!(
        store.delete(blackhole_order_producers().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );
    assert_eq!(
        store.using_selector(&selector("order-producers")).await,
        Ok(Vec::new()),
        "a rule that the store let go of is not given"
    );
}

/// Creates a rule that a behaviour starts from.
async fn created<S: RuleStore>(store: &S, rule: Rule) {
    assert_eq!(
        store.create(rule).await,
        Ok(()),
        "create takes a rule whose name is free"
    );
}

/// The presets that the contract of every store is checked with:
/// a hang-up as someone wrote it, the same with a title, and a silence.
fn presets() -> Samples<Preset> {
    let hang_up = Preset::new(preset("hang-up"), HangUpLie::new());
    let titled = hang_up.clone().with_title("Hang up".parse().expect("a preset title"));
    let blackhole = Preset::new(preset("blackhole"), LieDefinition::Silence);
    Samples::new(hang_up, titled, blackhole)
}

/// The rules that the contract of every store is checked with:
/// a silence on every Metadata call as someone wrote it, the same with a title, and a silence on every Fetch.
fn rules() -> Samples<Rule> {
    let titled = blackhole_metadata().with_title("Blackhole Metadata".parse().expect("a rule title"));
    let fetches = Rule::new(name("blackhole-fetches"), on(ApiKey::FETCH), LieDefinition::Silence);
    Samples::new(blackhole_metadata(), titled, fetches)
}

fn name(name: &str) -> RuleName {
    name.parse().expect("a rule name")
}

fn selector(name: &str) -> SelectorName {
    name.parse().expect("a selector name")
}

fn preset(name: &str) -> PresetName {
    name.parse().expect("a preset name")
}

/// The conditions of a rule that names no selector: every call of one API.
fn on(api: ApiKey) -> Conditions {
    Conditions::new().with(Condition::is(api))
}

/// The rule `slow-order-producers`: the selector `order-producers` with the preset `delay-1s`.
fn slow_order_producers() -> Rule {
    Rule::new(
        name("slow-order-producers"),
        selector("order-producers"),
        preset("delay-1s"),
    )
}

/// The rule `blackhole-order-producers`: the selector `order-producers` with a silence of its own.
fn blackhole_order_producers() -> Rule {
    Rule::new(
        name("blackhole-order-producers"),
        selector("order-producers"),
        LieDefinition::Silence,
    )
}

/// The rule `slow-fetches`: conditions of its own with the preset `delay-1s`.
fn slow_fetches() -> Rule {
    Rule::new(name("slow-fetches"), on(ApiKey::FETCH), preset("delay-1s"))
}

/// The rule `blackhole-metadata`, which names no selector and no preset.
fn blackhole_metadata() -> Rule {
    Rule::new(name("blackhole-metadata"), on(ApiKey::METADATA), LieDefinition::Silence)
}
