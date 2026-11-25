//! What a store of rules and a store of presets do, as checks that the tests of any such store run.
//!
//! The checks are for the tests of a store, so they are behind the `conformance` feature.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_kernel::ApiKey;
use gaslightmq_lie::{HangUpLie, LieDefinition};
use gaslightmq_selector::{Condition, Conditions, SelectorName};

use crate::{Preset, PresetName, PresetStore, PresetUuid, Rule, RuleName, RuleStore, RuleUuid};

/// The time someone created the definition that has a UUID, in milliseconds since the Unix epoch: 11 December 2024.
const CREATED: u64 = 0x0193_b6c2_5f3e;

/// The random bits of that UUID.
const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

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
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_rule::conformance::assert_rule_store;
/// use gaslightmq_rule::prelude::*;
///
/// /// The test of a store of rules: it hands the check a way to make an empty one.
/// async fn a_store_of_rules_does_what_the_trait_asks<S: RuleStore + Default>() {
///     assert_rule_store(S::default).await;
/// }
/// ```
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
/// a hang-up as someone wrote it, the same with a UUID and a title, and a silence.
fn presets() -> Samples<Preset> {
    let hang_up = Preset::new(preset("hang-up"), HangUpLie::new());
    let titled = hang_up
        .clone()
        .with_uuid(PresetUuid::new(CREATED, RANDOM))
        .with_title("Hang up".parse().expect("a preset title"));
    let blackhole = Preset::new(preset("blackhole"), LieDefinition::Silence);
    Samples::new(hang_up, titled, blackhole)
}

/// The rules that the contract of every store is checked with:
/// a silence on every Metadata call as someone wrote it, the same with a UUID and a title, and a silence on every Fetch.
fn rules() -> Samples<Rule> {
    let titled = blackhole_metadata()
        .with_uuid(RuleUuid::new(CREATED, RANDOM))
        .with_title("Blackhole Metadata".parse().expect("a rule title"));
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
