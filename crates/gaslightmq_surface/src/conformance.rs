//! What a store of pad layouts does, as a check that the tests of any store of layouts run.
//!
//! The check is for the tests of a store, so it is behind the `conformance` feature.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_rule::PresetName;
use gaslightmq_selector::SelectorName;

use crate::{Layer, LayoutStore, LayoutUuid, PadLayout};

/// Fails when a store of pad layouts does not do what a [`LayoutStore`] does.
///
/// `empty` makes a store that holds no layout, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | The store keeps the contract of every store. | [`assert_store_contract`], with layouts. |
/// | It gives the layouts with a row for a selector. | In the order of their names, and none without such a row. |
/// | It gives the layouts with a column for a preset. | In the order of their names, and none without such a column. |
/// | A selector and a preset of one name are two things. | A layout with a row for the one is not given for the other. |
/// | The answers follow every change. | An update and a delete change who uses a selector and a preset. |
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_surface::conformance::assert_layout_store;
/// use gaslightmq_surface::prelude::*;
///
/// /// The test of a store of layouts: it hands the check a way to make an empty one.
/// async fn a_store_of_layouts_does_what_the_trait_asks<S: LayoutStore + Default>() {
///     assert_layout_store(S::default).await;
/// }
/// ```
///
/// # Panics
///
/// When the store does not do one of them. The message says which behaviour it broke.
pub async fn assert_layout_store<S: LayoutStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &samples()).await;
    it_gives_the_layouts_with_a_row_for_a_selector(&empty()).await;
    it_gives_the_layouts_with_a_column_for_a_preset(&empty()).await;
    a_selector_and_a_preset_of_one_name_are_two_things(&empty()).await;
    the_answers_follow_every_change(&empty()).await;
}

async fn it_gives_the_layouts_with_a_row_for_a_selector<S: LayoutStore>(store: &S) {
    // The layout that sorts last comes first.
    created(store, order_desk()).await;
    created(store, fetch_desk()).await;

    assert_eq!(
        store.using_selector(&selector("fetches")).await,
        Ok(vec![fetch_desk(), order_desk()]),
        "a store gives the layouts with a row for a selector, in the order of their names"
    );
    assert_eq!(
        store.using_selector(&selector("order-producers")).await,
        Ok(vec![order_desk()]),
        "a store gives no layout without a row for the selector"
    );
    assert_eq!(
        store.using_selector(&selector("metadata")).await,
        Ok(Vec::new()),
        "a store gives no layout for a selector that no layout has a row for"
    );
}

async fn it_gives_the_layouts_with_a_column_for_a_preset<S: LayoutStore>(store: &S) {
    // The layout that sorts last comes first.
    created(store, order_desk()).await;
    created(store, fetch_desk()).await;

    assert_eq!(
        store.using_preset(&preset("blackhole")).await,
        Ok(vec![fetch_desk(), order_desk()]),
        "a store gives the layouts with a column for a preset, in the order of their names"
    );
    assert_eq!(
        store.using_preset(&preset("delay-1s")).await,
        Ok(vec![order_desk()]),
        "a store gives no layout without a column for the preset"
    );
    assert_eq!(
        store.using_preset(&preset("hang-up")).await,
        Ok(Vec::new()),
        "a store gives no layout for a preset that no layout has a column for"
    );
}

async fn a_selector_and_a_preset_of_one_name_are_two_things<S: LayoutStore>(store: &S) {
    created(store, order_desk()).await;

    assert_eq!(
        store.using_selector(&selector("blackhole")).await,
        Ok(Vec::new()),
        "a layout with a column for a preset has no row for the selector of the same name"
    );
    assert_eq!(
        store.using_preset(&preset("fetches")).await,
        Ok(Vec::new()),
        "a layout with a row for a selector has no column for the preset of the same name"
    );
}

async fn the_answers_follow_every_change<S: LayoutStore>(store: &S) {
    created(store, order_desk()).await;
    created(store, fetch_desk()).await;

    // The order desk loses its row for the fetches and its column for the blackhole.
    let smaller = layout("order-desk", &["order-producers"], &["delay-1s"]);
    assert_eq!(
        store.update(smaller.clone()).await,
        Ok(()),
        "update takes a layout whose name the store holds"
    );
    assert_eq!(
        store.using_selector(&selector("fetches")).await,
        Ok(vec![fetch_desk()]),
        "a layout that no longer has a row for a selector is not given for it"
    );
    assert_eq!(
        store.using_preset(&preset("delay-1s")).await,
        Ok(vec![smaller]),
        "a layout is given as the store holds it now"
    );

    assert_eq!(
        store.delete(fetch_desk().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );
    assert_eq!(
        store.using_preset(&preset("blackhole")).await,
        Ok(Vec::new()),
        "a layout that the store let go of is not given"
    );
}

/// Creates a layout that a behaviour starts from.
async fn created<S: LayoutStore>(store: &S, layout: PadLayout) {
    assert_eq!(
        store.create(layout).await,
        Ok(()),
        "create takes a layout whose name is free"
    );
}

/// The layouts that the contract of every store is checked with:
/// the order desk as someone wrote it, the same with a UUID and a title, and the fetch desk.
fn samples() -> Samples<PadLayout> {
    /// The time someone created the layout, in milliseconds since the Unix epoch: 11 December 2024.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of its UUID.
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

    let titled = order_desk()
        .with_uuid(LayoutUuid::new(CREATED, RANDOM))
        .with_title("Order desk".parse().expect("a layout title"));
    Samples::new(order_desk(), titled, fetch_desk())
}

fn selector(name: &str) -> SelectorName {
    name.parse().expect("a selector name")
}

fn preset(name: &str) -> PresetName {
    name.parse().expect("a preset name")
}

/// A layout with one layer, a row for each of those selectors and a column for each of those presets.
fn layout(name: &str, rows: &[&str], columns: &[&str]) -> PadLayout {
    PadLayout::new(
        name.parse().expect("a layout name"),
        rows.iter().copied().map(selector),
        columns.iter().copied().map(preset),
        [Layer::all_clients()],
    )
    .expect("a layout with one layer, and no row or column twice")
}

/// The layout `order-desk`: rows for `order-producers` and `fetches`, columns for `delay-1s` and `blackhole`.
fn order_desk() -> PadLayout {
    layout(
        "order-desk",
        &["order-producers", "fetches"],
        &["delay-1s", "blackhole"],
    )
}

/// The layout `fetch-desk`: a row for `fetches`, and a column for `blackhole`.
fn fetch_desk() -> PadLayout {
    layout("fetch-desk", &["fetches"], &["blackhole"])
}
