//! What a store of selectors does, as a check that the tests of any store of selectors run.
//!
//! [`SelectorStore`] says what a store of selectors is asked.
//! This crate holds that behaviour once: [`assert_selector_store`] takes a way to make an empty store,
//! and fails when the store does not do what the trait asks.
//! It runs the check of every store, [`assert_store_contract`], with selectors of its own,
//! and then asks who includes a selector.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_memory::MemoryStore;
//! use gaslightmq_selector_conformance::assert_selector_store;
//!
//! // The test of a store hands the check a way to make an empty one, and drives it with the executor it has.
//! block_on(assert_selector_store(MemoryStore::new));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the selectors, on the check of every store, and on `gaslightmq_kernel` for the API keys
//! that its selectors test.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_kernel::ApiKey;
use gaslightmq_selector::{Condition, Conditions, Selector, SelectorName, SelectorStore, SelectorUuid};

/// Fails when a store of selectors does not do what a [`SelectorStore`] does.
///
/// `empty` makes a store that holds no selector, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | The store keeps the contract of every store. | [`assert_store_contract`], with selectors. |
/// | It gives the selectors that include a selector. | In the order of their names, and none that includes another one. |
/// | It need not hold the selector that is included. | A selector that includes what nobody defined is given too. |
/// | The answer follows every change. | An update and a delete change who includes a selector. |
///
/// # Panics
///
/// When the store does not do one of them. The message says which behaviour it broke.
pub async fn assert_selector_store<S: SelectorStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &samples()).await;
    it_gives_the_selectors_that_include_a_selector(&empty()).await;
    it_need_not_hold_the_selector_that_is_included(&empty()).await;
    the_answer_follows_every_change(&empty()).await;
}

async fn it_gives_the_selectors_that_include_a_selector<S: SelectorStore>(store: &S) {
    // The selector that sorts last comes first.
    created(store, from_ingest()).await;
    created(store, fetches()).await;
    created(store, order_producers()).await;
    created(store, from_billing()).await;

    assert_eq!(
        store.using_selector(order_producers().name()).await,
        Ok(vec![from_billing(), from_ingest()]),
        "a store gives the selectors that include a selector, in the order of their names"
    );
    assert_eq!(
        store.using_selector(fetches().name()).await,
        Ok(Vec::new()),
        "a store gives no selector for one that nothing includes"
    );
}

async fn it_need_not_hold_the_selector_that_is_included<S: SelectorStore>(store: &S) {
    created(store, from_ingest()).await;

    assert_eq!(
        store.using_selector(order_producers().name()).await,
        Ok(vec![from_ingest()]),
        "a store gives the selectors that include a selector which it does not hold"
    );
}

async fn the_answer_follows_every_change<S: SelectorStore>(store: &S) {
    created(store, order_producers()).await;
    created(store, from_ingest()).await;
    created(store, from_billing()).await;

    assert_eq!(
        store.update(from_ingest().not_including()).await,
        Ok(()),
        "update takes a selector whose name the store holds"
    );
    assert_eq!(
        store.using_selector(order_producers().name()).await,
        Ok(vec![from_billing()]),
        "a selector that no longer includes another one is not given for it"
    );

    assert_eq!(
        store.delete(from_billing().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );
    assert_eq!(
        store.using_selector(order_producers().name()).await,
        Ok(Vec::new()),
        "a selector that the store let go of is not given"
    );
}

/// Creates a selector that a behaviour starts from.
async fn created<S: SelectorStore>(store: &S, selector: Selector) {
    assert_eq!(
        store.create(selector).await,
        Ok(()),
        "create takes a selector whose name is free"
    );
}

/// The selectors that the contract of every store is checked with:
/// the producers of orders as someone wrote them, the same with a UUID and a title, and every fetch.
fn samples() -> Samples<Selector> {
    /// The time someone created the selector, in milliseconds since the Unix epoch: 11 December 2024.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of its UUID.
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

    let titled = order_producers()
        .with_uuid(SelectorUuid::new(CREATED, RANDOM))
        .with_title("Order producers".parse().expect("a selector title"));
    Samples::new(order_producers(), titled, fetches())
}

fn name(name: &str) -> SelectorName {
    name.parse().expect("a selector name")
}

/// The selector `order-producers`: every Produce. It includes nothing.
fn order_producers() -> Selector {
    Selector::new(
        name("order-producers"),
        Conditions::new().with(Condition::is(ApiKey::PRODUCE)),
    )
}

/// The selector `fetches`: every Fetch. It includes nothing, and nothing includes it.
fn fetches() -> Selector {
    Selector::new(name("fetches"), Conditions::new().with(Condition::is(ApiKey::FETCH)))
}

/// The selector `order-producers-from-ingest`, which includes `order-producers`.
fn from_ingest() -> Selector {
    Selector::new(name("order-producers-from-ingest"), Conditions::new()).including(name("order-producers"))
}

/// The selector `order-producers-from-billing`, which includes `order-producers` too.
fn from_billing() -> Selector {
    Selector::new(name("order-producers-from-billing"), Conditions::new()).including(name("order-producers"))
}
