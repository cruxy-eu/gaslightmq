//! What a store of selectors does: the contract, and who includes a selector.

use gaslightmq_kernel::ApiKey;
use gaslightmq_selector::{Condition, Conditions, Selector, SelectorName, SelectorStore};

use crate::contract::created;
use crate::{Samples, assert_store_contract};

/// Fails when a store of selectors does not do what a [`SelectorStore`] does.
///
/// `empty` makes a store that holds no selector, and the check asks for a new one for each behaviour.
///
/// | Behaviour | The check |
/// |---|---|
/// | The store keeps the contract of every store. | [`assert_store_contract`], with [`Samples::selectors`]. |
/// | It gives the selectors that include a selector. | In the order of their names, and none that includes another one. |
/// | It need not hold the selector that is included. | A selector that includes what nobody defined is given too. |
/// | The answer follows every change. | An update and a delete change who includes a selector. |
///
/// # Panics
///
/// When the store does not do one of them. The message says which behaviour it broke.
pub async fn assert_selector_store<S: SelectorStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &Samples::selectors()).await;
    it_gives_the_selectors_that_include_a_selector(&empty()).await;
    it_need_not_hold_the_selector_that_is_included(&empty()).await;
    the_answer_follows_every_change(&empty()).await;
}

async fn it_gives_the_selectors_that_include_a_selector<S: SelectorStore>(store: &S) {
    // The selector that sorts last comes first.
    created(store, &from_ingest()).await;
    created(store, &fetches()).await;
    created(store, &order_producers()).await;
    created(store, &from_billing()).await;

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
    created(store, &from_ingest()).await;

    assert_eq!(
        store.using_selector(order_producers().name()).await,
        Ok(vec![from_ingest()]),
        "a store gives the selectors that include a selector which it does not hold"
    );
}

async fn the_answer_follows_every_change<S: SelectorStore>(store: &S) {
    created(store, &order_producers()).await;
    created(store, &from_ingest()).await;
    created(store, &from_billing()).await;

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
