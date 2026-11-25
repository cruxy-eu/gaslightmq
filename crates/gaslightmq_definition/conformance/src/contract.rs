//! What the store contract asks of every store, whatever kind of definition it holds.

use std::fmt;

use gaslightmq_definition_api::{Definition, Store, StoreChangeError, StoreCreateError};

use crate::Samples;

/// Fails when a store does not do what the [contract](Store) asks of every store.
///
/// `empty` makes a store that holds no definition, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
///
/// | Behaviour | The check |
/// |---|---|
/// | A new store holds nothing. | Lists nothing, and gets nothing. |
/// | Create takes a definition. | Gets it back as it went in, and lists it. |
/// | Create refuses a name that is taken. | The definition that was there stays as it is. |
/// | List gives every definition. | In the order of their names, in whichever order they came. |
/// | Update replaces the definition of its name. | Gets the new one back as it went in, and no other definition changed. |
/// | Update needs a name that exists. | Nothing is created. |
/// | Delete lets go of a definition. | No other definition goes with it. |
/// | Delete needs a name that exists. | Nothing changed. |
/// | A name is free again after a delete. | Create takes a definition of that name. |
///
/// The check of a kind, such as [`assert_rule_store`](crate::assert_rule_store), runs this one with the samples
/// of its kind.
///
/// # Panics
///
/// When the store breaks the contract. The message says which behaviour it broke.
pub async fn assert_store_contract<D, S>(empty: impl Fn() -> S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    a_new_store_holds_nothing(&empty(), samples).await;
    create_takes_a_definition(&empty(), samples).await;
    create_refuses_a_name_that_is_taken(&empty(), samples).await;
    list_gives_every_definition_in_the_order_of_the_names(&empty(), samples).await;
    update_replaces_the_definition_of_its_name(&empty(), samples).await;
    update_needs_a_name_that_exists(&empty(), samples).await;
    delete_lets_go_of_a_definition(&empty(), samples).await;
    delete_needs_a_name_that_exists(&empty(), samples).await;
    a_name_is_free_again_after_a_delete(&empty(), samples).await;
}

async fn a_new_store_holds_nothing<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    assert!(listed(store).await.is_empty(), "a new store lists nothing");
    assert_eq!(
        got(store, samples.definition()).await,
        None,
        "a new store has no definition of any name"
    );
}

async fn create_takes_a_definition<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    // The changed definition has a UUID and a title, so it shows that a store keeps every part.
    let definition = samples.changed();

    assert_eq!(
        store.create(definition.clone()).await,
        Ok(()),
        "create takes a definition whose name is free"
    );

    assert_eq!(
        got(store, definition).await.as_ref(),
        Some(definition),
        "a definition comes back as it went in"
    );
    assert_eq!(
        listed(store).await,
        std::slice::from_ref(definition),
        "a store lists the definition it took"
    );
}

async fn create_refuses_a_name_that_is_taken<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.definition()).await;

    assert_eq!(
        store.create(samples.changed().clone()).await,
        Err(StoreCreateError::NameTaken),
        "create refuses a name that is taken"
    );

    assert_eq!(
        listed(store).await,
        [samples.definition().clone()],
        "a create that was refused overwrote nothing"
    );
}

async fn list_gives_every_definition_in_the_order_of_the_names<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    // The definition that sorts last comes first.
    created(store, samples.definition()).await;
    created(store, samples.other()).await;

    assert_eq!(
        listed(store).await,
        [samples.other().clone(), samples.definition().clone()],
        "a store lists its definitions in the order of their names"
    );
}

async fn update_replaces_the_definition_of_its_name<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.definition()).await;
    created(store, samples.other()).await;

    assert_eq!(
        store.update(samples.changed().clone()).await,
        Ok(()),
        "update takes a definition whose name the store holds"
    );

    assert_eq!(
        got(store, samples.definition()).await.as_ref(),
        Some(samples.changed()),
        "an update replaces the definition as a whole"
    );
    assert_eq!(
        got(store, samples.other()).await.as_ref(),
        Some(samples.other()),
        "an update changes no other definition"
    );
}

async fn update_needs_a_name_that_exists<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.other()).await;

    assert_eq!(
        store.update(samples.definition().clone()).await,
        Err(StoreChangeError::NotFound),
        "update refuses a name that the store does not hold"
    );

    assert_eq!(
        listed(store).await,
        [samples.other().clone()],
        "an update that was refused created nothing"
    );
}

async fn delete_lets_go_of_a_definition<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.definition()).await;
    created(store, samples.other()).await;

    assert_eq!(
        store.delete(samples.definition().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );

    assert_eq!(
        got(store, samples.definition()).await,
        None,
        "a store no longer has a definition that it deleted"
    );
    assert_eq!(
        listed(store).await,
        [samples.other().clone()],
        "a delete lets go of no other definition"
    );
}

async fn delete_needs_a_name_that_exists<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.other()).await;

    assert_eq!(
        store.delete(samples.definition().name()).await,
        Err(StoreChangeError::NotFound),
        "delete refuses a name that the store does not hold"
    );

    assert_eq!(
        listed(store).await,
        [samples.other().clone()],
        "a delete that was refused changed nothing"
    );
}

async fn a_name_is_free_again_after_a_delete<D, S>(store: &S, samples: &Samples<D>)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    created(store, samples.definition()).await;
    assert_eq!(
        store.delete(samples.definition().name()).await,
        Ok(()),
        "delete takes a name that the store holds"
    );

    assert_eq!(
        store.create(samples.changed().clone()).await,
        Ok(()),
        "create takes a name that a delete set free"
    );
    assert_eq!(
        listed(store).await,
        [samples.changed().clone()],
        "a store holds what was created last under the name"
    );
}

/// Creates a definition that a behaviour starts from.
pub(crate) async fn created<D, S>(store: &S, definition: &D)
where
    D: Definition + PartialEq + fmt::Debug,
    S: Store<D>,
{
    assert_eq!(
        store.create(definition.clone()).await,
        Ok(()),
        "create takes a definition whose name is free"
    );
}

/// Every definition of a store that reads.
async fn listed<D: Definition, S: Store<D>>(store: &S) -> Vec<D> {
    store.list().await.expect("a store lists what the check stored")
}

/// What a store that reads has under the name of `definition`.
async fn got<D: Definition, S: Store<D>>(store: &S, definition: &D) -> Option<D> {
    store
        .get(definition.name())
        .await
        .expect("a store looks for a name among what the check stored")
}
