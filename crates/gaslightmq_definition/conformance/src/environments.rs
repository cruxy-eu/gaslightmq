//! What a store of environments does: the contract.

use gaslightmq_environment::EnvironmentStore;

use crate::{Samples, assert_store_contract};

/// Fails when a store of environments does not do what an [`EnvironmentStore`] does.
///
/// `empty` makes a store that holds no environment, and the check asks for a new one for each behaviour.
/// A store of environments is asked nothing but the contract of every store,
/// so this is [`assert_store_contract`] with [`Samples::environments`].
///
/// # Panics
///
/// When the store breaks the contract. The message says which behaviour it broke.
pub async fn assert_environment_store<S: EnvironmentStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &Samples::environments()).await;
}
