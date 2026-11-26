//! What a store of environments does, as a check that the tests of any store of environments run.
//!
//! [`EnvironmentStore`] says what a store of environments is asked.
//! This crate holds that behaviour once: [`assert_environment_store`] takes a way to make an empty store,
//! and fails when the store does not do what the trait asks.
//! It runs the check of every store, [`assert_store_contract`], with environments of its own,
//! and asks nothing more, because nothing asks a store of environments more.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_memory::MemoryStore;
//! use gaslightmq_environment_conformance::assert_environment_store;
//!
//! // The test of a store hands the check a way to make an empty one, and drives it with the executor it has.
//! block_on(assert_environment_store(MemoryStore::new));
//! ```
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the environments, and on the check of every store.
//! It awaits the store it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};
use gaslightmq_environment::{Environment, EnvironmentStore, Instances, Listeners, ProxyDefinition, Upstream};

/// Fails when a store of environments does not do what an [`EnvironmentStore`] does.
///
/// `empty` makes a store that holds no environment, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
/// A store of environments is asked nothing but the contract of every store,
/// so this is [`assert_store_contract`] with environments.
///
/// # Panics
///
/// When the store breaks the contract. The message says which behaviour it broke.
pub async fn assert_environment_store<S: EnvironmentStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &samples()).await;
}

/// The environments that the contract of every store is checked with:
/// a proxy in front of the cluster of staging as someone wrote it, the same with a title,
/// and a proxy in front of a cluster on this machine.
fn samples() -> Samples<Environment> {
    let staging = proxy("staging-proxy", "gaslight.staging", "kafka-1.staging:9092");
    let titled = staging
        .clone()
        .with_title("Staging, behind the proxy".parse().expect("an environment title"));
    let local = proxy("local-proxy", "localhost", "localhost:9094");
    Samples::new(staging, titled, local)
}

/// A proxy that clients reach under `advertise`, in front of the cluster behind `bootstrap`.
fn proxy(name: &str, advertise: &str, bootstrap: &str) -> Environment {
    let listen = Listeners::new(
        "0.0.0.0:9092".parse().expect("a listen address"),
        "0.0.0.0:19092-19191".parse().expect("broker listeners"),
        advertise.parse().expect("a host name"),
    )
    .expect("a bootstrap port outside the ports of the brokers");
    Environment::new(
        name.parse().expect("an environment name"),
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen),
            upstream: Upstream::new(bootstrap.parse().expect("a server address")),
        },
    )
}
