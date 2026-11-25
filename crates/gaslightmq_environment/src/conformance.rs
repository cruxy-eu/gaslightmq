//! What a store of environments does, as a check that the tests of any store of environments run.
//!
//! The check is for the tests of a store, so it is behind the `conformance` feature.

use gaslightmq_definition_conformance::{Samples, assert_store_contract};

use crate::{Environment, EnvironmentStore, EnvironmentUuid, Instances, Listeners, ProxyDefinition, Upstream};

/// Fails when a store of environments does not do what an [`EnvironmentStore`] does.
///
/// `empty` makes a store that holds no environment, and the check asks for a new one for each behaviour.
/// A store of files hands out a new directory each time.
/// A store of environments is asked nothing but the contract of every store,
/// so this is [`assert_store_contract`] with environments.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_environment::conformance::assert_environment_store;
/// use gaslightmq_environment::prelude::*;
///
/// /// The test of a store of environments: it hands the check a way to make an empty one.
/// async fn a_store_of_environments_does_what_the_trait_asks<S: EnvironmentStore + Default>() {
///     assert_environment_store(S::default).await;
/// }
/// ```
///
/// # Panics
///
/// When the store breaks the contract. The message says which behaviour it broke.
pub async fn assert_environment_store<S: EnvironmentStore>(empty: impl Fn() -> S) {
    assert_store_contract(&empty, &samples()).await;
}

/// The environments that the contract of every store is checked with:
/// a proxy in front of the cluster of staging as someone wrote it, the same with a UUID and a title,
/// and a proxy in front of a cluster on this machine.
fn samples() -> Samples<Environment> {
    /// The time someone created the environment, in milliseconds since the Unix epoch: 11 December 2024.
    const CREATED: u64 = 0x0193_b6c2_5f3e;
    /// The random bits of its UUID.
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

    let staging = proxy("staging-proxy", "gaslight.staging", "kafka-1.staging:9092");
    let titled = staging
        .clone()
        .with_uuid(EnvironmentUuid::new(CREATED, RANDOM))
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
