//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that code imports next to a host.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::future::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_backend::prelude::*;
    use gaslightmq_backend_upstream::prelude::*;
    use gaslightmq_definition_api::prelude::*;
    use gaslightmq_engine::prelude::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_host::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_quick_inject::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use gaslightmq_workspace::prelude::*;

    /// One value of every item of the prelude that is no error, each named without a path.
    pub fn every_item() -> Vec<String> {
        let listen = Listeners::new(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:19092-19191".parse().unwrap(),
            "localhost".parse().unwrap(),
        );
        let staging = Environment::new(
            "staging-proxy".parse().unwrap(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listen.unwrap()),
                upstream: Upstream::new("kafka-1.staging:9092".parse().unwrap()),
            },
        );
        let configuration: HostConfiguration = HostConfiguration::default();
        let environment: EnvironmentConfiguration = EnvironmentConfiguration::new(&staging).unwrap();
        let host: Host<MemoryContext> = Host::new(configuration.clone(), MemoryContext::new(), Lies::of_every_build());
        vec![
            format!("{configuration:?}"),
            environment.name().to_string(),
            format!("{host:?}"),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        let staging: EnvironmentName = "staging-proxy".parse().unwrap();
        let unsupported: EnvironmentConfigurationError = EnvironmentConfigurationError::UnsupportedBroker {
            name: "orders-sim".parse().unwrap(),
        };
        let running: EnvironmentStartError = EnvironmentStartError::AlreadyRunning { name: staging.clone() };
        let not_running: NotRunningError = NotRunningError { name: staging.clone() };
        let armed: ArmRuleError = ArmRuleError::AlreadyArmed {
            environment: staging,
            rule: "blackhole".parse().unwrap(),
        };
        let unread: ReadSentenceError = ReadSentenceError::StoreFailed {
            reason: "the preset store could not be read: disk full".to_owned(),
        };
        // A message says everything, so no error has a source.
        assert!(std::error::Error::source(&unsupported).is_none());
        assert!(std::error::Error::source(&running).is_none());
        assert!(std::error::Error::source(&not_running).is_none());
        assert!(std::error::Error::source(&armed).is_none());
        assert!(std::error::Error::source(&unread).is_none());
        vec![
            unsupported.to_string(),
            running.to_string(),
            not_running.to_string(),
            armed.to_string(),
            unread.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 3);
    assert_eq!(items[1], "staging-proxy");
    assert_eq!(items[2], "Host { running: [], .. }");
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "environment \"orders-sim\" runs a broker, and a host cannot run a broker yet",
            "environment \"staging-proxy\" runs already",
            "environment \"staging-proxy\" does not run",
            "rule \"blackhole\" is armed already in environment \"staging-proxy\"",
            "the names of the workspace could not be read: the preset store could not be read: disk full",
        ]
    );
}
