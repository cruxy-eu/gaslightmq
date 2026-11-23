//! The prelude holds every item, and its names clash with none of `std` or of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that a host imports next to a backend:
/// the contract, the overlay, the environment, and the vocabulary of all three.
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
    use std::pin::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::task::*;
    use std::time::*;

    use gaslightmq_backend::prelude::*;
    use gaslightmq_backend_upstream::prelude::*;
    use gaslightmq_call::prelude::*;
    use gaslightmq_clock::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_overlay::prelude::*;
    use gaslightmq_wire::*;

    /// A backend that is named without a path, as the type a host starts. Starting one takes a cluster.
    #[expect(dead_code, reason = "the signature only has to compile next to the other preludes")]
    fn support_of(backend: &UpstreamBackend) -> IncidentSupport {
        backend.incident_support()
    }

    /// One value of every item of the prelude that needs no cluster, each named without a path.
    pub fn every_item() -> Vec<String> {
        let limit: ResponseSizeLimit = ResponseSizeLimit::new(1024).unwrap();
        let upstream = Upstream::new("kafka-1.staging:9092".parse().unwrap());
        let advertised_host: HostName = "gaslight.staging".parse().unwrap();
        let mut configuration: UpstreamBackendConfiguration =
            UpstreamBackendConfiguration::new(upstream, advertised_host);
        configuration.response_max_bytes = limit;
        configuration.request_timeout = TimeSpan::from_secs(5);

        vec![
            configuration.advertised_host.to_string(),
            configuration.response_max_bytes.to_string(),
            configuration.request_timeout.to_string(),
        ]
    }

    /// The one message of `error`, which is complete.
    fn message(error: &impl std::error::Error) -> String {
        assert!(std::error::Error::source(error).is_none(), "{error}");
        error.to_string()
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        let too_large: ResponseSizeLimitError = ResponseSizeLimit::new(usize::MAX).unwrap_err();
        let unsupported: UpstreamBackendError = UpstreamBackendError::UnsupportedTls;

        vec![message(&too_large), message(&unsupported)]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        ["gaslight.staging", "1024 bytes", "5s"]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    let errors = next_to_the_others::every_error();

    assert!(errors[0].starts_with("response size limit of "), "{}", errors[0]);
    assert_eq!(
        errors[1],
        "the environment asks for TLS to its cluster, and a proxy cannot connect to a cluster over TLS yet"
    );
}

#[test]
fn the_prelude_and_the_crate_root_export_the_same_types() {
    let from_root: gaslightmq_backend_upstream::ResponseSizeLimitError =
        gaslightmq_backend_upstream::ResponseSizeLimitError::Zero;
    let from_prelude: gaslightmq_backend_upstream::prelude::ResponseSizeLimitError = from_root.clone();
    assert_eq!(from_root, from_prelude);

    let limit: gaslightmq_backend_upstream::prelude::ResponseSizeLimit =
        gaslightmq_backend_upstream::ResponseSizeLimit::default();
    assert_eq!(limit, gaslightmq_backend_upstream::ResponseSizeLimit::default());
}
