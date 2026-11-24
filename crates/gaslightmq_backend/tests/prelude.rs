//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// the Kafka codec, and the preludes of the crates that code imports next to a backend:
/// the vocabulary of its messages, and what an engine drives next to it.
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
    use gaslightmq_call::prelude::*;
    use gaslightmq_clock::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_pipeline::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_session::prelude::*;
    use gaslightmq_wire::*;

    /// A backend that is named without a path, as a bound.
    fn support_of(backend: &impl Backend) -> IncidentSupport {
        backend.incident_support()
    }

    /// A backend that answers nothing and supports nothing.
    pub struct Silent;

    impl Backend for Silent {
        async fn call(&self, _request: BackendRequest) -> BackendAnswer {
            pending().await
        }

        async fn close(&self, _connection: ConnectionNumber) {}

        async fn endpoints(&self, _known: &Endpoints) -> Endpoints {
            pending().await
        }

        // This backend sends no client anywhere, so the method returns a future that is ready.
        fn listens(
            &self,
            _endpoint: Endpoint,
            _address: SocketAddr,
        ) -> impl Future<Output = std::result::Result<(), ListenerRejection>> + Send {
            ready(Ok(()))
        }

        fn incident_support(&self) -> IncidentSupport {
            IncidentSupport::none()
        }

        // This backend rejects at once, so the method returns a future that is ready.
        fn apply(
            &self,
            incident: Incident,
        ) -> impl Future<Output = std::result::Result<ExpandedIncident, IncidentRejection>> + Send {
            let unsupported = self.incident_support().check(&Requirements::from(&incident));
            ready(Err(unsupported.unwrap_err().into()))
        }
    }

    /// One value of every item of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let heartbeat = gaslightmq_wire::Request::new(
            4,
            41,
            Some("order-ingest"),
            &gaslightmq_wire::messages::HeartbeatRequest::default(),
        )
        .unwrap();
        let response = heartbeat
            .response(&gaslightmq_wire::messages::HeartbeatResponse::default())
            .unwrap();

        let request: BackendRequest =
            BackendRequest::new(CallId::FIRST, ConnectionNumber::new(7), Endpoint::Bootstrap, heartbeat);
        let truth: Truth = Truth::new(response);
        let failure: BackendFailure = BackendFailure::NoSuchEndpoint {
            endpoint: request.endpoint(),
        };
        let answer: BackendAnswer = BackendAnswer::Failed(failure.clone());
        let endpoints: Endpoints = Endpoints::none().with(request.endpoint());
        let rejection: IncidentRejection = IncidentRejection::Impossible {
            incident: IncidentKind::Rebalance,
            reason: "the cluster has no group payments".to_owned(),
        };

        let unadvertisable: ListenerRejection = ListenerRejection::Unadvertisable {
            endpoint: request.endpoint(),
            address: "127.0.0.1:0".parse().unwrap(),
            reason: "the listener did not say which port it got".to_owned(),
        };

        vec![
            format!("{:?}", support_of(&Silent)),
            request.call().to_string(),
            truth.response().correlation_id().to_string(),
            failure.to_string(),
            format!("{answer:?}"),
            endpoints.len().to_string(),
            rejection.to_string(),
            unadvertisable.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 8);
    assert!(items.iter().all(|item| !item.is_empty()));
    assert_eq!(
        items[1..4],
        ["1", "41", "the cluster takes no requests on endpoint `bootstrap`"]
    );
    assert_eq!(items[6], "rebalance cannot happen: the cluster has no group payments");
    assert_eq!(
        items[7],
        "clients cannot be told that endpoint `bootstrap` listens on 127.0.0.1:0: \
         the listener did not say which port it got"
    );
}

#[test]
fn the_prelude_is_enough_to_ask_a_backend_for_what_it_supports() {
    use gaslightmq_backend::prelude::*;

    fn rejects(backend: &impl Backend, requirements: &gaslightmq_incident::Requirements) -> bool {
        backend.incident_support().check(requirements).is_err()
    }

    let backend = next_to_the_others::Silent;
    let rebalance = gaslightmq_incident::IncidentDefinition::Rebalance { group: None };

    // A scenario that only lies runs on every backend, and one that needs an incident does not run on this one.
    assert!(!rejects(&backend, &gaslightmq_incident::Requirements::none()));
    assert!(rejects(&backend, &gaslightmq_incident::Requirements::from(&rebalance)));
}
