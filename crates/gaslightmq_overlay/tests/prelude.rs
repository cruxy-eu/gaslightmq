//! The prelude holds every item, and its names clash with none of `std` or of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the kernel, the fields, the incidents, the surgery and the backend contract.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
///
/// `gaslightmq_wire` has no prelude, and its `messages` hold a `TopicName` and a `BrokerId` of their own.
/// The request and the response therefore come by name.
mod next_to_the_others {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
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
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_overlay::prelude::*;
    use gaslightmq_wire_edit::prelude::*;

    use gaslightmq_wire::messages::{FetchRequest, HeartbeatRequest, HeartbeatResponse, MetadataRequest};
    use gaslightmq_wire::{Api, Request, Response};

    /// One value of every item of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let address: BrokerAddress = "gaslight.staging:19093".parse().unwrap();
        let advertised: AdvertisedAddresses = AdvertisedAddresses::new()
            .with(BrokerId::from(2), address.clone())
            .unwrap();
        let mut overlay: Overlay = Overlay::new(advertised.clone());

        let request: Request = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default()).unwrap();
        let forwarding: Forwarding = overlay.forward(Endpoint::Bootstrap, &request).unwrap();
        let Forwarding::Forward { to, .. } = &forwarding else {
            panic!("a Heartbeat is forwarded");
        };
        let destination: Destination = to.clone();
        let upstream: Response = request.response(&HeartbeatResponse::default()).unwrap();
        let truth: Truth = overlay.answer(&request, upstream).unwrap();
        let support: IncidentSupport = Overlay::incident_support();

        vec![
            address.to_string(),
            advertised.len().to_string(),
            forwarding.to_string(),
            destination.to_string(),
            truth.upstream().is_some().to_string(),
            support.support(IncidentKind::BrokerGoesDown).to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let address: BrokerAddress = "gaslight.staging:19093".parse().unwrap();
        let fetch = Request::new(12, 7, None, &FetchRequest::default()).unwrap();
        let unanswered = EditableRequest::decode(&fetch, &TopicNames::new())
            .unwrap()
            .error_response(&Failures::none())
            .unwrap_err();
        let of_another_api = fetch.body::<MetadataRequest>().unwrap_err();

        vec![
            message_of(BrokerAddressError::PortZero),
            message_of(AdvertisedAddressesError::SharedAddress {
                address,
                broker: BrokerId::from(3),
                advertised_there: BrokerId::from(2),
            }),
            message_of(ForwardError::UnknownBroker {
                broker: BrokerId::from(9),
            }),
            message_of(ForwardError::Undecided {
                reason: format!("the Fetch v12 request cannot be answered without the cluster: {unanswered}"),
            }),
            message_of(AnswerError::NotAnAnswer {
                reason: "the Heartbeat v4 response 7 does not answer the Metadata v12 request 7".to_owned(),
            }),
            message_of(AnswerError::UnwritableTruth {
                api: Api::from_wire(3, 12),
                reason: of_another_api.to_string(),
            }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        [
            "gaslight.staging:19093",
            "1",
            "forward Heartbeat v4 to any broker",
            "any broker",
            "false",
            "unsupported",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "the port of a broker address cannot be zero",
            "broker 3 cannot be advertised at gaslight.staging:19093: broker 2 is advertised there",
            "a request for broker 9 has nowhere to go: the cluster named no such broker",
            "the Fetch v12 request cannot be answered without the cluster: nothing that the Fetch v12 request asks failed",
            "the Heartbeat v4 response 7 does not answer the Metadata v12 request 7",
            "the truth of the Metadata v12 response cannot be written: \
             cannot decode request body: it is of api key 1, and it was read as api key 3",
        ]
    );
}

#[test]
fn the_prelude_and_the_crate_root_export_the_same_types() {
    let from_root: gaslightmq_overlay::BrokerAddressError =
        "kafka-2".parse::<gaslightmq_overlay::BrokerAddress>().unwrap_err();
    let from_prelude: gaslightmq_overlay::prelude::BrokerAddressError = from_root.clone();

    assert_eq!(from_root, from_prelude);
}
