//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the kernel and of the fields.
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

    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse, MetadataRequest};
    use gaslightmq_wire::{Api, Response};
    use gaslightmq_wire_edit::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let topics: TopicNames = TopicNames::new();
        let failures: Failures = Failures::whole_call(ErrorCode::REQUEST_TIMED_OUT);
        let request = gaslightmq_wire::Request::new(4, 7, None, &HeartbeatRequest::default()).unwrap();
        let asked: EditableRequest = EditableRequest::decode(&request, &topics).unwrap();
        let remainder: Remainder = asked.without([]).unwrap();
        let failed: HeartbeatResponse = asked.error_response(&failures).unwrap().body().unwrap();

        vec![
            topics.len().to_string(),
            failures.to_string(),
            asked.api().to_string(),
            format!("{remainder:?}"),
            failed.error_code.to_string(),
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

        // What the codec says: a Heartbeat is no Metadata request, and it has no version 5.
        let heartbeat = gaslightmq_wire::Request::new(4, 7, None, &HeartbeatRequest::default()).unwrap();
        let api = heartbeat.api();
        let of_another_api = heartbeat.body::<MetadataRequest>().unwrap_err();
        let no_such_request = gaslightmq_wire::Request::new(5, 7, None, &HeartbeatRequest::default()).unwrap_err();
        let no_such_response = Response::new(Api::from_wire(api.key, 5), 7, &HeartbeatResponse::default()).unwrap_err();

        vec![
            message_of(SurgeryError::NothingFailed { api }),
            message_of(SurgeryError::UnreadableRequest {
                api,
                reason: of_another_api.to_string(),
            }),
            message_of(SurgeryError::UnwritableRequest {
                api,
                reason: no_such_request.to_string(),
            }),
            message_of(SurgeryError::UnwritableResponse {
                api,
                reason: no_such_response.to_string(),
            }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        ["0", "call: REQUEST_TIMED_OUT", "Heartbeat v4", "Whole", "7"]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    let [nothing_failed, unreadable, unwritable_request, unwritable_response] = &next_to_the_others::every_error()[..]
    else {
        panic!("four messages of the one error of the prelude are checked");
    };

    assert_eq!(nothing_failed, "nothing that the Heartbeat v4 request asks failed");
    assert_eq!(
        unreadable,
        "the Heartbeat v4 request cannot be read: \
         cannot decode request body: it is of api key 12, and it was read as api key 3"
    );
    // What stands in the way of a body that does not encode is the text of the codec.
    let request_unwritable = "what is left of the Heartbeat v4 request cannot be written: cannot encode request body: ";
    assert!(
        unwritable_request.starts_with(request_unwritable),
        "{unwritable_request}"
    );
    let response_unwritable = "the Heartbeat v4 response cannot be written: cannot encode response body: ";
    assert!(
        unwritable_response.starts_with(response_unwritable),
        "{unwritable_response}"
    );
}

#[test]
fn the_prelude_is_enough_to_fail_a_call() {
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_wire_edit::prelude::*;

    let failures = Failures::of_target(&Target::Call, ErrorCode::REQUEST_TIMED_OUT);
    let part: Part = "orders.eu/3".parse().unwrap();

    assert_eq!(failures.of(&part), Some(ErrorCode::REQUEST_TIMED_OUT));
    assert!(TopicNames::new().is_empty());
}
