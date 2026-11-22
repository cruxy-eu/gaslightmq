//! The refusals that fail: each changes nothing, and says why.
//! Also the Produce that expects no response, which the refusal treats as any other.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_wire::messages::{MetadataResponse, ProduceRequest, ProduceResponse};
use gaslightmq_wire::{Frame, Request, Response};
use gaslightmq_wire_edit::prelude::*;

use crate::calls::*;

const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

/// What the contract says when a refusal targets `parts`, and `request` does not name them.
fn not_in_request(request: &Request, parts: &[&str]) -> LieError {
    LieError::PartsNotInRequest {
        kind: LieKind::Refusal,
        api: request.api(),
        parts: PartSet::new(parts.iter().map(|text| part(text))).unwrap(),
    }
}

/// What the contract says when the response to `request` has no place for the error of a refusal.
fn no_place_for_error(request: &Request) -> LieError {
    LieError::NoPlaceForError {
        kind: LieKind::Refusal,
        api: request.api(),
    }
}

// Parts that the request does not name.

#[test]
fn a_refusal_of_parts_cannot_be_told_on_a_call_without_parts() {
    let request = heartbeat();
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/3"]));
    let lies = every_lie();

    // It does not widen to the whole call, and it does not pass in silence.
    let error = lies.before_forwarding(&flap, &decoded(&request)).unwrap_err();
    assert_eq!(error, not_in_request(&request, &["orders.eu/3"]));
    assert_eq!(
        error.to_string(),
        "refusal targets parts that the Heartbeat v4 request does not name: orders.eu/3"
    );

    let error = lies
        .after_response(&flap, &decoded(&request), &backend(&request))
        .unwrap_err();
    assert_eq!(error, not_in_request(&request, &["orders.eu/3"]));
}

#[test]
fn a_refused_request_says_which_parts_the_request_does_not_name() {
    let asked = decoded(&heartbeat());

    let error = RefusedRequest::new(asked, &parts(&["orders.eu/4", "orders.eu/3"]), NOT_LEADER).unwrap_err();

    assert_eq!(
        error,
        RefusedRequestError::PartsNotInRequest {
            api: heartbeat().api(),
            parts: PartSet::new([part("orders.eu/3"), part("orders.eu/4")]).unwrap(),
        }
    );
    assert_eq!(
        error.to_string(),
        "the Heartbeat v4 request does not name orders.eu/3, orders.eu/4"
    );
}

#[test]
fn a_refusal_is_told_on_all_of_its_parts_or_on_none() {
    for request in calls_with_parts() {
        let [first, _, _] = three_parts(&request);
        // One part of the target is a part of the request, and the other is not.
        let missing = match first {
            Part::Topic(_) => part("inventory"),
            Part::Partition(_) => part("inventory/0"),
        };
        let target = Target::of_parts([first, missing.clone()]).unwrap();

        let error = RefusedRequest::new(decoded(&request), &target, NOT_LEADER).unwrap_err();
        assert_eq!(
            error,
            RefusedRequestError::PartsNotInRequest {
                api: request.api(),
                parts: PartSet::one(missing.clone()),
            }
        );

        // The contract keeps the reason, and names the part.
        let error = every_lie()
            .before_forwarding(&refusal(NOT_LEADER, target), &decoded(&request))
            .unwrap_err();
        assert_eq!(error, not_in_request(&request, &[&missing.to_string()]));
    }
}

#[test]
fn a_part_of_another_kind_than_the_request_names_is_not_in_the_request() {
    // A Produce names partitions, and a Metadata request names topics.
    let produce = produce(&["orders.eu/3"]);
    let error = RefusedRequest::new(decoded(&produce), &parts(&["orders.eu"]), NOT_LEADER).unwrap_err();
    assert_eq!(error.to_string(), "the Produce v9 request does not name orders.eu");

    let metadata = metadata(&["orders.eu"]);
    let error = RefusedRequest::new(decoded(&metadata), &parts(&["orders.eu/3"]), NOT_LEADER).unwrap_err();
    assert_eq!(error.to_string(), "the Metadata v9 request does not name orders.eu/3");
}

#[test]
fn the_behaviour_names_a_topic_by_the_id_that_the_pipeline_knows() {
    let request = fetch_by_id(&["orders.eu/3", "orders.eu/4"]);
    let lies = every_lie();
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/3"]));

    // The pipeline decoded the request with the topics it knows by ID, so the refusal finds its part.
    let Effect::Forward(left) = lies.before_forwarding(&flap, &decoded(&request)).unwrap() else {
        panic!("one partition is left to forward");
    };
    assert_eq!(decoded(&left).parts(), [part("orders.eu/4")]);

    // Decoded by someone who knows no topic, the request names no part.
    let unnamed = EditableRequest::decode(&request, &TopicNames::new()).unwrap();
    let error = lies.before_forwarding(&flap, &unnamed).unwrap_err();
    assert_eq!(error, not_in_request(&request, &["orders.eu/3"]));
    assert_eq!(
        error.to_string(),
        "refusal targets parts that the Fetch v13 request does not name: orders.eu/3"
    );

    // The whole call is refused either way, and its partitions fail with it.
    let Effect::Answer(response) = lies
        .before_forwarding(&refusal(NOT_LEADER, Target::Call), &unnamed)
        .unwrap()
    else {
        panic!("a refusal of the whole call answers it");
    };
    assert_eq!(
        told(&request, &response),
        every_part(&request, "NOT_LEADER_OR_FOLLOWER")
    );
}

// A response without a place for the error.

#[test]
fn a_refusal_cannot_be_told_where_the_response_has_no_place_for_the_error() {
    // A Metadata request for every topic names nothing, and its response has no error code of its own up to version 12.
    let request = metadata_for_every_topic(9);
    let unknown = refusal(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, Target::Call);
    let lies = every_lie();

    let error = lies.before_forwarding(&unknown, &decoded(&request)).unwrap_err();
    assert_eq!(error, no_place_for_error(&request));
    assert_eq!(
        error.to_string(),
        "refusal cannot be told on this Metadata v9 request: its response has no place for an error code"
    );

    // After the response it fails as well, so the client gets the truth, and not an empty cluster.
    let truth = request.response(&MetadataResponse::default()).unwrap();
    let error = lies.after_response(&unknown, &decoded(&request), &truth).unwrap_err();
    assert_eq!(error, no_place_for_error(&request));

    let error =
        RefusedRequest::new(decoded(&request), &Target::Call, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION).unwrap_err();
    assert_eq!(error, RefusedRequestError::NoPlaceForError { api: request.api() });
    assert_eq!(
        error.to_string(),
        "the response to this Metadata v9 request has no place for an error code"
    );
}

#[test]
fn the_same_refusal_is_told_where_the_response_has_a_place_for_the_error() {
    // Since version 13 the response has an error code of its own.
    let request = metadata_for_every_topic(13);
    let unknown = refusal(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, Target::Call);

    let walked = walk(&[unknown], &[], &request, no_backend);

    let body: MetadataResponse = walked.sent.body().unwrap();
    assert_eq!(
        ErrorCode::from_wire(body.error_code),
        Some(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION)
    );
    assert_eq!(body.topics, vec![]);
}

#[test]
fn every_error_code_finds_its_place() {
    // The check compares the response with the one of another error, so it must hold for the error it compares with.
    let request = produce(&["orders.eu/3"]);

    for error in [ErrorCode::UNKNOWN_SERVER_ERROR, TIMED_OUT, NOT_LEADER] {
        let refused = RefusedRequest::new(decoded(&request), &Target::Call, error).unwrap();
        let Effect::Answer(response) = refused.before_forwarding().unwrap() else {
            panic!("a refusal of the whole call answers it");
        };
        assert_eq!(told(&request, &response), [format!("orders.eu/3 {error}")]);
    }
}

// A call the refusal cannot read, and a lie that is no refusal.

#[test]
fn the_behaviour_only_tells_refusals() {
    let request = heartbeat();
    let silence = LieDefinition::Silence.on(Target::Call);

    let error = Refusal.before_forwarding(&silence, &decoded(&request)).unwrap_err();

    assert_eq!(
        error,
        LieError::OtherKind {
            tells: LieKind::Refusal,
            lie: LieKind::Silence
        }
    );
    assert_eq!(
        Refusal
            .after_response(&silence, &decoded(&request), &backend(&request))
            .unwrap_err(),
        error
    );
    assert_eq!(Refusal.kind(), LieKind::Refusal);
}

#[test]
fn a_request_that_does_not_decode_never_reaches_a_refusal() {
    // The header of a request of API key 9999: the key, the version, the correlation ID and no client ID.
    let header: &[u8] = &[0x27, 0x0F, 0, 0, 0, 0, 0, 7, 0xFF, 0xFF];
    let unknown = Request::decode(Frame::new(header.to_vec())).unwrap();
    // A Produce whose body is cut short.
    let whole = produce(&["orders.eu/3"]);
    let payload = whole.frame().payload();
    let cut = Request::decode(Frame::new(payload.slice(..payload.len() - 3))).unwrap();

    // A lie is asked with a decoded request, so whoever decodes finds out first, and no lie is asked.
    assert_eq!(
        EditableRequest::decode(&unknown, &TopicNames::new()).unwrap_err(),
        EditableRequestError::UnknownApi { api: unknown.api() }
    );
    let error = EditableRequest::decode(&cut, &TopicNames::new()).unwrap_err();
    assert!(
        matches!(error, EditableRequestError::UnreadableRequest { .. }),
        "{error}"
    );
}

#[test]
fn a_response_that_does_not_decode_cannot_take_a_refusal() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let truth = backend(&request);
    let payload = truth.frame().payload();
    let cut = Response::decode(request.api(), Frame::new(payload.slice(..payload.len() - 3))).unwrap();

    let error = every_lie()
        .after_response(&refusal(TIMED_OUT, parts(&["orders.eu/3"])), &decoded(&request), &cut)
        .unwrap_err();

    assert!(
        matches!(
            error,
            LieError::Unreadable {
                kind: LieKind::Refusal,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_response_to_another_request_cannot_take_a_refusal() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let other = fetch(&["orders.eu/3"]);
    let answer_to_other = backend(&other);
    let target = parts(&["orders.eu/3"]);

    let refused = RefusedRequest::new(decoded(&request), &target, TIMED_OUT).unwrap();
    let error = refused.after_response(&answer_to_other).unwrap_err();
    assert_eq!(
        error,
        RefusedRequestError::SurgeryFailed {
            error: EditableRequestError::ResponseOfAnotherCall {
                api: request.api(),
                correlation_id: CORRELATION_ID,
                answered_api: other.api(),
                answered_correlation_id: CORRELATION_ID,
            }
        }
    );
    assert_eq!(
        error.to_string(),
        "the refusal cannot be told: the Fetch v11 response 7 does not answer the Produce v9 request 7"
    );

    let error = every_lie()
        .after_response(&refusal(TIMED_OUT, target), &decoded(&request), &answer_to_other)
        .unwrap_err();
    assert_eq!(
        error,
        LieError::ResponseOfAnotherCall {
            kind: LieKind::Refusal,
            api: request.api(),
            correlation_id: CORRELATION_ID,
            answered_api: other.api(),
            answered_correlation_id: CORRELATION_ID,
        }
    );
    assert_eq!(
        error.to_string(),
        "refusal was given the Fetch v11 response 7, which does not answer the Produce v9 request 7"
    );
}

// A Produce that expects no response.

#[test]
fn a_produce_that_expects_no_response_is_refused_as_any_other() {
    let request = produce_with_acks(&["orders.eu/3", "orders.eu/4"], 0);
    let lies = every_lie();

    // The whole call: the refusal builds the response, and whoever sends responses knows that this call has none.
    let Effect::Answer(response) = lies
        .before_forwarding(&refusal(NOT_LEADER, Target::Call), &decoded(&request))
        .unwrap()
    else {
        panic!("a refusal of the whole call answers it");
    };
    assert_eq!(
        told(&request, &response),
        every_part(&request, "NOT_LEADER_OR_FOLLOWER")
    );
    assert!(response.body::<ProduceResponse>().is_ok());

    // Some parts: the refused records never reach the backend, and the request still expects no response.
    let Effect::Forward(forwarded) = lies
        .before_forwarding(&refusal(NOT_LEADER, parts(&["orders.eu/3"])), &decoded(&request))
        .unwrap()
    else {
        panic!("one partition is left to forward");
    };
    assert_eq!(named(&forwarded), ["orders.eu/4"]);
    assert_eq!(forwarded.body::<ProduceRequest>().unwrap().acks, 0);
}
