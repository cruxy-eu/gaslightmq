//! Every API with parts, at every version: remove parts, answer for them, and merge the responses of both sides.
//!
//! The requests come from `common`, and each names three parts.
//! A stand-in for the backend answers every part it sees with one error code, so a response shows who answered a part:
//! the backend, or the failures that the test put in.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod common;

use common::topic_names;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Request, Response};
use gaslightmq_wire_edit::prelude::*;

/// The error code of a part that the test refuses.
const REFUSED: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;

/// The error code that the stand-in for the backend gives every part it sees.
const FROM_BACKEND: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

fn part(text: &str) -> Part {
    text.parse().unwrap()
}

fn parts(texts: &[&str]) -> Vec<Part> {
    texts.iter().map(|text| part(text)).collect()
}

fn partitions() -> Vec<Part> {
    parts(&["orders.eu/0", "orders.eu/1", "payments.settled/0"])
}

fn topics() -> Vec<Part> {
    parts(&["orders.eu", "payments.settled", "audit.log"])
}

fn decode(request: &Request) -> EditableRequest {
    EditableRequest::decode(request, &topic_names()).unwrap()
}

/// Answers every part of `request` with [`FROM_BACKEND`], and nothing else.
fn backend(request: &Request) -> Response {
    let asked = decode(request);
    let every_part = asked.parts().iter().map(|part| (part.clone(), FROM_BACKEND));
    asked.error_response(&Failures::of_parts(every_part)).unwrap()
}

/// `named` as a response has them when `error_of` gives the error code of each.
fn answered(named: &[Part], error_of: impl Fn(&Part) -> ErrorCode) -> Vec<PartFields> {
    named
        .iter()
        .map(|part| PartFields::new(part.clone()).failed(error_of(part)))
        .collect()
}

/// Checks surgery on the request that `sample` builds, at every version of its API.
fn check_every_version<R: KafkaRequest>(sample: impl Fn(i16) -> R, named: &[Part]) {
    for version in R::VERSIONS.min..=R::VERSIONS.max {
        let request = Request::new(version, 42, Some("order-ingest"), &sample(version)).unwrap();
        let asked = decode(&request);
        let api = asked.api();
        assert_eq!(asked.parts(), named, "the parts of {api}");

        check_removing_nothing(&asked);
        for refused in named {
            check_one_refused_part(&asked, named, refused);
        }
        check_every_part_refused(&asked, named);
        check_the_whole_call_failing(&asked, named);
    }
}

fn check_removing_nothing(asked: &EditableRequest) {
    let api = asked.api();
    let elsewhere = part("elsewhere/9");
    assert!(
        matches!(asked.without(std::iter::empty()), Ok(Remainder::Whole)),
        "{api}"
    );
    assert!(matches!(asked.without([&elsewhere]), Ok(Remainder::Whole)), "{api}");

    // A response in which nothing fails is the response of the backend, byte for byte.
    let truth = backend(asked.request());
    let nothing = asked.merge(&truth, &Failures::none()).unwrap();
    assert_eq!(nothing.frame(), truth.frame(), "{api}");
    let elsewhere = Failures::none().with_part(elsewhere, REFUSED);
    assert_eq!(asked.merge(&truth, &elsewhere).unwrap().frame(), truth.frame(), "{api}");
    assert_eq!(
        asked.error_response(&elsewhere).unwrap_err().to_string(),
        format!("nothing that the {api} request asks failed"),
        "{api}"
    );
}

fn check_one_refused_part(asked: &EditableRequest, named: &[Part], refused: &Part) {
    let api = asked.api();
    let failures = Failures::none().with_part(refused.clone(), REFUSED);
    let expected = answered(named, |part| if part == refused { REFUSED } else { FROM_BACKEND });

    // Before forwarding: the backend never sees the part, and the response has it all the same.
    let Ok(Remainder::Rest(forwarded)) = asked.without([refused]) else {
        panic!("{api} without {refused} has parts left");
    };
    assert_eq!(forwarded.api(), api);
    assert_eq!(forwarded.correlation_id(), 42, "{api}");
    assert_eq!(forwarded.client_id(), Some("order-ingest"), "{api}");
    let kept: Vec<Part> = named.iter().filter(|part| *part != refused).cloned().collect();
    assert_eq!(decode(&forwarded).parts(), kept, "{api} without {refused}");

    let delivered = asked.merge(&backend(&forwarded), &failures).unwrap();
    assert_eq!(delivered.api(), api);
    assert_eq!(delivered.correlation_id(), 42, "{api}");
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        expected,
        "{api} with {refused} refused before forwarding"
    );

    // After the response: the backend answered the part, and the response says something else.
    let delivered = asked.merge(&backend(asked.request()), &failures).unwrap();
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        expected,
        "{api} with {refused} refused after the response"
    );
}

fn check_every_part_refused(asked: &EditableRequest, named: &[Part]) {
    let api = asked.api();
    assert!(matches!(asked.without(named), Ok(Remainder::Nothing)), "{api}");

    let failures: Failures = named.iter().map(|part| (part.clone(), REFUSED)).collect();
    let delivered = asked.error_response(&failures).unwrap();
    assert_eq!(delivered.correlation_id(), 42, "{api}");
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        answered(named, |_| REFUSED),
        "{api}"
    );
}

fn check_the_whole_call_failing(asked: &EditableRequest, named: &[Part]) {
    let api = asked.api();
    let own = &named[1];
    let failures = Failures::whole_call(FROM_BACKEND).with_part(own.clone(), REFUSED);
    let expected = answered(named, |part| if part == own { REFUSED } else { FROM_BACKEND });

    let delivered = asked.error_response(&failures).unwrap();
    assert_eq!(asked.parts_of(&delivered).unwrap(), expected, "{api}");

    // Nothing of the truth is left when the whole call fails.
    let merged = asked.merge(&backend(asked.request()), &failures).unwrap();
    assert_eq!(merged.frame(), delivered.frame(), "{api}");
}

#[test]
fn produce() {
    check_every_version(common::produce, &partitions());
}

#[test]
fn fetch() {
    check_every_version(common::fetch, &partitions());
}

#[test]
fn list_offsets() {
    check_every_version(common::list_offsets, &partitions());
}

#[test]
fn offset_for_leader_epoch() {
    check_every_version(common::offset_for_leader_epoch, &partitions());
}

#[test]
fn delete_records() {
    check_every_version(common::delete_records, &partitions());
}

#[test]
fn offset_commit() {
    check_every_version(common::offset_commit, &partitions());
}

#[test]
fn offset_fetch() {
    check_every_version(common::offset_fetch, &partitions());
}

#[test]
fn offset_delete() {
    check_every_version(common::offset_delete, &partitions());
}

#[test]
fn txn_offset_commit() {
    check_every_version(common::txn_offset_commit, &partitions());
}

#[test]
fn add_partitions_to_txn() {
    check_every_version(common::add_partitions_to_txn, &partitions());
}

#[test]
fn metadata() {
    check_every_version(common::metadata, &topics());
}

#[test]
fn create_topics() {
    check_every_version(common::create_topics, &topics());
}

#[test]
fn delete_topics() {
    check_every_version(common::delete_topics, &topics());
}

#[test]
fn create_partitions() {
    check_every_version(common::create_partitions, &topics());
}
