//! A refusal of some parts before forwarding: the rest is forwarded, and the refusal joins the response.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_wire::messages::{FetchResponse, MetadataResponse, ProduceRequest, ProduceResponse};

use crate::calls::*;

const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;

#[test]
fn a_refusal_of_one_part_forwards_the_rest_and_puts_its_error_into_the_response() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let flap = refusal(NOT_LEADER, only(&second));

        let walked = walk(&[flap], &[], &request, backend);

        // The refused part never reaches the backend.
        assert_eq!(
            named(walked.forwarded.as_ref().unwrap()),
            [first.to_string(), third.to_string()],
            "{}",
            request.api()
        );
        // The client receives one response that covers every part it asked for, in the order it asked.
        assert_eq!(
            told(&request, &walked.sent),
            [
                format!("{first} ok"),
                format!("{second} NOT_LEADER_OR_FOLLOWER"),
                format!("{third} ok"),
            ],
            "{}",
            request.api()
        );
        assert_eq!(walked.sent.correlation_id(), CORRELATION_ID);
    }
}

#[test]
fn a_refusal_of_several_parts_removes_all_of_them() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let target = Target::of_parts([first.clone(), third.clone()]).unwrap();

        let walked = walk(&[refusal(NOT_LEADER, target)], &[], &request, backend);

        assert_eq!(named(walked.forwarded.as_ref().unwrap()), [second.to_string()]);
        assert_eq!(
            told(&request, &walked.sent),
            [
                format!("{first} NOT_LEADER_OR_FOLLOWER"),
                format!("{second} ok"),
                format!("{third} NOT_LEADER_OR_FOLLOWER"),
            ],
            "{}",
            request.api()
        );
    }
}

#[test]
fn each_refusal_is_asked_with_what_the_ones_before_it_left() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let flap = refusal(NOT_LEADER, only(&first));
        let slow = refusal(ErrorCode::REQUEST_TIMED_OUT, only(&third));

        let walked = walk(&[flap, slow], &[], &request, backend);

        // Both refusals forward, and each puts its own error into the response.
        assert_eq!(named(walked.forwarded.as_ref().unwrap()), [second.to_string()]);
        assert_eq!(
            told(&request, &walked.sent),
            [
                format!("{first} NOT_LEADER_OR_FOLLOWER"),
                format!("{second} ok"),
                format!("{third} REQUEST_TIMED_OUT"),
            ],
            "{}",
            request.api()
        );
    }
}

#[test]
fn the_records_of_a_refused_partition_never_reach_the_backend() {
    let request = produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/3"]));

    let walked = walk(&[flap], &[], &request, backend);

    // What is forwarded is the Produce of the client without one partition.
    let forwarded = walked.forwarded.unwrap();
    assert_eq!(forwarded.api(), request.api());
    assert_eq!(forwarded.correlation_id(), CORRELATION_ID);
    assert_eq!(forwarded.client_id(), Some("order-ingest"));
    let left: ProduceRequest = forwarded.body().unwrap();
    assert_eq!(left.acks, -1);
    assert_eq!(left.timeout_ms, 30_000);
    assert_eq!(left.topic_data[0].partition_data.len(), 1);
    assert!(left.topic_data[0].partition_data[0].records.is_some());

    // The refused partition has no offset, and the others have the one the backend gave them.
    let body: ProduceResponse = walked.sent.body().unwrap();
    let offsets: Vec<(i32, i64)> = body
        .responses
        .iter()
        .flat_map(|topic| &topic.partition_responses)
        .map(|partition| (partition.index, partition.base_offset))
        .collect();
    assert_eq!(offsets, [(3, -1), (4, WRITTEN_AT), (0, WRITTEN_AT)]);
}

#[test]
fn a_topic_leaves_a_request_with_its_last_partition() {
    let request = produce(&["orders.eu/3", "payments.settled/0"]);
    let flap = refusal(NOT_LEADER, parts(&["payments.settled/0"]));

    let walked = walk(&[flap], &[], &request, backend);

    let left: ProduceRequest = walked.forwarded.unwrap().body().unwrap();
    assert_eq!(left.topic_data.len(), 1);
    assert_eq!(
        told(&request, &walked.sent),
        ["orders.eu/3 ok", "payments.settled/0 NOT_LEADER_OR_FOLLOWER"]
    );
}

#[test]
fn a_fetch_keeps_what_the_backend_read_for_the_other_partitions() {
    let request = fetch(&["orders.eu/3", "orders.eu/4"]);
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/4"]));

    let walked = walk(&[flap], &[], &request, backend);

    // Only a part failed, so the response itself does not fail.
    let body: FetchResponse = walked.sent.body().unwrap();
    assert_eq!(body.error_code, 0);
    let watermarks: Vec<(i32, i64)> = body.responses[0]
        .partitions
        .iter()
        .map(|partition| (partition.partition_index, partition.high_watermark))
        .collect();
    assert_eq!(watermarks, [(3, HIGH_WATERMARK), (4, -1)]);
}

#[test]
fn a_metadata_request_loses_a_topic_and_the_response_gets_it_back_with_the_error() {
    let request = metadata(&["orders.eu", "payments.settled"]);
    let unknown = refusal(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, parts(&["orders.eu"]));

    let walked = walk(&[unknown], &[], &request, backend);

    assert_eq!(named(walked.forwarded.as_ref().unwrap()), ["payments.settled"]);
    let body: MetadataResponse = walked.sent.body().unwrap();
    let topics: Vec<(i16, usize)> = body
        .topics
        .iter()
        .map(|topic| (topic.error_code, topic.partitions.len()))
        .collect();
    assert_eq!(topics, [(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.value(), 0), (0, 1)]);
}

#[test]
fn a_topic_named_by_its_id_has_parts_for_whoever_knows_its_name() {
    let request = fetch_by_id(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);
    let refused = RefusedRequest::new(decoded(&request), &parts(&["orders.eu/4"]), NOT_LEADER).unwrap();

    let Effect::Forward(forwarded) = refused.before_forwarding().unwrap() else {
        panic!("two partitions are left to forward");
    };
    assert_eq!(named(&forwarded), ["orders.eu/3", "payments.settled/0"]);

    let delivered = refused.after_response(&backend(&forwarded)).unwrap();
    assert_eq!(
        told(&request, &delivered),
        [
            "orders.eu/3 ok",
            "orders.eu/4 NOT_LEADER_OR_FOLLOWER",
            "payments.settled/0 ok"
        ]
    );
}

#[test]
fn a_refused_request_says_what_it_refuses() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);

    let of_part = RefusedRequest::new(decoded(&request), &parts(&["orders.eu/3"]), NOT_LEADER).unwrap();
    assert!(!of_part.refuses_whole_call());
    assert_eq!(of_part.failures().to_string(), "orders.eu/3: NOT_LEADER_OR_FOLLOWER");
    assert_eq!(of_part.api(), request.api());
    assert_eq!(of_part.asked().request().frame(), request.frame());

    let of_call = RefusedRequest::new(decoded(&request), &Target::Call, NOT_LEADER).unwrap();
    assert!(of_call.refuses_whole_call());
    assert_eq!(of_call.failures().to_string(), "call: NOT_LEADER_OR_FOLLOWER");
}
