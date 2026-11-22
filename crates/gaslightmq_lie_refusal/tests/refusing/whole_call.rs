//! A refusal of the whole call: every API fails as a whole, before forwarding and after the response.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::Request;
use gaslightmq_wire::messages::{
    ApiVersionsRequest, ApiVersionsResponse, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse, MetadataResponse,
};
use gaslightmq_wire::protocol::StrBytes;

use crate::calls::*;

const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

fn timed_out() -> TargetedLie {
    refusal(TIMED_OUT, Target::Call)
}

#[test]
fn a_refusal_of_the_whole_call_answers_it_without_the_backend() {
    for request in calls_with_parts() {
        let walked = walk(&[timed_out()], &[], &request, no_backend);

        // The backend never sees the request, and every part the client asked for failed.
        assert!(walked.forwarded.is_none(), "{}", request.api());
        assert_eq!(
            told(&request, &walked.sent),
            every_part(&request, "REQUEST_TIMED_OUT"),
            "{}",
            request.api()
        );
        assert_eq!(walked.sent.api(), request.api());
        assert_eq!(walked.sent.correlation_id(), CORRELATION_ID);
    }
}

#[test]
fn a_refusal_of_the_whole_call_after_the_response_leaves_nothing_of_the_truth() {
    for request in calls_with_parts() {
        let late = LieDefinition::from(RefusalLie::new(TIMED_OUT).at(Stage::AfterResponse)).on(Target::Call);

        let walked = walk(&[], &[late], &request, backend);

        // The backend handled the whole request, and the client is told that all of it failed.
        assert_eq!(named(walked.forwarded.as_ref().unwrap()), named(&request));
        assert_eq!(
            told(&request, &walked.sent),
            every_part(&request, "REQUEST_TIMED_OUT"),
            "{}",
            request.api()
        );
    }
}

#[test]
fn a_refused_call_gives_the_same_response_at_both_stages() {
    for request in calls_with_parts().into_iter().chain([heartbeat()]) {
        let lies = every_lie();

        let Effect::Answer(before) = lies.before_forwarding(&timed_out(), &decoded(&request)).unwrap() else {
            panic!("a refusal of the whole call answers it");
        };
        let Effect::Answer(after) = lies
            .after_response(&timed_out(), &decoded(&request), &backend(&request))
            .unwrap()
        else {
            panic!("a refusal of the whole call answers it");
        };

        assert_eq!(before.frame(), after.frame(), "{}", request.api());
    }
}

#[test]
fn a_fetch_also_fails_in_the_response_itself() {
    let request = fetch(&["orders.eu/3", "payments.settled/0"]);

    let walked = walk(&[timed_out()], &[], &request, no_backend);

    // A Fetch has an error code of its own since version 7, next to the ones of its partitions.
    let body: FetchResponse = walked.sent.body().unwrap();
    assert_eq!(ErrorCode::from_wire(body.error_code), Some(TIMED_OUT));
    assert_eq!(body.responses[0].partitions[0].high_watermark, -1);
}

#[test]
fn a_refused_metadata_names_the_topics_it_was_asked_for_and_none_of_their_partitions() {
    let request = metadata(&["orders.eu", "payments.settled"]);
    let unknown = refusal(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, Target::Call);

    let walked = walk(&[unknown], &[], &request, no_backend);

    let body: MetadataResponse = walked.sent.body().unwrap();
    assert_eq!(body.topics.len(), 2);
    for topic in &body.topics {
        assert_eq!(
            ErrorCode::from_wire(topic.error_code),
            Some(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION)
        );
        assert_eq!(topic.partitions, vec![]);
    }
}

#[test]
fn a_call_without_parts_is_refused_as_a_whole() {
    let request = heartbeat();
    let rebalance = refusal(ErrorCode::REBALANCE_IN_PROGRESS, Target::Call);

    let walked = walk(&[rebalance], &[], &request, no_backend);

    assert!(walked.forwarded.is_none());
    assert_eq!(walked.sent.correlation_id(), CORRELATION_ID);
    let body: HeartbeatResponse = walked.sent.body().unwrap();
    assert_eq!(
        ErrorCode::from_wire(body.error_code),
        Some(ErrorCode::REBALANCE_IN_PROGRESS)
    );
}

#[test]
fn a_call_without_parts_is_refused_after_the_response_too() {
    let request = heartbeat();
    let fenced = LieDefinition::from(RefusalLie::new(ErrorCode::FENCED_INSTANCE_ID).at(Stage::AfterResponse));

    let walked = walk(&[], &[fenced.on(Target::Call)], &request, backend);

    // The coordinator counted the Heartbeat, and the consumer is told that it was fenced.
    assert_eq!(walked.forwarded.unwrap().frame(), request.frame());
    let body: HeartbeatResponse = walked.sent.body().unwrap();
    assert_eq!(
        ErrorCode::from_wire(body.error_code),
        Some(ErrorCode::FENCED_INSTANCE_ID)
    );
}

#[test]
fn the_error_stands_wherever_the_response_of_an_api_has_a_place_for_it() {
    let lies = every_lie();
    let not_available = refusal(ErrorCode::COORDINATOR_NOT_AVAILABLE, Target::Call);
    let answer = |request: &Request| match lies.before_forwarding(&not_available, &decoded(request)).unwrap() {
        Effect::Answer(response) => response,
        other => panic!("the refusal asked to {other}"),
    };
    let code = ErrorCode::COORDINATOR_NOT_AVAILABLE.value();

    // In the response itself.
    let request = Request::new(5, CORRELATION_ID, None, &InitProducerIdRequest::default()).unwrap();
    assert_eq!(
        answer(&request).body::<InitProducerIdResponse>().unwrap().error_code,
        code
    );
    let request = Request::new(3, CORRELATION_ID, None, &ApiVersionsRequest::default()).unwrap();
    assert_eq!(answer(&request).body::<ApiVersionsResponse>().unwrap().error_code, code);

    // In the entry of each coordinator the request asks for.
    let keys = vec![
        StrBytes::from_static_str("analytics-etl"),
        StrBytes::from_static_str("billing"),
    ];
    let body = FindCoordinatorRequest::default().with_coordinator_keys(keys);
    let request = Request::new(4, CORRELATION_ID, None, &body).unwrap();
    let coordinators = answer(&request).body::<FindCoordinatorResponse>().unwrap().coordinators;
    assert_eq!(coordinators.len(), 2);
    assert!(coordinators.iter().all(|coordinator| coordinator.error_code == code));
}
