//! A refusal after the response: the backend handled the request, and the client is told it failed.
//! Also what asking a refusal again does, and in which order refusals are asked.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::Response;
use gaslightmq_wire::messages::{FetchResponse, MetadataResponse, ProduceResponse};

use crate::calls::*;

const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

/// A refusal with `error` on `target`, after the response.
fn late(error: ErrorCode, target: Target) -> TargetedLie {
    LieDefinition::from(RefusalLie::new(error).at(Stage::AfterResponse)).on(target)
}

/// The response that `lie` asks for after `response`.
fn answer_after(lie: &TargetedLie, request: &gaslightmq_wire::Request, response: &Response) -> Response {
    match every_lie().after_response(lie, &decoded(request), response).unwrap() {
        Effect::Answer(response) => response,
        other => panic!("{lie} asked to {other}"),
    }
}

#[test]
fn a_refusal_after_the_response_lets_the_backend_handle_the_whole_request() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let lost = late(TIMED_OUT, only(&first));

        let walked = walk(&[], &[lost], &request, backend);

        // The backend handles every part, and the client is told that one of them failed.
        assert_eq!(walked.forwarded.as_ref().unwrap().frame(), request.frame());
        assert_eq!(
            told(&request, &walked.sent),
            [
                format!("{first} REQUEST_TIMED_OUT"),
                format!("{second} ok"),
                format!("{third} ok"),
            ],
            "{}",
            request.api()
        );
        assert_eq!(walked.sent.correlation_id(), CORRELATION_ID);
    }
}

#[test]
fn a_lost_acknowledgement_hides_where_the_records_were_written() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let lost = late(TIMED_OUT, parts(&["orders.eu/3"]));

    let walked = walk(&[], &[lost], &request, backend);

    // The backend wrote both partitions. The refused one loses its offset, and the other keeps it.
    let body: ProduceResponse = walked.sent.body().unwrap();
    let offsets: Vec<(i32, i64)> = body.responses[0]
        .partition_responses
        .iter()
        .map(|partition| (partition.index, partition.base_offset))
        .collect();
    assert_eq!(offsets, [(3, -1), (4, WRITTEN_AT)]);
}

#[test]
fn a_fetch_refused_after_the_response_loses_what_the_backend_read() {
    let request = fetch(&["orders.eu/3", "orders.eu/4"]);
    let lost = late(NOT_LEADER, parts(&["orders.eu/4"]));

    let walked = walk(&[], &[lost], &request, backend);

    let body: FetchResponse = walked.sent.body().unwrap();
    let watermarks: Vec<(i32, i64)> = body.responses[0]
        .partitions
        .iter()
        .map(|partition| (partition.partition_index, partition.high_watermark))
        .collect();
    assert_eq!(watermarks, [(3, HIGH_WATERMARK), (4, -1)]);
}

#[test]
fn a_topic_refused_after_the_response_loses_its_partitions() {
    let request = metadata(&["orders.eu", "payments.settled"]);
    let unknown = late(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, parts(&["payments.settled"]));

    let walked = walk(&[], &[unknown], &request, backend);

    let body: MetadataResponse = walked.sent.body().unwrap();
    let topics: Vec<(i16, usize)> = body
        .topics
        .iter()
        .map(|topic| (topic.error_code, topic.partitions.len()))
        .collect();
    assert_eq!(topics, [(0, 1), (ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.value(), 0)]);
}

#[test]
fn the_method_says_the_stage_and_the_definition_does_not() {
    for request in calls_with_parts() {
        let [first, _, _] = three_parts(&request);
        let truth = backend(&request);

        // A lie of the response pass acts after the response, whatever stage its definition names.
        let named_before = answer_after(&refusal(TIMED_OUT, only(&first)), &request, &truth);
        let named_after = answer_after(&late(TIMED_OUT, only(&first)), &request, &truth);

        assert_eq!(named_before.frame(), named_after.frame(), "{}", request.api());
    }
}

#[test]
fn asking_a_refusal_again_gives_the_same_response() {
    for request in calls_with_parts().into_iter().chain([heartbeat()]) {
        // A Heartbeat has no parts, so only the whole call of it is refused.
        let targets = if decoded(&request).parts().is_empty() {
            vec![Target::Call]
        } else {
            let [first, second, _] = three_parts(&request);
            vec![Target::Call, only(&first), Target::of_parts([first, second]).unwrap()]
        };

        for target in targets {
            let lie = late(TIMED_OUT, target);

            let once = answer_after(&lie, &request, &backend(&request));
            let twice = answer_after(&lie, &request, &once);

            assert_eq!(once.frame(), twice.frame(), "{lie} on {}", request.api());
        }
    }
}

#[test]
fn a_refusal_that_forwarded_gives_the_same_response_when_it_is_asked_again() {
    for request in calls_with_parts() {
        let [_, second, _] = three_parts(&request);
        let flap = refusal(NOT_LEADER, only(&second));
        let Effect::Forward(left) = every_lie().before_forwarding(&flap, &decoded(&request)).unwrap() else {
            panic!("two parts are left to forward");
        };

        // The first time, the response lacks the part. The second time, it holds the part with the error.
        let once = answer_after(&flap, &request, &backend(&left));
        let twice = answer_after(&flap, &request, &once);

        assert_eq!(once.frame(), twice.frame(), "{}", request.api());
    }
}

#[test]
fn refusals_of_different_parts_are_asked_in_any_order() {
    for request in calls_with_parts() {
        let [first, _, third] = three_parts(&request);
        let lost = late(TIMED_OUT, only(&first));
        let flap = late(NOT_LEADER, only(&third));

        let one_way = walk(&[], &[lost.clone(), flap.clone()], &request, backend);
        let other_way = walk(&[], &[flap, lost], &request, backend);

        assert_eq!(one_way.sent.frame(), other_way.sent.frame(), "{}", request.api());
    }
}

#[test]
fn a_part_refused_before_forwarding_keeps_its_error_under_a_refusal_of_the_whole_call() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let flap = refusal(NOT_LEADER, only(&second));
        // A rule that waited for the truth refuses the whole call.
        let lost = late(TIMED_OUT, Target::Call);

        // The walk asks the refusal of the whole call first, and then the one that forwarded.
        let walked = walk(&[flap], &[lost], &request, backend);

        // The refusal of the whole call replaces the truth of every part that reached the backend.
        assert_eq!(
            named(walked.forwarded.as_ref().unwrap()),
            [first.to_string(), third.to_string()]
        );
        assert_eq!(
            told(&request, &walked.sent),
            [
                format!("{first} REQUEST_TIMED_OUT"),
                format!("{second} NOT_LEADER_OR_FOLLOWER"),
                format!("{third} REQUEST_TIMED_OUT"),
            ],
            "{}",
            request.api()
        );
    }
}

#[test]
fn the_last_refusal_that_is_asked_decides_a_part() {
    let request = produce(&["orders.eu/3", "orders.eu/4"]);
    let truth = backend(&request);
    let flap = late(NOT_LEADER, parts(&["orders.eu/3"]));
    let lost = late(TIMED_OUT, Target::Call);

    // A refusal of the whole call writes over every part, also over the one another refusal holds.
    let part_then_call = answer_after(&lost, &request, &answer_after(&flap, &request, &truth));
    assert_eq!(
        told(&request, &part_then_call),
        every_part(&request, "REQUEST_TIMED_OUT")
    );

    let call_then_part = answer_after(&flap, &request, &answer_after(&lost, &request, &truth));
    assert_eq!(
        told(&request, &call_then_part),
        ["orders.eu/3 NOT_LEADER_OR_FOLLOWER", "orders.eu/4 REQUEST_TIMED_OUT"]
    );
}
