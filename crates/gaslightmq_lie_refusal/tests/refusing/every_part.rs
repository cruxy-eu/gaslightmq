//! Refusals that leave nothing to forward: the call is answered without the backend.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_wire::messages::{FetchResponse, HeartbeatResponse};

use crate::calls::*;

const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

#[test]
fn refusals_of_every_part_answer_the_call_without_the_backend() {
    for request in calls_with_parts() {
        let [first, second, third] = three_parts(&request);
        let flap = refusal(NOT_LEADER, only(&second));
        let slow = refusal(TIMED_OUT, Target::of_parts([first.clone(), third.clone()]).unwrap());

        let walked = walk(&[flap, slow], &[], &request, no_backend);

        // The first refusal asks to forward the rest, and the second finds nothing left and answers.
        // The first is then asked again, and puts its error next to the others.
        assert!(walked.forwarded.is_none(), "{}", request.api());
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
        assert_eq!(walked.sent.correlation_id(), CORRELATION_ID);
    }
}

#[test]
fn one_refusal_of_every_part_answers_the_call_without_the_backend() {
    for request in calls_with_parts() {
        let every = Target::of_parts(three_parts(&request)).unwrap();

        let walked = walk(&[refusal(NOT_LEADER, every)], &[], &request, no_backend);

        assert!(walked.forwarded.is_none(), "{}", request.api());
        assert_eq!(
            told(&request, &walked.sent),
            every_part(&request, "NOT_LEADER_OR_FOLLOWER"),
            "{}",
            request.api()
        );
    }
}

#[test]
fn the_refusal_that_removes_the_last_part_answers_with_its_own_parts() {
    let request = produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);
    let lies = every_lie();
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/3"]));
    let slow = refusal(TIMED_OUT, parts(&["orders.eu/4", "payments.settled/0"]));

    // The first refusal leaves two partitions, so it asks to forward them.
    let Effect::Forward(left) = lies.before_forwarding(&flap, &decoded(&request)).unwrap() else {
        panic!("two partitions are left to forward");
    };
    assert_eq!(named(&left), ["orders.eu/4", "payments.settled/0"]);

    // The second refuses both, so it answers. It knows nothing of the first refusal.
    let Effect::Answer(answer) = lies.before_forwarding(&slow, &decoded(&left)).unwrap() else {
        panic!("nothing is left to forward");
    };
    assert_eq!(
        told(&request, &answer),
        ["orders.eu/4 REQUEST_TIMED_OUT", "payments.settled/0 REQUEST_TIMED_OUT"]
    );

    // The first is asked again with that answer, as after a response of the backend.
    let Effect::Answer(delivered) = lies.after_response(&flap, &decoded(&request), &answer).unwrap() else {
        panic!("the refusal puts its error into the response");
    };
    assert_eq!(
        told(&request, &delivered),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "orders.eu/4 REQUEST_TIMED_OUT",
            "payments.settled/0 REQUEST_TIMED_OUT"
        ]
    );
}

#[test]
fn a_call_whose_every_part_is_refused_does_not_fail_as_a_whole() {
    let request = fetch(&["orders.eu/3", "orders.eu/4"]);
    let flap = refusal(NOT_LEADER, parts(&["orders.eu/3", "orders.eu/4"]));

    let walked = walk(&[flap], &[], &request, no_backend);

    // The client sees what a broker says about two partitions, and not a failure of the Fetch.
    let body: FetchResponse = walked.sent.body().unwrap();
    assert_eq!(body.error_code, 0);
    assert_eq!(
        told(&request, &walked.sent),
        every_part(&request, "NOT_LEADER_OR_FOLLOWER")
    );
}

#[test]
fn a_call_without_parts_has_no_part_to_leave_it() {
    // Its only refusal is the one of the whole call, which answers.
    let request = heartbeat();

    let walked = walk(&[refusal(TIMED_OUT, Target::Call)], &[], &request, no_backend);

    assert!(walked.forwarded.is_none());
    let body: HeartbeatResponse = walked.sent.body().unwrap();
    assert_eq!(ErrorCode::from_wire(body.error_code), Some(TIMED_OUT));
}
