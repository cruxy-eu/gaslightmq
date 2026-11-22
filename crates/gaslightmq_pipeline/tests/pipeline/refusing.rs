//! A refusal tells the client that the request failed: the whole call, or some of its parts.

use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::HeartbeatResponse;

use crate::driver::*;

#[test]
fn a_refusal_of_the_whole_call_answers_without_the_backend() {
    let mut driver = Driver::with([on_api("Rebalance", ApiKey::HEARTBEAT, refusal(REBALANCING))]);
    let request = heartbeat(41);

    // The backend never sees the Heartbeat, and the client gets the error at once.
    let refused = driver.send(&request);
    assert_eq!(refused.kinds(), ["send", "emit"]);
    let response = refused.sent(&request);
    assert_eq!(response.correlation_id(), 41);
    let body: HeartbeatResponse = response.body().unwrap();
    assert_eq!(ErrorCode::from_wire(body.error_code), Some(REBALANCING));

    let record = refused.emitted();
    assert_eq!(record.verdict().to_string(), "REBALANCE_IN_PROGRESS");
    assert_eq!(record.delivered().to_string(), "REBALANCE_IN_PROGRESS");
    assert_eq!(
        explained(&record),
        ["Rebalance: matched the call; match 1: fired; REBALANCE_IN_PROGRESS applied"]
    );
    // The call has no truth: nobody asked the backend.
    assert!(record.read(Field::Outcome).is_absent());
    assert_eq!(record.timing().forwarded_at(), None);
    assert_eq!(record.timing().delivered_at(), Some(driver.now()));
}

#[test]
fn a_partial_refusal_of_a_produce_forwards_the_rest_and_merges() {
    let flap = on_partition("Leader flap", "orders.eu", "3", refusal(NOT_LEADER));
    let mut driver = Driver::with([flap]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4", "payments.settled/0"]);

    // The refused partition leaves the request, and the backend gets the rest.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward"]);
    let (call, forwarded) = arrived.forwarded();
    assert_eq!(asked(&forwarded), ["orders.eu/4", "payments.settled/0"]);
    assert_eq!(forwarded.correlation_id(), 41);

    // The backend answers for what it saw, and the client gets one response for everything it asked.
    let answered = driver.answer(&(call, forwarded));
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(
        told(&request, &answered.sent(&request)),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "orders.eu/4 ok",
            "payments.settled/0 ok"
        ]
    );

    let record = answered.emitted();
    assert_eq!(record.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
    assert_eq!(
        explained(&record),
        ["Leader flap: matched orders.eu/3; match 1: fired; NOT_LEADER_OR_FOLLOWER applied on orders.eu/3"]
    );
    assert_eq!(
        record.delivered().to_string(),
        "ok; NOT_LEADER_OR_FOLLOWER on orders.eu/3"
    );
    // The refused partition never reached the backend, so it has no truth. The others have theirs.
    assert_eq!(truth_of(&record, "orders.eu/3"), "none");
    assert_eq!(truth_of(&record, "orders.eu/4"), "ok");
    assert_eq!(truth_of(&record, "payments.settled/0"), "ok");
}

#[test]
fn refusals_of_every_partition_leave_nothing_to_forward() {
    let mut driver = Driver::with([
        on_topic("Leader flap", "orders.*", refusal(NOT_LEADER)),
        on_topic("Slow payments", "payments.*", refusal(TIMED_OUT)),
    ]);
    let request = produce(41, &["orders.eu/3", "payments.settled/0"]);

    // The first refusal forwards the rest, the second one finds nothing left, and the backend sees nothing.
    let refused = driver.send(&request);
    assert_eq!(refused.kinds(), ["send", "emit"]);
    assert_eq!(
        told(&request, &refused.sent(&request)),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "payments.settled/0 REQUEST_TIMED_OUT"
        ]
    );

    let record = refused.emitted();
    assert_eq!(
        record.verdict().to_string(),
        "NOT_LEADER_OR_FOLLOWER on orders.eu/3; REQUEST_TIMED_OUT on payments.settled/0"
    );
    assert_eq!(record.timing().forwarded_at(), None);
}

#[test]
fn a_refusal_after_the_response_lets_the_backend_handle_the_request() {
    let lost = on_topic("Lost acknowledgement", "orders.*", late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost]);
    let request = produce(41, &["orders.eu/3", "payments.settled/0"]);

    // The backend gets the whole request, and writes every record.
    let arrived = driver.send(&request);
    assert_eq!(arrived.forwarded().1.frame(), request.frame());

    // The client is told that its orders failed.
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(
        told(&request, &answered.sent(&request)),
        ["orders.eu/3 REQUEST_TIMED_OUT", "payments.settled/0 ok"]
    );

    // The record keeps both: the truth, and what the client received.
    let record = answered.emitted();
    assert_eq!(
        record.verdict().to_string(),
        "REQUEST_TIMED_OUT after the response on orders.eu/3"
    );
    assert_eq!(truth_of(&record, "orders.eu/3"), "ok");
    let delivered = record.delivered().response().unwrap();
    assert_eq!(delivered.part(&part("orders.eu/3")), Some(Answer::Error(TIMED_OUT)));
    assert_eq!(delivered.part(&part("payments.settled/0")), Some(Answer::Ok));
}

#[test]
fn a_refusal_with_a_higher_priority_shadows_one_of_the_same_partition() {
    let mut driver = Driver::with([
        on_partition("Leader flap", "orders.eu", "3", refusal(NOT_LEADER)),
        on_topic("Refuse orders", "orders.*", refusal(TIMED_OUT)),
    ]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4"]);

    let refused = driver.send(&request);
    assert_eq!(
        told(&request, &refused.sent(&request)),
        ["orders.eu/3 NOT_LEADER_OR_FOLLOWER", "orders.eu/4 REQUEST_TIMED_OUT"]
    );

    // The explanation names the winner and the partition.
    let record = refused.emitted();
    assert_eq!(
        explained(&record)[1],
        "Refuse orders: matched orders.eu/3, orders.eu/4; match 1: fired; \
         REQUEST_TIMED_OUT applied on orders.eu/4, shadowed on orders.eu/3 by Leader flap"
    );
    assert!(record.explanation().applied("Refuse orders"));
}

// A Produce that expects no response.

#[test]
fn a_produce_with_acks_0_has_no_answer() {
    let mut driver = Driver::with([]);
    let request = produce_with_acks(41, &["orders.eu/3"], 0);

    // The backend gets the records, and the call is over: no response comes, and none is sent.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward", "emit"]);
    let record = arrived.emitted();
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert!(record.verdict().is_pass());
    assert_eq!(driver.pipeline.in_flight(), 0);

    // A backend that answers anyway is not heard, and the next call is not held up.
    assert!(driver.answer(&arrived.forwarded()).is_empty());
    let next = heartbeat(42);
    let arrived = driver.send(&next);
    assert_eq!(driver.answer(&arrived.forwarded()).kinds(), ["send", "emit"]);
}

#[test]
fn a_refused_produce_with_acks_0_keeps_its_records_from_the_backend_and_sends_nothing() {
    let flap = on_partition("Leader flap", "orders.eu", "3", refusal(NOT_LEADER));
    let mut driver = Driver::with([flap]);

    // Some partitions: the backend gets the others, and the client gets nothing.
    let partly = driver.send(&produce_with_acks(41, &["orders.eu/3", "orders.eu/4"], 0));
    assert_eq!(partly.kinds(), ["forward", "emit"]);
    assert_eq!(asked(&partly.forwarded().1), ["orders.eu/4"]);
    let record = partly.emitted();
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert_eq!(record.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");

    // Every partition: the backend gets nothing either, and the answer of the refusal is not sent.
    let wholly = driver.send(&produce_with_acks(42, &["orders.eu/3"], 0));
    assert_eq!(wholly.kinds(), ["emit"]);
    assert_eq!(wholly.emitted().delivered(), &Delivered::Nothing);
}

#[test]
fn a_lie_after_the_response_is_not_told_on_a_call_without_a_response() {
    let lost = on_topic("Lost acknowledgement", "orders.*", late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost]);

    let arrived = driver.send(&produce_with_acks(41, &["orders.eu/3"], 0));

    assert_eq!(arrived.kinds(), ["forward", "emit"]);
    let record = arrived.emitted();
    assert!(record.verdict().is_pass());
    let fate = record
        .explanation()
        .rule("Lost acknowledgement")
        .unwrap()
        .lie()
        .unwrap();
    assert_eq!(
        fate.failure().map(Failure::message),
        Some("the call has no response: a Produce with acks=0 expects none")
    );
}

#[test]
fn an_answer_for_a_call_that_does_not_wait_changes_nothing() {
    let mut driver = Driver::with([]);
    let request = heartbeat(41);
    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();

    // No call has this number, and this connection has no such call.
    assert!(driver.answer_with(Driver::call(9), truth(&forwarded)).is_empty());
    let elsewhere = CallId::new(ConnectionNumber::new(8), call.number());
    assert!(driver.answer_with(elsewhere, truth(&forwarded)).is_empty());

    // The backend answers once.
    assert_eq!(driver.answer_with(call, truth(&forwarded)).kinds(), ["send", "emit"]);
    assert!(driver.answer_with(call, truth(&forwarded)).is_empty());
}

#[test]
fn a_lie_that_applied_spends_its_firing_for_good() {
    use gaslightmq_trigger::prelude::*;

    let once =
        on_api("Rebalance once", ApiKey::HEARTBEAT, refusal(REBALANCING)).with_lifetime(Lifetime::Firings(Count::ONE));
    let mut driver = Driver::with([once]);

    // The first Heartbeat takes the one firing of the lifetime.
    let first = driver.send(&heartbeat(41)).emitted();
    assert_eq!(first.verdict().to_string(), "REBALANCE_IN_PROGRESS");
    let snapshot = driver.pipeline.snapshot().clone();
    let rule = snapshot.get(&"Rebalance once".parse().unwrap()).unwrap();
    assert!(rule.arming().has_ended(driver.now()));

    // The second one finds the lifetime over, before the engine disarmed the rule: it passes, and it is no match.
    let arrived = driver.send(&heartbeat(42));
    let second = driver.answer(&arrived.forwarded()).emitted();
    assert!(second.verdict().is_pass());
    assert_eq!(
        explained(&second),
        ["Rebalance once: matched the call; 0 of 1 left: lifetime-over"]
    );
    assert_eq!(rule.arming().counter().matches(), 1);
}
