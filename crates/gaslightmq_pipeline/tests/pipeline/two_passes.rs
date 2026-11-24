//! A rule that tests the truth waits for the backend: the pipeline evaluates its rules in two passes.

use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_wire::messages::ProduceResponse;

use crate::driver::*;

/// The truth of a Produce in which the backend failed the first partition it answered.
fn truth_with_a_failed_partition(request: &gaslightmq_wire::Request, error: ErrorCode) -> gaslightmq_wire::Response {
    let mut body: ProduceResponse = truth(request).body().unwrap();
    body.responses[0].partition_responses[0].error_code = error.value();
    request.response(&body).unwrap()
}

#[test]
fn a_rule_of_the_response_pass_only_lies_about_what_the_backend_answered() {
    let lost = on_success("lost-acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4"]);

    // The rule waits for the truth, so the request pass leaves the request alone.
    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();
    assert_eq!(forwarded.frame(), request.frame());

    // The backend fails one partition by itself. The rule refuses the one that succeeded.
    let truth = truth_with_a_failed_partition(&forwarded, ErrorCode::NOT_ENOUGH_REPLICAS);
    let answered = driver.answer_with(call, truth);
    assert_eq!(
        told(&request, &answered.sent(&request)),
        ["orders.eu/3 NOT_ENOUGH_REPLICAS", "orders.eu/4 REQUEST_TIMED_OUT"]
    );

    let record = answered.emitted();
    assert_eq!(
        explained(&record),
        ["lost-acknowledgement: matched orders.eu/4; match 1: fired; \
          REQUEST_TIMED_OUT after the response applied on orders.eu/4"]
    );
    assert_eq!(truth_of(&record, "orders.eu/3"), "NOT_ENOUGH_REPLICAS");
    assert_eq!(truth_of(&record, "orders.eu/4"), "ok");
}

#[test]
fn a_rule_of_the_response_pass_that_does_not_match_the_truth_leaves_no_line() {
    let lost = on_success("lost-acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost]);
    let request = produce(41, &["orders.eu/3"]);

    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();
    let truth = truth_with_a_failed_partition(&forwarded, ErrorCode::NOT_ENOUGH_REPLICAS);
    let record = driver.answer_with(call, truth).emitted();

    assert!(record.verdict().is_pass());
    assert!(record.explanation().is_empty());
}

#[test]
fn the_response_pass_leaves_out_a_partition_that_was_refused_before_forwarding() {
    // The rule of the response pass takes every partition without an error, also the one that has no truth.
    let not_failed = Conditions::new()
        .with(Condition::is(ApiKey::PRODUCE))
        .with(Condition::is_not(Outcome::Error));
    let mut driver = Driver::with([
        on_partition("leader-flap", "orders.eu", "3", refusal(NOT_LEADER)),
        Rule::new(
            "lost-acknowledgement".parse().unwrap(),
            not_failed,
            LieDefinition::from(late_refusal(TIMED_OUT)),
        ),
    ]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4", "payments.settled/0"]);

    let arrived = driver.send(&request);
    assert_eq!(asked(&arrived.forwarded().1), ["orders.eu/4", "payments.settled/0"]);
    let answered = driver.answer(&arrived.forwarded());

    // The refused partition keeps its own refusal, and the others get the one of the response pass.
    assert_eq!(
        told(&request, &answered.sent(&request)),
        [
            "orders.eu/3 NOT_LEADER_OR_FOLLOWER",
            "orders.eu/4 REQUEST_TIMED_OUT",
            "payments.settled/0 REQUEST_TIMED_OUT"
        ]
    );
    // The explanation names the refusal that got there first.
    let record = answered.emitted();
    assert_eq!(
        explained(&record)[1],
        "lost-acknowledgement: matched orders.eu/3, orders.eu/4, payments.settled/0; match 1: fired; \
         REQUEST_TIMED_OUT after the response applied on orders.eu/4, payments.settled/0, \
         pre-empted on orders.eu/3 by leader-flap"
    );
}

#[test]
fn a_refusal_of_the_whole_call_in_the_response_pass_replaces_the_truth() {
    // A Heartbeat names no partition, so the rule holds the whole call.
    let lost = on_success("lost-heartbeat", ApiKey::HEARTBEAT, late_refusal(REBALANCING));
    let mut driver = Driver::with([lost]);
    let request = heartbeat(41);

    let arrived = driver.send(&request);
    let answered = driver.answer(&arrived.forwarded());

    let record = answered.emitted();
    assert_eq!(record.verdict().to_string(), "REBALANCE_IN_PROGRESS after the response");
    assert_eq!(record.delivered().to_string(), "REBALANCE_IN_PROGRESS");
    // The truth was a success, and the rule saw it.
    let outcome = record.read(Field::Outcome).present().unwrap();
    assert_eq!(outcome.iter().next().unwrap().to_string(), "ok");
}

#[test]
fn a_call_in_flight_finishes_on_the_snapshot_it_started_with() {
    let lost = || on_success("lost-acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost()]);
    let first = produce(41, &["orders.eu/3"]);
    let second = produce(42, &["orders.eu/3"]);

    // The first call starts while the rule is armed.
    let in_flight = driver.send(&first);
    let old_snapshot = driver.pipeline.snapshot().clone();

    // Someone disarms the rule: the engine publishes a snapshot without it.
    assert!(driver.publish([]).is_empty());
    assert!(driver.pipeline.snapshot().is_empty());
    let after = driver.send(&second);

    // The first call finishes on its old snapshot: its response pass still has the rule.
    let answered = driver.answer(&in_flight.forwarded());
    assert_eq!(told(&first, &answered.sent(&first)), ["orders.eu/3 REQUEST_TIMED_OUT"]);
    let record = answered.emitted();
    assert_eq!(record.id(), Driver::call(1));
    assert!(record.explanation().applied("lost-acknowledgement"));

    // The call that arrived after the change uses the new one.
    let answered = driver.answer(&after.forwarded());
    assert_eq!(told(&second, &answered.sent(&second)), ["orders.eu/3 ok"]);
    assert!(answered.emitted().explanation().is_empty());

    // The rule counted the one call that it matched.
    let rule = old_snapshot.get(&"lost-acknowledgement".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().counter().matches(), 1);
}

#[test]
fn a_rule_that_is_armed_while_a_call_is_in_flight_does_not_touch_that_call() {
    let mut driver = Driver::with([]);
    let request = produce(41, &["orders.eu/3"]);

    let in_flight = driver.send(&request);
    driver.publish([on_topic("lost-acknowledgement", "orders.*", late_refusal(TIMED_OUT))]);

    let answered = driver.answer(&in_flight.forwarded());
    assert_eq!(told(&request, &answered.sent(&request)), ["orders.eu/3 ok"]);
    assert!(answered.emitted().verdict().is_pass());
}

#[test]
fn a_hang_up_of_the_response_pass_and_a_refusal_before_forwarding_both_apply() {
    let late = on_success(
        "late-disconnect",
        ApiKey::PRODUCE,
        HangUpLie::new().at(Stage::AfterResponse),
    );
    let flap = on_partition("leader-flap", "orders.eu", "3", refusal(NOT_LEADER));
    let mut driver = Driver::with([late, flap]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4"]);

    // The refused partition never reaches the backend.
    let arrived = driver.send(&request);
    assert_eq!(asked(&arrived.forwarded().1), ["orders.eu/4"]);

    // The backend answers the rest, and the connection closes.
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(answered.kinds(), ["emit", "close"]);
    let record = answered.emitted();
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);
    assert_eq!(
        record.verdict().to_string(),
        "hang-up after the response; NOT_LEADER_OR_FOLLOWER on orders.eu/3"
    );
}
