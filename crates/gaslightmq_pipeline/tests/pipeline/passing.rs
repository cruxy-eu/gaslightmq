//! A call that no lie touches goes through as it is.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::Duration;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_pipeline::prelude::*;
use gaslightmq_session::prelude::*;

use crate::driver::*;

#[test]
fn a_call_with_no_rule_goes_through_untouched() {
    let mut driver = Driver::with([]);
    let request = produce(41, &["orders.eu/3", "payments.settled/0"]);

    // The request goes to the backend as the client sent it, byte for byte.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward"]);
    let (call, forwarded) = arrived.forwarded();
    assert_eq!(call, Driver::call(1));
    assert_eq!(forwarded.frame(), request.frame());

    // The backend takes four milliseconds, and the truth goes to the client, byte for byte.
    driver.clock.advance(Duration::from_millis(4));
    let truth = truth(&forwarded);
    let answered = driver.answer_with(call, truth.clone());
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(answered.sent(&request).frame(), truth.frame());

    // The record says that nothing happened to the call.
    let record = answered.emitted();
    assert_eq!(record.id(), call);
    assert!(record.verdict().is_pass());
    assert!(record.explanation().is_empty());
    assert_eq!(record.explanation().failure(), None);
    assert_eq!(
        record.delivered().to_string(),
        "ok",
        "every partition was written, so no part has an error"
    );
    let delivered = record.delivered().response().unwrap();
    assert_eq!(delivered.part(&part("orders.eu/3")), Some(Answer::Ok));
    assert_eq!(delivered.part(&part("payments.settled/0")), Some(Answer::Ok));

    // It holds the fields the rules saw: who called, what the request asked, and the truth.
    let read = |field| {
        record
            .read(field)
            .present()
            .map(|values| values.iter().next().unwrap().to_string())
    };
    assert_eq!(read(Field::Connection).as_deref(), Some("#7"));
    assert_eq!(read(Field::ClientId).as_deref(), Some("order-ingest"));
    assert_eq!(read(Field::Api).as_deref(), Some("Produce"));
    assert_eq!(read(Field::ApiVersion).as_deref(), Some("9"));
    assert_eq!(read(Field::Acks).as_deref(), Some("-1"));
    assert_eq!(read(Field::Latency).as_deref(), Some("4ms"));
    assert_eq!(truth_of(&record, "orders.eu/3"), "ok");

    // And the moments of the call.
    let timing = record.timing();
    assert_eq!(timing.backend_took(), Some(Duration::from_millis(4)));
    assert_eq!(timing.client_waited(), Some(Duration::from_millis(4)));
    assert_eq!(driver.pipeline.in_flight(), 0);
}

#[test]
fn the_first_request_identifies_the_session() {
    let mut driver = Driver::with([]);
    assert_eq!(driver.pipeline.lifecycle(), Lifecycle::Opened);
    assert_eq!(driver.pipeline.key().to_string(), "10.0.4.27 broker 2 #7");

    driver.send(&heartbeat(1));

    assert_eq!(driver.pipeline.lifecycle(), Lifecycle::Identified);
    let client_id = driver.pipeline.identity().client_id();
    assert_eq!(client_id.map(ClientId::as_str), Some("order-ingest"));
}

#[test]
fn bytes_that_end_in_the_middle_of_a_request_wait_for_the_rest() {
    let mut driver = Driver::with([]);
    let first = heartbeat(1).frame().to_bytes();
    let second = heartbeat(2).frame().to_bytes();
    let both = [first, second].concat();
    let (start, rest) = both.split_at(both.len() - 5);

    // The first request is whole, and the second one is not.
    let arrived = driver.handle(Input::ClientBytes(start));
    assert_eq!(arrived.forwarded().0, Driver::call(1));

    let arrived = driver.handle(Input::ClientBytes(rest));
    assert_eq!(arrived.forwarded().0, Driver::call(2));
}

#[test]
fn a_rule_that_does_not_fire_changes_nothing_and_shows_in_the_explanation() {
    use gaslightmq_trigger::prelude::*;

    let third = on_api("refuse-the-3rd-heartbeat", ApiKey::HEARTBEAT, refusal(REBALANCING))
        .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(3).unwrap()));
    let mut driver = Driver::with([third]);
    let request = heartbeat(1);

    let arrived = driver.send(&request);
    let record = driver.answer(&arrived.forwarded()).emitted();

    assert!(record.verdict().is_pass());
    assert_eq!(
        explained(&record),
        ["refuse-the-3rd-heartbeat: matched the call; match 1, count did not agree: held"]
    );
}

#[test]
fn an_incident_of_a_rule_goes_to_the_engine_and_the_call_passes() {
    use gaslightmq_incident::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;

    let election = Rule::new(
        "leader-election".parse().unwrap(),
        Conditions::from(Condition::parse(Field::Topic, "orders.*").unwrap()),
        IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        },
    );
    let mut driver = Driver::with([election]);
    let request = produce(1, &["orders.eu/3", "payments.settled/0"]);

    // The incident leaves at the resolve stage, before the request does.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["fire", "forward"]);
    let incidents = arrived.incidents();
    let [Incident::LeaderMoves { partition, .. }] = incidents.as_slice() else {
        panic!("the rule moves one leader, and not {incidents:?}");
    };
    assert_eq!(partition.to_string(), "orders.eu/3");

    // An incident is no lie: the call passes, and the explanation keeps the incident.
    let record = driver.answer(&arrived.forwarded()).emitted();
    assert!(record.verdict().is_pass());
    assert!(record.explanation().applied("leader-election"));
    assert_eq!(record.explanation().incidents().count(), 1);
}
