//! A mistake of GaslightMQ never looks like a lie: what cannot be told changes nothing, and the record says why.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse, ProduceResponse};
use gaslightmq_wire::{Frame, Request, Response};

use crate::driver::*;

#[test]
fn a_lie_of_a_kind_that_the_build_leaves_out_is_not_told() {
    // This build tells no refusal.
    let flap = on_topic("Leader flap", "orders.*", refusal(NOT_LEADER)).with_lifetime(Lifetime::Firings(Count::ONE));
    let mut driver = Driver::telling(Lies::of_every_build(), [flap]);
    let request = produce(41, &["orders.eu/3"]);

    // The call goes through untouched.
    let arrived = driver.send(&request);
    assert_eq!(arrived.forwarded().1.frame(), request.frame());
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(told(&request, &answered.sent(&request)), ["orders.eu/3 ok"]);

    // The record says that the rule fired, that its lie was not told, and why.
    let record = answered.emitted();
    assert!(record.verdict().is_pass());
    assert_eq!(
        explained(&record),
        ["Leader flap: matched orders.eu/3; match 1, 0 of 1 left: fired; \
          NOT_LEADER_OR_FOLLOWER not told: no behaviour is registered for refusal"]
    );
    assert!(!record.explanation().applied("Leader flap"));

    // The firing goes back to the budget, so the lifetime of the rule has not ended.
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"Leader flap".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().budget().map(Budget::left), Some(1));
}

#[test]
fn only_the_lie_that_fails_changes_nothing() {
    // A Metadata request for every topic names nothing, so its response has no place for an error.
    let refuse = on_api(
        "No metadata",
        ApiKey::METADATA,
        refusal(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION),
    );
    let slow = on_api(
        "Delay 1 s",
        ApiKey::METADATA,
        TimingLie::new().with_delay(TimeSpan::from_secs(1)),
    );
    let mut driver = Driver::with([refuse, slow]);
    let request = Request::new(
        9,
        41,
        Some("order-ingest"),
        &MetadataRequest::default().with_topics(None),
    )
    .unwrap();
    let truth = request.response(&MetadataResponse::default()).unwrap();

    // The refusal cannot be told, so the backend gets the request.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward"]);

    // The delay of the other rule still applies.
    let answered = driver.answer_with(arrived.forwarded().0, truth.clone());
    assert_eq!(answered.kinds(), ["set timer"]);
    let fired = driver.fire(answered.timer());
    assert_eq!(fired.sent(&request).frame(), truth.frame());

    let record = fired.emitted();
    assert_eq!(record.verdict().to_string(), "+1 s");
    let fate = record.explanation().rule("No metadata").unwrap().lie().unwrap();
    assert_eq!(
        fate.failure().map(Failure::message),
        Some("refusal cannot be told on this Metadata v9 request: its response has no place for an error code")
    );
}

#[test]
fn a_terminal_lie_that_cannot_be_told_ends_nothing_and_takes_nothing_from_the_other_lies() {
    // This build tells refusals, and no silence.
    let lost = on_success("Lost acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let blackhole = on_api("Blackhole", ApiKey::PRODUCE, LieDefinition::Silence);
    let mut driver = Driver::telling(Lies::new().with(Refusal).unwrap(), [lost, blackhole]);
    let request = produce(41, &["orders.eu/3"]);

    // The call is forwarded, and the session is not stalled.
    let arrived = driver.send(&request);
    assert_eq!(arrived.forwarded().1.frame(), request.frame());
    assert_eq!(driver.pipeline.stalled(), None);

    // The silence never acted, so the rule that waits for the truth gets its pass, and its lie applies.
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(
        told(&request, &answered.sent(&request)),
        ["orders.eu/3 REQUEST_TIMED_OUT"]
    );
    let record = answered.emitted();
    assert_eq!(
        explained(&record),
        [
            "Lost acknowledgement: matched orders.eu/3; match 1: fired; \
             REQUEST_TIMED_OUT after the response applied on orders.eu/3",
            "Blackhole: matched orders.eu/3; match 1: fired; silence not told: no behaviour is registered for silence",
        ]
    );
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"Lost acknowledgement".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().counter().matches(), 1);
}

#[test]
fn a_lie_that_lost_to_a_terminal_lie_that_cannot_be_told_applies_after_all() {
    // This build tells hang-ups and refusals, and no silence.
    let lies = Lies::new().with(Refusal).unwrap().with(HangUp).unwrap();
    let blackhole = on_api("Blackhole", ApiKey::HEARTBEAT, LieDefinition::Silence);
    let disconnect = on_api("Disconnect", ApiKey::HEARTBEAT, HangUpLie::new());
    let mut driver = Driver::telling(lies, [blackhole, disconnect]);

    // The silence has the higher priority, and it cannot be told. The hang-up is asked in its place.
    let hung_up = driver.send(&heartbeat(41));

    assert_eq!(hung_up.kinds(), ["emit", "close"]);
    let record = hung_up.emitted();
    assert_eq!(record.verdict().to_string(), "hang-up");
    assert_eq!(
        explained(&record),
        [
            "Blackhole: matched the call; match 1: fired; silence not told: no behaviour is registered for silence",
            "Disconnect: matched the call; match 1: fired; hang-up applied",
        ]
    );
}

#[test]
fn a_request_that_does_not_decode_goes_through_and_no_rule_sees_it() {
    // A rule that would silence every call, and a request of an API that the codec has no messages for.
    let everything = gaslightmq_rule::Rule::new(
        "Blackhole".parse().unwrap(),
        gaslightmq_selector::Conditions::new(),
        LieDefinition::Silence,
    );
    let mut driver = Driver::with([everything]);
    // The header of a request of API key 9999: the key, the version, the correlation ID and no client ID.
    let header: &[u8] = &[0x27, 0x0F, 0, 0, 0, 0, 0, 41, 0xFF, 0xFF];
    let request = Request::decode(Frame::new(header.to_vec())).unwrap();

    // The backend gets the request as it arrived.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward"]);
    let (call, forwarded) = arrived.forwarded();
    assert_eq!(forwarded.frame(), request.frame());

    // Its answer goes to the client as it is.
    let answer = Response::decode(request.api(), Frame::new(vec![0, 0, 0, 41, 1, 2, 3])).unwrap();
    let answered = driver.answer_with(call, answer.clone());
    assert_eq!(answered.sent(&request).frame(), answer.frame());

    // The record keeps the failure, and no rule has a line.
    let record = answered.emitted();
    assert!(record.explanation().is_empty());
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some("no rule saw the call: a request of api 9999 v0 cannot be edited: its messages are unknown")
    );
}

#[test]
fn the_truth_carries_the_correlation_id_of_the_client_before_a_lie_reads_it() {
    let lost = on_partition("Lost acknowledgement", "orders.eu", "3", late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4"]);

    // The backend numbers its requests anew, and answers under its own number.
    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();
    let body: ProduceResponse = truth(&forwarded).body().unwrap();
    let renumbered = Response::new(forwarded.api(), 9000, &body).unwrap();
    let answered = driver.answer_with(call, renumbered);

    // The refusal merged its error into a response that answers the request of the client.
    let sent = answered.sent(&request);
    assert_eq!(sent.correlation_id(), 41);
    assert_eq!(
        told(&request, &sent),
        ["orders.eu/3 REQUEST_TIMED_OUT", "orders.eu/4 ok"]
    );
    assert_eq!(answered.emitted().explanation().failure(), None);
}

// A backend that answers what nobody can read.

/// A response to `request` whose body ends too early.
fn cut_short(request: &Request) -> Response {
    let whole = truth(request);
    let payload = whole.frame().payload();
    Response::decode(request.api(), Frame::new(payload.slice(..payload.len() - 3))).unwrap()
}

#[test]
fn a_truth_that_nobody_can_read_has_no_outcome_and_no_rule_tests_it() {
    // Both rules wait for the truth: one takes what succeeded, and one takes whatever did not fail.
    let not_failed = gaslightmq_selector::Conditions::new()
        .with(gaslightmq_selector::Condition::is(ApiKey::PRODUCE))
        .with(gaslightmq_selector::Condition::is_not(Outcome::Error));
    let unless_failed = gaslightmq_rule::Rule::new(
        "Unless it failed".parse().unwrap(),
        not_failed,
        LieDefinition::from(late_refusal(NOT_LEADER)),
    );
    let lost = on_success("Lost acknowledgement", ApiKey::PRODUCE, late_refusal(TIMED_OUT));
    let mut driver = Driver::with([lost, unless_failed]);
    let request = produce(41, &["orders.eu/3"]);

    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();
    let unreadable = cut_short(&forwarded);
    let answered = driver.answer_with(call, unreadable.clone());

    // The client gets what the backend sent, and the record does not guess what it says.
    assert_eq!(answered.sent(&request).frame(), unreadable.frame());
    let record = answered.emitted();
    assert_eq!(record.delivered(), &Delivered::UnreadResponse);
    assert_eq!(truth_of(&record, "orders.eu/3"), "none");
    assert!(record.read(Field::Latency).is_present());

    // A truth that nobody read is no success, so neither rule saw the call.
    assert!(record.verdict().is_pass());
    assert!(record.explanation().is_empty());
    let failure = record.explanation().failure().unwrap().message();
    assert!(
        failure.starts_with("the rules saw no truth: the Produce v9 response cannot be read: "),
        "{failure}"
    );
    let snapshot = driver.pipeline.snapshot();
    for rule in snapshot.iter() {
        assert_eq!(rule.arming().counter().matches(), 0, "{}", rule.name());
    }
}

#[test]
fn a_refusal_that_withheld_a_partition_stays_told_when_its_error_cannot_join_the_response() {
    let flap =
        on_partition("Leader flap", "orders.eu", "3", refusal(NOT_LEADER)).with_lifetime(Lifetime::Firings(Count::ONE));
    let mut driver = Driver::with([flap]);
    let request = produce(41, &["orders.eu/3", "orders.eu/4"]);

    // The refused partition never reaches the backend.
    let arrived = driver.send(&request);
    let (call, forwarded) = arrived.forwarded();
    assert_eq!(asked(&forwarded), ["orders.eu/4"]);

    // The backend answers what nobody can read, so the refusal cannot put its error into the response.
    let answered = driver.answer_with(call, cut_short(&forwarded));
    let record = answered.emitted();

    // The lie did act: its partition was withheld. So it is told, and its firing is spent.
    assert_eq!(record.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
    assert!(record.explanation().applied("Leader flap"));
    let fate = record.explanation().rule("Leader flap").unwrap().lie().unwrap();
    assert_eq!(fate.failure(), None);
    let snapshot = driver.pipeline.snapshot();
    let rule = snapshot.get(&"Leader flap".parse().unwrap()).unwrap();
    assert_eq!(rule.arming().budget().map(Budget::left), Some(0));

    // The call says that its response lacks what the lie was to add.
    let failure = record.explanation().failure().unwrap().message();
    assert!(
        failure.contains("the lie of \"Leader flap\" changed the request, and could not make the response fit it: "),
        "{failure}"
    );
    assert_eq!(record.delivered(), &Delivered::UnreadResponse);
}

#[test]
fn an_answer_that_is_no_response_to_the_request_is_not_sent() {
    let mut driver = Driver::with([]);
    let first = driver.send(&heartbeat(41)).forwarded();
    let second = driver.send(&heartbeat(42)).forwarded();

    // The backend answers the first call under its own number, with a header of another version.
    // Read as a response to the request of the client, the frame ends before its header does.
    let other_version = gaslightmq_wire::Api::from_wire(first.1.api().key, 0);
    let no_response = Response::decode(other_version, Frame::new(vec![0, 0, 0x23, 0x28])).unwrap();
    let closed = driver.answer_with(first.0, no_response);

    // The client must not get a frame under the number of the backend. The proxy has no answer, so it closes.
    assert_eq!(closed.kinds(), ["emit", "emit", "close"]);
    assert_eq!(closed.sends().len(), 0, "nothing is sent");
    let [unanswered, behind] = closed.calls().try_into().unwrap();
    assert_eq!(unanswered.delivered(), &Delivered::ConnectionClosed);
    let failure = unanswered.explanation().failure().unwrap().message();
    assert!(
        failure.starts_with("the response of the backend does not read as a Heartbeat v4 response: "),
        "{failure}"
    );
    assert_eq!(behind.delivered(), &Delivered::Nothing);
    let closed_by = driver.pipeline.closed().map(|closed| closed.by());
    assert_eq!(closed_by, Some(gaslightmq_session::ClosedBy::Proxy));
    assert!(driver.answer(&second).is_empty());
}

// A Produce whose body does not decode.

/// `request` without the last `bytes` of its body.
fn without_the_end(request: &Request, bytes: usize) -> Request {
    let payload = request.frame().payload();
    Request::decode(Frame::new(payload.slice(..payload.len() - bytes))).unwrap()
}

#[test]
fn a_produce_that_does_not_decode_still_says_whether_it_expects_a_response() {
    let mut driver = Driver::with([]);

    // `acks=0`: the backend gets the request as it is, and no response comes.
    let no_acks = without_the_end(&produce_with_acks(41, &["orders.eu/3"], 0), 3);
    let arrived = driver.send(&no_acks);
    assert_eq!(arrived.kinds(), ["forward", "emit"]);
    assert_eq!(arrived.forwarded().1.frame(), no_acks.frame());
    let record = arrived.emitted();
    assert_eq!(record.delivered(), &Delivered::Nothing);
    let failure = record.explanation().failure().unwrap().message();
    assert!(
        failure.starts_with("no rule saw the call: the Produce v9 request cannot be read: "),
        "{failure}"
    );

    // So the call behind it is not held up.
    let next = heartbeat(42);
    let arrived = driver.send(&next);
    assert_eq!(driver.answer(&arrived.forwarded()).kinds(), ["send", "emit"]);

    // `acks=-1`: a response comes, and the pipeline waits for it.
    let all_acks = without_the_end(&produce(43, &["orders.eu/3"]), 3);
    let arrived = driver.send(&all_acks);
    assert_eq!(arrived.kinds(), ["forward"]);
    let answer = Response::decode(all_acks.api(), Frame::new(vec![0, 0, 0, 43, 0])).unwrap();
    let answered = driver.answer_with(arrived.forwarded().0, answer);
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(answered.emitted().delivered(), &Delivered::UnreadResponse);
}

#[test]
fn a_produce_that_does_not_say_its_acks_closes_the_connection() {
    let mut driver = Driver::with([]);
    let in_flight = driver.send(&heartbeat(41));
    assert_eq!(in_flight.kinds(), ["forward"]);

    // The body ends before the `acks`: nothing says whether a response comes.
    let whole = produce(42, &["orders.eu/3"]);
    let nothing_of_the_body = without_the_end(&whole, whole.body_bytes().len());
    let closed = driver.send(&nothing_of_the_body);

    // Waiting for a response that never comes would stall every answer behind it, and not waiting could mix them up.
    // The proxy closes the connection, as a broker does, and the backend never sees the request.
    assert_eq!(closed.kinds(), ["emit", "emit", "close"]);
    let [_, produce] = closed.calls().try_into().unwrap();
    assert_eq!(produce.delivered(), &Delivered::Nothing);
    let failure = produce.explanation().failure().unwrap().message();
    assert!(
        failure.starts_with("the Produce request does not say whether it expects a response: "),
        "{failure}"
    );
    let closed_by = driver.pipeline.closed().map(|closed| closed.by());
    assert_eq!(closed_by, Some(gaslightmq_session::ClosedBy::Proxy));
}

#[test]
fn a_moment_that_does_not_follow_the_one_before_it_is_missing_and_the_record_says_so() {
    let mut driver = Driver::with([]);
    driver.clock.advance(gaslightmq_clock::Duration::from_secs(5));
    let request = heartbeat(41);
    let (call, forwarded) = driver.send(&request).forwarded();

    // The engine reports the answer with an instant before the one it forwarded the request at.
    let earlier = gaslightmq_clock::Instant::ZERO;
    let input = gaslightmq_pipeline::Input::BackendAnswered {
        call,
        response: truth(&forwarded),
    };
    let outputs: Vec<_> = driver.pipeline.handle(input, earlier).into_iter().collect();
    let answered = Seen(outputs);

    // The client gets its answer, and the call gets its record: it says which moments it lacks, and why.
    assert_eq!(answered.kinds(), ["send", "emit"]);
    let record = answered.emitted();
    assert_eq!(record.timing().answered_at(), None);
    assert_eq!(record.timing().delivered_at(), None);
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some(
            "a moment of the call is missing: a backend does not answer before the request leaves for it; \
             a moment of the call is missing: a response does not leave for the client before the call got that far"
        )
    );
}
