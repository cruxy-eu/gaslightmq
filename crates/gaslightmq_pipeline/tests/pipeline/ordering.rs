//! Requests reach the backend in the order they arrived, and answers leave in that order.

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;

use crate::driver::*;

fn delay(millis: u64) -> TimingLie {
    TimingLie::new().with_delay(TimeSpan::from_millis(millis))
}

#[test]
fn answers_leave_in_the_order_of_their_requests() {
    let mut driver = Driver::with([]);
    let first = driver.send(&heartbeat(41)).forwarded();
    let second = driver.send(&heartbeat(42)).forwarded();

    // The backend answers the second call first. Its answer waits for the one before it.
    assert!(driver.answer(&second).is_empty());

    let answered = driver.answer(&first);
    assert_eq!(answered.kinds(), ["send", "emit", "send", "emit"]);
    let [one, two] = answered.calls().try_into().unwrap();
    assert_eq!((one.id(), two.id()), (Driver::call(1), Driver::call(2)));
}

#[test]
fn a_delay_after_the_response_holds_back_the_answers_behind_it_and_no_request() {
    let mut driver = Driver::with([on_api("delay-1s", ApiKey::PRODUCE, delay(1000))]);
    let slow = driver.send(&produce(41, &["orders.eu/3"])).forwarded();

    // The answer of the Produce waits for its timer.
    let timer = driver.answer(&slow).timer();

    // The Heartbeat behind it reaches the backend on time, and its answer waits for the Produce.
    let behind = driver.send(&heartbeat(42));
    assert_eq!(behind.kinds(), ["forward"]);
    assert!(driver.answer(&behind.forwarded()).is_empty());

    // The timer fires, and both answers leave in order.
    let fired = driver.fire(timer);
    assert_eq!(fired.kinds(), ["send", "emit", "send", "emit"]);
    assert_eq!(fired.calls()[1].id(), Driver::call(2));
    // The Heartbeat was not delayed by a rule, and still waited a second.
    assert!(fired.calls()[1].verdict().is_pass());
}

#[test]
fn a_delay_before_forwarding_holds_back_the_requests_behind_it() {
    let late = delay(1000).at(Stage::BeforeForwarding);
    let mut driver = Driver::with([on_api("late-produce", ApiKey::PRODUCE, late)]);

    let timer = driver.send(&produce(41, &["orders.eu/3"])).timer();
    // The Heartbeat is ready at once, and it waits for the request before it.
    assert!(driver.send(&heartbeat(42)).is_empty());

    let fired = driver.fire(timer);
    assert_eq!(fired.kinds(), ["forward", "forward"]);
    let [first, second] = fired.forwards().try_into().unwrap();
    assert_eq!((first.0, second.0), (Driver::call(1), Driver::call(2)));
}

#[test]
fn a_call_that_the_lies_answered_waits_for_the_answers_before_it() {
    let mut driver = Driver::with([on_api("rebalance", ApiKey::HEARTBEAT, refusal(REBALANCING))]);
    let produce = produce(41, &["orders.eu/3"]);
    let slow = driver.send(&produce).forwarded();

    // The Heartbeat is refused without the backend, and its answer is ready at once.
    // The client still gets the answer of the Produce first.
    assert!(driver.send(&heartbeat(42)).is_empty());

    let answered = driver.answer(&slow);
    assert_eq!(answered.kinds(), ["send", "emit", "send", "emit"]);
    let [first, second] = answered.calls().try_into().unwrap();
    assert!(first.verdict().is_pass());
    assert_eq!(second.verdict().to_string(), "REBALANCE_IN_PROGRESS");
}

#[test]
fn a_hang_up_before_forwarding_waits_for_the_requests_before_it() {
    let late = delay(1000).at(Stage::BeforeForwarding);
    let mut driver = Driver::with([
        on_api("late-produce", ApiKey::PRODUCE, late),
        on_api("disconnect", ApiKey::HEARTBEAT, HangUpLie::new()),
    ]);

    // The Produce is held back, and the Heartbeat behind it hangs up once it is its turn.
    let timer = driver.send(&produce(41, &["orders.eu/3"])).timer();
    assert!(driver.send(&heartbeat(42)).is_empty());
    assert_eq!(driver.pipeline.closed(), None);

    // The Produce goes on first. Then the connection closes, and neither call gets an answer.
    let fired = driver.fire(timer);
    assert_eq!(fired.kinds(), ["forward", "emit", "emit", "close"]);
    assert_eq!(fired.forwarded().0, Driver::call(1));
    let [produce, heartbeat] = fired.calls().try_into().unwrap();
    assert_eq!(produce.delivered().to_string(), "nothing");
    assert_eq!(heartbeat.delivered().to_string(), "connection closed");
    // The delay of the Produce did hold its request back, so that lie applied.
    assert_eq!(produce.verdict().to_string(), "+1 s before forwarding");
}

#[test]
fn a_delay_after_the_response_also_holds_an_answer_that_a_refusal_gave() {
    let mut driver = Driver::with([
        on_api("rebalance", ApiKey::HEARTBEAT, refusal(REBALANCING)),
        on_api("delay-1s", ApiKey::HEARTBEAT, delay(1000)),
    ]);
    let request = heartbeat(41);

    // The backend never sees the Heartbeat, and the client still waits a second for its error.
    let refused = driver.send(&request);
    assert_eq!(refused.kinds(), ["set timer"]);
    let fired = driver.fire(refused.timer());
    assert_eq!(fired.kinds(), ["send", "emit"]);

    let record = fired.emitted();
    assert_eq!(record.verdict().to_string(), "REBALANCE_IN_PROGRESS; +1 s");
    assert_eq!(
        record.timing().client_waited(),
        Some(gaslightmq_clock::Duration::from_secs(1))
    );
}

#[test]
fn a_timer_of_a_session_that_ended_changes_nothing() {
    let mut driver = Driver::with([on_api("delay-1s", ApiKey::HEARTBEAT, delay(1000))]);
    let arrived = driver.send(&heartbeat(41));
    let timer = driver.answer(&arrived.forwarded()).timer();

    // The client gives up while its answer waits. The delay did hold the answer back.
    let closed = driver.handle(gaslightmq_pipeline::Input::ClientClosed);
    assert_eq!(closed.emitted().delivered().to_string(), "nothing");
    assert_eq!(closed.emitted().verdict().to_string(), "+1 s");

    assert!(driver.fire(timer).is_empty());
}
