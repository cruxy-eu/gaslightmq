//! A delay is a timer: the pipeline asks for it, and the held request or answer leaves when it fires.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_pipeline::prelude::*;
use gaslightmq_trigger::prelude::*;

use crate::driver::*;

const SECOND: Duration = Duration::from_secs(1);

fn delay(millis: u64) -> TimingLie {
    TimingLie::new().with_delay(TimeSpan::from_millis(millis))
}

#[test]
fn a_delay_after_the_response_holds_the_answer_until_its_timer_fires() {
    let mut driver = Driver::with([on_api("Delay 1 s", ApiKey::PRODUCE, delay(1000))]);
    let request = produce(41, &["orders.eu/3"]);

    // The backend sees the request on time.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["forward"]);

    // The answer waits: the pipeline asks for a timer, and sends nothing.
    driver.clock.advance(Duration::from_millis(4));
    let answered = driver.answer(&arrived.forwarded());
    assert_eq!(answered.kinds(), ["set timer"]);
    let (timer, at) = answered.timer();
    assert_eq!(timer, TimerId::new(Driver::call(1), Stage::AfterResponse));
    assert_eq!(at, driver.now() + SECOND);

    // The timer fires, and the answer leaves. It is the truth: a delay changes when, and not what.
    let fired = driver.fire((timer, at));
    assert_eq!(fired.kinds(), ["send", "emit"]);
    assert_eq!(told(&request, &fired.sent(&request)), ["orders.eu/3 ok"]);

    let record = fired.emitted();
    assert_eq!(record.verdict().to_string(), "+1 s");
    assert_eq!(
        explained(&record),
        ["Delay 1 s: matched orders.eu/3; match 1: fired; +1 s applied"]
    );
    assert_eq!(record.timing().backend_took(), Some(Duration::from_millis(4)));
    assert_eq!(record.timing().client_waited(), Some(Duration::from_millis(1004)));
}

#[test]
fn a_delay_before_forwarding_holds_the_request_until_its_timer_fires() {
    let late = delay(45_000).at(Stage::BeforeForwarding);
    let mut driver = Driver::with([on_api("Late heartbeat", ApiKey::HEARTBEAT, late)]);
    let request = heartbeat(41);

    // The backend does not see the request yet.
    let arrived = driver.send(&request);
    assert_eq!(arrived.kinds(), ["set timer"]);
    let (timer, at) = arrived.timer();
    assert_eq!(timer, TimerId::new(Driver::call(1), Stage::BeforeForwarding));
    assert_eq!(at, driver.now() + Duration::from_secs(45));

    // The timer fires, and the backend sees the request late.
    let fired = driver.fire((timer, at));
    assert_eq!(fired.kinds(), ["forward"]);
    assert_eq!(fired.forwarded().1.frame(), request.frame());

    // The answer leaves as soon as the backend gives it.
    let answered = driver.answer(&fired.forwarded());
    assert_eq!(answered.kinds(), ["send", "emit"]);
    let record = answered.emitted();
    assert_eq!(record.verdict().to_string(), "+45 s before forwarding");
    let timing = record.timing();
    assert_eq!(
        timing.forwarded_at().map(|at| at - timing.arrived_at()),
        Some(Duration::from_secs(45))
    );
    assert_eq!(timing.backend_took(), Some(Duration::ZERO));
}

#[test]
fn the_delays_of_one_stage_add_up() {
    let mut driver = Driver::with([
        on_api("Delay 1 s", ApiKey::HEARTBEAT, delay(1000)),
        on_api("Background lag", ApiKey::HEARTBEAT, delay(500)),
    ]);

    let arrived = driver.send(&heartbeat(41));
    let answered = driver.answer(&arrived.forwarded());

    assert_eq!(answered.timer().1, driver.now() + Duration::from_millis(1500));
    let record = driver.fire(answered.timer()).emitted();
    assert_eq!(record.verdict().to_string(), "+1 s; +500 ms");
}

#[test]
fn the_jitter_of_a_delay_rolls_on_the_die_of_its_rule() {
    let jittery = || delay(1000).with_jitter(TimeSpan::from_millis(200));
    let waits = || {
        let mut driver = Driver::with([on_api("Slow heartbeat", ApiKey::HEARTBEAT, jittery())]);
        let mut waits = Vec::new();
        for correlation_id in 1..=3 {
            let arrived = driver.send(&heartbeat(correlation_id));
            let answered = driver.answer(&arrived.forwarded());
            waits.push(answered.timer().1 - driver.now());
            driver.fire(answered.timer());
        }
        waits
    };

    // The wait strays from the delay by at most the jitter.
    let first_run = waits();
    for wait in &first_run {
        assert!((800..=1200).contains(&wait.as_millis()), "{wait:?}");
    }

    // The die is the one of the rule, keyed with `jitter`, and it rolls for the match that fired the rule.
    let die = Die::new(Seed::new(SEED), &DiceKey::new("Slow heartbeat").unwrap())
        .for_counter(&CounterKey::new("jitter").unwrap());
    let rolled = |number| u64::from(die.roll(MatchNumber::new(number).unwrap()).ppm());
    let expected: Vec<Duration> = (1..=3)
        .map(|number| Duration::from_millis(800 + 400 * rolled(number) / 1_000_000))
        .collect();
    assert_eq!(first_run, expected);

    // So the same seed gives the same waits in every run, and each match its own.
    assert_eq!(waits(), first_run);
    assert_ne!(first_run[0], first_run[1]);
}

#[test]
fn a_lie_that_only_throttles_is_not_told_and_its_line_says_so_once() {
    let throttle = TimingLie::new().with_throttle(TimeSpan::from_millis(500));
    let mut driver = Driver::with([on_api("Back off", ApiKey::HEARTBEAT, throttle)]);
    let request = heartbeat(41);

    let arrived = driver.send(&request);
    let answered = driver.answer(&arrived.forwarded());

    // The answer leaves untouched, and the record does not claim a lie that nobody told.
    assert_eq!(answered.kinds(), ["send", "emit"]);
    assert_eq!(answered.sent(&request).frame(), truth(&request).frame());
    let record = answered.emitted();
    assert!(record.verdict().is_pass());
    let fate = record.explanation().rule("Back off").unwrap().lie().unwrap();
    assert_eq!(
        fate.failure().map(Failure::message),
        Some("the lie only throttles, and the throttle of an answer cannot be written yet")
    );
    // The line of the rule says it, so the call does not say it a second time.
    assert_eq!(record.explanation().failure(), None);
}

#[test]
fn a_lie_that_delays_and_throttles_is_told_and_the_call_says_that_the_throttle_is_missing() {
    let slow = delay(1000).with_throttle(TimeSpan::from_millis(500));
    let mut driver = Driver::with([on_api("Slow down", ApiKey::HEARTBEAT, slow)]);

    let arrived = driver.send(&heartbeat(41));
    let answered = driver.answer(&arrived.forwarded());
    let record = driver.fire(answered.timer()).emitted();

    // The call did wait, so the lie applied. Its line has no failure, and the call says what is missing, once.
    assert!(record.explanation().applied("Slow down"));
    let fate = record.explanation().rule("Slow down").unwrap().lie().unwrap();
    assert_eq!(fate.failure(), None);
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some("the answer carries no throttle of 500 ms: the throttle of an answer cannot be written yet")
    );
}

#[test]
fn a_timer_that_ends_no_wait_changes_nothing() {
    let mut driver = Driver::with([]);
    let arrived = driver.send(&heartbeat(41));

    // The call waits for the backend, and not for a timer.
    for stage in Stage::ALL {
        let timer = TimerId::new(Driver::call(1), stage);
        assert!(driver.handle(Input::TimerFired(timer)).is_empty(), "{timer}");
    }
    // No call has this number.
    let timer = TimerId::new(Driver::call(9), Stage::AfterResponse);
    assert!(driver.handle(Input::TimerFired(timer)).is_empty());

    // The call goes on as if nothing happened.
    assert_eq!(driver.answer(&arrived.forwarded()).kinds(), ["send", "emit"]);
}
