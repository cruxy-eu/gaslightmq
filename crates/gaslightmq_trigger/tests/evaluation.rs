//! Evaluating matching calls: every trigger kind, combined triggers, counters per rule and per session, and hand firings.

use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_trigger::prelude::*;

const RULE: &str = "Slow down order producers";
const ARMED_AT: Instant = Instant::from_nanos(5_000_000_000);

fn die() -> Die {
    Die::new(Seed::new(42), &DiceKey::new(RULE).unwrap())
}

fn arm(trigger: impl Into<Trigger>) -> Arming {
    Arming::new(trigger.into(), Lifetime::UntilDisarmed, die(), ARMED_AT)
}

fn session(ordinal: u32) -> SessionCounters {
    SessionCounters::new(CounterKey::new(format!("10.0.4.27 broker 2 #{ordinal}")).unwrap())
}

fn count(value: u64) -> Count {
    Count::new(value).unwrap()
}

fn number(value: u64) -> MatchNumber {
    MatchNumber::new(value).unwrap()
}

fn chance(text: &str) -> Chance {
    text.parse().unwrap()
}

fn after_arming(secs: u64) -> Instant {
    ARMED_AT + Duration::from_secs(secs)
}

/// Evaluates that many matching calls on one session, and returns the match numbers on which the rule fired.
fn fired_on(arming: &Arming, session: &mut SessionCounters, calls: u64) -> Vec<u64> {
    (0..calls)
        .filter_map(|_| {
            let (rolled, firing) = arming.evaluate(ARMED_AT, session).into_parts();
            let _ = firing?.confirm();
            rolled.match_number().map(MatchNumber::value)
        })
        .collect()
}

#[test]
fn a_rule_without_a_trigger_fires_on_every_matching_call() {
    let arming = arm(Trigger::always());
    assert_eq!(fired_on(&arming, &mut session(1), 5), [1, 2, 3, 4, 5]);
}

#[test]
fn a_chance_fires_when_the_die_rolls_under_it() {
    let arming = arm(TriggerParts::new().with_chance(chance("40%")));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [2, 6, 8]);
}

#[test]
fn the_record_shows_the_roll_and_the_chance() {
    let arming = arm(TriggerParts::new().with_chance(chance("40%")));
    let mut session = session(1);

    let first = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(first.outcome(), TriggerOutcome::Held);
    assert_eq!(first.match_number(), Some(number(1)));
    assert_eq!(first.roll().map(Roll::ppm), Some(444_610));
    assert_eq!(first.chance(), Some(chance("40%")));
    assert_eq!(first.count_agrees(), None);
    assert_eq!(first.window(), None);
    assert_eq!(first.budget(), None);

    let second = arming.evaluate(ARMED_AT, &mut session);
    assert!(second.fired());
    assert_eq!(second.rolled().outcome(), TriggerOutcome::Fired);
    assert_eq!(second.rolled().roll().map(Roll::ppm), Some(116_519));
    let _ = second.into_firing().unwrap().confirm();
}

#[test]
fn a_chance_of_zero_never_fires_and_a_chance_of_one_hundred_always_does() {
    let never = arm(TriggerParts::new().with_chance(Chance::NEVER));
    assert_eq!(fired_on(&never, &mut session(1), 100), [0_u64; 0]);

    let always = arm(TriggerParts::new().with_chance(Chance::ALWAYS));
    assert_eq!(fired_on(&always, &mut session(1), 100).len(), 100);
}

#[test]
fn nth_fires_on_one_match() {
    let arming = arm(TriggerParts::new().with_nth(number(3)));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [3]);
}

#[test]
fn every_fires_on_each_multiple() {
    let arming = arm(TriggerParts::new().with_every(count(4)));
    assert_eq!(fired_on(&arming, &mut session(1), 13), [4, 8, 12]);
}

#[test]
fn first_fires_on_the_first_matches() {
    let arming = arm(TriggerParts::new().with_first(count(5)));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [1, 2, 3, 4, 5]);
}

#[test]
fn skip_passes_over_the_first_matches() {
    let arming = arm(TriggerParts::new().with_skip(count(7)));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [8, 9, 10]);
}

#[test]
fn skip_combines_with_the_other_count_parts() {
    let skip_then_first = arm(TriggerParts::new().with_skip(count(3)).with_first(count(2)));
    assert_eq!(fired_on(&skip_then_first, &mut session(1), 10), [4, 5]);

    let skip_then_every = arm(TriggerParts::new().with_skip(count(1)).with_every(count(3)));
    assert_eq!(fired_on(&skip_then_every, &mut session(1), 10), [4, 7, 10]);
}

#[test]
fn the_record_shows_whether_the_count_parts_agreed() {
    let arming = arm(TriggerParts::new().with_nth(number(2)));
    let mut session = session(1);
    assert_eq!(
        arming.evaluate(ARMED_AT, &mut session).rolled().count_agrees(),
        Some(false)
    );
    let second = arming.evaluate(ARMED_AT, &mut session);
    assert_eq!(second.rolled().count_agrees(), Some(true));
    assert_eq!(second.rolled().roll(), None);
    second.into_firing().unwrap().withdraw();
}

#[test]
fn nth_with_a_chance_fires_on_that_match_some_of_the_time() {
    // The die rolls 11.6519% for the 2nd match, and 73.8645% for the 3rd.
    let second = TriggerParts::new().with_nth(number(2));
    assert_eq!(
        fired_on(&arm(second.with_chance(chance("50%"))), &mut session(1), 10),
        [2]
    );
    assert_eq!(
        fired_on(&arm(second.with_chance(chance("10%"))), &mut session(1), 10),
        [0_u64; 0]
    );

    let third = TriggerParts::new().with_nth(number(3));
    assert_eq!(
        fired_on(&arm(third.with_chance(chance("50%"))), &mut session(1), 10),
        [0_u64; 0]
    );
    assert_eq!(
        fired_on(&arm(third.with_chance(chance("80%"))), &mut session(1), 10),
        [3]
    );
}

#[test]
fn the_die_rolls_for_the_match_number_itself_whatever_the_count_parts_say() {
    // The die fires at 40% on the matches 2, 6 and 8. Skipping three leaves 6 and 8.
    let arming = arm(TriggerParts::new().with_chance(chance("40%")).with_skip(count(3)));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [6, 8]);
}

#[test]
fn the_record_shows_every_part_of_a_combined_trigger_even_when_one_disagrees() {
    let window = TimeWindow::after(TimeSpan::from_secs(10));
    let arming = arm(TriggerParts::new()
        .with_nth(number(3))
        .with_chance(chance("40%"))
        .with_window(window));

    let rolled = *arming.evaluate(after_arming(4), &mut session(1)).rolled();
    assert_eq!(rolled.outcome(), TriggerOutcome::Held);
    assert_eq!(rolled.match_number(), Some(number(1)));
    assert_eq!(rolled.count_agrees(), Some(false));
    assert_eq!(rolled.roll().map(Roll::ppm), Some(444_610));
    assert_eq!(rolled.window(), Some(window));
    assert_eq!(rolled.window_open(), Some(false));
    assert_eq!(rolled.since_armed(), Duration::from_secs(4));
    assert!(!rolled.ends_lifetime());
}

#[test]
fn after_opens_a_window_from_the_moment_of_arming() {
    let arming = arm(TriggerParts::new().with_window(TimeWindow::after(TimeSpan::from_secs(10))));
    let mut session = session(1);
    let fires = |session: &mut SessionCounters, now| arming.evaluate(now, session).fired();

    assert!(!fires(&mut session, ARMED_AT));
    assert!(!fires(&mut session, ARMED_AT + Duration::from_millis(9_999)));
    assert!(fires(&mut session, after_arming(10)));
    assert!(fires(&mut session, after_arming(3_600)));
}

#[test]
fn between_opens_and_closes_a_window() {
    let window = TimeWindow::between(TimeSpan::from_secs(10), TimeSpan::from_secs(40)).unwrap();
    let arming = arm(TriggerParts::new().with_window(window));
    let mut session = session(1);
    let fires = |session: &mut SessionCounters, now| arming.evaluate(now, session).fired();

    assert!(!fires(&mut session, after_arming(9)));
    assert!(fires(&mut session, after_arming(10)));
    assert!(fires(&mut session, after_arming(39)));
    assert!(!fires(&mut session, after_arming(40)));
    assert!(!fires(&mut session, after_arming(3_600)));
}

#[test]
fn a_closed_window_does_not_disarm_the_rule() {
    let window = TimeWindow::between(TimeSpan::from_secs(10), TimeSpan::from_secs(40)).unwrap();
    let arming = arm(TriggerParts::new().with_window(window));
    assert!(!arming.has_ended(after_arming(3_600)));
    assert_eq!(arming.ends_at(), None);
    assert_eq!(
        arming.evaluate(after_arming(3_600), &mut session(1)).rolled().outcome(),
        TriggerOutcome::Held
    );
}

#[test]
fn matches_count_while_the_window_is_closed() {
    // A count part counts matching calls, whatever happens to them.
    let arming = arm(TriggerParts::new()
        .with_nth(number(3))
        .with_window(TimeWindow::after(TimeSpan::from_secs(10))));
    let mut session = session(1);

    for _ in 0..2 {
        assert!(!arming.evaluate(after_arming(1), &mut session).fired());
    }
    let third = arming.evaluate(after_arming(11), &mut session);
    assert_eq!(third.rolled().match_number(), Some(number(3)));
    assert_eq!(third.rolled().window_open(), Some(true));
    let _ = third
        .into_firing()
        .expect("the 3rd match is inside the window")
        .confirm();

    // A 3rd match before the window opened would have been lost.
    let early = arm(TriggerParts::new()
        .with_nth(number(3))
        .with_window(TimeWindow::after(TimeSpan::from_secs(10))));
    assert_eq!(fired_on(&early, &mut session, 10), [0_u64; 0]);
}

#[test]
fn a_counter_per_rule_is_shared_by_all_sessions() {
    let arming = arm(TriggerParts::new().with_nth(number(3)));
    let (mut first, mut second) = (session(1), session(2));

    assert!(!arming.evaluate(ARMED_AT, &mut first).fired());
    assert!(!arming.evaluate(ARMED_AT, &mut second).fired());
    // The 3rd match of the rule, from anyone.
    let third = arming.evaluate(ARMED_AT, &mut first);
    assert_eq!(third.rolled().match_number(), Some(number(3)));
    let _ = third.into_firing().expect("the 3rd match fires").confirm();

    // `nth: 3` fires once per arming.
    assert_eq!(fired_on(&arming, &mut first, 10), [0_u64; 0]);
    assert_eq!(fired_on(&arming, &mut second, 10), [0_u64; 0]);
    assert_eq!(arming.counter().matches(), 23);
    assert!(first.is_empty(), "a counter per rule keeps nothing in the session");
}

#[test]
fn a_counter_per_session_counts_each_session_apart() {
    let arming = arm(TriggerParts::new().with_first(count(2)).with_per(Per::Session));
    let (mut first, mut second) = (session(1), session(2));

    assert_eq!(fired_on(&arming, &mut first, 5), [1, 2]);
    assert_eq!(fired_on(&arming, &mut second, 5), [1, 2]);
    assert_eq!(arming.counter().matches(), 0, "the shared counter stays out of it");
}

#[test]
fn a_session_that_reconnects_counts_from_one_again() {
    let arming = arm(TriggerParts::new().with_first(count(2)).with_per(Per::Session));
    assert_eq!(fired_on(&arming, &mut session(1), 5), [1, 2]);
    // The session died with its connection, and the new one has the same session key.
    assert_eq!(fired_on(&arming, &mut session(1), 5), [1, 2]);
}

#[test]
fn a_counter_per_session_rolls_a_die_per_session() {
    let arming = arm(TriggerParts::new().with_chance(chance("40%")).with_per(Per::Session));
    assert_eq!(fired_on(&arming, &mut session(1), 10), [1, 2, 10]);
    assert_eq!(fired_on(&arming, &mut session(2), 10), [2, 5, 6, 7]);

    let rolled = *arming.evaluate(ARMED_AT, &mut session(1)).rolled();
    assert_eq!(rolled.roll().map(Roll::ppm), Some(83_381));
}

#[test]
fn the_rolls_of_a_session_do_not_depend_on_the_order_in_which_connections_open() {
    let one_run = arm(TriggerParts::new().with_chance(chance("40%")).with_per(Per::Session));
    let (mut first, mut second) = (session(1), session(2));
    let first_fired = fired_on(&one_run, &mut first, 10);
    let second_fired = fired_on(&one_run, &mut second, 10);

    // Another run: the second connection opens first, and the calls interleave.
    let other_run = arm(TriggerParts::new().with_chance(chance("40%")).with_per(Per::Session));
    let (mut first, mut second) = (session(1), session(2));
    let (mut first_again, mut second_again) = (Vec::new(), Vec::new());
    for _ in 0..10 {
        second_again.extend(fired_on(&other_run, &mut second, 1));
        first_again.extend(fired_on(&other_run, &mut first, 1));
    }

    assert_eq!(first_again, first_fired);
    assert_eq!(second_again, second_fired);
}

#[test]
fn a_manual_trigger_never_fires_by_itself() {
    let arming = arm(Trigger::Manual);
    let mut session = session(1);
    assert_eq!(fired_on(&arming, &mut session, 20), [0_u64; 0]);

    let rolled = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(rolled.outcome(), TriggerOutcome::Held);
    assert_eq!(
        rolled.match_number(),
        Some(number(21)),
        "a manual rule still counts its matches"
    );
}

#[test]
fn a_hand_firing_fires_on_the_next_matching_call_once() {
    let arming = arm(Trigger::Manual);
    let mut session = session(1);
    assert!(!arming.evaluate(ARMED_AT, &mut session).fired());

    arming.fire_on_next_match();
    assert_eq!(arming.waiting_by_hand(), 1);

    let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
    assert_eq!(rolled.outcome(), TriggerOutcome::FiredByHand);
    assert_eq!(rolled.match_number(), Some(number(2)));
    let firing = firing.expect("the rule fires by hand");
    assert!(firing.by_hand());
    let _ = firing.confirm();

    assert_eq!(arming.waiting_by_hand(), 0);
    assert!(!arming.evaluate(ARMED_AT, &mut session).fired());
}

#[test]
fn every_hand_firing_is_one_firing() {
    let arming = arm(Trigger::Manual);
    arming.fire_on_next_match();
    arming.fire_on_next_match();
    assert_eq!(fired_on(&arming, &mut session(1), 5), [1, 2]);
}

#[test]
fn a_hand_firing_goes_to_the_next_matching_call_of_any_session() {
    let arming = arm(Trigger::Manual);
    let (mut first, mut second) = (session(1), session(2));
    arming.fire_on_next_match();
    assert_eq!(fired_on(&arming, &mut second, 1), [1]);
    assert_eq!(fired_on(&arming, &mut first, 3), [0_u64; 0]);
}

#[test]
fn a_hand_firing_skips_the_trigger_once() {
    let window = TimeWindow::after(TimeSpan::from_secs(10));
    let arming = arm(TriggerParts::new()
        .with_nth(number(100))
        .with_chance(Chance::NEVER)
        .with_window(window));
    let mut session = session(1);

    arming.fire_on_next_match();
    let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
    assert_eq!(rolled.outcome(), TriggerOutcome::FiredByHand);
    assert!(rolled.outcome().fired());
    // A hand firing skips the trigger, so the record shows none of its parts.
    assert_eq!(rolled.count_agrees(), None);
    assert_eq!(rolled.roll(), None);
    assert_eq!(rolled.window(), None);
    let _ = firing.unwrap().confirm();

    // The call after it goes through the trigger again.
    let rolled = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(rolled.outcome(), TriggerOutcome::Held);
    assert_eq!(rolled.match_number(), Some(number(2)));
    assert_eq!(rolled.count_agrees(), Some(false));
}

#[test]
fn a_rule_with_an_event_fires_by_hand_at_once() {
    let arming = arm(Trigger::Manual);
    let firing = arming.fire_now(ARMED_AT).expect("an armed rule fires by hand");
    assert!(firing.by_hand());
    assert!(!firing.confirm().ends_lifetime());
    assert_eq!(arming.counter().matches(), 0, "no call matched");
    assert_eq!(arming.waiting_by_hand(), 0);
}

#[test]
fn a_firing_by_trigger_is_not_by_hand() {
    let arming = arm(Trigger::always());
    let firing = arming.evaluate(ARMED_AT, &mut session(1)).into_firing().unwrap();
    assert!(!firing.by_hand());
    assert!(!firing.confirm().ends_lifetime());
}

#[test]
fn an_arming_keeps_what_it_was_armed_with() {
    let trigger = Trigger::from(TriggerParts::new().with_chance(chance("40%")));
    let lifetime = Lifetime::Duration(TimeSpan::from_secs(30));
    let arming = Arming::new(trigger, lifetime, die(), ARMED_AT);
    assert_eq!(arming.trigger(), trigger);
    assert_eq!(arming.lifetime(), lifetime);
    assert_eq!(arming.die(), die());
    assert_eq!(arming.armed_at(), ARMED_AT);
    assert!(arming.budget().is_none());
}

#[test]
fn an_evaluation_before_the_moment_of_arming_counts_as_that_moment() {
    let arming = arm(TriggerParts::new().with_window(TimeWindow::after(TimeSpan::ZERO)));
    let evaluation = arming.evaluate(Instant::ZERO, &mut session(1));
    assert_eq!(evaluation.rolled().since_armed(), Duration::ZERO);
    assert!(evaluation.fired());
}
