//! Lifetimes and budgets: when a rule ends, what counts as a firing, and what arming resets.

use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_trigger::prelude::*;

const ARMED_AT: Instant = Instant::from_nanos(5_000_000_000);

fn die() -> Die {
    Die::new(Seed::new(42), &DiceKey::new("Refuse with NOT_LEADER").unwrap())
}

fn arm(trigger: impl Into<Trigger>, lifetime: Lifetime) -> Arming {
    Arming::new(trigger.into(), lifetime, die(), ARMED_AT)
}

fn session(ordinal: usize) -> SessionCounters {
    SessionCounters::new(CounterKey::new(format!("10.0.4.27 broker 2 #{ordinal}")).unwrap())
}

fn count(value: u64) -> Count {
    Count::new(value).unwrap()
}

fn after_arming(secs: u64) -> Instant {
    ARMED_AT + Duration::from_secs(secs)
}

fn outcome(arming: &Arming, session: &mut SessionCounters) -> TriggerOutcome {
    arming.evaluate(ARMED_AT, session).rolled().outcome()
}

#[test]
fn a_rule_armed_until_disarmed_never_ends_by_itself() {
    let arming = arm(Trigger::always(), Lifetime::UntilDisarmed);
    let mut session = session(1);
    for _ in 0..1_000 {
        let firing = arming
            .evaluate(after_arming(86_400), &mut session)
            .into_firing()
            .unwrap();
        assert!(!firing.confirm().ends_lifetime());
    }
    assert!(!arming.has_ended(Instant::MAX));
    assert_eq!(arming.ends_at(), None);
    assert!(arming.budget().is_none());
}

#[test]
fn a_duration_ends_that_long_after_arming() {
    let arming = arm(Trigger::always(), Lifetime::Duration(TimeSpan::from_secs(30)));
    let mut session = session(1);
    assert_eq!(arming.ends_at(), Some(after_arming(30)));

    assert!(!arming.has_ended(after_arming(29)));
    assert!(arming.evaluate(after_arming(29), &mut session).fired());

    assert!(arming.has_ended(after_arming(30)));
    let late = *arming.evaluate(after_arming(30), &mut session).rolled();
    assert_eq!(late.outcome(), TriggerOutcome::LifetimeOver);
    assert_eq!(late.match_number(), None, "a call after the lifetime is not counted");
    assert_eq!(arming.counter().matches(), 1);
    assert!(arming.fire_now(after_arming(30)).is_none());
}

#[test]
fn a_lifetime_of_matches_ends_after_that_many_matching_calls() {
    // The rule fires on no call at all, and the lifetime still counts every match.
    let arming = arm(
        TriggerParts::new().with_chance(Chance::NEVER),
        Lifetime::Matches(count(3)),
    );
    let mut session = session(1);

    let first = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(first.outcome(), TriggerOutcome::Held);
    assert_eq!(first.budget().map(BudgetReading::left), Some(2));
    assert!(!first.ends_lifetime());

    let second = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert!(!second.ends_lifetime());
    assert!(!arming.has_ended(ARMED_AT));

    let third = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(third.match_number().map(MatchNumber::value), Some(3));
    assert_eq!(third.budget().map(BudgetReading::left), Some(0));
    assert!(third.ends_lifetime(), "the last match ends the lifetime");
    assert!(arming.has_ended(ARMED_AT));

    let fourth = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(fourth.outcome(), TriggerOutcome::LifetimeOver);
    assert_eq!(fourth.match_number(), None);
    assert!(!fourth.ends_lifetime());
    assert_eq!(arming.counter().matches(), 3);
}

#[test]
fn the_last_match_of_a_lifetime_of_matches_can_still_fire() {
    let arming = arm(Trigger::always(), Lifetime::Matches(count(2)));
    let mut session = session(1);
    for ends in [false, true] {
        let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
        assert_eq!(rolled.ends_lifetime(), ends);
        // The matches ended the lifetime, so the firing has nothing left to end.
        assert!(!firing.expect("every match fires").confirm().ends_lifetime());
    }
    assert_eq!(outcome(&arming, &mut session), TriggerOutcome::LifetimeOver);
}

#[test]
fn a_lifetime_of_firings_ends_after_the_action_applied_that_many_times() {
    let arming = arm(Trigger::always(), Lifetime::Firings(count(3)));
    let mut session = session(1);

    for (left, ends) in [(2, false), (1, false), (0, true)] {
        let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
        assert_eq!(rolled.outcome(), TriggerOutcome::Fired);
        assert_eq!(rolled.budget().map(BudgetReading::left), Some(left));
        assert_eq!(rolled.budget().map(|budget| budget.limit().value()), Some(3));
        assert!(
            !rolled.ends_lifetime(),
            "only confirming a firing ends a lifetime of firings"
        );
        assert_eq!(firing.unwrap().confirm().ends_lifetime(), ends);
    }

    assert!(arming.has_ended(ARMED_AT));
    let fourth = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(fourth.outcome(), TriggerOutcome::LifetimeOver);
    assert_eq!(fourth.match_number(), None);
    assert_eq!(arming.counter().matches(), 3);
}

#[test]
fn a_call_that_does_not_fire_takes_nothing_from_a_budget_of_firings() {
    let arming = arm(TriggerParts::new().with_every(count(2)), Lifetime::Firings(count(2)));
    let mut session = session(1);

    let held = *arming.evaluate(ARMED_AT, &mut session).rolled();
    assert_eq!(held.outcome(), TriggerOutcome::Held);
    assert_eq!(
        held.budget(),
        None,
        "a call that does not fire does not look at the budget"
    );
    assert_eq!(arming.budget().unwrap().left(), 2);
}

#[test]
fn a_shadowed_firing_does_not_count_for_the_lifetime() {
    let arming = arm(Trigger::always(), Lifetime::Firings(count(1)));
    let mut session = session(1);

    // The rule fires, and a rule with a higher priority shadows it on every target.
    for _ in 0..5 {
        let firing = arming
            .evaluate(ARMED_AT, &mut session)
            .into_firing()
            .expect("the rule fires");
        firing.withdraw();
        assert!(!arming.has_ended(ARMED_AT));
    }
    assert_eq!(arming.budget().unwrap().spent(), 0);
    assert_eq!(
        arming.counter().matches(),
        5,
        "the matches count, whatever happens to them"
    );

    // The firing that applies is the one that ends the lifetime.
    let firing = arming.evaluate(ARMED_AT, &mut session).into_firing().unwrap();
    assert!(firing.confirm().ends_lifetime());
    assert!(arming.has_ended(ARMED_AT));
}

#[test]
fn a_call_that_ends_early_gives_its_firing_back() {
    let arming = arm(Trigger::always(), Lifetime::Firings(count(1)));
    let mut session = session(1);

    // The client hangs up while the call waits for the backend, and the call is dropped with its firing.
    let in_flight = arming.evaluate(ARMED_AT, &mut session);
    assert_eq!(arming.budget().unwrap().left(), 0);
    drop(in_flight);

    assert_eq!(arming.budget().unwrap().left(), 1);
    assert!(arming.evaluate(ARMED_AT, &mut session).fired());
}

#[test]
fn a_budget_of_three_firings_never_yields_a_fourth_when_five_connections_match_at_once() {
    let arming = arm(Trigger::always(), Lifetime::Firings(count(3)));
    let mut sessions: Vec<SessionCounters> = (1..=5).map(session).collect();

    // All five calls pass the trigger stage before any of them resolves.
    let mut evaluations: Vec<Evaluation> = sessions
        .iter_mut()
        .map(|session| arming.evaluate(ARMED_AT, session))
        .collect();
    let outcomes: Vec<TriggerOutcome> = evaluations
        .iter()
        .map(|evaluation| evaluation.rolled().outcome())
        .collect();
    assert_eq!(
        outcomes,
        [
            TriggerOutcome::Fired,
            TriggerOutcome::Fired,
            TriggerOutcome::Fired,
            TriggerOutcome::BudgetEmpty,
            TriggerOutcome::BudgetEmpty,
        ]
    );
    let empty = evaluations[3].rolled();
    assert_eq!(empty.match_number().map(MatchNumber::value), Some(4));
    assert_eq!(empty.budget().map(BudgetReading::left), Some(0));
    assert!(
        !arming.has_ended(ARMED_AT),
        "nothing is spent while the calls are in flight"
    );

    // The calls resolve in another order than they arrived. The second one is shadowed.
    let third = evaluations.remove(2).into_firing().unwrap();
    let second = evaluations.remove(1).into_firing().unwrap();
    let first = evaluations.remove(0).into_firing().unwrap();
    assert!(!third.confirm().ends_lifetime());
    second.withdraw();
    assert!(!first.confirm().ends_lifetime());
    assert!(!arming.has_ended(ARMED_AT));
    assert_eq!(arming.budget().unwrap().left(), 1);

    // A later call takes the firing that came back, and no call after it fires.
    let later = arming
        .evaluate(ARMED_AT, &mut sessions[4])
        .into_firing()
        .expect("one firing is free");
    assert_eq!(outcome(&arming, &mut sessions[0]), TriggerOutcome::BudgetEmpty);
    assert!(later.confirm().ends_lifetime());
    assert_eq!(arming.budget().unwrap().spent(), 3);
    for session in &mut sessions {
        assert_eq!(outcome(&arming, session), TriggerOutcome::LifetimeOver);
    }
}

#[test]
fn a_budget_holds_under_sessions_on_many_threads() {
    const SESSIONS: usize = 8;
    const CALLS: u64 = 2_000;
    const LIMIT: u64 = 100;

    let arming = arm(Trigger::always(), Lifetime::Firings(count(LIMIT)));
    let applied = AtomicU64::new(0);
    let ended = AtomicU64::new(0);

    thread::scope(|scope| {
        for ordinal in 0..SESSIONS {
            let (arming, applied, ended) = (arming.clone(), &applied, &ended);
            scope.spawn(move || {
                let mut session = session(ordinal);
                for call in 0..CALLS {
                    let Some(firing) = arming.evaluate(ARMED_AT, &mut session).into_firing() else {
                        continue;
                    };
                    // Every third firing is shadowed.
                    if call % 3 == 0 {
                        firing.withdraw();
                    } else {
                        applied.fetch_add(1, Ordering::Relaxed);
                        if firing.confirm().ends_lifetime() {
                            ended.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    assert_eq!(
        applied.load(Ordering::Relaxed),
        LIMIT,
        "the action applied exactly as often as the budget allows"
    );
    assert_eq!(ended.load(Ordering::Relaxed), 1, "exactly one call ends the lifetime");
    assert_eq!(arming.budget().unwrap().spent(), LIMIT);
    assert_eq!(arming.budget().unwrap().left(), 0);
    assert!(arming.has_ended(ARMED_AT));
}

#[test]
fn a_lifetime_of_matches_holds_under_sessions_on_many_threads() {
    const SESSIONS: usize = 8;
    const LIMIT: u64 = 1_000;

    let arming = arm(
        TriggerParts::new().with_nth(MatchNumber::new(LIMIT).unwrap()),
        Lifetime::Matches(count(LIMIT)),
    );
    let counted = AtomicU64::new(0);
    let fired = AtomicU64::new(0);
    let ended = AtomicU64::new(0);

    thread::scope(|scope| {
        for ordinal in 0..SESSIONS {
            let (arming, counted, fired, ended) = (arming.clone(), &counted, &fired, &ended);
            scope.spawn(move || {
                let mut session = session(ordinal);
                for _ in 0..LIMIT {
                    let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
                    counted.fetch_add(u64::from(rolled.match_number().is_some()), Ordering::Relaxed);
                    ended.fetch_add(u64::from(rolled.ends_lifetime()), Ordering::Relaxed);
                    if let Some(firing) = firing {
                        fired.fetch_add(1, Ordering::Relaxed);
                        let _ = firing.confirm();
                    }
                }
            });
        }
    });

    assert_eq!(
        counted.load(Ordering::Relaxed),
        LIMIT,
        "no call past the lifetime was counted"
    );
    assert_eq!(
        fired.load(Ordering::Relaxed),
        1,
        "every match number went to exactly one call"
    );
    assert_eq!(ended.load(Ordering::Relaxed), 1, "exactly one call ends the lifetime");
    assert_eq!(arming.counter().matches(), LIMIT);
}

#[test]
fn a_hand_firing_takes_from_a_budget_of_firings() {
    let arming = arm(Trigger::Manual, Lifetime::Firings(count(2)));
    let mut session = session(1);

    // An incident fires at once.
    let incident = arming.fire_now(ARMED_AT).expect("two firings are free");
    assert!(!incident.confirm().ends_lifetime());

    // A lie fires on the next matching call.
    arming.fire_on_next_match();
    let (rolled, firing) = arming.evaluate(ARMED_AT, &mut session).into_parts();
    assert_eq!(rolled.outcome(), TriggerOutcome::FiredByHand);
    assert_eq!(rolled.budget().map(BudgetReading::left), Some(0));
    assert!(firing.unwrap().confirm().ends_lifetime());

    assert!(arming.fire_now(ARMED_AT).is_none(), "the lifetime is over");
}

#[test]
fn a_hand_firing_waits_while_every_firing_is_set_aside() {
    let arming = arm(Trigger::Manual, Lifetime::Firings(count(1)));
    let mut session = session(1);

    let in_flight = arming.fire_now(ARMED_AT).expect("one firing is free");
    assert!(arming.fire_now(ARMED_AT).is_none(), "no firing is free");

    arming.fire_on_next_match();
    assert_eq!(outcome(&arming, &mut session), TriggerOutcome::BudgetEmpty);
    assert_eq!(arming.waiting_by_hand(), 1, "the hand firing still waits");

    // The firing in flight was pre-empted, so the next matching call fires by hand.
    in_flight.withdraw();
    assert_eq!(outcome(&arming, &mut session), TriggerOutcome::FiredByHand);
    assert_eq!(arming.waiting_by_hand(), 0);
}

#[test]
fn a_firing_by_hand_does_not_take_from_a_lifetime_of_matches() {
    let arming = arm(Trigger::Manual, Lifetime::Matches(count(2)));
    let incident = arming.fire_now(ARMED_AT).expect("the rule is armed");
    assert!(!incident.confirm().ends_lifetime());
    assert_eq!(arming.budget().unwrap().left(), 2, "no call matched");
}

#[test]
fn arming_again_resets_the_counters_the_budget_and_the_window() {
    let trigger = Trigger::from(
        TriggerParts::new()
            .with_nth(MatchNumber::new(2).unwrap())
            .with_window(TimeWindow::between(TimeSpan::ZERO, TimeSpan::from_secs(10)).unwrap()),
    );
    let lifetime = Lifetime::Firings(count(1));
    let mut session = session(1);

    let first_arming = Arming::new(trigger, lifetime, die(), ARMED_AT);
    assert!(!first_arming.evaluate(ARMED_AT, &mut session).fired());
    let firing = first_arming.evaluate(ARMED_AT, &mut session).into_firing().unwrap();
    assert!(firing.confirm().ends_lifetime());
    first_arming.fire_on_next_match();

    // Someone arms the rule again, an hour later.
    let armed_again_at = after_arming(3_600);
    let second_arming = Arming::new(trigger, lifetime, die(), armed_again_at);
    assert!(!second_arming.is(&first_arming));
    assert_eq!(second_arming.counter().matches(), 0);
    assert_eq!(second_arming.budget().unwrap().left(), 1);
    assert_eq!(second_arming.waiting_by_hand(), 0);
    assert!(!second_arming.has_ended(armed_again_at));

    // The 2nd match fires again, inside a window that starts at the new moment of arming.
    assert!(!second_arming.evaluate(armed_again_at, &mut session).fired());
    let second = second_arming.evaluate(armed_again_at, &mut session);
    assert_eq!(second.rolled().match_number().map(MatchNumber::value), Some(2));
    assert_eq!(second.rolled().window_open(), Some(true));
    assert!(second.into_firing().unwrap().confirm().ends_lifetime());
}

#[test]
fn arming_again_resets_the_counters_of_every_session() {
    let trigger = Trigger::from(TriggerParts::new().with_first(count(1)).with_per(Per::Session));
    let mut session = session(1);

    let first_arming = Arming::new(trigger, Lifetime::UntilDisarmed, die(), ARMED_AT);
    assert!(first_arming.evaluate(ARMED_AT, &mut session).fired());
    assert!(!first_arming.evaluate(ARMED_AT, &mut session).fired());

    // The session stays open while someone arms the rule again.
    let second_arming = Arming::new(trigger, Lifetime::UntilDisarmed, die(), ARMED_AT);
    let again = second_arming.evaluate(ARMED_AT, &mut session);
    assert_eq!(again.rolled().match_number(), Some(MatchNumber::FIRST));
    assert!(again.fired());

    // The first arming is gone, and the session forgets its count the next time it meets a new arming.
    drop(first_arming);
    let third_arming = Arming::new(trigger, Lifetime::UntilDisarmed, die(), ARMED_AT);
    assert!(third_arming.evaluate(ARMED_AT, &mut session).fired());
    assert_eq!(session.len(), 2);
}

#[test]
fn changing_a_parameter_of_an_armed_rule_resets_nothing() {
    let window = TimeWindow::after(TimeSpan::from_secs(10));
    let parts = TriggerParts::new().with_every(count(2)).with_window(window);
    let arming = arm(parts, Lifetime::Firings(count(3)));
    let mut session = session(1);

    assert!(!arming.evaluate(after_arming(10), &mut session).fired());
    let firing = arming.evaluate(after_arming(10), &mut session).into_firing().unwrap();
    assert!(!firing.confirm().ends_lifetime());
    arming.fire_on_next_match();

    // Someone turns the rule to every 3rd match, and to a lifetime of two firings.
    let retuned = arming.retuned(parts.with_every(count(3)).into(), Lifetime::Firings(count(2)));
    assert!(retuned.is(&arming));
    assert_eq!(retuned.armed_at(), ARMED_AT);
    assert_eq!(retuned.counter().matches(), 2);
    assert_eq!(retuned.budget().unwrap().spent(), 1);
    assert_eq!(retuned.budget().unwrap().left(), 1);
    assert_eq!(retuned.waiting_by_hand(), 1);

    // The hand firing that waited still fires, on the 3rd match of the same counter.
    let third = retuned.evaluate(after_arming(11), &mut session);
    assert_eq!(third.rolled().outcome(), TriggerOutcome::FiredByHand);
    assert_eq!(third.rolled().match_number().map(MatchNumber::value), Some(3));
    assert!(third.into_firing().unwrap().confirm().ends_lifetime());
    assert!(retuned.has_ended(after_arming(11)));

    // A call in flight on the old parameters still sees the old limit, and the same spent firings.
    assert_eq!(arming.budget().unwrap().spent(), 2);
    assert!(!arming.has_ended(after_arming(11)));
}

#[test]
fn changing_a_parameter_keeps_the_counts_of_every_session() {
    let parts = TriggerParts::new().with_first(count(2)).with_per(Per::Session);
    let arming = arm(parts, Lifetime::UntilDisarmed);
    let mut session = session(1);
    assert!(arming.evaluate(ARMED_AT, &mut session).fired());

    let retuned = arming.retuned(parts.with_first(count(3)).into(), Lifetime::UntilDisarmed);
    let fired: Vec<bool> = (0..3)
        .map(|_| retuned.evaluate(ARMED_AT, &mut session).fired())
        .collect();
    assert_eq!(fired, [true, true, false], "the session went on from its 2nd match");
}

#[test]
fn a_lifetime_that_counts_something_else_starts_a_new_budget() {
    let arming = arm(Trigger::always(), Lifetime::Firings(count(3)));
    let _ = arming
        .evaluate(ARMED_AT, &mut session(1))
        .into_firing()
        .unwrap()
        .confirm();

    let matches = arming.retuned(Trigger::always(), Lifetime::Matches(count(3)));
    assert_eq!(matches.budget().unwrap().spent(), 0);
    assert!(!matches.budget().unwrap().is(arming.budget().unwrap()));

    let unlimited = arming.retuned(Trigger::always(), Lifetime::UntilDisarmed);
    assert!(unlimited.budget().is_none());
    assert_eq!(unlimited.counter().matches(), 1);

    let limited_again = unlimited.retuned(Trigger::always(), Lifetime::Firings(count(1)));
    assert_eq!(limited_again.budget().unwrap().left(), 1);
}

#[test]
fn a_counted_until_of_a_sequence_step_is_a_budget() {
    // `until: { calls: 100, selector: Order producers }` counts matching calls, with no rule around it.
    let calls = Budget::new(count(100));
    let ended: Vec<bool> = (0..100)
        .map(|_| calls.take().expect("the step runs").is_last())
        .collect();
    assert_eq!(ended.iter().filter(|&&last| last).count(), 1);
    assert_eq!(ended.last(), Some(&true), "the 100th call ends the step");
    assert!(calls.take().is_none());
}

#[test]
fn the_engine_shares_live_state_between_threads() {
    fn shared<T: Send + Sync + Clone>() {}
    fn sent<T: Send>() {}

    shared::<Arming>();
    shared::<Counter>();
    shared::<Budget>();

    sent::<Firing>();
    sent::<Evaluation>();
    sent::<Reservation>();
    sent::<SessionCounters>();
}
