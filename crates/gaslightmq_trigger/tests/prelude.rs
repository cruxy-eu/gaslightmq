//! The prelude holds every type, and its names clash with none of `std`, of the clock or of the kernel's prelude.

/// Imports the prelude next to the standard prelude, the clock, the kernel's prelude
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_the_clock_and_the_kernel {
    #![allow(unused_imports)]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;

    use gaslightmq_clock::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let chance: Chance = "40%".parse().unwrap();
        let count = Count::new(3).unwrap();
        let nth = MatchNumber::new(2).unwrap();
        let window = TimeWindow::after(TimeSpan::from_secs(10));
        let parts: TriggerParts = TriggerParts::new()
            .with_chance(chance)
            .with_nth(nth)
            .with_window(window)
            .with_per(Per::Session);
        let trigger = Trigger::from(parts);
        let lifetime = Lifetime::Firings(count);
        let zero: TriggerError = Count::new(0).unwrap_err();

        let seed = Seed::new(42);
        let dice_key = DiceKey::new("Stale metadata").unwrap();
        let counter_key = CounterKey::new("10.0.4.27 broker 2 #1").unwrap();
        let die: Die = Die::new(seed, &dice_key);
        let roll: Roll = die.for_counter(&counter_key).roll(nth);

        let counter = Counter::new();
        let mut session = SessionCounters::new(counter_key);
        session.count(&counter);

        let budget = Budget::new(count);
        let reading: BudgetReading = budget.reading();
        let reservation: Reservation = budget.reserve().unwrap();
        let spent: Spent = reservation.confirm();

        let arming = Arming::new(Trigger::always(), lifetime, die, Instant::ZERO);
        let evaluation: Evaluation = arming.evaluate(Instant::ZERO, &mut session);
        let rolled: Rolled = *evaluation.rolled();
        let outcome: TriggerOutcome = rolled.outcome();
        let firing: Firing = evaluation.into_firing().unwrap();
        let confirmed: Confirmed = firing.confirm();

        vec![
            chance.to_string(),
            lifetime.to_string(),
            zero.to_string(),
            roll.to_string(),
            window.to_string(),
            format!("{trigger:?} {reading:?} {spent:?} {outcome:?} {confirmed:?}"),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_the_clock_and_the_kernel() {
    let items = next_to_std_the_clock_and_the_kernel::every_item();
    assert_eq!(items[0], "40%");
    assert_eq!(items[1], "3 firings");
    assert_eq!(items[2], "a count is at least one");
    assert_eq!(items[4], "after 10s");
}
