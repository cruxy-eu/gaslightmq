//! The prelude holds every type and every error, and its names clash with none of `std`, of the clock or of the kernel's prelude.

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
            roll.to_string(),
            window.to_string(),
            format!("{trigger:?} {reading:?} {spent:?} {outcome:?} {confirmed:?}"),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        vec![
            ChanceError::NotAPercentage { input: "40".into() }.to_string(),
            RollError::OutOfRange { ppm: 1_000_000 }.to_string(),
            CountError::Zero.to_string(),
            MatchNumberError::Zero.to_string(),
            TimeWindowError::Empty {
                from: TimeSpan::from_secs(40),
                until: TimeSpan::from_secs(10),
            }
            .to_string(),
            PerError::Unknown {
                name: "connection".into(),
            }
            .to_string(),
            LifetimeError::Malformed {
                input: "forever".into(),
            }
            .to_string(),
            LifetimeError::InvalidCount {
                error: CountError::Zero,
            }
            .to_string(),
            DiceKeyError::Empty.to_string(),
            CounterKeyError::Empty.to_string(),
        ]
    }
}

/// Imports the prelude alone, as a consumer does, and uses the standard prelude as usual.
mod alone {
    use gaslightmq_trigger::prelude::*;

    /// Each constructor returns the error of its own type, and `?` turns every one into a standard error.
    pub fn every_error_is_a_standard_error() -> Vec<Box<dyn std::error::Error>> {
        fn boxed<T, E: std::error::Error + 'static>(result: Result<T, E>) -> Box<dyn std::error::Error> {
            match result {
                Ok(_) => panic!("the value should not be valid"),
                Err(error) => Box::new(error),
            }
        }

        let span = gaslightmq_clock::TimeSpan::from_secs;
        vec![
            boxed::<Chance, ChanceError>("40".parse()),
            boxed::<Chance, ChanceError>(Chance::from_ppm(1_400_000)),
            boxed::<Roll, RollError>(Roll::from_ppm(1_000_000)),
            boxed::<Count, CountError>(Count::try_from(0)),
            boxed::<MatchNumber, MatchNumberError>(MatchNumber::try_from(0)),
            boxed::<TimeWindow, TimeWindowError>(TimeWindow::between(span(40), span(10))),
            boxed::<Per, PerError>("connection".parse()),
            boxed::<Lifetime, LifetimeError>("forever".parse()),
            boxed::<Lifetime, LifetimeError>("0 firings".parse()),
            boxed::<DiceKey, DiceKeyError>(String::new().try_into()),
            boxed::<CounterKey, CounterKeyError>("".parse()),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_the_clock_and_the_kernel() {
    let items = next_to_std_the_clock_and_the_kernel::every_item();
    assert_eq!(items[0], "40%");
    assert_eq!(items[1], "3 firings");
    assert_eq!(items[3], "after 10s");
}

#[test]
fn the_prelude_holds_the_error_of_every_type() {
    assert_eq!(
        next_to_std_the_clock_and_the_kernel::every_error(),
        [
            "`40` is not a percentage such as `40%`",
            "a roll of 1000000 parts per million is not below one million",
            "a count is at least one",
            "a match number is at least one",
            "a window between 40s and 10s never opens",
            "counters are per `rule` or per `session`, not per `connection`",
            "`forever` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
             or a count such as `100 matches` or `3 firings`",
            "a count is at least one",
            "a dice key is not empty",
            "a counter key is not empty",
        ]
    );
}

#[test]
fn every_error_is_a_standard_error_with_a_complete_message() {
    let messages: Vec<String> = alone::every_error_is_a_standard_error()
        .iter()
        .map(|error| {
            assert!(error.source().is_none(), "{error} has a source");
            error.to_string()
        })
        .collect();
    assert_eq!(
        messages,
        [
            "`40` is not a percentage such as `40%`",
            "chance `140%` is not between 0% and 100%",
            "a roll of 1000000 parts per million is not below one million",
            "a count is at least one",
            "a match number is at least one",
            "a window between 40s and 10s never opens",
            "counters are per `rule` or per `session`, not per `connection`",
            "`forever` is not a lifetime: expected `until disarmed`, a time span such as `30s`, \
             or a count such as `100 matches` or `3 firings`",
            "a count is at least one",
            "a dice key is not empty",
            "a counter key is not empty",
        ]
    );
}
