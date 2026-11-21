//! Triggers, counters, dice, budgets and lifetimes: when a rule fires, and how long it stays armed.
//!
//! A rule's selector decides which calls the rule considers.
//! This crate decides which of those calls fire the rule.
//!
//! | Type | Is |
//! |---|---|
//! | [`Trigger`] | When a rule fires among its matching calls: by its [parts](TriggerParts), or only by hand. |
//! | [`TriggerParts`] | A chance, count parts and a time window, which all have to agree. |
//! | [`Chance`] | How often a chance trigger fires, such as `40%`. |
//! | [`MatchNumber`] | Which matching call this is: the 1st, the 2nd, the 3rd. |
//! | [`Count`] | How many matches or firings a definition names. |
//! | [`TimeWindow`] | When a time trigger agrees, measured from the moment of arming. |
//! | [`Per`] | Whose matches a rule counts: the rule's, or each session's. |
//! | [`Lifetime`] | When an armed rule disarms itself. |
//! | [`Seed`], [`DiceKey`], [`CounterKey`] | What a die derives from, next to the match number. |
//! | [`Die`], [`Roll`] | The die of one rule, and what it rolled for one match. |
//! | [`Counter`] | The match counter of one armed rule, shared by all sessions. |
//! | [`SessionCounters`] | The counter key and the match counts that one session keeps for itself. |
//! | [`Budget`], [`Reservation`] | The matches or firings a lifetime allows, and one unit that a call set aside. |
//! | [`Arming`] | The live state of one arming of one rule. It evaluates matching calls. |
//! | [`Evaluation`], [`Rolled`], [`TriggerOutcome`] | What one evaluation gave, as the explanation of a call shows it. |
//! | [`Firing`] | One firing, which the call confirms or withdraws. |
//!
//! ```
//! use gaslightmq_clock::{Duration, Instant, TimeSpan};
//! use gaslightmq_trigger::prelude::*;
//!
//! // `trigger: { first: 2, per: session }` and `lifetime: 3 firings`, in a scenario with `seed: 42`.
//! let trigger = Trigger::from(TriggerParts::new().with_first(Count::new(2)?).with_per(Per::Session));
//! let lifetime: Lifetime = "3 firings".parse()?;
//! let die = Die::new(Seed::new(42), &DiceKey::new("Stale metadata")?);
//!
//! // The engine arms the rule, and shares the arming with every session.
//! let armed_at = Instant::ZERO + Duration::from_secs(1);
//! let arming = Arming::new(trigger, lifetime, die, armed_at);
//!
//! // Each session keeps its own counters, under its own counter key.
//! let mut first = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1")?);
//! let mut second = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #2")?);
//! let now = armed_at + TimeSpan::from_millis(20);
//!
//! // The first two Metadata calls of the first session fire, and their lies apply.
//! for _ in 0..2 {
//!     let firing = arming.evaluate(now, &mut first).into_firing().expect("the rule fires");
//!     assert!(!firing.confirm().ends_lifetime());
//! }
//! // Its third call does not fire: `first: 2` counts per session.
//! assert_eq!(arming.evaluate(now, &mut first).rolled().outcome(), TriggerOutcome::Held);
//!
//! // The second session counts from one. Its first call takes the last of the three firings.
//! let firing = arming.evaluate(now, &mut second).into_firing().expect("the rule fires");
//! assert!(firing.confirm().ends_lifetime());
//!
//! // The budget is shared, so its second call finds the lifetime over.
//! assert_eq!(arming.evaluate(now, &mut second).rolled().outcome(), TriggerOutcome::LifetimeOver);
//! # Ok::<(), TriggerError>(())
//! ```
//!
//! # Definitions and live state
//!
//! A [`Trigger`] and a [`Lifetime`] are definitions.
//! They are plain values that only hold valid parts, a file stores them, and the UI edits them.
//!
//! An [`Arming`] is live state.
//! Arming a rule makes one, and arming it again makes a new one,
//! which is how arming resets the counters, the budget and the time window.
//! The engine shares an arming with every session.
//! Its counter, its budget and its hand firings are atomic numbers behind handles, so no session waits for another.
//!
//! # Evaluating one call
//!
//! A session calls [`Arming::evaluate`] for every armed rule whose selector matched the call.
//! It passes the current instant and its own [`SessionCounters`].
//!
//! The [`Evaluation`] holds a [`Rolled`] record with the match number, the die roll, the time window and the budget.
//! When the rule fired, it also holds a [`Firing`].
//!
//! A rule fires before anyone knows whether its action applies, because another rule can shadow or pre-empt it.
//! Shadowed and pre-empted firings do not count for a lifetime of firings.
//! The call therefore settles its firing later:
//! [`Firing::confirm`] when the action applied, and [`Firing::withdraw`] when it did not.
//! See [`Budget`] for how that never overspends.
//!
//! Only whoever holds the action knows whether it has something to act on in this call.
//! When it has not, [`Evaluation::nothing_to_act_on`] gives the firing back and says so in the record,
//! so the record does not read fired for a rule that had nothing to do on this call.
//!
//! # Firing by hand
//!
//! Someone can fire any armed rule by hand, which skips its trigger once.
//!
//! - [`Arming::fire_now`] fires at once, for a rule with an incident.
//! - [`Arming::fire_on_next_match`] fires on the next call the selector matches, for a rule with a lie.
//!
//! A hand firing takes from a budget of firings like any other firing.
//!
//! Every hand firing lies on a matching call.
//! A call that takes a hand firing and then withdraws or drops its [`Firing`] puts the hand firing back,
//! and the next matching call takes it.
//!
//! # Dice
//!
//! A roll derives from the scenario seed, the dice key, the counter key and the match number, and from nothing else.
//! The derivation is fixed integer arithmetic, written out at [`Die`].
//! It uses no hasher of the standard library, so the same seed gives the same rolls
//! in every run, on every platform and with every version of Rust.
//!
//! A [`Chance`] and a [`Roll`] are whole parts per million.
//! `40%` is 400 000 parts per million, and a roll fires when it is under the chance.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every definition type has a `serde` form.
//!
//! | Type | Form |
//! |---|---|
//! | [`Trigger`] | The text `manual`, or the map of its parts. |
//! | [`TriggerParts`] | A map with the parts it has: `{ chance: 40%, nth: 3, per: session }`. |
//! | [`Chance`] | Its text: `40%`. A bare number is an error. |
//! | [`Count`], [`MatchNumber`], [`Seed`] | A number. |
//! | [`TimeWindow`] | An entry in the map of the parts: `after: 10s` or `between: [10s, 40s]`. |
//! | [`Per`] | Its name: `rule` or `session`. The map leaves out `per: rule`. |
//! | [`Lifetime`] | Its text: `until disarmed`, `30s`, `100 matches` or `3 firings`. |
//! | [`DiceKey`] | Its text. |
//!
//! Reading rejects a part it does not know, a part that a map names twice, and a map with both `after` and `between`.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_clock` for instants and time spans.
//! It has no async code, no IO, no system clock and no global randomness, and it compiles to WebAssembly.
//! The current time comes in as an [`Instant`](gaslightmq_clock::Instant) argument.

mod arming;
mod budget;
mod chance;
mod count;
mod counter;
mod dice;
mod error;
mod hand;
mod lifetime;
#[cfg(feature = "serde")]
mod serde;
mod trigger;
mod window;

pub use arming::{Arming, Confirmed, Evaluation, Firing, Rolled, TriggerOutcome};
pub use budget::{Budget, BudgetReading, Reservation, Spent};
pub use chance::{Chance, Roll};
pub use count::{Count, MatchNumber};
pub use counter::{Counter, SessionCounters};
pub use dice::{CounterKey, DiceKey, Die, Seed};
pub use error::TriggerError;
pub use lifetime::Lifetime;
pub use trigger::{Per, Trigger, TriggerParts};
pub use window::TimeWindow;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in triggers.
/// No name in it is a name of the standard prelude or of the kernel's prelude, so the import shadows nothing.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let chance: Result<Chance, TriggerError> = "40%".parse();
/// assert_eq!(chance?.ppm(), 400_000);
/// # Ok::<(), TriggerError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Arming, Budget, BudgetReading, Chance, Confirmed, Count, Counter, CounterKey, DiceKey, Die, Evaluation, Firing,
        Lifetime, MatchNumber, Per, Reservation, Roll, Rolled, Seed, SessionCounters, Spent, TimeWindow, Trigger,
        TriggerError, TriggerOutcome, TriggerParts,
    };
}
