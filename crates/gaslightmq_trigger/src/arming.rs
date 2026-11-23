//! One arming of one rule: its live state, and the evaluation of a matching call.

use gaslightmq_clock::{Duration, Instant};

use crate::hand::{HandFirings, TakenHandFiring};
use crate::{
    Budget, BudgetReading, Chance, Counter, Die, Lifetime, MatchNumber, Per, Reservation, Roll, SessionCounters,
    TimeWindow, Trigger, TriggerParts,
};

/// The live state a rule gets each time someone arms it.
///
/// | Part | Is |
/// |---|---|
/// | The [`Trigger`] and the [`Lifetime`] | What the definition says. |
/// | The [`Die`] | What the seed and the rule's dice key give. |
/// | The moment of arming | Where the time window and a duration lifetime start. |
/// | The shared [`Counter`] | The match numbers of a rule that counts per rule. |
/// | The [`Budget`] | The matches or firings a lifetime allows. |
/// | The hand firings | How many matching calls still fire because someone fired the rule by hand. |
///
/// An arming is a handle.
/// Cloning it gives a second handle to the same counter, budget and hand firings.
/// The engine arms a rule, puts the arming into the snapshot of armed rules,
/// and every session evaluates its matching calls against it.
/// All shared state is atomic, so sessions on different threads need no lock.
///
/// Arming a rule again makes a new arming, which resets its counters, its budget and its time window.
/// Changing a parameter of an armed rule resets nothing: [`retuned`](Arming::retuned) keeps the live state.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_trigger::prelude::*;
///
/// // `trigger: { nth: 2 }` with `lifetime: 1 firing`.
/// let trigger = Trigger::from(TriggerParts::new().with_nth(MatchNumber::new(2)?));
/// let die = Die::new(Seed::new(42), &DiceKey::new("Refuse the 2nd Produce").expect("a dice key"));
/// let armed_at = Instant::ZERO;
/// let arming = Arming::new(trigger, Lifetime::Firings(Count::ONE), die, armed_at);
///
/// // A session evaluates each call the rule's selector matches.
/// let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").expect("a counter key"));
/// let now = armed_at + Duration::from_millis(5);
/// assert_eq!(arming.evaluate(now, &mut session).rolled().outcome(), TriggerOutcome::Held);
///
/// // The 2nd match fires, and the call holds a firing until it knows whether the action applied.
/// let (rolled, firing) = arming.evaluate(now, &mut session).into_parts();
/// assert_eq!(rolled.outcome(), TriggerOutcome::Fired);
/// assert_eq!(rolled.match_number(), Some(MatchNumber::new(2)?));
/// let firing = firing.expect("the rule fired");
///
/// // The action applied, which spends the one firing and ends the lifetime.
/// assert!(firing.confirm().ends_lifetime());
/// assert!(arming.has_ended(now));
/// # Ok::<(), MatchNumberError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Arming {
    trigger: Trigger,
    lifetime: Lifetime,
    die: Die,
    armed_at: Instant,
    counter: Counter,
    budget: Option<Budget>,
    by_hand: HandFirings,
}

impl Arming {
    /// Arms a rule at `now`: every counter at zero, the whole budget free, and the time window starting.
    pub fn new(trigger: Trigger, lifetime: Lifetime, die: Die, now: Instant) -> Arming {
        Arming {
            trigger,
            lifetime,
            die,
            armed_at: now,
            counter: Counter::new(),
            budget: match lifetime {
                Lifetime::Matches(limit) | Lifetime::Firings(limit) => Some(Budget::new(limit)),
                Lifetime::UntilDisarmed | Lifetime::Duration(_) => None,
            },
            by_hand: HandFirings::default(),
        }
    }

    /// The same arming with another trigger and lifetime, for a parameter change on an armed rule.
    ///
    /// Nothing resets: the counters, the hand firings and the moment of arming stay.
    /// A budget keeps what was spent when the lifetime still counts the same thing, and takes the new limit.
    /// A lifetime that counts something else starts a new budget.
    ///
    /// The old handle keeps its trigger and its limit, for the calls that are in flight on it.
    /// After a change, [`has_ended`](Arming::has_ended) on the new handle says whether the lifetime is over.
    #[must_use]
    pub fn retuned(&self, trigger: Trigger, lifetime: Lifetime) -> Arming {
        let budget = match (self.lifetime, lifetime, &self.budget) {
            (Lifetime::Matches(_), Lifetime::Matches(limit), Some(budget))
            | (Lifetime::Firings(_), Lifetime::Firings(limit), Some(budget)) => Some(budget.with_limit(limit)),
            (_, Lifetime::Matches(limit) | Lifetime::Firings(limit), _) => Some(Budget::new(limit)),
            (_, Lifetime::UntilDisarmed | Lifetime::Duration(_), _) => None,
        };
        Arming {
            trigger,
            lifetime,
            budget,
            ..self.clone()
        }
    }

    /// When the rule fires among its matching calls.
    pub const fn trigger(&self) -> Trigger {
        self.trigger
    }

    /// When the rule disarms itself.
    pub const fn lifetime(&self) -> Lifetime {
        self.lifetime
    }

    /// The die the rule rolls.
    pub const fn die(&self) -> Die {
        self.die
    }

    /// The moment the rule was armed.
    pub const fn armed_at(&self) -> Instant {
        self.armed_at
    }

    /// The counter that every session shares.
    pub const fn counter(&self) -> &Counter {
        &self.counter
    }

    /// The budget of a lifetime that counts matches or firings.
    pub const fn budget(&self) -> Option<&Budget> {
        self.budget.as_ref()
    }

    /// Whether the two handles are one arming of one rule.
    pub fn is(&self, other: &Arming) -> bool {
        self.counter.is(&other.counter)
    }

    /// The instant a duration lifetime ends, so the engine can set a timer for it.
    pub fn ends_at(&self) -> Option<Instant> {
        self.lifetime.ends_at(self.armed_at)
    }

    /// Whether the lifetime has ended at `now`: the duration passed, or calls confirmed the whole budget.
    ///
    /// A rule whose lifetime ended neither counts nor fires, even before the engine disarms it.
    pub fn has_ended(&self, now: Instant) -> bool {
        self.ends_at().is_some_and(|ends_at| now >= ends_at) || self.budget.as_ref().is_some_and(Budget::is_spent)
    }

    /// Evaluates the trigger for one call that the rule's selector matched.
    ///
    /// 1. A rule whose lifetime ended does nothing: the call is not counted.
    ///    A lifetime of matches takes one match from its budget here.
    /// 2. The call gets its match number, from the shared counter or from the session's own.
    /// 3. A hand firing that waits for a call fires on this one, and skips the trigger.
    /// 4. Otherwise every part of the trigger has to agree: the count parts, the die roll and the time window.
    /// 5. A rule that fires under a lifetime of firings sets one firing of its budget aside,
    ///    and does not fire when none is free.
    ///    A hand firing that finds none free keeps waiting.
    ///
    /// The [`Evaluation`] holds what happened as a [`Rolled`] record, and a [`Firing`] when the rule fired.
    pub fn evaluate(&self, now: Instant, session: &mut SessionCounters) -> Evaluation {
        let since_armed = now - self.armed_at;
        let mut rolled = Rolled::new(since_armed);

        if self.has_ended(now) {
            rolled.budget = self.budget.as_ref().map(Budget::reading);
            return Evaluation::held(rolled.with_outcome(TriggerOutcome::LifetimeOver));
        }
        if let (Lifetime::Matches(_), Some(budget)) = (self.lifetime, &self.budget) {
            let Some(spent) = budget.take() else {
                rolled.budget = Some(budget.reading());
                return Evaluation::held(rolled.with_outcome(TriggerOutcome::LifetimeOver));
            };
            rolled.budget = Some(budget.reading());
            rolled.ends_lifetime = spent.is_last();
        }

        let match_number = match self.trigger.per() {
            Per::Rule => self.counter.count(),
            Per::Session => session.count(&self.counter),
        };
        rolled.match_number = Some(match_number);

        let (by, outcome) = if let Some(waited) = self.by_hand.take() {
            (FiredBy::HandOnMatch(waited), TriggerOutcome::FiredByHand)
        } else {
            let agrees = match self.trigger {
                Trigger::Parts(parts) => self.roll(&parts, match_number, since_armed, session, &mut rolled),
                Trigger::Manual => false,
            };
            if !agrees {
                return Evaluation::held(rolled.with_outcome(TriggerOutcome::Held));
            }
            (FiredBy::Trigger, TriggerOutcome::Fired)
        };

        let Some(firing) = self.fire(by, &mut rolled) else {
            return Evaluation::held(rolled.with_outcome(TriggerOutcome::BudgetEmpty));
        };
        Evaluation {
            rolled: rolled.with_outcome(outcome),
            firing: Some(firing),
        }
    }

    /// Fires the rule by hand at once, without a call.
    ///
    /// A rule with an incident uses this: the incident fires immediately.
    /// The firing skips the trigger and counts no match.
    /// Under a lifetime of firings it sets one firing of the budget aside.
    ///
    /// Returns `None` when the lifetime has ended, or when no firing is free.
    pub fn fire_now(&self, now: Instant) -> Option<Firing> {
        if self.has_ended(now) {
            return None;
        }
        let reservation = match (self.lifetime, &self.budget) {
            (Lifetime::Firings(_), Some(budget)) => Some(budget.reserve()?),
            _ => None,
        };
        Some(Firing {
            reservation,
            by: FiredBy::HandAtOnce,
        })
    }

    /// Fires the rule by hand on the next call its selector matches, once.
    ///
    /// A rule with a lie uses this: it has no call to lie about yet.
    /// The next matching call on any session fires the rule and skips its trigger.
    /// Every hand firing is one firing, so two of them fire on the next two matching calls.
    ///
    /// A hand firing is only used up when a call [confirms](Firing::confirm) it.
    /// When the call withdraws its firing or ends early, the hand firing waits for the next matching call.
    pub fn fire_on_next_match(&self) {
        self.by_hand.add();
    }

    /// How many hand firings still wait for a matching call.
    ///
    /// A hand firing that a call took and has not settled yet is not among them.
    pub fn waiting_by_hand(&self) -> u64 {
        self.by_hand.waiting()
    }

    /// Checks every part of the trigger, writes each check down, and returns whether all agree.
    fn roll(
        &self,
        parts: &TriggerParts,
        match_number: MatchNumber,
        since_armed: Duration,
        session: &SessionCounters,
        rolled: &mut Rolled,
    ) -> bool {
        if parts.has_count() {
            rolled.count_agrees = Some(parts.counts(match_number));
        }
        if let Some(chance) = parts.chance() {
            let die = match parts.per() {
                Per::Rule => self.die,
                Per::Session => self.die.for_counter(session.key()),
            };
            rolled.die = Some((die.roll(match_number), chance));
        }
        rolled.window = parts.window();

        rolled.count_agrees != Some(false)
            && rolled.die.is_none_or(|(roll, chance)| roll.is_under(chance))
            && rolled.window.is_none_or(|window| window.is_open(since_armed))
    }

    /// Makes the firing of a rule that fires, or returns `None` when its budget has no firing free.
    fn fire(&self, by: FiredBy, rolled: &mut Rolled) -> Option<Firing> {
        let reservation = match (self.lifetime, &self.budget) {
            (Lifetime::Firings(_), Some(budget)) => {
                let reservation = budget.reserve();
                rolled.budget = Some(budget.reading());
                // Without a firing free, `by` is dropped here.
                // A hand firing then waits for a call that finds a firing free.
                Some(reservation?)
            }
            _ => None,
        };
        Some(Firing { reservation, by })
    }
}

/// What evaluating a trigger for one matching call gave: a record for the explanation, and a firing when the rule fired.
#[derive(Debug)]
#[must_use = "a rule that fired holds a firing, which the call has to settle"]
pub struct Evaluation {
    rolled: Rolled,
    firing: Option<Firing>,
}

impl Evaluation {
    fn held(rolled: Rolled) -> Evaluation {
        Evaluation { rolled, firing: None }
    }

    /// What happened, as the explanation of the call shows it.
    pub const fn rolled(&self) -> &Rolled {
        &self.rolled
    }

    /// Whether the rule fired.
    pub const fn fired(&self) -> bool {
        self.firing.is_some()
    }

    /// The firing, when the rule fired.
    pub fn into_firing(self) -> Option<Firing> {
        self.firing
    }

    /// The record, and the firing when the rule fired.
    pub fn into_parts(self) -> (Rolled, Option<Firing>) {
        (self.rolled, self.firing)
    }

    /// The record of a rule whose action has nothing to act on in this call, so the rule does not fire after all.
    ///
    /// Only whoever holds the action knows this: an incident that takes its subject from the call
    /// has nothing to change on a call that offers none.
    /// The call still counted as a match, and the record keeps every part the trigger looked at.
    ///
    /// - The outcome of a rule that fired becomes [`TriggerOutcome::NothingToActOn`].
    /// - Its firing goes back: the budget has the firing free again, and the record reads the budget after that.
    ///   A hand firing waits for the next matching call.
    ///
    /// The record of a rule that did not fire stays as it is.
    pub fn nothing_to_act_on(self) -> Rolled {
        let Evaluation { mut rolled, firing } = self;
        if let Some(firing) = firing {
            let budget = firing
                .reservation
                .as_ref()
                .map(|reservation| reservation.budget().clone());
            firing.withdraw();
            rolled.budget = budget.as_ref().map(Budget::reading).or(rolled.budget);
            rolled.outcome = TriggerOutcome::NothingToActOn;
        }
        rolled
    }
}

/// How the evaluation of a trigger came out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TriggerOutcome {
    /// Every part of the trigger agreed, and the rule fired.
    Fired,
    /// Someone fired the rule by hand, and this call took that firing.
    FiredByHand,
    /// A part of the trigger did not agree, or the trigger is manual. The rule did not fire.
    Held,
    /// The trigger agreed, or someone fired the rule by hand, but the budget had no firing free.
    /// The rule did not fire.
    BudgetEmpty,
    /// The trigger agreed, or someone fired the rule by hand, but the action had nothing to act on in this call.
    /// The rule did not fire. See [`Evaluation::nothing_to_act_on`].
    NothingToActOn,
    /// The lifetime of the rule had ended. The call was not counted.
    LifetimeOver,
}

impl TriggerOutcome {
    /// Whether the rule fired.
    pub const fn fired(self) -> bool {
        matches!(self, TriggerOutcome::Fired | TriggerOutcome::FiredByHand)
    }
}

/// What the evaluation of a trigger looked at, and how it came out.
///
/// The explanation of a call shows this as the status Rolled:
/// the match number, the die roll, the time window and the budget.
/// A part that the trigger does not have reads `None`, and so does every part after a hand firing,
/// because a hand firing skips the trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rolled {
    outcome: TriggerOutcome,
    match_number: Option<MatchNumber>,
    count_agrees: Option<bool>,
    die: Option<(Roll, Chance)>,
    window: Option<TimeWindow>,
    since_armed: Duration,
    budget: Option<BudgetReading>,
    ends_lifetime: bool,
}

impl Rolled {
    fn new(since_armed: Duration) -> Rolled {
        Rolled {
            outcome: TriggerOutcome::Held,
            match_number: None,
            count_agrees: None,
            die: None,
            window: None,
            since_armed,
            budget: None,
            ends_lifetime: false,
        }
    }

    fn with_outcome(mut self, outcome: TriggerOutcome) -> Rolled {
        self.outcome = outcome;
        self
    }

    /// How the evaluation came out.
    pub const fn outcome(&self) -> TriggerOutcome {
        self.outcome
    }

    /// The number of this match, or `None` when the lifetime had ended and the call was not counted.
    pub const fn match_number(&self) -> Option<MatchNumber> {
        self.match_number
    }

    /// Whether the count parts agreed on the match number, when the trigger has count parts.
    pub const fn count_agrees(&self) -> Option<bool> {
        self.count_agrees
    }

    /// What the die rolled, when the trigger has a chance.
    pub fn roll(&self) -> Option<Roll> {
        self.die.map(|(roll, _)| roll)
    }

    /// The chance the roll had to be under, when the trigger has a chance.
    pub fn chance(&self) -> Option<Chance> {
        self.die.map(|(_, chance)| chance)
    }

    /// The time window, when the trigger has one.
    pub const fn window(&self) -> Option<TimeWindow> {
        self.window
    }

    /// Whether the time window was open, when the trigger has one.
    pub fn window_open(&self) -> Option<bool> {
        self.window.map(|window| window.is_open(self.since_armed))
    }

    /// How long the rule had been armed when the call was evaluated.
    pub const fn since_armed(&self) -> Duration {
        self.since_armed
    }

    /// What the budget held after this call took from it, when the lifetime counts matches or firings.
    ///
    /// A budget of firings only shows when the trigger agreed, because only then does the call take from it.
    /// When the action had [nothing to act on](TriggerOutcome::NothingToActOn),
    /// it shows the budget after the firing went back.
    pub const fn budget(&self) -> Option<BudgetReading> {
        self.budget
    }

    /// Whether this call was the last match of a lifetime of matches.
    ///
    /// Exactly one call sees this for an arming, and the engine disarms the rule after it.
    /// A lifetime of firings ends when a call confirms the last [`Firing`].
    pub const fn ends_lifetime(&self) -> bool {
        self.ends_lifetime
    }
}

/// One firing of a rule, which the call settles when it knows whether the action applied.
///
/// A rule fires at the trigger stage, and only the resolve stage knows whether its action applies:
/// another rule can shadow it, or pre-empt it.
/// Shadowed and pre-empted firings do not count as firings for the lifetime.
///
/// - [`confirm`](Firing::confirm) when the action applied, on at least one target.
/// - [`withdraw`](Firing::withdraw) when the rule was shadowed or pre-empted on every target.
///
/// Nothing shadows an incident, so a rule with an incident confirms its firing at once.
///
/// Under a lifetime of firings, the firing holds a [`Reservation`] of the rule's budget.
/// Dropping a firing withdraws it, so a call that ends early gives the firing back.
///
/// # Hand firings
///
/// Every hand firing lies on a matching call, so a press is only used up when its action applied.
/// A firing that a call took from the [hand firings that waited](Arming::fire_on_next_match) goes back to them
/// when the call withdraws or drops it, and the next matching call takes it.
/// A firing of [`Arming::fire_now`] had no call to wait for, so withdrawing it only gives its reservation back.
#[derive(Debug)]
#[must_use = "a firing that is dropped does not count for the lifetime"]
pub struct Firing {
    reservation: Option<Reservation>,
    by: FiredBy,
}

/// What made a rule fire.
#[derive(Debug)]
enum FiredBy {
    /// Every part of the trigger agreed.
    Trigger,
    /// Someone fired the rule by hand at once, without a call.
    HandAtOnce,
    /// Someone fired the rule by hand, and a matching call took that firing from those that waited.
    HandOnMatch(TakenHandFiring),
}

impl Firing {
    /// Whether someone fired the rule by hand.
    pub const fn by_hand(&self) -> bool {
        !matches!(self.by, FiredBy::Trigger)
    }

    /// Counts the firing for the lifetime: the action applied.
    ///
    /// This also uses up the hand firing that the call took.
    pub fn confirm(self) -> Confirmed {
        if let FiredBy::HandOnMatch(waited) = self.by {
            waited.confirm();
        }
        Confirmed {
            ends_lifetime: self
                .reservation
                .is_some_and(|reservation| reservation.confirm().is_last()),
        }
    }

    /// Gives the firing back: the rule was shadowed or pre-empted, and its action did not apply.
    ///
    /// The budget has the firing free again.
    /// A hand firing that the call took waits for the next matching call.
    pub fn withdraw(self) {
        drop(self);
    }
}

/// What confirming a [`Firing`] did to the lifetime of its rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Confirmed {
    ends_lifetime: bool,
}

impl Confirmed {
    /// Whether this firing was the last of a lifetime of firings.
    ///
    /// Exactly one call sees this for an arming, and the engine disarms the rule after it.
    pub const fn ends_lifetime(self) -> bool {
        self.ends_lifetime
    }
}
