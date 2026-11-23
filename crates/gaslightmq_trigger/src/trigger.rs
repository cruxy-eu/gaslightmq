//! The trigger definition: its parts, and what they count per.

use std::fmt;
use std::str::FromStr;

use crate::{Chance, Count, MatchNumber, TimeWindow};

/// Whose matches a rule counts: `per: rule` or `per: session`.
///
/// | Counter | Kept in | "The 3rd Produce" means |
/// |---|---|---|
/// | [`Per::Rule`] | A [`Counter`](crate::Counter) that every session shares. | The 3rd Produce the rule matched, from anyone. |
/// | [`Per::Session`] | The [`SessionCounters`](crate::SessionCounters) of each session. | The 3rd Produce the rule matched on this connection. |
///
/// A definition that names no counter counts per rule.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Per {
    /// One counter for the rule, shared by all sessions.
    #[default]
    Rule,
    /// One counter for each session.
    Session,
}

impl Per {
    /// The name a definition writes: `rule` or `session`.
    pub const fn name(self) -> &'static str {
        match self {
            Per::Rule => "rule",
            Per::Session => "session",
        }
    }
}

impl FromStr for Per {
    type Err = PerError;

    fn from_str(text: &str) -> Result<Per, PerError> {
        match text {
            "rule" => Ok(Per::Rule),
            "session" => Ok(Per::Session),
            _ => Err(PerError::Unknown { name: text.to_owned() }),
        }
    }
}

impl fmt::Display for Per {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Why a text is not a [`Per`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PerError {
    /// The text is not `rule` and not `session`.
    #[error("counters are per `rule` or per `session`, not per `{name}`")]
    Unknown { name: String },
}

/// The parts of a trigger that fires by itself: a chance, count parts, a time window, and what it counts per.
///
/// | Part | Agrees | Definition |
/// |---|---|---|
/// | Chance | When the rule's die rolls under the chance. | `chance: 40%` |
/// | `nth` | On that one match. | `nth: 3` |
/// | `every` | On every match whose number that count divides. | `every: 10` |
/// | `first` | On the matches up to that count. | `first: 5` |
/// | `skip` | Never on the first matches, up to that count. | `skip: 100` |
/// | Window | While the time window is open. | `after: 10s`, `between: [10s, 40s]` |
///
/// The trigger fires on a matching call when every part it has agrees.
/// With no part at all it fires on every matching call.
///
/// `skip` passes over the first matches, and `nth`, `every` and `first` count from the match after them.
/// `skip: 100` with `first: 5` therefore agrees on the matches 101 to 105.
/// The die still rolls for the match number itself.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// // `trigger: { chance: 50%, nth: 3 }` fires on the 3rd matching call, half of the time.
/// let parts = TriggerParts::new()
///     .with_chance("50%".parse().expect("a chance"))
///     .with_nth(MatchNumber::new(3)?);
/// assert!(parts.counts(MatchNumber::new(3)?));
/// assert!(!parts.counts(MatchNumber::new(4)?));
///
/// // `trigger: { skip: 100, first: 5 }` passes over 100 matches, and then takes five.
/// let parts = TriggerParts::new()
///     .with_skip(Count::new(100).expect("a count"))
///     .with_first(Count::new(5).expect("a count"));
/// assert!(!parts.counts(MatchNumber::new(100)?));
/// assert!(parts.counts(MatchNumber::new(105)?));
/// assert!(!parts.counts(MatchNumber::new(106)?));
/// # Ok::<(), MatchNumberError>(())
/// ```
///
/// With the `serde` feature, the parts serialize as a map that only holds the parts they have.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TriggerParts {
    chance: Option<Chance>,
    nth: Option<MatchNumber>,
    every: Option<Count>,
    first: Option<Count>,
    skip: Option<Count>,
    window: Option<TimeWindow>,
    per: Per,
}

impl TriggerParts {
    /// No part: every matching call agrees.
    pub fn new() -> TriggerParts {
        TriggerParts::default()
    }

    /// Adds a chance: `chance: 40%`.
    #[must_use]
    pub fn with_chance(mut self, chance: Chance) -> TriggerParts {
        self.chance = Some(chance);
        self
    }

    /// Adds the one match to fire on: `nth: 3`.
    #[must_use]
    pub fn with_nth(mut self, nth: MatchNumber) -> TriggerParts {
        self.nth = Some(nth);
        self
    }

    /// Adds a step between the matches to fire on: `every: 10`.
    #[must_use]
    pub fn with_every(mut self, every: Count) -> TriggerParts {
        self.every = Some(every);
        self
    }

    /// Adds how many matches to fire on, from the start: `first: 5`.
    #[must_use]
    pub fn with_first(mut self, first: Count) -> TriggerParts {
        self.first = Some(first);
        self
    }

    /// Adds how many matches to pass over, from the start: `skip: 100`.
    #[must_use]
    pub fn with_skip(mut self, skip: Count) -> TriggerParts {
        self.skip = Some(skip);
        self
    }

    /// Adds a time window: `after: 10s` or `between: [10s, 40s]`.
    #[must_use]
    pub fn with_window(mut self, window: TimeWindow) -> TriggerParts {
        self.window = Some(window);
        self
    }

    /// Says whose matches the rule counts: `per: session`.
    #[must_use]
    pub fn with_per(mut self, per: Per) -> TriggerParts {
        self.per = per;
        self
    }

    /// The chance, when the trigger rolls a die.
    pub const fn chance(&self) -> Option<Chance> {
        self.chance
    }

    /// The one match to fire on.
    pub const fn nth(&self) -> Option<MatchNumber> {
        self.nth
    }

    /// The step between the matches to fire on.
    pub const fn every(&self) -> Option<Count> {
        self.every
    }

    /// How many matches to fire on, from the start.
    pub const fn first(&self) -> Option<Count> {
        self.first
    }

    /// How many matches to pass over, from the start.
    pub const fn skip(&self) -> Option<Count> {
        self.skip
    }

    /// The time window, when the trigger has one.
    pub const fn window(&self) -> Option<TimeWindow> {
        self.window
    }

    /// Whose matches the rule counts.
    pub const fn per(&self) -> Per {
        self.per
    }

    /// Whether the trigger has a count part: `nth`, `every`, `first` or `skip`.
    pub const fn has_count(&self) -> bool {
        self.nth.is_some() || self.every.is_some() || self.first.is_some() || self.skip.is_some()
    }

    /// Whether the trigger has no chance, no count part and no window, so every matching call fires it.
    pub const fn is_always(&self) -> bool {
        self.chance.is_none() && !self.has_count() && self.window.is_none()
    }

    /// Whether every count part agrees on that match.
    ///
    /// Without a count part, every match agrees.
    pub fn counts(&self, match_number: MatchNumber) -> bool {
        let number = match_number.value();
        let number = match self.skip {
            None => number,
            Some(skip) if number > skip.value() => number - skip.value(),
            Some(_) => return false,
        };
        self.nth.is_none_or(|nth| number == nth.value())
            && self.every.is_none_or(|every| number.is_multiple_of(every.value()))
            && self.first.is_none_or(|first| number <= first.value())
    }
}

/// When a rule fires among the calls its selector matches.
///
/// | Trigger | Fires | Definition |
/// |---|---|---|
/// | [`Trigger::Parts`] | By itself, on the matching calls on which every part agrees. | `trigger: { chance: 40%, nth: 3 }` |
/// | [`Trigger::Manual`] | Only when someone fires the rule by hand. | `trigger: manual` |
///
/// A rule without a trigger fires on every matching call, which is [`Trigger::always`] and the default.
/// Someone can fire any rule by hand, whatever its trigger.
/// A manual trigger only says that the rule never fires by itself.
///
/// An [`Arming`](crate::Arming) evaluates a trigger for a matching call.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let trigger = Trigger::from(TriggerParts::new().with_first(Count::new(2)?).with_per(Per::Session));
/// assert_eq!(trigger.per(), Per::Session);
/// assert!(!trigger.is_always());
/// assert!(Trigger::default().is_always());
/// assert!(Trigger::Manual.is_manual());
/// # Ok::<(), CountError>(())
/// ```
///
/// With the `serde` feature, a trigger serializes as the text `manual` or as the map of its parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Trigger {
    /// The rule fires by itself, when every part agrees.
    Parts(TriggerParts),
    /// The rule fires only by hand.
    Manual,
}

impl Trigger {
    /// The trigger of a rule that names none: it fires on every matching call.
    pub fn always() -> Trigger {
        Trigger::Parts(TriggerParts::new())
    }

    /// Whether the rule fires on every matching call.
    pub const fn is_always(&self) -> bool {
        match self {
            Trigger::Parts(parts) => parts.is_always(),
            Trigger::Manual => false,
        }
    }

    /// Whether the rule fires only by hand.
    pub const fn is_manual(&self) -> bool {
        matches!(self, Trigger::Manual)
    }

    /// The parts, unless the trigger is manual.
    pub const fn parts(&self) -> Option<&TriggerParts> {
        match self {
            Trigger::Parts(parts) => Some(parts),
            Trigger::Manual => None,
        }
    }

    /// Whose matches the rule counts.
    /// A manual trigger counts per rule.
    pub const fn per(&self) -> Per {
        match self {
            Trigger::Parts(parts) => parts.per(),
            Trigger::Manual => Per::Rule,
        }
    }
}

/// Fires on every matching call.
impl Default for Trigger {
    fn default() -> Trigger {
        Trigger::always()
    }
}

impl From<TriggerParts> for Trigger {
    fn from(parts: TriggerParts) -> Trigger {
        Trigger::Parts(parts)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;

    use super::*;

    fn number(value: u64) -> MatchNumber {
        MatchNumber::new(value).unwrap()
    }

    fn count(value: u64) -> Count {
        Count::new(value).unwrap()
    }

    /// The match numbers up to `limit` on which the count parts agree.
    fn agreed(parts: &TriggerParts, limit: u64) -> Vec<u64> {
        (1..=limit).filter(|&value| parts.counts(number(value))).collect()
    }

    #[test]
    fn without_a_count_part_every_match_agrees() {
        let parts = TriggerParts::new();
        assert!(!parts.has_count());
        assert_eq!(agreed(&parts, 4), [1, 2, 3, 4]);
    }

    #[test]
    fn nth_agrees_on_one_match() {
        let parts = TriggerParts::new().with_nth(number(3));
        assert_eq!(agreed(&parts, 10), [3]);
    }

    #[test]
    fn every_agrees_on_each_multiple() {
        let parts = TriggerParts::new().with_every(count(3));
        assert_eq!(agreed(&parts, 10), [3, 6, 9]);
        assert_eq!(agreed(&TriggerParts::new().with_every(Count::ONE), 3), [1, 2, 3]);
    }

    #[test]
    fn first_agrees_on_the_matches_up_to_its_count() {
        let parts = TriggerParts::new().with_first(count(2));
        assert_eq!(agreed(&parts, 10), [1, 2]);
    }

    #[test]
    fn skip_passes_over_the_first_matches() {
        let parts = TriggerParts::new().with_skip(count(3));
        assert_eq!(agreed(&parts, 6), [4, 5, 6]);
    }

    #[test]
    fn the_other_count_parts_count_from_the_match_after_the_skipped_ones() {
        let skip = TriggerParts::new().with_skip(count(100));
        assert_eq!(agreed(&skip.with_first(count(5)), 200), [101, 102, 103, 104, 105]);
        assert_eq!(agreed(&skip.with_nth(number(3)), 200), [103]);
        assert_eq!(agreed(&skip.with_every(count(40)), 200), [140, 180]);
        assert_eq!(
            agreed(&TriggerParts::new().with_skip(count(5)).with_every(count(10)), 30),
            [15, 25]
        );
    }

    #[test]
    fn all_count_parts_must_agree() {
        let parts = TriggerParts::new().with_first(count(50)).with_every(count(20));
        assert_eq!(agreed(&parts, 100), [20, 40]);

        let never = TriggerParts::new().with_first(count(2)).with_nth(number(3));
        assert_eq!(agreed(&never, 100), [0_u64; 0]);
    }

    #[test]
    fn a_trigger_without_parts_fires_always() {
        assert!(TriggerParts::new().is_always());
        assert!(TriggerParts::new().with_per(Per::Session).is_always());
        assert!(!TriggerParts::new().with_chance(Chance::ALWAYS).is_always());
        assert!(!TriggerParts::new().with_skip(Count::ONE).is_always());
        assert!(
            !TriggerParts::new()
                .with_window(TimeWindow::after(TimeSpan::ZERO))
                .is_always()
        );
        assert_eq!(Trigger::default(), Trigger::always());
        assert!(Trigger::always().is_always());
        assert!(!Trigger::Manual.is_always());
    }

    #[test]
    fn the_parts_keep_what_they_were_given() {
        let window = TimeWindow::after(TimeSpan::from_secs(10));
        let parts = TriggerParts::new()
            .with_chance(Chance::ALWAYS)
            .with_nth(number(3))
            .with_every(count(10))
            .with_first(count(5))
            .with_skip(count(100))
            .with_window(window)
            .with_per(Per::Session);
        assert_eq!(parts.chance(), Some(Chance::ALWAYS));
        assert_eq!(parts.nth(), Some(number(3)));
        assert_eq!(parts.every(), Some(count(10)));
        assert_eq!(parts.first(), Some(count(5)));
        assert_eq!(parts.skip(), Some(count(100)));
        assert_eq!(parts.window(), Some(window));
        assert_eq!(parts.per(), Per::Session);
        assert!(parts.has_count());
    }

    #[test]
    fn a_manual_trigger_has_no_parts_and_counts_per_rule() {
        assert!(Trigger::Manual.is_manual());
        assert_eq!(Trigger::Manual.parts(), None);
        assert_eq!(Trigger::Manual.per(), Per::Rule);

        let parts = TriggerParts::new().with_per(Per::Session);
        let trigger = Trigger::from(parts);
        assert!(!trigger.is_manual());
        assert_eq!(trigger.parts(), Some(&parts));
        assert_eq!(trigger.per(), Per::Session);
    }

    #[test]
    fn per_reads_and_writes_its_name() {
        assert_eq!("rule".parse(), Ok(Per::Rule));
        assert_eq!("session".parse(), Ok(Per::Session));
        assert_eq!(Per::Session.to_string(), "session");
        assert_eq!(Per::default(), Per::Rule);
        assert_eq!(
            "connection".parse::<Per>(),
            Err(PerError::Unknown {
                name: "connection".into()
            }),
        );
    }

    #[test]
    fn a_per_error_says_what_is_wrong() {
        let error = PerError::Unknown {
            name: "connection".into(),
        };
        assert_eq!(
            error.to_string(),
            "counters are per `rule` or per `session`, not per `connection`",
        );
    }
}
