//! What the trigger of one rule looked at on one call, and what it decided.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use gaslightmq_clock::TimeSpan;

/// One whole, in parts per million.
const WHOLE: u32 = 1_000_000;

/// How the trigger of a rule decided on one call: whether the rule fired, and why not when it did not.
///
/// The name is the text form: `fired`, `fired-by-hand`, `held`, `budget-empty`, `nothing-to-act-on` or `lifetime-over`.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// assert!(TriggerDecision::FiredByHand.fired());
/// assert!(!TriggerDecision::BudgetEmpty.fired());
/// assert_eq!("budget-empty".parse(), Ok(TriggerDecision::BudgetEmpty));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TriggerDecision {
    /// Every part of the trigger agreed, and the rule fired.
    Fired,
    /// Someone fired the rule by hand, and this call took that firing. The trigger was skipped.
    FiredByHand,
    /// A part of the trigger did not agree, or the trigger only fires by hand. The rule did not fire.
    Held,
    /// The trigger agreed, or someone fired the rule by hand, but the budget had no firing free.
    /// The rule did not fire.
    BudgetEmpty,
    /// The trigger agreed, or someone fired the rule by hand, but the action had nothing to act on in this call:
    /// an incident takes its subject from the call, and the call offers none.
    /// The rule did not fire.
    NothingToActOn,
    /// The lifetime of the rule had ended. The call was not counted, and nothing rolled.
    LifetimeOver,
}

impl TriggerDecision {
    /// Every decision.
    pub const ALL: [TriggerDecision; 6] = [
        TriggerDecision::Fired,
        TriggerDecision::FiredByHand,
        TriggerDecision::Held,
        TriggerDecision::BudgetEmpty,
        TriggerDecision::NothingToActOn,
        TriggerDecision::LifetimeOver,
    ];

    /// The name of the decision.
    pub const fn name(self) -> &'static str {
        match self {
            TriggerDecision::Fired => "fired",
            TriggerDecision::FiredByHand => "fired-by-hand",
            TriggerDecision::Held => "held",
            TriggerDecision::BudgetEmpty => "budget-empty",
            TriggerDecision::NothingToActOn => "nothing-to-act-on",
            TriggerDecision::LifetimeOver => "lifetime-over",
        }
    }

    /// Whether the rule fired.
    pub const fn fired(self) -> bool {
        matches!(self, TriggerDecision::Fired | TriggerDecision::FiredByHand)
    }
}

impl FromStr for TriggerDecision {
    type Err = TriggerDecisionError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|decision| decision.name() == text)
            .ok_or_else(|| TriggerDecisionError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for TriggerDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Why a text is not a [`TriggerDecision`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TriggerDecisionError {
    /// No decision has this name.
    Unknown { name: String },
}

impl fmt::Display for TriggerDecisionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { name } => write!(f, "no trigger decision is called {name:?}"),
        }
    }
}

impl std::error::Error for TriggerDecisionError {}

/// The die of a chance trigger on one call: what it rolled, and the chance the roll had to be under.
///
/// Both are whole parts per million.
/// A roll is below one million, and a chance is at most one million, which is `100%`.
/// The part agreed when the roll is under the chance.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let die = DieCheck::new(374_213, 400_000)?;
/// assert!(die.agreed());
/// assert_eq!(die.to_string(), "rolled 37.4213% under 40%");
/// assert_eq!(DieCheck::new(400_000, 400_000)?.to_string(), "rolled 40%, not under 40%");
/// # Ok::<(), DieCheckError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DieCheck {
    roll: u32,
    chance: u32,
}

impl DieCheck {
    /// Check that `roll` and `chance` are parts per million that a die and a chance can hold, and keep them.
    ///
    /// # Errors
    ///
    /// - [`DieCheckError::RollOutOfRange`] when `roll` is one million or more.
    /// - [`DieCheckError::ChanceOutOfRange`] when `chance` is above one million.
    pub fn new(roll: u32, chance: u32) -> Result<DieCheck, DieCheckError> {
        if roll >= WHOLE {
            return Err(DieCheckError::RollOutOfRange { ppm: roll });
        }
        if chance > WHOLE {
            return Err(DieCheckError::ChanceOutOfRange { ppm: chance });
        }
        Ok(DieCheck { roll, chance })
    }

    /// What the die rolled, in parts per million.
    pub const fn roll_ppm(self) -> u32 {
        self.roll
    }

    /// The chance the roll had to be under, in parts per million.
    pub const fn chance_ppm(self) -> u32 {
        self.chance
    }

    /// Whether the roll was under the chance.
    pub const fn agreed(self) -> bool {
        self.roll < self.chance
    }
}

/// Writes both as percentages: `rolled 37.4213% under 40%`, or `rolled 62.5%, not under 40%`.
impl fmt::Display for DieCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (roll, chance) = (Percentage(self.roll), Percentage(self.chance));
        if self.agreed() {
            write!(f, "rolled {roll} under {chance}")
        } else {
            write!(f, "rolled {roll}, not under {chance}")
        }
    }
}

/// Why two numbers are not a [`DieCheck`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DieCheckError {
    /// The roll is one million or more, and a die never rolls a whole million.
    RollOutOfRange { ppm: u32 },
    /// The chance is above one million, which is `100%`.
    ChanceOutOfRange { ppm: u32 },
}

impl fmt::Display for DieCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RollOutOfRange { ppm } => {
                write!(f, "a roll of {ppm} parts per million is not below one million")
            }
            Self::ChanceOutOfRange { ppm } => {
                write!(f, "a chance of {ppm} parts per million is above one million")
            }
        }
    }
}

impl std::error::Error for DieCheckError {}

/// Parts per million as a percentage with as few decimals as it needs.
struct Percentage(u32);

impl fmt::Display for Percentage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const PERCENT: u32 = 10_000;
        let (whole, rest) = (self.0 / PERCENT, self.0 % PERCENT);
        if rest == 0 {
            return write!(f, "{whole}%");
        }
        let decimals = format!("{rest:04}");
        write!(f, "{whole}.{}%", decimals.trim_end_matches('0'))
    }
}

/// The time window of a time trigger on one call: the window, and how long the rule had been armed.
///
/// A window opens a time span after arming, and it can close a later time span after arming.
/// It is open from the moment it opens, and closed again from the moment it closes.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::TimeSpan;
///
/// // `between: [10s, 40s]`, on a call 12 s after arming.
/// let secs = TimeSpan::from_secs;
/// let window = WindowCheck::new(secs(10), Some(secs(40)), secs(12))?;
/// assert!(window.agreed());
/// assert_eq!(window.to_string(), "window open 12s after arming");
/// # Ok::<(), WindowCheckError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WindowCheck {
    opens_after: TimeSpan,
    closes_after: Option<TimeSpan>,
    since_armed: TimeSpan,
}

impl WindowCheck {
    /// The window that opens `opens_after` arming and closes `closes_after` arming,
    /// as a call found it `since_armed` after arming.
    ///
    /// A measured time rounds down to the whole milliseconds of `since_armed`.
    /// A window opens and closes on a whole millisecond, so that never changes whether it was open.
    ///
    /// # Errors
    ///
    /// [`WindowCheckError::Empty`] when the window closes before it opens, or at the moment it opens.
    pub fn new(
        opens_after: TimeSpan,
        closes_after: Option<TimeSpan>,
        since_armed: TimeSpan,
    ) -> Result<WindowCheck, WindowCheckError> {
        match closes_after {
            Some(closes_after) if closes_after <= opens_after => Err(WindowCheckError::Empty {
                opens_after,
                closes_after,
            }),
            _ => Ok(WindowCheck {
                opens_after,
                closes_after,
                since_armed,
            }),
        }
    }

    /// How long after arming the window opens.
    pub const fn opens_after(self) -> TimeSpan {
        self.opens_after
    }

    /// How long after arming the window closes, or `None` for a window that stays open.
    pub const fn closes_after(self) -> Option<TimeSpan> {
        self.closes_after
    }

    /// How long the rule had been armed when the call was evaluated.
    pub const fn since_armed(self) -> TimeSpan {
        self.since_armed
    }

    /// Whether the window was open.
    pub fn agreed(self) -> bool {
        self.since_armed >= self.opens_after && self.closes_after.is_none_or(|closes| self.since_armed < closes)
    }
}

/// Writes whether the window was open, and when: `window open 12s after arming`.
impl fmt::Display for WindowCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.agreed() { "open" } else { "closed" };
        write!(f, "window {state} {} after arming", self.since_armed)
    }
}

/// Why three time spans are not a [`WindowCheck`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WindowCheckError {
    /// The window closes before it opens, or at the moment it opens, so it is never open.
    Empty {
        opens_after: TimeSpan,
        closes_after: TimeSpan,
    },
}

impl fmt::Display for WindowCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty {
                opens_after,
                closes_after,
            } => write!(
                f,
                "a time window from {opens_after} to {closes_after} after arming is never open"
            ),
        }
    }
}

impl std::error::Error for WindowCheckError {}

/// The budget of a rule after one call took from it: how much the lifetime allows, and how much was left.
///
/// A lifetime of matches counts matches, and a lifetime of firings counts firings.
/// A firing that a call has set aside is not left, although it can still come back.
///
/// ```
/// use std::num::NonZeroU64;
///
/// use gaslightmq_call::prelude::*;
///
/// let three = NonZeroU64::new(3).expect("three is not zero");
/// let budget = BudgetCheck::new(2, three)?;
/// assert_eq!(budget.to_string(), "2 of 3 left");
/// assert_eq!(BudgetCheck::new(4, three), Err(BudgetCheckError::LeftAboveLimit { left: 4, limit: three }));
/// # Ok::<(), BudgetCheckError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BudgetCheck {
    left: u64,
    limit: NonZeroU64,
}

impl BudgetCheck {
    /// Check that no more is left than the budget allows, and keep both numbers.
    ///
    /// # Errors
    ///
    /// [`BudgetCheckError::LeftAboveLimit`] when `left` is above `limit`.
    pub fn new(left: u64, limit: NonZeroU64) -> Result<BudgetCheck, BudgetCheckError> {
        if left > limit.get() {
            return Err(BudgetCheckError::LeftAboveLimit { left, limit });
        }
        Ok(BudgetCheck { left, limit })
    }

    /// How many matches or firings were left.
    pub const fn left(self) -> u64 {
        self.left
    }

    /// How many matches or firings the lifetime allows.
    pub const fn limit(self) -> NonZeroU64 {
        self.limit
    }
}

/// Writes what was left of the whole: `2 of 3 left`.
impl fmt::Display for BudgetCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {} left", self.left, self.limit)
    }
}

/// Why two numbers are not a [`BudgetCheck`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BudgetCheckError {
    /// More is left than the budget allows.
    LeftAboveLimit { left: u64, limit: NonZeroU64 },
}

impl fmt::Display for BudgetCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeftAboveLimit { left, limit } => {
                write!(f, "a budget of {limit} does not have {left} left")
            }
        }
    }
}

impl std::error::Error for BudgetCheckError {}

/// What each part of a trigger looked at on one call.
///
/// A trigger has a part or it does not, so every check is optional.
/// A trigger without parts fires on every match, and all of its checks agree.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// // `trigger: { chance: 40%, nth: 3 }` on the 3rd match.
/// let checks = TriggerChecks::new().with_count(true).with_die(DieCheck::new(620_000, 400_000)?);
/// assert!(!checks.all_agreed());
/// assert!(TriggerChecks::new().all_agreed());
/// # Ok::<(), DieCheckError>(())
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TriggerChecks {
    count_agreed: Option<bool>,
    die: Option<DieCheck>,
    window: Option<WindowCheck>,
}

impl TriggerChecks {
    /// No check: the trigger has no part, or nobody looked at its parts.
    pub const fn new() -> TriggerChecks {
        TriggerChecks {
            count_agreed: None,
            die: None,
            window: None,
        }
    }

    /// The count parts of the trigger agreed on the match number, or they did not.
    #[must_use]
    pub const fn with_count(mut self, agreed: bool) -> TriggerChecks {
        self.count_agreed = Some(agreed);
        self
    }

    /// The trigger has a chance, and its die rolled.
    #[must_use]
    pub const fn with_die(mut self, die: DieCheck) -> TriggerChecks {
        self.die = Some(die);
        self
    }

    /// The trigger has a time window.
    #[must_use]
    pub const fn with_window(mut self, window: WindowCheck) -> TriggerChecks {
        self.window = Some(window);
        self
    }

    /// Whether the count parts agreed on the match number, when the trigger has count parts.
    pub const fn count_agreed(&self) -> Option<bool> {
        self.count_agreed
    }

    /// What the die rolled, when the trigger has a chance.
    pub const fn die(&self) -> Option<DieCheck> {
        self.die
    }

    /// What the time window was, when the trigger has one.
    pub const fn window(&self) -> Option<WindowCheck> {
        self.window
    }

    /// Whether there is no check at all.
    pub const fn is_empty(&self) -> bool {
        self.count_agreed.is_none() && self.die.is_none() && self.window.is_none()
    }

    /// Whether every check there is agreed.
    pub fn all_agreed(&self) -> bool {
        self.count_agreed != Some(false)
            && self.die.is_none_or(DieCheck::agreed)
            && self.window.is_none_or(WindowCheck::agreed)
    }
}

/// What the trigger of one rule looked at on one call, and what it decided: the status Rolled of an explanation.
///
/// | Part | Is |
/// |---|---|
/// | [`decision`](Self::decision) | Whether the rule fired, and why not when it did not. |
/// | [`match_number`](Self::match_number) | Which matching call this was for the rule's counter. |
/// | [`checks`](Self::checks) | What the count parts, the die and the time window said. |
/// | [`budget`](Self::budget) | What the budget of the lifetime held after this call took from it. |
/// | [`reason`](Self::reason) | Why the action had nothing to act on, in one line. |
///
/// This crate does not evaluate triggers.
/// Whoever does writes down what happened here, with plain numbers.
/// The record only checks that it does not contradict itself.
///
/// ```
/// use std::num::NonZeroU64;
///
/// use gaslightmq_call::prelude::*;
///
/// // `trigger: { chance: 40%, nth: 3 }` with `lifetime: 3 firings`, on the 3rd match.
/// let third = NonZeroU64::new(3).expect("three is not zero");
/// let checks = TriggerChecks::new().with_count(true).with_die(DieCheck::new(374_213, 400_000).expect("a die check"));
/// let rolled = TriggerExplanation::new(TriggerDecision::Fired, Some(third), checks)?
///     .with_budget(BudgetCheck::new(2, third).expect("a budget check"));
///
/// assert!(rolled.fired());
/// assert_eq!(rolled.to_string(), "match 3, count agreed, rolled 37.4213% under 40%, 2 of 3 left: fired");
///
/// // A record that says "fired" against its own die is no record.
/// let against = TriggerChecks::new().with_die(DieCheck::new(620_000, 400_000).expect("a die check"));
/// assert_eq!(
///     TriggerExplanation::new(TriggerDecision::Fired, Some(third), against),
///     Err(TriggerExplanationError::FiredAgainstCheck)
/// );
/// # Ok::<(), gaslightmq_call::TriggerExplanationError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TriggerExplanation {
    decision: TriggerDecision,
    match_number: Option<NonZeroU64>,
    checks: TriggerChecks,
    budget: Option<BudgetCheck>,
    reason: Option<String>,
}

impl TriggerExplanation {
    /// What a trigger decided on a call, and what it looked at.
    ///
    /// `match_number` is which matching call this was for the rule's counter.
    /// A rule whose lifetime had ended does not count the call, so it has no match number and no check.
    /// A firing by hand skips the trigger, so its checks are empty too, though nothing here insists on that.
    ///
    /// # Errors
    ///
    /// - [`TriggerExplanationError::Uncounted`] when there is no match number, and the lifetime had not ended.
    /// - [`TriggerExplanationError::EvaluatedAfterLifetime`] when the lifetime had ended,
    ///   and there is a match number or a check.
    /// - [`TriggerExplanationError::FiredAgainstCheck`] when the decision is [`TriggerDecision::Fired`],
    ///   and a check did not agree.
    pub fn new(
        decision: TriggerDecision,
        match_number: Option<NonZeroU64>,
        checks: TriggerChecks,
    ) -> Result<TriggerExplanation, TriggerExplanationError> {
        match (decision, match_number) {
            (TriggerDecision::LifetimeOver, None) if checks.is_empty() => {}
            (TriggerDecision::LifetimeOver, _) => return Err(TriggerExplanationError::EvaluatedAfterLifetime),
            (_, None) => return Err(TriggerExplanationError::Uncounted { decision }),
            (TriggerDecision::Fired, Some(_)) if !checks.all_agreed() => {
                return Err(TriggerExplanationError::FiredAgainstCheck);
            }
            (_, Some(_)) => {}
        }
        Ok(TriggerExplanation {
            decision,
            match_number,
            checks,
            budget: None,
            reason: None,
        })
    }

    /// The lifetime of the rule had ended: the call was not counted, and nothing rolled.
    pub const fn lifetime_over() -> TriggerExplanation {
        TriggerExplanation {
            decision: TriggerDecision::LifetimeOver,
            match_number: None,
            checks: TriggerChecks::new(),
            budget: None,
            reason: None,
        }
    }

    /// The lifetime counts matches or firings, and this is its budget after the call took from it.
    #[must_use]
    pub const fn with_budget(mut self, budget: BudgetCheck) -> TriggerExplanation {
        self.budget = Some(budget);
        self
    }

    /// Why the action had nothing to act on, in one line: `leader-moves needs a partition, and none is named`.
    #[must_use]
    pub fn with_reason(mut self, reason: impl fmt::Display) -> TriggerExplanation {
        self.reason = Some(reason.to_string());
        self
    }

    /// Whether the rule fired, and why not when it did not.
    pub const fn decision(&self) -> TriggerDecision {
        self.decision
    }

    /// Whether the rule fired.
    pub const fn fired(&self) -> bool {
        self.decision.fired()
    }

    /// Which matching call this was, or `None` when the lifetime had ended and the call was not counted.
    pub const fn match_number(&self) -> Option<NonZeroU64> {
        self.match_number
    }

    /// What each part of the trigger looked at.
    pub const fn checks(&self) -> &TriggerChecks {
        &self.checks
    }

    /// What the budget held after this call took from it, when the lifetime counts matches or firings.
    pub const fn budget(&self) -> Option<BudgetCheck> {
        self.budget
    }

    /// Why the action had nothing to act on, when someone wrote it down.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// Writes what the trigger looked at, and then the decision:
/// `match 3, count agreed, rolled 37.4213% under 40%, 2 of 3 left: fired`.
impl fmt::Display for TriggerExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = Vec::new();
        if let Some(number) = self.match_number {
            parts.push(format!("match {number}"));
        }
        if let Some(agreed) = self.checks.count_agreed {
            parts.push(if agreed { "count agreed" } else { "count did not agree" }.to_owned());
        }
        if let Some(die) = self.checks.die {
            parts.push(die.to_string());
        }
        if let Some(window) = self.checks.window {
            parts.push(window.to_string());
        }
        if let Some(budget) = self.budget {
            parts.push(budget.to_string());
        }
        if !parts.is_empty() {
            write!(f, "{}: ", parts.join(", "))?;
        }
        fmt::Display::fmt(&self.decision, f)?;
        match &self.reason {
            Some(reason) => write!(f, " ({reason})"),
            None => Ok(()),
        }
    }
}

/// Why a decision, a match number and checks are not a [`TriggerExplanation`]: they contradict each other.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TriggerExplanationError {
    /// There is no match number, and only a rule whose lifetime had ended does not count the call.
    Uncounted { decision: TriggerDecision },
    /// The lifetime had ended, and there is a match number or a check.
    /// Such a rule neither counts the call nor rolls for it.
    EvaluatedAfterLifetime,
    /// The trigger fired, and one of its checks did not agree. Every part of a trigger has to agree.
    FiredAgainstCheck,
}

impl fmt::Display for TriggerExplanationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncounted { decision } => {
                write!(
                    f,
                    "a trigger that decided `{decision}` counted the call, and the match number is missing"
                )
            }
            Self::EvaluatedAfterLifetime => {
                f.write_str("a rule whose lifetime is over neither counts the call nor checks its trigger")
            }
            Self::FiredAgainstCheck => f.write_str("a trigger does not fire while one of its parts does not agree"),
        }
    }
}

impl std::error::Error for TriggerExplanationError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn secs(secs: u64) -> TimeSpan {
        TimeSpan::from_secs(secs)
    }

    #[test]
    fn a_decision_reads_back_its_name() {
        for decision in TriggerDecision::ALL {
            assert_eq!(decision.name().parse(), Ok(decision));
            assert_eq!(decision.to_string(), decision.name());
        }
        let error = "rolled".parse::<TriggerDecision>().unwrap_err();
        assert_eq!(error.to_string(), "no trigger decision is called \"rolled\"");
    }

    #[test]
    fn only_the_two_firings_fire() {
        let fired: Vec<_> = TriggerDecision::ALL
            .into_iter()
            .filter(|decision| decision.fired())
            .collect();

        assert_eq!(fired, [TriggerDecision::Fired, TriggerDecision::FiredByHand]);
    }

    #[test]
    fn a_die_agrees_when_the_roll_is_under_the_chance() {
        assert!(DieCheck::new(399_999, 400_000).unwrap().agreed());
        assert!(!DieCheck::new(400_000, 400_000).unwrap().agreed());
        assert!(DieCheck::new(999_999, 1_000_000).unwrap().agreed());
        assert!(!DieCheck::new(0, 0).unwrap().agreed());
    }

    #[test]
    fn a_die_only_holds_what_a_die_and_a_chance_can_be() {
        let roll = DieCheck::new(1_000_000, 400_000).unwrap_err();
        let chance = DieCheck::new(0, 1_000_001).unwrap_err();

        assert_eq!(roll, DieCheckError::RollOutOfRange { ppm: 1_000_000 });
        assert_eq!(
            roll.to_string(),
            "a roll of 1000000 parts per million is not below one million"
        );
        assert_eq!(chance, DieCheckError::ChanceOutOfRange { ppm: 1_000_001 });
        assert_eq!(
            chance.to_string(),
            "a chance of 1000001 parts per million is above one million"
        );
    }

    #[test]
    fn a_die_writes_percentages_with_as_few_decimals_as_they_need() {
        let cases = [
            (374_213, 400_000, "rolled 37.4213% under 40%"),
            (5_000, 1_000_000, "rolled 0.5% under 100%"),
            (625_000, 400_000, "rolled 62.5%, not under 40%"),
            (1, 0, "rolled 0.0001%, not under 0%"),
        ];

        for (roll, chance, text) in cases {
            assert_eq!(DieCheck::new(roll, chance).unwrap().to_string(), text);
        }
    }

    #[test]
    fn a_window_is_open_from_its_opening_until_its_closing() {
        let between = |since| WindowCheck::new(secs(10), Some(secs(40)), secs(since)).unwrap();
        let after = |since| WindowCheck::new(secs(10), None, secs(since)).unwrap();

        assert!(!between(9).agreed());
        assert!(between(10).agreed());
        assert!(between(39).agreed());
        assert!(!between(40).agreed());
        assert!(!after(9).agreed());
        assert!(after(4000).agreed());
        assert_eq!(between(9).to_string(), "window closed 9s after arming");
    }

    #[test]
    fn a_window_that_is_never_open_is_an_error() {
        for closes in [5, 10] {
            let error = WindowCheck::new(secs(10), Some(secs(closes)), secs(0)).unwrap_err();

            assert_eq!(
                error,
                WindowCheckError::Empty {
                    opens_after: secs(10),
                    closes_after: secs(closes)
                }
            );
        }
        assert_eq!(
            WindowCheck::new(secs(10), Some(secs(5)), secs(0))
                .unwrap_err()
                .to_string(),
            "a time window from 10s to 5s after arming is never open"
        );
    }

    #[test]
    fn a_budget_has_at_most_its_limit_left() {
        assert_eq!(BudgetCheck::new(3, number(3)).unwrap().left(), 3);
        assert_eq!(BudgetCheck::new(0, number(3)).unwrap().limit(), number(3));
        assert_eq!(
            BudgetCheck::new(4, number(3)).unwrap_err().to_string(),
            "a budget of 3 does not have 4 left"
        );
    }

    #[test]
    fn a_trigger_without_checks_agrees() {
        let none = TriggerChecks::new();

        assert!(none.is_empty());
        assert!(none.all_agreed());
        assert_eq!(none, TriggerChecks::default());
    }

    #[test]
    fn one_check_that_disagrees_is_enough() {
        let die = |roll| DieCheck::new(roll, 400_000).unwrap();
        let window = |since| WindowCheck::new(secs(10), None, secs(since)).unwrap();
        let all = TriggerChecks::new()
            .with_count(true)
            .with_die(die(1))
            .with_window(window(10));

        assert!(all.all_agreed());
        assert!(!all.is_empty());
        assert!(!all.with_count(false).all_agreed());
        assert!(!all.with_die(die(400_000)).all_agreed());
        assert!(!all.with_window(window(9)).all_agreed());
    }

    #[test]
    fn every_decision_but_lifetime_over_needs_a_match_number() {
        for decision in TriggerDecision::ALL {
            let counted = TriggerExplanation::new(decision, Some(number(1)), TriggerChecks::new());
            let uncounted = TriggerExplanation::new(decision, None, TriggerChecks::new());

            if decision == TriggerDecision::LifetimeOver {
                assert_eq!(counted, Err(TriggerExplanationError::EvaluatedAfterLifetime));
                assert_eq!(uncounted, Ok(TriggerExplanation::lifetime_over()));
            } else {
                assert_eq!(counted.unwrap().match_number(), Some(number(1)));
                assert_eq!(uncounted, Err(TriggerExplanationError::Uncounted { decision }));
            }
        }
    }

    #[test]
    fn a_rule_whose_lifetime_is_over_checks_nothing() {
        let checks = TriggerChecks::new().with_count(true);

        assert_eq!(
            TriggerExplanation::new(TriggerDecision::LifetimeOver, None, checks),
            Err(TriggerExplanationError::EvaluatedAfterLifetime)
        );
    }

    #[test]
    fn only_a_trigger_whose_parts_all_agree_fires() {
        let against = TriggerChecks::new().with_count(false);

        assert_eq!(
            TriggerExplanation::new(TriggerDecision::Fired, Some(number(4)), against),
            Err(TriggerExplanationError::FiredAgainstCheck)
        );
        // A hand firing skips the trigger, and every other decision did not fire.
        for decision in TriggerDecision::ALL {
            if !matches!(decision, TriggerDecision::Fired | TriggerDecision::LifetimeOver) {
                assert!(
                    TriggerExplanation::new(decision, Some(number(4)), against).is_ok(),
                    "{decision}"
                );
            }
        }
    }

    #[test]
    fn a_trigger_explanation_writes_what_it_looked_at_and_then_the_decision() {
        let held = TriggerChecks::new().with_count(false);
        let cases = [
            (TriggerExplanation::lifetime_over(), "lifetime-over"),
            (
                TriggerExplanation::new(TriggerDecision::Held, Some(number(4)), held).unwrap(),
                "match 4, count did not agree: held",
            ),
            (
                TriggerExplanation::new(TriggerDecision::FiredByHand, Some(number(1)), TriggerChecks::new()).unwrap(),
                "match 1: fired-by-hand",
            ),
            (
                TriggerExplanation::new(TriggerDecision::BudgetEmpty, Some(number(9)), TriggerChecks::new())
                    .unwrap()
                    .with_budget(BudgetCheck::new(0, number(3)).unwrap()),
                "match 9, 0 of 3 left: budget-empty",
            ),
            (
                TriggerExplanation::new(TriggerDecision::NothingToActOn, Some(number(2)), TriggerChecks::new())
                    .unwrap()
                    .with_reason("leader-moves needs a partition, and none is named"),
                "match 2: nothing-to-act-on (leader-moves needs a partition, and none is named)",
            ),
        ];

        for (explanation, text) in cases {
            assert_eq!(explanation.to_string(), text);
        }
    }

    #[test]
    fn a_trigger_explanation_error_says_what_contradicts() {
        let cases = [
            (
                TriggerExplanationError::Uncounted {
                    decision: TriggerDecision::Held,
                },
                "a trigger that decided `held` counted the call, and the match number is missing",
            ),
            (
                TriggerExplanationError::EvaluatedAfterLifetime,
                "a rule whose lifetime is over neither counts the call nor checks its trigger",
            ),
            (
                TriggerExplanationError::FiredAgainstCheck,
                "a trigger does not fire while one of its parts does not agree",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }
}
