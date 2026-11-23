//! The explanation of a call: every rule it touched, how far each one got, and why.

use std::collections::BTreeSet;
use std::fmt;

use gaslightmq_field::Target;
use gaslightmq_incident::Incident;
use gaslightmq_lie::{Priority, TargetedLie};

use crate::{RecordedRuleName, TriggerDecision, TriggerExplanation, Verdict};

/// A target that a lie lost, and the rule whose lie took it.
///
/// A lie that is **shadowed** lost a conflict: the other rule has a higher priority.
/// A lie that is **pre-empted** had nothing left to change: the lie of the other rule got there first.
/// A [`LieFate`] says which of the two a loss is.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
///
/// let partition = Target::of_parts(["orders.eu/3".parse().expect("a part")]).expect("one part");
/// let loss = LostTarget::new(partition, "Leader flap".parse()?);
/// assert_eq!(loss.to_string(), "on orders.eu/3 by Leader flap");
/// # Ok::<(), RecordedRuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LostTarget {
    on: Target,
    to: RecordedRuleName,
}

impl LostTarget {
    /// The lie lost `on` to the lie of the rule `to`.
    pub const fn new(on: Target, to: RecordedRuleName) -> LostTarget {
        LostTarget { on, to }
    }

    /// The target the lie lost: some of its parts, or the whole call.
    pub const fn on(&self) -> &Target {
        &self.on
    }

    /// The rule whose lie took the target.
    pub const fn to(&self) -> &RecordedRuleName {
        &self.to
    }
}

/// Writes the target and the other rule: `on orders.eu/3 by Leader flap`, or `by Blackhole` for the whole call.
impl fmt::Display for LostTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.on.is_call() {
            write!(f, "by {}", self.to)
        } else {
            write!(f, "on {} by {}", self.on, self.to)
        }
    }
}

/// The lie a rule fired, and what became of it: where it applied, and where it lost to which other rule.
///
/// A lie on several parts can meet a different fate on each.
/// A refusal of `orders.eu/3` and `orders.eu/4` can apply on one partition and be shadowed on the other.
///
/// | Status of an explanation | Here |
/// |---|---|
/// | Applied | [`applied`](Self::applied) names the target. |
/// | Shadowed | [`shadowed`](Self::shadowed) names each target and the rule that won it. |
/// | Pre-empted | [`pre_empted`](Self::pre_empted) names each target and the rule that got there first. |
///
/// A firing [counts](Self::counts) when the lie applied on at least one target.
///
/// # A lie that could not be told
///
/// A lie can win its target and still change nothing, because GaslightMQ itself failed to tell it:
/// its kind is not in the build, or the request does not name a part it targets.
/// Such a lie is [untold](Self::untold). It applied nowhere, and its [`failure`](Self::failure) says why.
/// What it lost to other rules stays, and so does what other rules lost to it.
/// A mistake of GaslightMQ never looks like a lie, so an untold lie is no line of the verdict, and its firing does not count.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let on = |parts: &[&str]| Target::of_parts(parts.iter().map(|part| part.parse().expect("a part"))).expect("a part");
/// let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
///
/// // The rule refused two partitions, and a rule with a higher priority refused one of them first.
/// let lost = LostTarget::new(on(&["orders.eu/3"]), "Leader flap".parse().expect("a recorded rule name"));
/// let fate = LieFate::new(
///     refusal.clone().on(on(&["orders.eu/3", "orders.eu/4"])),
///     Some(on(&["orders.eu/4"])),
///     vec![lost],
///     vec![],
/// )?;
///
/// // Shadowed on one partition and applied on another: one firing, and one line of the verdict.
/// assert!(fate.counts());
/// assert_eq!(fate.verdict_line(), Some(refusal.clone().on(on(&["orders.eu/4"]))));
///
/// // A refusal of a partition that the request does not name cannot be told.
/// let untold = LieFate::all_applied(refusal.on(on(&["orders.eu/9"])))
///     .untold(Failure::new("the Produce v9 request does not name orders.eu/9"));
/// assert!(!untold.counts());
/// assert_eq!(untold.verdict_line(), None);
/// assert_eq!(
///     untold.to_string(),
///     "NOT_LEADER_OR_FOLLOWER not told: the Produce v9 request does not name orders.eu/9"
/// );
/// # Ok::<(), LieFateError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LieFate {
    lie: TargetedLie,
    applied: Option<Target>,
    shadowed: Vec<LostTarget>,
    pre_empted: Vec<LostTarget>,
    untold: Option<Failure>,
}

impl LieFate {
    /// What became of `lie`, which is the lie on the target it claimed.
    ///
    /// # Errors
    ///
    /// - [`LieFateError::Nowhere`] when the lie neither applied nor lost a target. A lie that fired went somewhere.
    /// - [`LieFateError::AppliedOutsideTarget`] when the lie applied on something it did not claim.
    pub fn new(
        lie: TargetedLie,
        applied: Option<Target>,
        shadowed: Vec<LostTarget>,
        pre_empted: Vec<LostTarget>,
    ) -> Result<LieFate, LieFateError> {
        if applied.is_none() && shadowed.is_empty() && pre_empted.is_empty() {
            return Err(LieFateError::Nowhere);
        }
        if let Some(applied) = applied.as_ref().filter(|applied| !within(applied, lie.target())) {
            return Err(LieFateError::AppliedOutsideTarget {
                applied: applied.clone(),
                claimed: lie.target().clone(),
            });
        }
        Ok(LieFate {
            lie,
            applied,
            shadowed,
            pre_empted,
            untold: None,
        })
    }

    /// The lie applied on all of its target: no other lie was in its way.
    pub fn all_applied(lie: TargetedLie) -> LieFate {
        LieFate {
            applied: Some(lie.target().clone()),
            lie,
            shadowed: Vec::new(),
            pre_empted: Vec::new(),
            untold: None,
        }
    }

    /// GaslightMQ could not tell the lie, so it applied nowhere: `failure` says why.
    ///
    /// What the lie lost to other rules stays as it is.
    #[must_use]
    pub fn untold(mut self, failure: Failure) -> LieFate {
        self.applied = None;
        self.untold = Some(failure);
        self
    }

    /// The fate of a lie that could not be told, from the parts of its `serde` form.
    #[cfg(feature = "serde")]
    pub(crate) const fn untold_with_losses(
        lie: TargetedLie,
        shadowed: Vec<LostTarget>,
        pre_empted: Vec<LostTarget>,
        failure: Failure,
    ) -> LieFate {
        LieFate {
            lie,
            applied: None,
            shadowed,
            pre_empted,
            untold: Some(failure),
        }
    }

    /// The lie the rule fired, on the target it claimed.
    pub const fn lie(&self) -> &TargetedLie {
        &self.lie
    }

    /// The target the lie applied to, or `None` when it applied nowhere.
    pub const fn applied(&self) -> Option<&Target> {
        self.applied.as_ref()
    }

    /// The targets on which a rule with a higher priority won the conflict.
    pub fn shadowed(&self) -> &[LostTarget] {
        &self.shadowed
    }

    /// The targets on which the lie had nothing left to change.
    pub fn pre_empted(&self) -> &[LostTarget] {
        &self.pre_empted
    }

    /// Why GaslightMQ could not tell the lie, or `None` for a lie that it told or that lost every target.
    pub const fn failure(&self) -> Option<&Failure> {
        self.untold.as_ref()
    }

    /// Whether the firing counts for a lifetime of firings: the lie applied on at least one target.
    pub const fn counts(&self) -> bool {
        self.applied.is_some()
    }

    /// The line of the verdict: the lie on the target it applied to, or `None` when it applied nowhere.
    pub fn verdict_line(&self) -> Option<TargetedLie> {
        let applied = self.applied.as_ref()?;
        Some(TargetedLie::new(self.lie.lie().clone(), applied.clone()))
    }
}

/// Whether `inner` is all of `outer`, or a part of it.
fn within(inner: &Target, outer: &Target) -> bool {
    match (inner, outer) {
        (_, Target::Call) => true,
        (Target::Call, Target::Parts(_)) => false,
        (Target::Parts(inner), Target::Parts(outer)) => inner.is_subset(outer),
    }
}

/// Writes the lie and then each fate: `NOT_LEADER_OR_FOLLOWER applied on orders.eu/4, shadowed on orders.eu/3 by Leader flap`.
/// A lie that could not be told ends with the reason: `NOT_LEADER_OR_FOLLOWER not told: …`.
impl fmt::Display for LieFate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.lie.lie(), f)?;
        let mut separator = " ";
        if let Some(applied) = &self.applied {
            if applied.is_call() {
                f.write_str(" applied")?;
            } else {
                write!(f, " applied on {applied}")?;
            }
            separator = ", ";
        }
        for (status, losses) in [("shadowed", &self.shadowed), ("pre-empted", &self.pre_empted)] {
            for loss in losses {
                write!(f, "{separator}{status} {loss}")?;
                separator = ", ";
            }
        }
        if let Some(failure) = &self.untold {
            write!(f, "{separator}not told: {failure}")?;
        }
        Ok(())
    }
}

/// Why a lie and its targets are not a [`LieFate`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum LieFateError {
    /// The lie neither applied nor lost a target, and a lie that fired went somewhere.
    #[error("a lie that fired applied or lost somewhere, and this one did neither")]
    Nowhere,
    /// The lie applied on a target that is no part of the target it claimed.
    #[error("a lie on {claimed} does not apply on {applied}")]
    AppliedOutsideTarget { applied: Target, claimed: Target },
    /// The lie applied on a target, and it could not be told. A lie that could not be told applied nowhere.
    #[error("a lie that could not be told does not apply on {applied}")]
    AppliedUntold { applied: Target },
}

/// A status a rule can have in the explanation of a call.
///
/// A rule has every status it reached, and [`RuleExplanation::has`] says which.
/// A rule that is shadowed on one partition and applies on another is fired, applied and shadowed.
///
/// | Status | Meaning |
/// |---|---|
/// | Matched | The selector matched, with the target it matched. |
/// | Rolled | The trigger was evaluated: the call got a match number, and every part of the trigger was checked. |
/// | Fired | The trigger said yes. |
/// | Applied | The action took effect. |
/// | Shadowed | The rule fired, but a rule with a higher priority won the conflict. |
/// | Pre-empted | The rule had nothing left to change. |
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExplanationStatus {
    /// The selector of the rule matched the call.
    Matched,
    /// The trigger of the rule was evaluated.
    Rolled,
    /// The trigger said yes, or someone fired the rule by hand.
    Fired,
    /// The action took effect: the lie applied on at least one target, or the incidents are on their way.
    Applied,
    /// A rule with a higher priority won a conflict on at least one target.
    Shadowed,
    /// The rule had nothing left to change on at least one target, or the call ended before the rule saw the truth.
    PreEmpted,
}

impl ExplanationStatus {
    /// Every status, in the order a rule reaches them.
    pub const ALL: [ExplanationStatus; 6] = [
        ExplanationStatus::Matched,
        ExplanationStatus::Rolled,
        ExplanationStatus::Fired,
        ExplanationStatus::Applied,
        ExplanationStatus::Shadowed,
        ExplanationStatus::PreEmpted,
    ];

    /// The name of the status in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            ExplanationStatus::Matched => "matched",
            ExplanationStatus::Rolled => "rolled",
            ExplanationStatus::Fired => "fired",
            ExplanationStatus::Applied => "applied",
            ExplanationStatus::Shadowed => "shadowed",
            ExplanationStatus::PreEmpted => "pre-empted",
        }
    }
}

impl fmt::Display for ExplanationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How far a rule got on a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Course {
    /// The rule waited for the truth, and the call ended before the truth existed. Nobody evaluated the rule.
    EndedBeforeTruth { ended_by: RecordedRuleName },
    /// The selector matched, and the rule did not fire.
    Held {
        matched: Target,
        trigger: TriggerExplanation,
    },
    /// The rule fired its lie.
    Lied {
        matched: Target,
        trigger: TriggerExplanation,
        lie: LieFate,
    },
    /// The rule fired its incidents.
    FiredIncidents {
        matched: Target,
        trigger: TriggerExplanation,
        incidents: Vec<Incident>,
    },
}

/// What one rule did on one call, and why: one line of an [`Explanation`].
///
/// A rule gets as far as one of four ends, and each has its constructor.
///
/// | The rule | Constructor | Statuses |
/// |---|---|---|
/// | Matched, and did not fire. | [`held`](Self::held) | Matched, and Rolled unless its lifetime was over. |
/// | Fired its lie. | [`lied`](Self::lied) | Matched, Rolled, Fired, and what became of the lie. |
/// | Fired its incidents. | [`fired_incidents`](Self::fired_incidents) | Matched, Rolled, Fired, Applied. |
/// | Waited for the truth of a call that ended before the truth existed. | [`ended_before_truth`](Self::ended_before_truth) | Pre-empted. |
///
/// *Why did my rule not fire?* is the [`decision`](TriggerExplanation::decision) of its [`trigger`](Self::trigger),
/// next to what the trigger looked at.
/// *Why did my lie not apply?* is the [`LieFate`] of its [`lie`](Self::lie), which names the rule that won.
///
/// ```
/// use std::num::NonZeroU64;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let partition = Target::of_parts(["orders.eu/3".parse().expect("a part")]).expect("one part");
/// let third = NonZeroU64::new(3).expect("three is not zero");
/// let rolled = TriggerExplanation::new(TriggerDecision::Fired, Some(third), TriggerChecks::new().with_count(true))
///     .expect("the decision fits its checks");
/// let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(partition.clone());
///
/// let line = RuleExplanation::lied(
///     "Refuse the 3rd order".parse().expect("a recorded rule name"),
///     Priority::HIGHEST,
///     partition,
///     rolled,
///     LieFate::all_applied(refusal),
/// )?;
///
/// assert!(line.has(ExplanationStatus::Applied));
/// assert!(!line.has(ExplanationStatus::Shadowed));
/// assert_eq!(
///     line.to_string(),
///     "Refuse the 3rd order: matched orders.eu/3; match 3, count agreed: fired; \
///      NOT_LEADER_OR_FOLLOWER applied on orders.eu/3"
/// );
/// # Ok::<(), RuleExplanationError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleExplanation {
    rule: RecordedRuleName,
    priority: Priority,
    course: Course,
}

impl RuleExplanation {
    /// The selector of `rule` matched `matched`, and the rule did not fire: `trigger` says why.
    ///
    /// # Errors
    ///
    /// [`RuleExplanationError::FiredWithoutAction`] when `trigger` says that the rule fired.
    pub fn held(
        rule: RecordedRuleName,
        priority: Priority,
        matched: Target,
        trigger: TriggerExplanation,
    ) -> Result<RuleExplanation, RuleExplanationError> {
        if trigger.fired() {
            return Err(RuleExplanationError::FiredWithoutAction { rule });
        }
        Ok(RuleExplanation {
            rule,
            priority,
            course: Course::Held { matched, trigger },
        })
    }

    /// The selector of `rule` matched `matched`, the rule fired, and `lie` is its lie with what became of it.
    ///
    /// The target of the lie can differ from `matched`: a delay holds the whole call back, whatever the selector matched.
    ///
    /// # Errors
    ///
    /// [`RuleExplanationError::ActionWithoutFiring`] when `trigger` says that the rule did not fire.
    pub fn lied(
        rule: RecordedRuleName,
        priority: Priority,
        matched: Target,
        trigger: TriggerExplanation,
        lie: LieFate,
    ) -> Result<RuleExplanation, RuleExplanationError> {
        if !trigger.fired() {
            return Err(RuleExplanationError::ActionWithoutFiring {
                rule,
                decision: trigger.decision(),
            });
        }
        Ok(RuleExplanation {
            rule,
            priority,
            course: Course::Lied { matched, trigger, lie },
        })
    }

    /// The selector of `rule` matched `matched`, the rule fired, and `incidents` are on their way.
    ///
    /// No rule shadows an incident, so all of them take effect, after this call.
    ///
    /// # Errors
    ///
    /// - [`RuleExplanationError::ActionWithoutFiring`] when `trigger` says that the rule did not fire.
    /// - [`RuleExplanationError::NoIncident`] when `incidents` is empty.
    ///   A rule whose incident finds no subject in the call does not fire, and it is [`held`](Self::held).
    pub fn fired_incidents(
        rule: RecordedRuleName,
        priority: Priority,
        matched: Target,
        trigger: TriggerExplanation,
        incidents: Vec<Incident>,
    ) -> Result<RuleExplanation, RuleExplanationError> {
        if !trigger.fired() {
            return Err(RuleExplanationError::ActionWithoutFiring {
                rule,
                decision: trigger.decision(),
            });
        }
        if incidents.is_empty() {
            return Err(RuleExplanationError::NoIncident { rule });
        }
        Ok(RuleExplanation {
            rule,
            priority,
            course: Course::FiredIncidents {
                matched,
                trigger,
                incidents,
            },
        })
    }

    /// `rule` waited for the truth, and the lie of `ended_by` ended the call before the truth existed.
    ///
    /// Nobody evaluated the rule, so the call is no match of it, and its trigger did not roll.
    /// It is pre-empted, even when it has the higher priority of the two.
    pub const fn ended_before_truth(
        rule: RecordedRuleName,
        priority: Priority,
        ended_by: RecordedRuleName,
    ) -> RuleExplanation {
        RuleExplanation {
            rule,
            priority,
            course: Course::EndedBeforeTruth { ended_by },
        }
    }

    /// How far the rule got, for the `serde` form.
    #[cfg(feature = "serde")]
    pub(crate) const fn course(&self) -> &Course {
        &self.course
    }

    /// The name of the rule.
    pub const fn rule(&self) -> &RecordedRuleName {
        &self.rule
    }

    /// Where the rule stood among the armed rules when the call met it.
    pub const fn priority(&self) -> Priority {
        self.priority
    }

    /// The part of the call the selector matched, or `None` for a rule that nobody evaluated.
    pub const fn matched(&self) -> Option<&Target> {
        match &self.course {
            Course::EndedBeforeTruth { .. } => None,
            Course::Held { matched, .. } | Course::Lied { matched, .. } | Course::FiredIncidents { matched, .. } => {
                Some(matched)
            }
        }
    }

    /// What the trigger looked at and decided, or `None` for a rule that nobody evaluated.
    pub const fn trigger(&self) -> Option<&TriggerExplanation> {
        match &self.course {
            Course::EndedBeforeTruth { .. } => None,
            Course::Held { trigger, .. } | Course::Lied { trigger, .. } | Course::FiredIncidents { trigger, .. } => {
                Some(trigger)
            }
        }
    }

    /// Whether the rule fired.
    pub const fn fired(&self) -> bool {
        matches!(self.course, Course::Lied { .. } | Course::FiredIncidents { .. })
    }

    /// The lie the rule fired, with what became of it.
    pub const fn lie(&self) -> Option<&LieFate> {
        match &self.course {
            Course::Lied { lie, .. } => Some(lie),
            _ => None,
        }
    }

    /// The incidents the rule fired, and none for a rule that did not fire or that has a lie.
    pub fn incidents(&self) -> &[Incident] {
        match &self.course {
            Course::FiredIncidents { incidents, .. } => incidents,
            _ => &[],
        }
    }

    /// The rule whose lie ended the call before this rule saw the truth.
    pub const fn ended_by(&self) -> Option<&RecordedRuleName> {
        match &self.course {
            Course::EndedBeforeTruth { ended_by } => Some(ended_by),
            _ => None,
        }
    }

    /// Whether the action took effect, so the firing counts for a lifetime of firings.
    ///
    /// A lie applied on at least one target, or the rule fired incidents.
    pub fn applied(&self) -> bool {
        self.has(ExplanationStatus::Applied)
    }

    /// Whether the rule reached `status` on this call.
    pub fn has(&self, status: ExplanationStatus) -> bool {
        let lie = self.lie();
        match status {
            ExplanationStatus::Matched => self.matched().is_some(),
            ExplanationStatus::Rolled => self
                .trigger()
                .is_some_and(|trigger| trigger.decision() != TriggerDecision::LifetimeOver),
            ExplanationStatus::Fired => self.fired(),
            ExplanationStatus::Applied => {
                lie.is_some_and(LieFate::counts) || matches!(self.course, Course::FiredIncidents { .. })
            }
            ExplanationStatus::Shadowed => lie.is_some_and(|lie| !lie.shadowed().is_empty()),
            ExplanationStatus::PreEmpted => {
                lie.is_some_and(|lie| !lie.pre_empted().is_empty())
                    || matches!(self.course, Course::EndedBeforeTruth { .. })
            }
        }
    }

    /// Every status the rule reached, in the order of [`ExplanationStatus::ALL`].
    pub fn statuses(&self) -> impl Iterator<Item = ExplanationStatus> + '_ {
        ExplanationStatus::ALL.into_iter().filter(|status| self.has(*status))
    }

    /// The names this line gives to other rules: the winners of its losses, and the rule that ended the call.
    fn named(&self) -> impl Iterator<Item = &RecordedRuleName> {
        let losses = self
            .lie()
            .into_iter()
            .flat_map(|lie| lie.shadowed().iter().chain(lie.pre_empted()))
            .map(LostTarget::to);
        losses.chain(self.ended_by())
    }
}

/// Writes one line: the rule, what its selector matched, what its trigger decided, and what its action did.
impl fmt::Display for RuleExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.rule)?;
        let (matched, trigger) = match &self.course {
            Course::EndedBeforeTruth { ended_by } => {
                return write!(f, "pre-empted by {ended_by}, which ended the call before the truth");
            }
            Course::Held { matched, trigger }
            | Course::Lied { matched, trigger, .. }
            | Course::FiredIncidents { matched, trigger, .. } => (matched, trigger),
        };
        if matched.is_call() {
            write!(f, "matched the call; {trigger}")?;
        } else {
            write!(f, "matched {matched}; {trigger}")?;
        }
        match &self.course {
            Course::Lied { lie, .. } => write!(f, "; {lie}"),
            Course::FiredIncidents { incidents, .. } => {
                let mut separator = "; ";
                for incident in incidents {
                    write!(f, "{separator}{}", incident.kind())?;
                    separator = ", ";
                }
                Ok(())
            }
            Course::Held { .. } | Course::EndedBeforeTruth { .. } => Ok(()),
        }
    }
}

/// Why the parts of a line are not a [`RuleExplanation`]: the trigger and the action contradict each other.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RuleExplanationError {
    /// The trigger says that the rule fired, and the line has no lie and no incident.
    #[error("rule {:?} fired, and the explanation has no lie and no incident of it", .rule.as_str())]
    FiredWithoutAction { rule: RecordedRuleName },
    /// The line has a lie or incidents, and the trigger says that the rule did not fire.
    #[error(
        "rule {:?} did not fire (`{decision}`), and the explanation has a lie or an incident of it",
        .rule.as_str()
    )]
    ActionWithoutFiring {
        rule: RecordedRuleName,
        decision: TriggerDecision,
    },
    /// The rule fired incidents, and the list has none.
    #[error("rule {:?} fired incidents, and the explanation lists none", .rule.as_str())]
    NoIncident { rule: RecordedRuleName },
}

/// What went wrong inside GaslightMQ while it handled a call, in one line.
///
/// A stage of the pipeline can fail, for example because a frame cannot be decoded.
/// The call is then forwarded untouched, and its explanation keeps the failure.
/// A bug in GaslightMQ must never look like a lie.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let failure = Failure::new("the request frame ends after 12 of 48 bytes");
/// assert_eq!(failure.message(), "the request frame ends after 12 of 48 bytes");
/// ```
///
/// With the `serde` feature, a failure serializes as its message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Failure(String);

impl Failure {
    /// The failure that `message` describes. An error is its own message.
    pub fn new(message: impl fmt::Display) -> Failure {
        Failure(message.to_string())
    }

    /// What went wrong.
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a call came out the way it did: every rule the call touched, in priority order.
///
/// A rule is here when its selector matched the call,
/// or when it waited for the truth of a call that ended before the truth existed.
/// A rule is in one evaluation pass, so it has one line.
///
/// | Question | Answer |
/// |---|---|
/// | Why did this call fail? | [`verdict`](Self::verdict) lists the lies that applied, and each line names its rule. |
/// | Why did my rule not fire? | [`rule`](Self::rule) gives its line, with what its trigger looked at and decided. |
/// | Did my rule's action apply? | [`applied`](Self::applied). A step that ends on firings counts these. |
/// | What did this call do to the world? | [`incidents`](Self::incidents). They are no part of the verdict. |
/// | Did GaslightMQ itself fail? | [`failure`](Self::failure). |
///
/// ```
/// use std::num::NonZeroU64;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let first = NonZeroU64::MIN;
/// let fired = || {
///     TriggerExplanation::new(TriggerDecision::Fired, Some(first), TriggerChecks::new()).expect("a match that fired")
/// };
/// let rule = |name: &str| name.parse::<RecordedRuleName>().expect("a rule name");
/// let silence = LieDefinition::Silence.on(Target::Call);
/// let hang_up = LieDefinition::from(HangUpLie::new()).on(Target::Call);
/// let lost = LostTarget::new(Target::Call, rule("Blackhole"));
/// let shadowed = LieFate::new(hang_up, None, vec![lost], vec![]).expect("a lie that lost its whole target");
///
/// // Two rules fired a terminal lie, and the rule with the higher priority won.
/// let blackhole = RuleExplanation::lied(
///     rule("Blackhole"),
///     Priority::new(0),
///     Target::Call,
///     fired(),
///     LieFate::all_applied(silence.clone()),
/// );
/// let hang_up = RuleExplanation::lied(rule("Hang up"), Priority::new(1), Target::Call, fired(), shadowed);
/// let explanation = Explanation::new([
///     blackhole.expect("a rule that fired tells its lie"),
///     hang_up.expect("a rule that fired tells its lie"),
/// ])?;
///
/// assert_eq!(explanation.verdict(), Verdict::lied([silence]));
/// assert!(explanation.applied("Blackhole"));
/// assert!(!explanation.applied("Hang up"));
///
/// let shadowed: Vec<_> = explanation.with(ExplanationStatus::Shadowed).map(|line| line.rule().as_str()).collect();
/// assert_eq!(shadowed, ["Hang up"]);
/// # Ok::<(), ExplanationError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Explanation {
    rules: Vec<RuleExplanation>,
    failure: Option<Failure>,
}

impl Explanation {
    /// The lines of the rules a call touched, in priority order: the rule that wins a conflict first.
    ///
    /// # Errors
    ///
    /// - [`ExplanationError::RuleTwice`] when two lines are of one rule.
    /// - [`ExplanationError::UnlistedRule`] when a line names a rule that won or ended the call,
    ///   and that rule has no line of its own.
    pub fn new(rules: impl IntoIterator<Item = RuleExplanation>) -> Result<Explanation, ExplanationError> {
        let rules: Vec<RuleExplanation> = rules.into_iter().collect();
        let mut names = BTreeSet::new();
        for line in &rules {
            if !names.insert(line.rule()) {
                return Err(ExplanationError::RuleTwice {
                    rule: line.rule().clone(),
                });
            }
        }
        for line in &rules {
            if let Some(named) = line.named().find(|named| !names.contains(named)) {
                return Err(ExplanationError::UnlistedRule {
                    rule: named.clone(),
                    named_by: line.rule().clone(),
                });
            }
        }
        Ok(Explanation { rules, failure: None })
    }

    /// The explanation of a call that no rule touched.
    pub const fn none() -> Explanation {
        Explanation {
            rules: Vec::new(),
            failure: None,
        }
    }

    /// GaslightMQ itself failed while it handled the call.
    #[must_use]
    pub fn with_failure(mut self, failure: Failure) -> Explanation {
        self.failure = Some(failure);
        self
    }

    /// Every rule the call touched, in priority order.
    pub fn rules(&self) -> &[RuleExplanation] {
        &self.rules
    }

    /// The same lines, one by one.
    pub fn iter(&self) -> std::slice::Iter<'_, RuleExplanation> {
        self.rules.iter()
    }

    /// How many rules the call touched.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether no rule touched the call.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The line of the rule with that name, when the call touched it.
    pub fn rule(&self, name: &str) -> Option<&RuleExplanation> {
        self.rules.iter().find(|line| line.rule() == name)
    }

    /// The rules that reached `status`, in priority order.
    pub fn with(&self, status: ExplanationStatus) -> impl Iterator<Item = &RuleExplanation> {
        self.rules.iter().filter(move |line| line.has(status))
    }

    /// Whether the action of the rule with that name took effect on this call.
    ///
    /// This is what counts as a firing for a lifetime, for a step of a sequence, and for a test.
    pub fn applied(&self, rule: &str) -> bool {
        self.rule(rule).is_some_and(RuleExplanation::applied)
    }

    /// The incidents the call fired, in the priority order of their rules.
    pub fn incidents(&self) -> impl Iterator<Item = &Incident> {
        self.rules.iter().flat_map(RuleExplanation::incidents)
    }

    /// What went wrong inside GaslightMQ while it handled the call, when something did.
    pub const fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// The verdict that follows: the lies that applied, in priority order, or a pass.
    pub fn verdict(&self) -> Verdict {
        Verdict::lied(
            self.rules
                .iter()
                .filter_map(RuleExplanation::lie)
                .filter_map(LieFate::verdict_line),
        )
    }
}

impl<'a> IntoIterator for &'a Explanation {
    type Item = &'a RuleExplanation;
    type IntoIter = std::slice::Iter<'a, RuleExplanation>;

    fn into_iter(self) -> Self::IntoIter {
        self.rules.iter()
    }
}

/// Why a list of lines is not an [`Explanation`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ExplanationError {
    /// Two lines are of one rule, and a rule is in one evaluation pass.
    #[error("rule {:?} has two lines in one explanation", .rule.as_str())]
    RuleTwice { rule: RecordedRuleName },
    /// The line of `named_by` says that `rule` won a target or ended the call, and `rule` has no line of its own.
    #[error("rule {:?} lost to rule {:?}, which has no line in the explanation", .named_by.as_str(), .rule.as_str())]
    UnlistedRule {
        rule: RecordedRuleName,
        named_by: RecordedRuleName,
    },
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::{LieDefinition, RefusalLie};

    use super::*;
    use crate::TriggerChecks;

    fn name(text: &str) -> RecordedRuleName {
        text.parse().unwrap()
    }

    fn parts(texts: &[&str]) -> Target {
        Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
    }

    fn trigger(decision: TriggerDecision) -> TriggerExplanation {
        TriggerExplanation::new(decision, Some(NonZeroU64::MIN), TriggerChecks::new()).unwrap()
    }

    fn refusal(target: Target) -> TargetedLie {
        LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(target)
    }

    fn leader_moves() -> Incident {
        Incident::LeaderMoves {
            partition: "orders.eu/3".parse().unwrap(),
            to: None,
        }
    }

    fn lied(rule: &str, priority: usize, lie: LieFate) -> RuleExplanation {
        let matched = lie.lie().target().clone();
        RuleExplanation::lied(
            name(rule),
            Priority::new(priority),
            matched,
            trigger(TriggerDecision::Fired),
            lie,
        )
        .unwrap()
    }

    fn statuses(line: &RuleExplanation) -> String {
        line.statuses()
            .map(ExplanationStatus::name)
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn a_lie_that_fired_went_somewhere() {
        let lie = refusal(parts(&["orders.eu/3"]));

        assert_eq!(LieFate::new(lie, None, vec![], vec![]), Err(LieFateError::Nowhere));
    }

    #[test]
    fn a_lie_that_could_not_be_told_applied_nowhere_and_keeps_what_it_lost() {
        let three = parts(&["orders.eu/3"]);
        let both = parts(&["orders.eu/3", "orders.eu/4"]);
        let lost = LostTarget::new(three, name("Leader flap"));
        let failure = Failure::new("no behaviour is registered for refusal");
        let won_a_part =
            LieFate::new(refusal(both), Some(parts(&["orders.eu/4"])), vec![lost.clone()], vec![]).unwrap();

        let untold = won_a_part.untold(failure.clone());

        assert_eq!(untold.applied(), None);
        assert_eq!(untold.shadowed(), [lost]);
        assert_eq!(untold.failure(), Some(&failure));
        assert!(!untold.counts());
        assert_eq!(untold.verdict_line(), None);
        assert_eq!(
            untold.to_string(),
            "NOT_LEADER_OR_FOLLOWER shadowed on orders.eu/3 by Leader flap, \
             not told: no behaviour is registered for refusal"
        );

        // The rule fired, and its action did not take effect.
        let line = lied("Refuse orders", 1, untold);
        assert_eq!(statuses(&line), "matched rolled fired shadowed");
        assert!(!line.applied());
        // A lie that was told has no failure.
        assert_eq!(LieFate::all_applied(refusal(Target::Call)).failure(), None);
    }

    #[test]
    fn a_lie_applies_within_the_target_it_claimed() {
        let three = parts(&["orders.eu/3"]);
        let both = parts(&["orders.eu/3", "orders.eu/4"]);

        assert!(LieFate::new(refusal(both.clone()), Some(three.clone()), vec![], vec![]).is_ok());
        assert!(LieFate::new(refusal(Target::Call), Some(three.clone()), vec![], vec![]).is_ok());
        for outside in [both, Target::Call] {
            let error = LieFate::new(refusal(three.clone()), Some(outside.clone()), vec![], vec![]).unwrap_err();
            assert_eq!(
                error,
                LieFateError::AppliedOutsideTarget {
                    applied: outside,
                    claimed: three.clone()
                }
            );
        }
        assert_eq!(
            LieFate::new(refusal(three), Some(Target::Call), vec![], vec![])
                .unwrap_err()
                .to_string(),
            "a lie on orders.eu/3 does not apply on call"
        );
    }

    #[test]
    fn a_fate_writes_where_the_lie_applied_and_where_it_lost() {
        let three = parts(&["orders.eu/3"]);
        let four = parts(&["orders.eu/4"]);
        let both = parts(&["orders.eu/3", "orders.eu/4"]);
        let lost = |target: &Target, to: &str| LostTarget::new(target.clone(), name(to));
        let cases = [
            (
                LieFate::all_applied(refusal(Target::Call)),
                "NOT_LEADER_OR_FOLLOWER applied",
            ),
            (
                LieFate::new(
                    refusal(both.clone()),
                    Some(four.clone()),
                    vec![lost(&three, "Leader flap")],
                    vec![],
                )
                .unwrap(),
                "NOT_LEADER_OR_FOLLOWER applied on orders.eu/4, shadowed on orders.eu/3 by Leader flap",
            ),
            (
                LieFate::new(
                    refusal(both),
                    None,
                    vec![lost(&three, "Leader flap")],
                    vec![lost(&four, "Blackhole")],
                )
                .unwrap(),
                "NOT_LEADER_OR_FOLLOWER shadowed on orders.eu/3 by Leader flap, pre-empted on orders.eu/4 by Blackhole",
            ),
            (
                LieFate::new(
                    refusal(Target::Call),
                    None,
                    vec![],
                    vec![lost(&Target::Call, "Blackhole")],
                )
                .unwrap(),
                "NOT_LEADER_OR_FOLLOWER pre-empted by Blackhole",
            ),
        ];

        for (fate, text) in cases {
            assert_eq!(fate.to_string(), text);
        }
    }

    #[test]
    fn a_firing_counts_when_the_lie_applied_somewhere() {
        let three = parts(&["orders.eu/3"]);
        let lost = vec![LostTarget::new(three.clone(), name("Leader flap"))];
        let applied = LieFate::all_applied(refusal(three.clone()));
        let shadowed = LieFate::new(refusal(three.clone()), None, lost, vec![]).unwrap();

        assert!(applied.counts());
        assert_eq!(applied.verdict_line(), Some(refusal(three)));
        assert!(!shadowed.counts());
        assert_eq!(shadowed.verdict_line(), None);
    }

    #[test]
    fn every_end_of_a_rule_has_its_statuses() {
        let three = parts(&["orders.eu/3"]);
        let lost = || vec![LostTarget::new(three.clone(), name("Leader flap"))];
        let held = |decision| {
            let trigger = if decision == TriggerDecision::LifetimeOver {
                TriggerExplanation::lifetime_over()
            } else {
                trigger(decision)
            };
            RuleExplanation::held(name("Rule"), Priority::HIGHEST, three.clone(), trigger).unwrap()
        };
        let incidents = RuleExplanation::fired_incidents(
            name("Rule"),
            Priority::HIGHEST,
            three.clone(),
            trigger(TriggerDecision::FiredByHand),
            vec![leader_moves()],
        )
        .unwrap();
        let both = parts(&["orders.eu/3", "orders.eu/4"]);
        let partly = LieFate::new(refusal(both), Some(parts(&["orders.eu/4"])), lost(), vec![]).unwrap();
        let cases = [
            (held(TriggerDecision::Held), "matched rolled"),
            (held(TriggerDecision::BudgetEmpty), "matched rolled"),
            (held(TriggerDecision::NothingToActOn), "matched rolled"),
            (held(TriggerDecision::LifetimeOver), "matched"),
            (
                lied("Rule", 0, LieFate::all_applied(refusal(three.clone()))),
                "matched rolled fired applied",
            ),
            (lied("Rule", 0, partly), "matched rolled fired applied shadowed"),
            (
                lied(
                    "Rule",
                    0,
                    LieFate::new(refusal(three.clone()), None, vec![], lost()).unwrap(),
                ),
                "matched rolled fired pre-empted",
            ),
            (incidents, "matched rolled fired applied"),
            (
                RuleExplanation::ended_before_truth(name("Rule"), Priority::HIGHEST, name("Blackhole")),
                "pre-empted",
            ),
        ];

        for (line, expected) in cases {
            assert_eq!(statuses(&line), expected, "{line}");
            assert_eq!(line.applied(), expected.contains("applied"));
            assert_eq!(line.fired(), expected.contains("fired"));
        }
    }

    #[test]
    fn a_line_shows_each_part_it_has() {
        let three = parts(&["orders.eu/3"]);
        let ended = RuleExplanation::ended_before_truth(name("Lost ack"), Priority::new(2), name("Blackhole"));
        let fired = RuleExplanation::fired_incidents(
            name("Move"),
            Priority::new(1),
            three.clone(),
            trigger(TriggerDecision::Fired),
            vec![leader_moves()],
        )
        .unwrap();

        assert_eq!(ended.rule(), "Lost ack");
        assert_eq!(ended.priority(), Priority::new(2));
        assert_eq!(ended.matched(), None);
        assert_eq!(ended.trigger(), None);
        assert_eq!(ended.ended_by(), Some(&name("Blackhole")));
        assert_eq!(ended.incidents(), []);

        assert_eq!(fired.matched(), Some(&three));
        assert_eq!(
            fired.trigger().map(TriggerExplanation::decision),
            Some(TriggerDecision::Fired)
        );
        assert_eq!(fired.incidents(), [leader_moves()]);
        assert_eq!(fired.lie(), None);
        assert_eq!(fired.ended_by(), None);
    }

    #[test]
    fn a_line_writes_what_happened() {
        let three = parts(&["orders.eu/3"]);
        let held = RuleExplanation::held(
            name("Slow"),
            Priority::HIGHEST,
            Target::Call,
            trigger(TriggerDecision::Held),
        )
        .unwrap();
        let fired = RuleExplanation::fired_incidents(
            name("Move"),
            Priority::HIGHEST,
            three,
            trigger(TriggerDecision::Fired),
            vec![leader_moves(), leader_moves()],
        )
        .unwrap();
        let ended = RuleExplanation::ended_before_truth(name("Lost ack"), Priority::HIGHEST, name("Blackhole"));

        assert_eq!(held.to_string(), "Slow: matched the call; match 1: held");
        assert_eq!(
            fired.to_string(),
            "Move: matched orders.eu/3; match 1: fired; leader-moves, leader-moves"
        );
        assert_eq!(
            ended.to_string(),
            "Lost ack: pre-empted by Blackhole, which ended the call before the truth"
        );
    }

    #[test]
    fn a_trigger_and_an_action_that_contradict_are_an_error() {
        let three = parts(&["orders.eu/3"]);
        let line = |trigger| RuleExplanation::held(name("Rule"), Priority::HIGHEST, three.clone(), trigger);
        let fate = LieFate::all_applied(refusal(three.clone()));

        assert_eq!(
            line(trigger(TriggerDecision::Fired)),
            Err(RuleExplanationError::FiredWithoutAction { rule: name("Rule") })
        );
        assert_eq!(
            RuleExplanation::lied(
                name("Rule"),
                Priority::HIGHEST,
                three.clone(),
                trigger(TriggerDecision::BudgetEmpty),
                fate
            ),
            Err(RuleExplanationError::ActionWithoutFiring {
                rule: name("Rule"),
                decision: TriggerDecision::BudgetEmpty
            })
        );
        assert_eq!(
            RuleExplanation::fired_incidents(
                name("Rule"),
                Priority::HIGHEST,
                three.clone(),
                trigger(TriggerDecision::Held),
                vec![leader_moves()]
            ),
            Err(RuleExplanationError::ActionWithoutFiring {
                rule: name("Rule"),
                decision: TriggerDecision::Held
            })
        );
        assert_eq!(
            RuleExplanation::fired_incidents(
                name("Rule"),
                Priority::HIGHEST,
                three,
                trigger(TriggerDecision::Fired),
                vec![]
            ),
            Err(RuleExplanationError::NoIncident { rule: name("Rule") })
        );
    }

    #[test]
    fn a_rule_explanation_error_says_what_is_wrong() {
        let cases = [
            (
                RuleExplanationError::FiredWithoutAction { rule: name("Rule") },
                "rule \"Rule\" fired, and the explanation has no lie and no incident of it",
            ),
            (
                RuleExplanationError::ActionWithoutFiring {
                    rule: name("Rule"),
                    decision: TriggerDecision::Held,
                },
                "rule \"Rule\" did not fire (`held`), and the explanation has a lie or an incident of it",
            ),
            (
                RuleExplanationError::NoIncident { rule: name("Rule") },
                "rule \"Rule\" fired incidents, and the explanation lists none",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn an_explanation_has_one_line_per_rule() {
        let three = parts(&["orders.eu/3"]);
        let line = || lied("Leader flap", 0, LieFate::all_applied(refusal(three.clone())));
        let error = Explanation::new([line(), line()]).unwrap_err();

        assert_eq!(
            error,
            ExplanationError::RuleTwice {
                rule: name("Leader flap")
            }
        );
        assert_eq!(
            error.to_string(),
            "rule \"Leader flap\" has two lines in one explanation"
        );
    }

    #[test]
    fn an_explanation_lists_every_rule_it_names() {
        let three = parts(&["orders.eu/3"]);
        let lost = vec![LostTarget::new(three.clone(), name("Leader flap"))];
        let shadowed = lied(
            "Refuse orders",
            1,
            LieFate::new(refusal(three.clone()), None, lost, vec![]).unwrap(),
        );
        let ended = RuleExplanation::ended_before_truth(name("Lost ack"), Priority::new(2), name("Blackhole"));
        let winner = lied("Leader flap", 0, LieFate::all_applied(refusal(three)));

        let error = Explanation::new([shadowed.clone()]).unwrap_err();
        assert_eq!(
            error,
            ExplanationError::UnlistedRule {
                rule: name("Leader flap"),
                named_by: name("Refuse orders")
            }
        );
        assert_eq!(
            error.to_string(),
            "rule \"Refuse orders\" lost to rule \"Leader flap\", which has no line in the explanation"
        );
        assert!(matches!(
            Explanation::new([ended]),
            Err(ExplanationError::UnlistedRule { .. })
        ));
        assert!(Explanation::new([winner, shadowed]).is_ok());
    }

    #[test]
    fn an_explanation_answers_by_rule_and_by_status() {
        let three = parts(&["orders.eu/3"]);
        let lost = vec![LostTarget::new(three.clone(), name("Leader flap"))];
        let held = RuleExplanation::held(
            name("Slow"),
            Priority::new(3),
            Target::Call,
            trigger(TriggerDecision::Held),
        )
        .unwrap();
        let moved = RuleExplanation::fired_incidents(
            name("Move"),
            Priority::new(2),
            three.clone(),
            trigger(TriggerDecision::Fired),
            vec![leader_moves()],
        )
        .unwrap();
        let explanation = Explanation::new([
            lied("Leader flap", 0, LieFate::all_applied(refusal(three.clone()))),
            lied(
                "Refuse orders",
                1,
                LieFate::new(refusal(three.clone()), None, lost, vec![]).unwrap(),
            ),
            moved,
            held,
        ])
        .unwrap();

        assert_eq!(explanation.len(), 4);
        assert!(!explanation.is_empty());
        assert_eq!(explanation.iter().count(), 4);
        assert_eq!((&explanation).into_iter().count(), 4);
        assert_eq!(explanation.rule("Slow").map(RuleExplanation::fired), Some(false));
        assert_eq!(explanation.rule("slow"), None);
        assert!(explanation.applied("Leader flap"));
        assert!(explanation.applied("Move"));
        assert!(!explanation.applied("Refuse orders"));
        assert!(!explanation.applied("Nobody"));
        assert_eq!(explanation.with(ExplanationStatus::Fired).count(), 3);
        assert_eq!(explanation.incidents().collect::<Vec<_>>(), [&leader_moves()]);
        assert_eq!(explanation.verdict(), Verdict::lied([refusal(three)]));
        assert_eq!(explanation.failure(), None);
    }

    #[test]
    fn a_call_that_no_rule_touched_passes() {
        let explanation = Explanation::none();

        assert!(explanation.is_empty());
        assert_eq!(explanation, Explanation::default());
        assert_eq!(explanation.verdict(), Verdict::pass());
    }

    #[test]
    fn an_explanation_keeps_a_failure() {
        let failure = Failure::new("the request frame ends after 12 of 48 bytes");
        let explanation = Explanation::none().with_failure(failure.clone());

        assert_eq!(explanation.failure(), Some(&failure));
        assert_eq!(failure.to_string(), "the request frame ends after 12 of 48 bytes");
        assert_eq!(explanation.verdict(), Verdict::pass());
    }
}
