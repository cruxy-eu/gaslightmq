use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::{Incident, IncidentError, Subjects};
use gaslightmq_lie::{Priority, TargetedLie};
use gaslightmq_selector::Pass;
use gaslightmq_trigger::{Confirmed, Firing, Rolled, SessionCounters};

use crate::{Action, ArmedRule, RuleName};

/// The action of a rule that fired: what the rule does to its call, or to the world.
///
/// An [`Action`] is a definition, and the call that fires the rule makes it specific:
///
/// - A lie gets the [target](Target) the rule's selector matched on this call.
/// - An incident that leaves its subject out gets it from this call.
///   A call that targets three partitions moves three leaders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FiredAction {
    /// The lie, on the part of the call it changes.
    /// Whether it applies is for the other lies on the call to say: one can shadow it or pre-empt it.
    Lie(TargetedLie),
    /// The incidents that happen, and at least one.
    /// No rule shadows an incident, so all of them take effect, after this call.
    Incidents(Vec<Incident>),
}

impl FiredAction {
    /// The lie, when the rule fired one.
    pub const fn lie(&self) -> Option<&TargetedLie> {
        match self {
            FiredAction::Lie(lie) => Some(lie),
            FiredAction::Incidents(_) => None,
        }
    }

    /// The incidents the rule fired, and none for a rule with a lie.
    pub fn incidents(&self) -> &[Incident] {
        match self {
            FiredAction::Lie(_) => &[],
            FiredAction::Incidents(incidents) => incidents,
        }
    }

    /// What `action` does when its rule fires on `call`, where the rule's selector matched `target`.
    fn of<S: FieldSource + ?Sized>(action: &Action, call: &S, target: &Target) -> Result<FiredAction, IncidentError> {
        match action {
            // Every target is the whole call or at least one part, so every target takes the lie.
            Action::Lie(lie) => Ok(FiredAction::Lie(lie.clone().on(target.clone()))),
            Action::Incident(incident) => {
                let incidents = incident.resolve(&Self::subjects(call, target))?;
                Ok(FiredAction::Incidents(incidents))
            }
        }
    }

    /// What the call offers to an incident that names no subject.
    ///
    /// | Subject | Comes from |
    /// |---|---|
    /// | Partitions and topics | The parts the rule's selector matched. |
    /// | Broker | The `endpoint` field, when the call arrived on the endpoint of a broker. |
    /// | Groups | The `group` field, whether the request carries it or the client context provides it. |
    /// | Transactional IDs | The `transactional.id` field, likewise. |
    fn subjects<S: FieldSource + ?Sized>(call: &S, target: &Target) -> Subjects {
        let mut subjects = Subjects::new();
        for part in target.parts() {
            subjects = match part {
                Part::Partition(partition) => subjects.with_partition(partition.clone()),
                Part::Topic(topic) => subjects.with_topic(topic.clone()),
            };
        }
        for field in [Field::Endpoint, Field::Group, Field::TransactionalId] {
            let Some(values) = call.read(field).present() else {
                continue;
            };
            for value in &values {
                subjects = match value {
                    FieldValue::Endpoint(Endpoint::Broker(broker)) => subjects.with_broker(*broker),
                    FieldValue::Group(group) => subjects.with_group(group.clone()),
                    FieldValue::TransactionalId(id) => subjects.with_transactional_id(id.clone()),
                    _ => subjects,
                };
            }
        }
        subjects
    }
}

/// What evaluating one armed rule against one call gave: a line of the call's explanation.
///
/// The selector of the rule matched the call, or the rule would not be here.
///
/// | Part | Explanation status | Is |
/// |---|---|---|
/// | [`target`](Self::target) | Matched | The part of the call the selector matched. |
/// | [`rolled`](Self::rolled) | Rolled | The match number, the die roll, the time window, the budget, and how it came out. |
/// | [`fired_action`](Self::fired_action) | Fired | What the rule does: its lie on its target, or its incidents. |
/// | [`take_firing`](Self::take_firing) | | The firing that the call settles. |
///
/// # Fired means one thing
///
/// The record and the evaluation agree on whether the rule fired:
/// [`fired`](Self::fired) is true exactly when the outcome of [`rolled`](Self::rolled) is.
///
/// A trigger can agree on a call the rule can do nothing with:
/// an incident that takes its subject from the call fires on a call that offers none.
/// The rule does not fire then, and nothing says it did.
/// The outcome is [`TriggerOutcome::NothingToActOn`](gaslightmq_trigger::TriggerOutcome::NothingToActOn), the firing went back,
/// and [`missing_subject`](Self::missing_subject) names what the call lacks.
/// The record still shows the match number and what the trigger looked at, because the call counted as a match.
///
/// # Settling
///
/// A rule fires before anyone knows whether its action applies.
/// Whoever resolves the lies of the call settles each firing afterwards:
/// [`confirm`](Self::confirm) when the action applied on at least one target,
/// and [`withdraw`](Self::withdraw) when the rule was shadowed or pre-empted on every target.
/// Nothing shadows an incident, so a rule with incidents confirms once they are on their way.
///
/// A firing that someone made [by hand](ArmedRule::fire_by_hand) is only used up when it is confirmed.
/// Withdrawn, it waits for the next call the selector matches, so every hand firing lies on a matching call.
///
/// The evaluation keeps what it says after its firing is settled, so it can go into the explanation.
/// An evaluation that is dropped with its firing withdraws it, which is right for a call that ended early.
#[derive(Debug)]
pub struct RuleEvaluation {
    rule: ArmedRule,
    priority: Priority,
    target: Target,
    rolled: Rolled,
    fired_action: Option<FiredAction>,
    firing: Option<Firing>,
    missing_subject: Option<IncidentError>,
}

impl RuleEvaluation {
    /// Evaluates the trigger of `rule` for a call that its selector matched on `target`.
    pub(crate) fn of<S: FieldSource + ?Sized>(
        rule: &ArmedRule,
        priority: Priority,
        target: Target,
        call: &S,
        now: Instant,
        session: &mut SessionCounters,
    ) -> Self {
        let evaluated = rule.arming().evaluate(now, session);
        let (rolled, fired_action, firing, missing_subject) = if evaluated.fired() {
            match FiredAction::of(rule.rule().action(), call, &target) {
                Ok(action) => {
                    let (rolled, firing) = evaluated.into_parts();
                    (rolled, Some(action), firing, None)
                }
                // The incident has no subject in this call, so the rule does not fire after all.
                // The firing goes back, and the record says so and keeps the reason.
                Err(error) => (evaluated.nothing_to_act_on(), None, None, Some(error)),
            }
        } else {
            (*evaluated.rolled(), None, None, None)
        };
        Self {
            rule: rule.clone(),
            priority,
            target,
            rolled,
            fired_action,
            firing,
            missing_subject,
        }
    }

    /// The rule, with the arming the call evaluated.
    pub fn rule(&self) -> &ArmedRule {
        &self.rule
    }

    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        self.rule.name()
    }

    /// Where the rule stands among the armed rules, in both passes alike.
    pub fn priority(&self) -> Priority {
        self.priority
    }

    /// The part of the call the rule's selector matched.
    ///
    /// The target of the rule's lie can differ: a delay holds the whole call back, whatever the selector matched.
    /// [`lie`](Self::lie) has the target the lie changes.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// What the trigger looked at, and how it came out.
    ///
    /// Its outcome says that the rule fired exactly when [`fired`](Self::fired) does.
    pub fn rolled(&self) -> &Rolled {
        &self.rolled
    }

    /// Whether the rule fired, so it has an [action on this call](Self::fired_action).
    ///
    /// The outcome of [`rolled`](Self::rolled) says the same.
    pub fn fired(&self) -> bool {
        self.fired_action.is_some()
    }

    /// What the rule does, when it fired.
    pub fn fired_action(&self) -> Option<&FiredAction> {
        self.fired_action.as_ref()
    }

    /// The lie the rule fired, on the part of the call it changes.
    pub fn lie(&self) -> Option<&TargetedLie> {
        self.fired_action.as_ref().and_then(FiredAction::lie)
    }

    /// The incidents the rule fired, and none for a rule that did not fire or that has a lie.
    pub fn incidents(&self) -> &[Incident] {
        self.fired_action.as_ref().map_or(&[], FiredAction::incidents)
    }

    /// Why the trigger agreed and the rule did not fire: the incident takes a subject from the call,
    /// and this call offers none.
    ///
    /// A `leader-moves` without a partition has nothing to move on a `Heartbeat`.
    /// The call still counted as a match, and the firing went back to the budget.
    /// The outcome of [`rolled`](Self::rolled) is [`TriggerOutcome::NothingToActOn`](gaslightmq_trigger::TriggerOutcome::NothingToActOn) then.
    pub fn missing_subject(&self) -> Option<&IncidentError> {
        self.missing_subject.as_ref()
    }

    /// Whether the rule fired and its firing is not settled yet.
    pub fn is_unsettled(&self) -> bool {
        self.firing.is_some()
    }

    /// The firing of the rule, to settle it.
    ///
    /// The result is `None` when the rule did not fire, and when the firing was taken before.
    pub fn take_firing(&mut self) -> Option<Firing> {
        self.firing.take()
    }

    /// Counts the firing for the lifetime: the action applied.
    ///
    /// The result says whether this firing was the last of a lifetime of firings.
    /// It is `None` when there is no firing left to settle.
    pub fn confirm(&mut self) -> Option<Confirmed> {
        self.take_firing().map(Firing::confirm)
    }

    /// Gives the firing back: the rule was shadowed or pre-empted on every target, and its action did not apply.
    ///
    /// The budget has the firing free again, and a hand firing waits for the next matching call.
    pub fn withdraw(&mut self) {
        if let Some(firing) = self.take_firing() {
            firing.withdraw();
        }
    }
}

/// What one evaluation pass gave for one call: every armed rule whose selector matched, in priority order.
///
/// The [request pass](Pass::Request) holds the rules that are decided before forwarding.
/// The [response pass](Pass::Response) holds the rules that waited for the truth.
/// A rule is in one of the two, so a call counts as one match of it.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let orders = Conditions::from(Condition::parse(Field::Topic, "orders.*").expect("a condition"));
/// let name = "leader-flap".parse().expect("a rule name");
/// let flap = Rule::new(name, orders, RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// let armed = flap.resolve(&SelectorSet::default(), &PresetSet::default())?.arm(Seed::new(42), Instant::ZERO);
/// let snapshot = Snapshot::new([armed]).expect("a snapshot");
///
/// let produce = Fields::new()
///     .with(ApiKey::PRODUCE)
///     .with_part("orders.eu/3".parse::<TopicPartition>().expect("a topic partition"))
///     .with_part("payments.settled/0".parse::<TopicPartition>().expect("a topic partition"));
/// let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").expect("a counter key"));
///
/// let mut pass = snapshot.evaluate(Pass::Request, &produce, Instant::ZERO, &mut session);
///
/// // The lies go to whoever combines them, in priority order.
/// let lies: Vec<String> = pass.lies().map(|(_, lie)| lie.to_string()).collect();
/// assert_eq!(lies, ["NOT_LEADER_OR_FOLLOWER on orders.eu/3"]);
///
/// // Nothing shadowed the refusal, so its firing counts.
/// for rule in pass.rules_mut() {
///     rule.confirm();
/// }
/// # Ok::<(), RuleError>(())
/// ```
#[derive(Debug)]
pub struct PassEvaluation {
    pass: Pass,
    rules: Vec<RuleEvaluation>,
}

impl PassEvaluation {
    pub(crate) fn new(pass: Pass, rules: Vec<RuleEvaluation>) -> Self {
        Self { pass, rules }
    }

    /// The pass that was evaluated.
    pub fn pass(&self) -> Pass {
        self.pass
    }

    /// Every rule whose selector matched the call, in priority order: the rule that wins a conflict first.
    pub fn rules(&self) -> &[RuleEvaluation] {
        &self.rules
    }

    /// The same rules, to settle their firings.
    pub fn rules_mut(&mut self) -> &mut [RuleEvaluation] {
        &mut self.rules
    }

    /// The rules, to keep them for the explanation of the call.
    pub fn into_rules(self) -> Vec<RuleEvaluation> {
        self.rules
    }

    /// The rules that matched, in priority order.
    pub fn iter(&self) -> std::slice::Iter<'_, RuleEvaluation> {
        self.rules.iter()
    }

    /// The rules that matched, in priority order, to settle their firings.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, RuleEvaluation> {
        self.rules.iter_mut()
    }

    /// How many rules matched the call.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether no rule matched the call.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The rules that fired, in priority order.
    pub fn fired(&self) -> impl Iterator<Item = &RuleEvaluation> {
        self.rules.iter().filter(|rule| rule.fired())
    }

    /// The lies of the rules that fired, in priority order, each with the priority of its rule.
    ///
    /// This is what goes to whoever combines the lies of a call.
    /// The priority also orders the lies of this pass against those of the other one.
    pub fn lies(&self) -> impl Iterator<Item = (Priority, &TargetedLie)> {
        self.rules
            .iter()
            .filter_map(|rule| Some((rule.priority(), rule.lie()?)))
    }

    /// The incidents of the rules that fired, in priority order.
    pub fn incidents(&self) -> impl Iterator<Item = &Incident> {
        self.rules.iter().flat_map(RuleEvaluation::incidents)
    }
}

impl IntoIterator for PassEvaluation {
    type Item = RuleEvaluation;
    type IntoIter = std::vec::IntoIter<RuleEvaluation>;

    fn into_iter(self) -> Self::IntoIter {
        self.rules.into_iter()
    }
}

impl<'a> IntoIterator for &'a PassEvaluation {
    type Item = &'a RuleEvaluation;
    type IntoIter = std::slice::Iter<'a, RuleEvaluation>;

    fn into_iter(self) -> Self::IntoIter {
        self.rules.iter()
    }
}

impl<'a> IntoIterator for &'a mut PassEvaluation {
    type Item = &'a mut RuleEvaluation;
    type IntoIter = std::slice::IterMut<'a, RuleEvaluation>;

    fn into_iter(self) -> Self::IntoIter {
        self.rules.iter_mut()
    }
}
