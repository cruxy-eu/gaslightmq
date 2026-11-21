use std::sync::Arc;

use gaslightmq_clock::Instant;
use gaslightmq_incident::{Incident, Subjects};
use gaslightmq_trigger::{Arming, Firing};

use crate::{Action, ResolvedRule, RuleError, RuleName};

/// An armed rule: what the rule says, and the live state of this arming.
///
/// | Part | Is |
/// |---|---|
/// | The [`ResolvedRule`] | The selector, the action, the trigger and the lifetime, without names to look up. |
/// | The [`Arming`] | The counter, the budget, the die, the hand firings and the moment of arming. |
///
/// An armed rule is a handle, and cloning it gives a second handle to the same arming.
/// The engine arms a rule, puts the handle into a [`Snapshot`](crate::Snapshot),
/// and every session evaluates its matching calls against it.
///
/// [`ResolvedRule::arm`] makes one.
/// Arming the rule again makes a new one, with new counters.
/// Changing a parameter of an armed rule resets nothing, and [`retuned`](Self::retuned) is that change.
///
/// ```
/// use gaslightmq_clock::Instant;
/// use gaslightmq_incident::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// // A pad that moves the leader of one partition when someone presses it.
/// let election = IncidentDefinition::LeaderMoves { partition: Some("orders.eu/3".parse()?), to: None };
/// let rule = Rule::new("Leader election".parse()?, Conditions::new(), election).with_trigger(Trigger::Manual);
/// let armed = rule.resolve(&SelectorSet::default(), &PresetSet::default())?.arm(Seed::new(42), Instant::ZERO);
///
/// // The press fires the incident at once. Nothing shadows an incident, so its firing counts.
/// let HandFiring::Incidents { incidents, firing } = armed.fire_by_hand(Instant::ZERO)? else {
///     panic!("the rule is armed");
/// };
/// assert_eq!(incidents, [Incident::LeaderMoves { partition: "orders.eu/3".parse()?, to: None }]);
/// firing.confirm();
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct ArmedRule(Arc<Armed>);

#[derive(Debug)]
struct Armed {
    rule: ResolvedRule,
    arming: Arming,
}

impl ArmedRule {
    pub(crate) fn new(rule: ResolvedRule, arming: Arming) -> Self {
        Self(Arc::new(Armed { rule, arming }))
    }

    /// What the rule says.
    pub fn rule(&self) -> &ResolvedRule {
        &self.0.rule
    }

    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        self.0.rule.name()
    }

    /// The live state of this arming: its counter, its budget, and when its lifetime ends.
    pub fn arming(&self) -> &Arming {
        &self.0.arming
    }

    /// Whether the two handles are one arming of one rule.
    ///
    /// A [retuned](Self::retuned) rule is still the same arming.
    /// A rule that was disarmed and armed again is another one.
    pub fn is(&self, other: &ArmedRule) -> bool {
        self.0.arming.is(&other.0.arming)
    }

    /// The same arming with a changed rule, for a parameter change on an armed rule.
    ///
    /// Nothing resets: the counters, the hand firings, the die and the moment of arming stay.
    /// A budget keeps what calls spent, and takes the new number.
    /// See [`Arming::retuned`].
    ///
    /// The old handle keeps the old rule, for the calls that are in flight on an older snapshot.
    #[must_use]
    pub fn retuned(&self, rule: ResolvedRule) -> ArmedRule {
        let arming = self.0.arming.retuned(rule.trigger(), rule.lifetime());
        ArmedRule::new(rule, arming)
    }

    /// Fires the rule by hand, which skips its trigger once.
    ///
    /// - A rule with an **incident** fires the incident at once.
    ///   The result holds the incidents and their [`Firing`], which counts once the incidents are on their way.
    /// - A rule with a **lie** has no call to lie about yet.
    ///   It lies on the next call its selector matches, once, and that call shows it in its evaluation.
    ///
    /// Every hand firing is one firing: two of them lie on the next two matching calls.
    ///
    /// # Errors
    ///
    /// [`RuleError::NoCallForIncident`] when the incident leaves its subject to the call.
    /// A hand firing has no call, so nothing can say which partition or group is meant.
    /// The rule takes no firing from its budget then.
    pub fn fire_by_hand(&self, now: Instant) -> Result<HandFiring, RuleError> {
        let arming = self.arming();
        match self.rule().action() {
            Action::Incident(definition) => {
                let incidents = definition
                    .resolve(&Subjects::new())
                    .map_err(|error| RuleError::NoCallForIncident {
                        rule: self.name().clone(),
                        error,
                    })?;
                Ok(match arming.fire_now(now) {
                    Some(firing) => HandFiring::Incidents { incidents, firing },
                    None => HandFiring::NotFired,
                })
            }
            Action::Lie(_) if arming.has_ended(now) => Ok(HandFiring::NotFired),
            Action::Lie(_) => {
                arming.fire_on_next_match();
                Ok(HandFiring::OnNextMatch)
            }
        }
    }
}

/// What firing a rule by hand did.
#[derive(Debug)]
#[must_use = "a hand firing of an incident holds a firing, which counts only when it is confirmed"]
pub enum HandFiring {
    /// The rule has an incident, and these incidents fire at once.
    ///
    /// Nothing shadows an incident, so whoever passes the incidents on [confirms](Firing::confirm) the firing.
    /// Dropping the firing gives it back to the budget.
    Incidents {
        /// The incidents that happen, and at least one.
        incidents: Vec<Incident>,
        /// The firing, which counts for a lifetime of firings like any other.
        firing: Firing,
    },
    /// The rule has a lie, which waits for the next call its selector matches.
    OnNextMatch,
    /// The rule did not fire: its lifetime has ended, or its budget has no firing free.
    NotFired,
}

impl HandFiring {
    /// Whether the rule fired, now or on the next matching call.
    pub const fn fired(&self) -> bool {
        !matches!(self, HandFiring::NotFired)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    use super::*;
    use crate::{PresetSet, Rule};

    const NOW: Instant = Instant::ZERO;

    fn count(value: u64) -> Count {
        Count::new(value).unwrap()
    }

    fn arm(rule: &Rule) -> ArmedRule {
        resolve(rule).arm(Seed::new(42), NOW)
    }

    fn resolve(rule: &Rule) -> ResolvedRule {
        rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap()
    }

    fn election(partition: Option<&str>) -> Rule {
        let incident = IncidentDefinition::LeaderMoves {
            partition: partition.map(|text| text.parse().unwrap()),
            to: None,
        };
        Rule::new("Leader election".parse().unwrap(), Conditions::new(), incident)
    }

    fn flap() -> Rule {
        Rule::new(
            "Leader flap".parse().unwrap(),
            Conditions::from(Condition::is(ApiKey::PRODUCE)),
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        )
    }

    fn session() -> SessionCounters {
        SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").unwrap())
    }

    #[test]
    fn a_clone_is_a_handle_to_the_same_arming() {
        let armed = arm(&flap());
        let handle = armed.clone();

        assert!(armed.is(&handle));
        assert_eq!(handle.name(), "Leader flap");
        assert_eq!(handle.rule(), &resolve(&flap()));

        let _ = handle.arming().evaluate(NOW, &mut session());
        assert_eq!(armed.arming().counter().matches(), 1);
    }

    #[test]
    fn arming_again_is_another_arming() {
        let first = arm(&flap());
        let second = arm(&flap());

        assert!(!first.is(&second));
    }

    #[test]
    fn retuning_changes_the_rule_and_resets_nothing() {
        let armed = arm(&flap().with_lifetime(Lifetime::Firings(count(3))));
        let firing = armed.arming().evaluate(NOW, &mut session()).into_firing().unwrap();
        firing.confirm();

        let changed = flap()
            .with_action(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT))
            .with_lifetime(Lifetime::Firings(count(5)));
        let retuned = armed.retuned(resolve(&changed));

        assert!(retuned.is(&armed));
        assert_eq!(retuned.rule(), &resolve(&changed));
        assert_eq!(retuned.arming().counter().matches(), 1);
        assert_eq!(retuned.arming().die(), armed.arming().die());
        assert_eq!(retuned.arming().budget().map(Budget::left), Some(4));
        // The handle that calls in flight hold still says what it said.
        assert_eq!(
            armed.rule(),
            &resolve(&flap().with_lifetime(Lifetime::Firings(count(3))))
        );
    }

    #[test]
    fn a_rule_with_an_incident_fires_by_hand_at_once() {
        let armed = arm(&election(Some("orders.eu/3")).with_trigger(Trigger::Manual));

        let HandFiring::Incidents { incidents, firing } = armed.fire_by_hand(NOW).unwrap() else {
            panic!("the rule fires");
        };

        assert_eq!(
            incidents,
            [Incident::LeaderMoves {
                partition: "orders.eu/3".parse().unwrap(),
                to: None
            }]
        );
        assert!(firing.by_hand());
        // A hand firing of an incident counts no match.
        assert_eq!(armed.arming().counter().matches(), 0);
    }

    #[test]
    fn a_hand_firing_counts_for_a_lifetime_of_firings() {
        let armed = arm(&election(Some("orders.eu/3")).with_lifetime(Lifetime::Firings(count(1))));

        let HandFiring::Incidents { firing, .. } = armed.fire_by_hand(NOW).unwrap() else {
            panic!("the rule fires");
        };
        // While the firing is set aside, the budget has none free.
        assert!(!armed.fire_by_hand(NOW).unwrap().fired());

        assert!(firing.confirm().ends_lifetime());
        assert!(armed.arming().has_ended(NOW));
        assert!(matches!(armed.fire_by_hand(NOW), Ok(HandFiring::NotFired)));
    }

    #[test]
    fn a_hand_firing_that_is_dropped_goes_back_to_the_budget() {
        let armed = arm(&election(Some("orders.eu/3")).with_lifetime(Lifetime::Firings(count(1))));

        drop(armed.fire_by_hand(NOW).unwrap());

        assert!(armed.fire_by_hand(NOW).unwrap().fired());
    }

    #[test]
    fn an_incident_that_takes_its_subject_from_the_call_cannot_fire_by_hand() {
        let armed = arm(&election(None).with_lifetime(Lifetime::Firings(count(1))));

        let error = armed.fire_by_hand(NOW).unwrap_err();

        assert_eq!(
            error,
            RuleError::NoCallForIncident {
                rule: "Leader election".parse().unwrap(),
                error: IncidentError::MissingSubject {
                    incident: IncidentKind::LeaderMoves,
                    subject: "partition",
                },
            }
        );
        assert_eq!(armed.arming().budget().map(Budget::left), Some(1));
    }

    #[test]
    fn a_rule_with_a_lie_fires_by_hand_on_the_next_matching_calls() {
        let armed = arm(&flap().with_trigger(Trigger::Manual));

        assert!(matches!(armed.fire_by_hand(NOW), Ok(HandFiring::OnNextMatch)));
        assert!(armed.fire_by_hand(NOW).unwrap().fired());
        assert_eq!(armed.arming().waiting_by_hand(), 2);

        let mut session = session();
        for _ in 0..2 {
            let rolled = *armed.arming().evaluate(NOW, &mut session).rolled();
            assert_eq!(rolled.outcome(), TriggerOutcome::FiredByHand);
        }
        let rolled = *armed.arming().evaluate(NOW, &mut session).rolled();
        assert_eq!(rolled.outcome(), TriggerOutcome::Held);
    }

    #[test]
    fn a_rule_whose_lifetime_ended_does_not_fire_by_hand() {
        let armed = arm(&flap().with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30))));
        let later = NOW + TimeSpan::from_secs(30);

        assert!(matches!(armed.fire_by_hand(later), Ok(HandFiring::NotFired)));
        assert_eq!(armed.arming().waiting_by_hand(), 0);
    }
}
