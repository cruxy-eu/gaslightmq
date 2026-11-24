//! The armed rules of one proxy, published to every session as a snapshot.

use std::fmt;
use std::sync::Arc;

use gaslightmq_clock::{Clock, Instant};
use gaslightmq_rule::{ArmedRule, ResolvedRule, RuleName, Snapshot, SnapshotError};
use gaslightmq_trigger::Seed;
use tokio::sync::watch;

use crate::{ArmError, DisarmError, WaitableClock};

/// The armed rules of one proxy, in the order in which they win a conflict.
///
/// Arming and disarming a rule publishes a new [`Snapshot`] to every session.
/// A session hands it to its pipeline, so the next call of every connection is evaluated against it.
/// A call that is in flight finishes on the snapshot it started with.
///
/// The type is a handle, and a clone leads to the same rules.
/// No method waits.
///
/// # Order
///
/// No scenario runs yet, so the rules are a live set: the rule that was armed last wins a conflict.
///
/// # What the handle keeps, and what a rule keeps
///
/// The counter, the budget and the die of a rule live in its [arming](ArmedRule::arming), and not in a snapshot.
/// A new snapshot therefore continues every count of the rules that stay armed.
/// Arming a rule makes a new arming, with the [seed](crate::EngineConfiguration::seed) of the engine
/// and the instant of its clock.
///
/// # Lifetimes
///
/// The handle disarms a rule whose lifetime ended, and publishes a snapshot without it:
/// when the clock reaches the end of a lifetime that is a duration,
/// and after the call that spent the last match or the last firing of a budget.
/// A rule whose lifetime ended neither counts nor fires in the moment before that.
#[derive(Clone)]
pub struct ArmedRules {
    inner: Arc<Inner>,
}

struct Inner {
    seed: Seed,
    clock: Arc<dyn Clock + Send + Sync>,
    /// The snapshot that every session received last. Its rules are the armed rules, in priority order.
    published: watch::Sender<Arc<Snapshot>>,
}

impl ArmedRules {
    /// No armed rules yet. The handle arms with `seed` at the instants of `clock`.
    pub(crate) fn new(seed: Seed, clock: Arc<dyn Clock + Send + Sync>) -> Self {
        let (published, _) = watch::channel(Arc::new(Snapshot::default()));
        Self {
            inner: Arc::new(Inner { seed, clock, published }),
        }
    }

    /// Arms `rule`, above every rule that is armed, and publishes the snapshot with it.
    ///
    /// The arming starts now: its counters are at zero, its budget is whole and its time window opens from here.
    /// The handle it returns is the one in the snapshot, so it shows what the sessions count and spend.
    ///
    /// # Errors
    ///
    /// [`ArmError::AlreadyArmed`] when a rule of that name is armed.
    /// Arming it again would reset it, and whoever wants that [disarms](Self::disarm) it first.
    pub fn arm(&self, rule: ResolvedRule) -> Result<ArmedRule, ArmError> {
        let now = self.inner.clock.now();
        let name = rule.name().clone();
        let mut armed = None;
        self.inner.published.send_if_modified(|published| {
            // A rule whose lifetime ended is as good as disarmed, so it does not stand in the way of a new arming.
            let staying = published.iter().filter(|other| !other.arming().has_ended(now));
            let arming = rule.arm(self.inner.seed, now);
            let rules = std::iter::once(arming.clone()).chain(staying.cloned());
            match Snapshot::new(rules) {
                Ok(snapshot) => {
                    *published = Arc::new(snapshot);
                    armed = Some(arming);
                    true
                }
                // The rules that stay have one name each, so the name that comes twice is the one of this rule:
                // a rule of that name is armed.
                Err(SnapshotError::DuplicateName { .. }) => false,
            }
        });
        armed.ok_or(ArmError::AlreadyArmed { rule: name })
    }

    /// Disarms the rule `name`, publishes the snapshot without it, and returns its arming as it ended.
    ///
    /// A call in flight that the rule fired on still finishes with it.
    ///
    /// # Errors
    ///
    /// [`DisarmError::NotArmed`] when no rule of that name is armed: nobody armed it, someone disarmed it,
    /// or its lifetime ended. A rule whose lifetime ended is not armed from that moment on,
    /// also in the moment before the handle publishes a snapshot without it.
    pub fn disarm(&self, name: &RuleName) -> Result<ArmedRule, DisarmError> {
        let now = self.inner.clock.now();
        let has_ended = |rule: &ArmedRule| rule.arming().has_ended(now);
        let mut disarmed = None;
        self.inner.published.send_if_modified(|published| {
            disarmed = published.get(name).filter(|rule| !has_ended(rule)).cloned();
            // The rule leaves, and so does every rule whose lifetime ended, which is as good as disarmed.
            Self::keep(published, |rule| rule.name() != name && !has_ended(rule))
        });
        disarmed.ok_or_else(|| DisarmError::NotArmed { rule: name.clone() })
    }

    /// The armed rules at this moment, as every session received them last.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.inner.published.borrow())
    }

    /// Whether the two handles lead to the same rules.
    pub fn is(&self, other: &ArmedRules) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Every snapshot from now on, for one session. The one that is published now counts as seen.
    pub(crate) fn published(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.inner.published.subscribe()
    }

    /// Disarms every rule whose lifetime has ended at `now`, and publishes the snapshot without them.
    ///
    /// A session calls it after every call it recorded, because that call may have spent the last of a budget.
    /// It takes no lock while no lifetime ended.
    pub(crate) fn disarm_ended(&self, now: Instant) {
        let has_ended = |rule: &ArmedRule| rule.arming().has_ended(now);
        if !self.inner.published.borrow().iter().any(has_ended) {
            return;
        }
        self.inner
            .published
            .send_if_modified(|published| Self::keep(published, |rule| !has_ended(rule)));
    }

    /// Disarms every rule whose lifetime is a duration, when `clock` reaches its end. It never returns.
    pub(crate) async fn end_lifetimes(self, clock: &impl WaitableClock) {
        let mut published = self.published();
        loop {
            let ends = |rule: &ArmedRule| rule.arming().ends_at();
            let next = published.borrow_and_update().iter().filter_map(ends).min();
            let reached = async {
                match next {
                    Some(at) => clock.until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                // The armed rules changed, so another lifetime may end first.
                _ = published.changed() => {}
                () = reached => self.disarm_ended(clock.now()),
            }
        }
    }

    /// Swaps in a snapshot of the rules that `stays` keeps, in their order. Returns whether a rule left.
    fn keep(published: &mut Arc<Snapshot>, stays: impl Fn(&ArmedRule) -> bool) -> bool {
        if published.iter().all(&stays) {
            return false;
        }
        *published = Arc::new(published.keeping(stays));
        true
    }
}

/// Writes the names of the armed rules, in priority order.
impl fmt::Debug for ArmedRules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self.snapshot();
        let armed: Vec<&str> = snapshot.iter().map(|rule| rule.name().as_str()).collect();
        f.debug_struct("ArmedRules")
            .field("seed", &self.inner.seed)
            .field("armed", &armed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::{Duration, TimeSpan};
    use gaslightmq_lie::LieDefinition;
    use gaslightmq_rule::{PresetSet, Rule};
    use gaslightmq_selector::{Conditions, SelectorSet};
    use gaslightmq_trigger::{Count, CounterKey, Lifetime, SessionCounters};

    use super::*;
    use crate::SteppedClock;

    fn armed_rules(clock: &SteppedClock) -> ArmedRules {
        ArmedRules::new(Seed::new(42), Arc::new(clock.clone()))
    }

    /// A silence on every call, under `name`.
    fn rule(name: &str) -> Rule {
        Rule::new(name.parse().unwrap(), Conditions::new(), LieDefinition::Silence)
    }

    fn resolved(rule: &Rule) -> ResolvedRule {
        rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap()
    }

    fn armed(rules: &ArmedRules) -> Vec<String> {
        rules.snapshot().iter().map(|rule| rule.name().to_string()).collect()
    }

    fn name(text: &str) -> RuleName {
        text.parse().unwrap()
    }

    #[test]
    fn the_rule_that_was_armed_last_wins() {
        let rules = armed_rules(&SteppedClock::new());

        rules.arm(resolved(&rule("blackhole"))).unwrap();
        rules.arm(resolved(&rule("leader-flap"))).unwrap();

        assert_eq!(armed(&rules), ["leader-flap", "blackhole"]);
    }

    #[test]
    fn a_rule_is_armed_with_the_seed_of_the_engine_at_the_instant_of_its_clock() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let now = clock.advance(Duration::from_secs(7));

        let arming = rules.arm(resolved(&rule("blackhole"))).unwrap();

        assert_eq!(arming.arming().armed_at(), now);
        let with_the_seed = resolved(&rule("blackhole")).arm(Seed::new(42), now);
        assert_eq!(arming.arming().die(), with_the_seed.arming().die());
        // The handle is the one that the sessions evaluate.
        assert!(rules.snapshot().get(&name("blackhole")).unwrap().is(&arming));
    }

    #[test]
    fn every_change_reaches_a_session_as_a_new_snapshot() {
        let rules = armed_rules(&SteppedClock::new());
        let mut session = rules.published();
        assert!(!session.has_changed().unwrap());

        rules.arm(resolved(&rule("blackhole"))).unwrap();
        assert!(session.has_changed().unwrap());
        assert_eq!(session.borrow_and_update().len(), 1);

        rules.disarm(&name("blackhole")).unwrap();
        assert!(session.has_changed().unwrap());
        assert!(session.borrow_and_update().is_empty());
    }

    #[test]
    fn arming_an_armed_rule_is_an_error_and_changes_nothing() {
        let rules = armed_rules(&SteppedClock::new());
        let first = rules.arm(resolved(&rule("blackhole"))).unwrap();
        let session = rules.published();

        let again = rules.arm(resolved(&rule("blackhole")));

        assert_eq!(
            again.unwrap_err(),
            ArmError::AlreadyArmed {
                rule: name("blackhole")
            }
        );
        assert!(!session.has_changed().unwrap());
        assert!(rules.snapshot().get(&name("blackhole")).unwrap().is(&first));
    }

    #[test]
    fn disarming_a_rule_that_is_not_armed_is_an_error() {
        let rules = armed_rules(&SteppedClock::new());
        rules.arm(resolved(&rule("blackhole"))).unwrap();

        let unknown = rules.disarm(&name("leader-flap"));
        let first = rules.disarm(&name("blackhole"));
        let second = rules.disarm(&name("blackhole"));

        assert_eq!(
            unknown.unwrap_err(),
            DisarmError::NotArmed {
                rule: name("leader-flap")
            }
        );
        assert_eq!(first.unwrap().name(), "blackhole");
        assert_eq!(
            second.unwrap_err(),
            DisarmError::NotArmed {
                rule: name("blackhole")
            }
        );
    }

    #[test]
    fn arming_a_rule_again_starts_a_new_arming() {
        let rules = armed_rules(&SteppedClock::new());
        let first = rules.arm(resolved(&rule("blackhole"))).unwrap();
        rules.disarm(&name("blackhole")).unwrap();

        let second = rules.arm(resolved(&rule("blackhole"))).unwrap();

        assert!(!second.is(&first));
    }

    #[test]
    fn a_rule_that_spent_its_budget_is_disarmed_after_the_call_that_spent_it() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let once = rule("blackhole").with_lifetime(Lifetime::Firings(Count::ONE));
        let arming = rules.arm(resolved(&once)).unwrap();
        rules.arm(resolved(&rule("leader-flap"))).unwrap();

        // Nothing ended yet, so nothing changes.
        rules.disarm_ended(clock.now());
        assert_eq!(armed(&rules), ["leader-flap", "blackhole"]);

        // A session confirms the one firing, and says so after the call.
        let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").unwrap());
        let firing = arming
            .arming()
            .evaluate(clock.now(), &mut session)
            .into_firing()
            .unwrap();
        assert!(firing.confirm().ends_lifetime());
        rules.disarm_ended(clock.now());

        assert_eq!(armed(&rules), ["leader-flap"]);
        // Its name is free for a new arming.
        rules.arm(resolved(&once)).unwrap();
    }

    #[test]
    fn a_rule_whose_lifetime_ended_does_not_stand_in_the_way_of_a_new_arming() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let short = rule("blackhole").with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
        let first = rules.arm(resolved(&short)).unwrap();

        // The lifetime ended, and nobody disarmed the rule yet.
        clock.advance(Duration::from_secs(30));
        let second = rules.arm(resolved(&short)).unwrap();

        assert_eq!(armed(&rules), ["blackhole"]);
        assert!(!second.is(&first));
    }

    #[test]
    fn a_rule_whose_lifetime_ended_is_not_armed_also_before_it_is_swept() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let short = rule("blackhole").with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
        rules.arm(resolved(&short)).unwrap();
        rules.arm(resolved(&rule("leader-flap"))).unwrap();
        let mut session = rules.published();
        session.borrow_and_update();

        // The lifetime ended, and nothing published a snapshot without the rule yet.
        clock.advance(Duration::from_secs(30));
        assert_eq!(armed(&rules), ["leader-flap", "blackhole"]);
        let disarmed = rules.disarm(&name("blackhole"));

        assert_eq!(
            disarmed.unwrap_err(),
            DisarmError::NotArmed {
                rule: name("blackhole")
            }
        );
        // Asking was enough to see it: the sessions get a snapshot without the rule.
        assert!(session.has_changed().unwrap());
        assert_eq!(armed(&rules), ["leader-flap"]);
    }

    #[tokio::test]
    async fn a_rule_is_disarmed_when_the_clock_reaches_the_end_of_its_lifetime() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let mut session = rules.published();
        let ending = tokio::spawn({
            let (rules, clock) = (rules.clone(), clock.clone());
            async move { rules.end_lifetimes(&clock).await }
        });
        let short = rule("blackhole").with_lifetime(Lifetime::Duration(TimeSpan::from_secs(30)));
        let shorter = rule("leader-flap").with_lifetime(Lifetime::Duration(TimeSpan::from_secs(10)));
        rules.arm(resolved(&short)).unwrap();
        // The second rule is armed while the task waits for the first one, and it ends first.
        tokio::task::yield_now().await;
        rules.arm(resolved(&shorter)).unwrap();
        session.borrow_and_update();

        clock.advance(Duration::from_secs(10));
        session.changed().await.unwrap();
        assert_eq!(armed(&rules), ["blackhole"]);

        clock.advance(Duration::from_secs(20));
        session.changed().await.unwrap();
        assert_eq!(armed(&rules), Vec::<String>::new());
        ending.abort();
    }
}
