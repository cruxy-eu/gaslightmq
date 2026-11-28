//! The armed rules of one proxy, published to every session as a snapshot: the live set, and the scenario that runs.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_call::Call;
use gaslightmq_clock::{Clock, Instant};
use gaslightmq_clock_waitable::WaitableClock;
use gaslightmq_rule::{ArmedRule, ResolvedRule, RuleName, Snapshot};
use gaslightmq_scenario::{ResolvedScenario, ScenarioPosition, ScenarioRun, StepBudget};
use gaslightmq_session::CallIds;
use gaslightmq_trigger::Seed;
use tokio::sync::watch;

use crate::{ActivateScenarioError, ArmError, DeactivateScenarioError, DisarmError, RetuneError, StepScenarioError};

/// The armed rules of one proxy, in the order in which they win a conflict: the live set, and the scenario that is
/// active.
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
/// The rules are two sets, and the live set comes first:
///
/// - **The live set** holds the rules that someone [arms](Self::arm) by hand. The rule that was armed last wins
///   a conflict, and every live rule wins a conflict with a rule of the scenario.
/// - **The scenario** that is [active](Self::activate_scenario) holds its rules, as its mode orders them: see
///   [`ScenarioRun::armed_in_order`]. Its rules stay armed for the whole run, and the rules of a step from its start
///   to its end.
///
/// A rule is armed once: a rule that someone armed by hand is not armed again by the scenario, and stays in the
/// live set. Disarming a rule by hand takes it away from whichever set holds it, so whoever disarms a rule of the
/// scenario has it disarmed until the scenario arms it again.
///
/// # Scenarios
///
/// At most one scenario is active at a time. Activating it arms its rules, and the rules of its first step, with the
/// seed of the scenario. A step ends when its condition holds, on the call that makes it hold or at the instant that
/// it names, or when someone [advances](Self::step_scenario) the scenario: the rules of the step disarm, and the rules of
/// the next one arm. [Deactivating](Self::deactivate_scenario) it disarms every rule that it armed.
///
/// A step that ends by itself is a change that nobody asked for. The handle keeps it until whoever counts the
/// changes of the environment [takes it](Self::take_steps), with the snapshot after it.
///
/// # What the handle keeps, and what a rule keeps
///
/// The counter, the budget and the die of a rule live in its [arming](ArmedRule::arming), and not in a snapshot.
/// A new snapshot therefore continues every count of the rules that stay armed.
/// Arming a rule by hand makes a new arming, with the [seed](crate::EngineConfiguration::seed) of the engine
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
    /// The calls of the environment, which say where a step begins among them.
    call_ids: CallIds,
    /// The rules as the handle holds them. Whoever changes them publishes the snapshot before letting go.
    sets: Mutex<Sets>,
    /// The snapshot that every session received last. Its rules are the armed rules, in priority order.
    published: watch::Sender<Arc<Snapshot>>,
}

/// The two sets of armed rules, and the steps that nobody took yet.
#[derive(Default)]
struct Sets {
    /// The rules that someone armed by hand, the one armed last first.
    live: Vec<ArmedRule>,
    /// The scenario that is active, with the rules it armed.
    scenario: Option<Active>,
    /// The steps that advanced by themselves since whoever counts the changes took them last.
    stepped: Vec<ScenarioChange>,
}

/// The scenario that is active, and the armings of the rules that it keeps armed.
struct Active {
    run: ScenarioRun,
    /// The armings of the scenario: of its rules for the whole run, and of the rules of the step that runs.
    armed: BTreeMap<RuleName, ArmedRule>,
}

/// What a change of the scenario of an environment did: where the scenario stands after it, and the rules that it
/// disarmed and armed, in that order.
///
/// A deactivation says where the scenario stood when it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioChange {
    /// The scenario, and the step that runs after the change.
    pub position: ScenarioPosition,
    /// The rules that the change disarmed.
    pub disarmed: Vec<RuleName>,
    /// The rules that the change armed.
    pub armed: Vec<RuleName>,
}

/// The steps that advanced by themselves since the last time someone took them, and the armed rules after them.
///
/// [`ArmedRules::take_steps`] makes one, and both parts come from one moment: a rule that the snapshot holds or
/// misses for another reason than these steps disarmed or armed for another reason, such as a lifetime that ended.
#[derive(Debug, Clone)]
pub struct StepsTaken {
    /// The steps, in the order in which they advanced.
    pub steps: Vec<ScenarioChange>,
    /// The armed rules after them.
    pub snapshot: Arc<Snapshot>,
}

impl Sets {
    /// The armed rules in the order in which they win a conflict: the live set, then the scenario.
    fn snapshot(&self) -> Snapshot {
        let live = self.live.iter().cloned();
        let of_the_scenario = self.scenario.iter().flat_map(|active| {
            let names = active.run.armed_in_order().into_iter().map(ResolvedRule::name);
            names.filter_map(|name| active.armed.get(name).cloned())
        });
        // A rule is in one set alone, so no name comes twice.
        Snapshot::new(live.chain(of_the_scenario)).unwrap_or_default()
    }

    /// The armed rule of that name, in whichever set holds it, whose lifetime has not ended at `now`.
    fn get(&self, name: &RuleName, now: Instant) -> Option<&ArmedRule> {
        let live = self.live.iter().find(|rule| rule.name() == name);
        let of_the_scenario = || self.scenario.as_ref()?.armed.get(name);
        live.or_else(of_the_scenario)
            .filter(|rule| !rule.arming().has_ended(now))
    }

    /// Takes away every rule whose lifetime has ended at `now`. Returns whether one went.
    fn sweep(&mut self, now: Instant) -> bool {
        let ended = |rule: &ArmedRule| rule.arming().has_ended(now);
        let before = self.live.len();
        self.live.retain(|rule| !ended(rule));
        let mut swept = self.live.len() != before;
        if let Some(active) = self.scenario.as_mut() {
            let before = active.armed.len();
            active.armed.retain(|_, rule| !ended(rule));
            swept |= active.armed.len() != before;
        }
        swept
    }

    /// Replaces the arming of the rule of that name, in whichever set holds it, with what `change` makes of it.
    fn replace(&mut self, name: &RuleName, change: impl FnOnce(&ArmedRule) -> ArmedRule) -> Option<ArmedRule> {
        let held = match self.live.iter_mut().find(|rule| rule.name() == name) {
            Some(rule) => Some(rule),
            None => self.scenario.as_mut().and_then(|active| active.armed.get_mut(name)),
        }?;
        *held = change(held);
        Some(held.clone())
    }

    /// Arms the rules that the scenario keeps armed now and that no set holds, with the seed of the scenario, and
    /// hands the budget of the step to the rule it ends on. Returns the names of the rules it armed.
    fn arm_the_scenario(&mut self, now: Instant) -> Vec<RuleName> {
        let Some(active) = self.scenario.as_mut() else {
            return Vec::new();
        };
        let seed = active.run.scenario().seed();
        let mut armed = Vec::new();
        for rule in active.run.armed_in_order() {
            let by_hand = self.live.iter().any(|live| live.name() == rule.name());
            if by_hand || active.armed.contains_key(rule.name()) {
                continue;
            }
            active.armed.insert(rule.name().clone(), rule.clone().arm(seed, now));
            armed.push(rule.name().clone());
        }
        if let Some(StepBudget { rule, budget }) = active.run.step_budget() {
            self.replace(&rule, |armed| armed.with_step_budget(Some(budget)));
        }
        armed
    }

    /// Ends the step that runs, disarms its rules, and starts the next one at `now`.
    fn advance(&mut self, now: Instant, calls_before: u64) -> Result<ScenarioChange, StepScenarioError> {
        let active = self.scenario.as_mut().ok_or(StepScenarioError::NotActive)?;
        let Some(step) = active.run.step() else {
            let scenario = active.run.scenario().name().clone();
            return Err(StepScenarioError::NoStep { scenario });
        };
        let of_the_step: Vec<RuleName> = step.arm().iter().map(|rule| rule.name().clone()).collect();
        let ending_on = active.run.step_budget().map(|step| step.rule);
        let mut disarmed = Vec::new();
        for rule in &of_the_step {
            if active
                .armed
                .remove(rule)
                .is_some_and(|rule| !rule.arming().has_ended(now))
            {
                disarmed.push(rule.clone());
            }
        }
        active.run.advance(now, calls_before);
        // A rule that stays armed takes from no budget of a step that ended.
        if let Some(rule) = ending_on {
            self.replace(&rule, |armed| armed.with_step_budget(None));
        }
        let armed = self.arm_the_scenario(now);
        let position = self.position().ok_or(StepScenarioError::NotActive)?;
        Ok(ScenarioChange {
            position,
            disarmed,
            armed,
        })
    }

    /// Advances the scenario for as long as the condition of its step holds at `now`, and keeps every step it took.
    /// Returns whether it took one.
    fn advance_while_over(&mut self, now: Instant, calls_before: u64) -> bool {
        let mut advanced = false;
        while self
            .scenario
            .as_ref()
            .is_some_and(|active| active.run.step_is_over(now))
        {
            match self.advance(now, calls_before) {
                Ok(change) => self.stepped.push(change),
                Err(_) => break,
            }
            advanced = true;
        }
        advanced
    }

    fn position(&self) -> Option<ScenarioPosition> {
        Some(self.scenario.as_ref()?.run.position())
    }
}

impl ArmedRules {
    /// No armed rules yet. The handle arms with `seed` at the instants of `clock`, and knows the calls of the
    /// environment by `call_ids`.
    pub(crate) fn new(seed: Seed, clock: Arc<dyn Clock + Send + Sync>, call_ids: CallIds) -> Self {
        let (published, _) = watch::channel(Arc::new(Snapshot::default()));
        Self {
            inner: Arc::new(Inner {
                seed,
                clock,
                call_ids,
                sets: Mutex::new(Sets::default()),
                published,
            }),
        }
    }

    /// Arms `rule` by hand, above every rule that is armed, and publishes the snapshot with it.
    ///
    /// The arming starts now: its counters are at zero, its budget is whole and its time window opens from here.
    /// The handle it returns is the one in the snapshot, so it shows what the sessions count and spend.
    ///
    /// # Errors
    ///
    /// [`ArmError::AlreadyArmed`] when a rule of that name is armed, by hand or by the scenario.
    /// Arming it again would reset it, and whoever wants that [disarms](Self::disarm) it first.
    pub fn arm(&self, rule: ResolvedRule) -> Result<ArmedRule, ArmError> {
        let now = self.inner.clock.now();
        let mut sets = self.sets();
        // A rule whose lifetime ended is as good as disarmed, so it does not stand in the way of a new arming.
        sets.sweep(now);
        if sets.get(rule.name(), now).is_some() {
            return Err(ArmError::AlreadyArmed {
                rule: rule.name().clone(),
            });
        }
        let arming = rule.arm(self.inner.seed, now);
        sets.live.insert(0, arming.clone());
        self.publish(&sets);
        Ok(arming)
    }

    /// Disarms the rule `name`, publishes the snapshot without it, and returns its arming as it ended.
    ///
    /// A rule of the live set leaves the live set, and a rule of the scenario leaves the scenario until the scenario
    /// arms it again. A call in flight that the rule fired on still finishes with it.
    ///
    /// # Errors
    ///
    /// [`DisarmError::NotArmed`] when no rule of that name is armed: nobody armed it, someone disarmed it,
    /// or its lifetime ended. A rule whose lifetime ended is not armed from that moment on,
    /// also in the moment before the handle publishes a snapshot without it.
    pub fn disarm(&self, name: &RuleName) -> Result<ArmedRule, DisarmError> {
        let now = self.inner.clock.now();
        let mut sets = self.sets();
        let disarmed = sets.get(name, now).cloned();
        // The rule leaves, and so does every rule whose lifetime ended, which is as good as disarmed.
        let swept = sets.sweep(now);
        if disarmed.is_some() {
            sets.live.retain(|rule| rule.name() != name);
            if let Some(active) = sets.scenario.as_mut() {
                active.armed.remove(name);
            }
        }
        if swept || disarmed.is_some() {
            self.publish(&sets);
        }
        disarmed.ok_or_else(|| DisarmError::NotArmed { rule: name.clone() })
    }

    /// Replaces the armed rule of the same name with `rule`, and keeps its arming: its counters, its budget,
    /// its hand firings and its moment of arming stay. It keeps its place in the order too.
    ///
    /// The sessions get the snapshot with it, and a call in flight finishes on the rule it started with.
    /// See [`ArmedRule::retuned`].
    ///
    /// # Errors
    ///
    /// [`RetuneError::NotArmed`] when no rule of that name is armed, or its lifetime ended.
    pub fn retune(&self, rule: ResolvedRule) -> Result<ArmedRule, RetuneError> {
        let now = self.inner.clock.now();
        let name = rule.name().clone();
        let mut sets = self.sets();
        if sets.get(&name, now).is_none() {
            return Err(RetuneError::NotArmed { rule: name });
        }
        let retuned = sets.replace(&name, |armed| armed.retuned(rule));
        self.publish(&sets);
        retuned.ok_or(RetuneError::NotArmed { rule: name })
    }

    /// Activates `scenario`: arms its rules and the rules of its first step, with its seed, and publishes the
    /// snapshot with them.
    ///
    /// A rule that someone armed by hand stays in the live set, and the scenario does not arm it again.
    /// Returns where the scenario stands, and the rules it armed.
    ///
    /// # Errors
    ///
    /// [`ActivateScenarioError::AlreadyActive`] when a scenario is active: at most one is.
    pub fn activate_scenario(&self, scenario: ResolvedScenario) -> Result<ScenarioChange, ActivateScenarioError> {
        let now = self.inner.clock.now();
        let mut sets = self.sets();
        if let Some(active) = &sets.scenario {
            let scenario = active.run.scenario().name().clone();
            return Err(ActivateScenarioError::AlreadyActive { scenario });
        }
        sets.sweep(now);
        let run = ScenarioRun::start(scenario, now, self.inner.call_ids.assigned());
        let position = run.position();
        sets.scenario = Some(Active {
            run,
            armed: BTreeMap::new(),
        });
        let armed = sets.arm_the_scenario(now);
        // A step that is over from its start, such as one of no time at all, ends at once.
        sets.advance_while_over(now, self.inner.call_ids.assigned());
        self.publish(&sets);
        Ok(ScenarioChange {
            position,
            disarmed: Vec::new(),
            armed,
        })
    }

    /// Deactivates the scenario that is active: disarms every rule that it armed, and publishes the snapshot
    /// without them. The live set stays as it is.
    ///
    /// Returns where the scenario stood, and the rules it disarmed.
    ///
    /// # Errors
    ///
    /// [`DeactivateScenarioError::NotActive`] when no scenario is active.
    pub fn deactivate_scenario(&self) -> Result<ScenarioChange, DeactivateScenarioError> {
        let now = self.inner.clock.now();
        let mut sets = self.sets();
        let Some(active) = sets.scenario.take() else {
            return Err(DeactivateScenarioError::NotActive);
        };
        let position = active.run.position();
        let in_order = active.run.armed_in_order();
        let disarmed = in_order
            .into_iter()
            .filter_map(|rule| active.armed.get(rule.name()))
            .filter(|rule| !rule.arming().has_ended(now))
            .map(|rule| rule.name().clone())
            .collect();
        // A rule of the live set takes from no budget of a step any more.
        if let Some(StepBudget { rule, .. }) = active.run.step_budget() {
            sets.replace(&rule, |armed| armed.with_step_budget(None));
        }
        sets.sweep(now);
        self.publish(&sets);
        Ok(ScenarioChange {
            position,
            disarmed,
            armed: Vec::new(),
        })
    }

    /// Ends the step of the active scenario, whatever its condition says, and starts the next one: the rules of the
    /// step disarm, and those of the next step arm. After the last step no step runs, and the rules of the scenario
    /// stay armed.
    ///
    /// Returns where the scenario stands after it, and the rules it disarmed and armed.
    ///
    /// # Errors
    ///
    /// - [`StepScenarioError::NotActive`] when no scenario is active.
    /// - [`StepScenarioError::NoStep`] when the scenario runs no step: it is no Sequence, or it ran its last step.
    pub fn step_scenario(&self) -> Result<ScenarioChange, StepScenarioError> {
        let now = self.inner.clock.now();
        let calls_before = self.inner.call_ids.assigned();
        let mut sets = self.sets();
        sets.sweep(now);
        let advanced = sets.advance(now, calls_before)?;
        sets.advance_while_over(now, calls_before);
        self.publish(&sets);
        Ok(advanced)
    }

    /// Where the active scenario stands: the scenario, and the step that runs. `None` while no scenario is active.
    pub fn scenario(&self) -> Option<ScenarioPosition> {
        self.sets().position()
    }

    /// The steps that advanced by themselves since the last call, with the armed rules after them.
    ///
    /// Whoever counts the changes of the environment takes them, and nobody else: each comes once.
    pub fn take_steps(&self) -> StepsTaken {
        let mut sets = self.sets();
        StepsTaken {
            steps: std::mem::take(&mut sets.stepped),
            snapshot: self.snapshot(),
        }
    }

    /// Every snapshot that the armed rules publish from now on: when a rule is armed, disarmed or retuned,
    /// when a lifetime ends, and when the scenario changes or advances a step. The one that is published now counts
    /// as seen.
    pub fn changes(&self) -> RuleChanges {
        RuleChanges {
            published: self.published(),
        }
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

    /// Takes in the record of a call that ended: counts it for the step of the active scenario, advances the
    /// scenario when the condition of its step holds, and disarms every rule whose lifetime has ended at `now`.
    ///
    /// A session calls it after every call it recorded, because that call may have ended a step, or spent the last
    /// of a budget.
    pub(crate) fn call_recorded(&self, call: &Call, now: Instant) {
        let mut sets = self.sets();
        if let Some(active) = sets.scenario.as_mut() {
            active.run.call_recorded(call.id(), call);
        }
        let advanced = sets.advance_while_over(now, self.inner.call_ids.assigned());
        let swept = sets.sweep(now);
        if advanced || swept {
            self.publish(&sets);
        }
    }

    /// Disarms every rule whose lifetime has ended at `now`, and ends the step of the scenario whose time is up.
    /// Publishes the snapshot after them.
    pub(crate) fn settle(&self, now: Instant) {
        let mut sets = self.sets();
        let advanced = sets.advance_while_over(now, self.inner.call_ids.assigned());
        let swept = sets.sweep(now);
        if advanced || swept {
            self.publish(&sets);
        }
    }

    /// Disarms every rule whose lifetime is a duration when `clock` reaches its end, and ends every step that ends
    /// after a time when `clock` reaches it. It never returns.
    pub(crate) async fn end_lifetimes(self, clock: &impl WaitableClock) {
        let mut published = self.published();
        loop {
            published.borrow_and_update();
            let next = {
                let sets = self.sets();
                let ends = |rule: &ArmedRule| rule.arming().ends_at();
                let lifetimes = sets.snapshot().iter().filter_map(ends).min();
                let step = sets.scenario.as_ref().and_then(|active| active.run.ends_at());
                lifetimes.into_iter().chain(step).min()
            };
            let reached = async {
                match next {
                    Some(at) => clock.until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                // The armed rules changed, so another lifetime or another step may end first.
                _ = published.changed() => {}
                () = reached => self.settle(clock.now()),
            }
        }
    }

    /// Publishes the snapshot of `sets` to every session.
    fn publish(&self, sets: &Sets) {
        self.inner.published.send_replace(Arc::new(sets.snapshot()));
    }

    /// The sets of rules. A task that panicked while it held them left them whole: each change is made in full
    /// before it is published.
    fn sets(&self) -> MutexGuard<'_, Sets> {
        self.inner.sets.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The snapshots that the [`ArmedRules`] of one proxy publish, for whoever follows what is armed.
///
/// [`ArmedRules::changes`] makes one. It never holds the rules up: a follower that is slow skips to the newest
/// snapshot, which holds every change before it.
#[derive(Debug)]
pub struct RuleChanges {
    published: watch::Receiver<Arc<Snapshot>>,
}

impl RuleChanges {
    /// Waits for the next snapshot, or returns `None` once every handle of the armed rules is gone,
    /// because their engine stopped.
    pub async fn next(&mut self) -> Option<Arc<Snapshot>> {
        self.published.changed().await.ok()?;
        Some(Arc::clone(&self.published.borrow_and_update()))
    }
}

/// Writes the names of the armed rules, in priority order, and where the scenario stands.
impl fmt::Debug for ArmedRules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self.snapshot();
        let armed: Vec<&str> = snapshot.iter().map(|rule| rule.name().as_str()).collect();
        f.debug_struct("ArmedRules")
            .field("seed", &self.inner.seed)
            .field("armed", &armed)
            .field("scenario", &self.scenario())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::{Duration, TimeSpan};
    use gaslightmq_lie::LieDefinition;
    use gaslightmq_rule::{PresetSet, Rule, RuleSet};
    use gaslightmq_scenario::{Mode, Scenario, ScenarioRule, Step, Until};
    use gaslightmq_selector::{Conditions, SelectorSet};
    use gaslightmq_trigger::{Count, CounterKey, Lifetime, SessionCounters};

    use super::*;
    use gaslightmq_clock_waitable::SteppedClock;

    fn armed_rules(clock: &SteppedClock) -> ArmedRules {
        ArmedRules::new(Seed::new(42), Arc::new(clock.clone()), CallIds::new())
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
    fn a_retuned_rule_keeps_its_arming_and_its_place() {
        let rules = armed_rules(&SteppedClock::new());
        let first = rules.arm(resolved(&rule("blackhole"))).unwrap();
        rules.arm(resolved(&rule("leader-flap"))).unwrap();
        let mut changes = rules.changes();
        let slower = Rule::new(
            name("blackhole"),
            Conditions::new(),
            gaslightmq_lie::TimingLie::new().with_delay(TimeSpan::from_millis(20)),
        );

        let retuned = rules.retune(resolved(&slower)).unwrap();

        assert!(retuned.is(&first), "the same arming, with its counters");
        assert_eq!(armed(&rules), ["leader-flap", "blackhole"]);
        assert_eq!(retuned.rule(), &resolved(&slower));
        let published = futures::executor::block_on(changes.next()).unwrap();
        assert!(published.get(&name("blackhole")).unwrap().is(&first));
        assert_eq!(
            rules.retune(resolved(&rule("throttle"))).unwrap_err(),
            RetuneError::NotArmed { rule: name("throttle") }
        );
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
        rules.settle(clock.now());
        assert_eq!(armed(&rules), ["leader-flap", "blackhole"]);

        // A session confirms the one firing, and says so after the call.
        let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").unwrap());
        let firing = arming
            .arming()
            .evaluate(clock.now(), &mut session)
            .into_firing()
            .unwrap();
        assert!(firing.confirm().ends_lifetime());
        rules.settle(clock.now());

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

    /// A scenario of these rules of the workspace, resolved against them.
    fn scenario(called: &str, mode: Mode, rules: &[&str], workspace: &[&str]) -> ResolvedScenario {
        let listed = rules.iter().map(|rule| ScenarioRule::from(name(rule)));
        let scenario = Scenario::new(called.parse().unwrap(), mode, Seed::new(7), listed).unwrap();
        let workspace = RuleSet::new(workspace.iter().map(|named| rule(named))).unwrap();
        scenario
            .resolve(&workspace, &SelectorSet::default(), &PresetSet::default())
            .unwrap()
    }

    #[test]
    fn the_live_set_comes_before_the_rules_of_the_scenario_and_a_chain_keeps_its_order() {
        let rules = armed_rules(&SteppedClock::new());
        rules.arm(resolved(&rule("leader-flap"))).unwrap();
        let chain = scenario(
            "halves",
            Mode::Chain,
            &["blackhole-half", "refuse-half"],
            &["blackhole-half", "refuse-half"],
        );

        let activated = rules.activate_scenario(chain).unwrap();

        assert_eq!(activated.armed, [name("blackhole-half"), name("refuse-half")]);
        assert_eq!(armed(&rules), ["leader-flap", "blackhole-half", "refuse-half"]);
        // A rule armed by hand later still wins over the scenario.
        rules.arm(resolved(&rule("throttle"))).unwrap();
        assert_eq!(
            armed(&rules),
            ["throttle", "leader-flap", "blackhole-half", "refuse-half"]
        );
    }

    #[test]
    fn one_scenario_is_active_at_a_time_and_deactivating_it_leaves_the_live_set() {
        let rules = armed_rules(&SteppedClock::new());
        rules.arm(resolved(&rule("leader-flap"))).unwrap();
        let chain = || scenario("jitter", Mode::Chain, &["background-jitter"], &["background-jitter"]);
        rules.activate_scenario(chain()).unwrap();

        let again = rules.activate_scenario(chain());
        let deactivated = rules.deactivate_scenario().unwrap();

        assert_eq!(
            again.unwrap_err(),
            ActivateScenarioError::AlreadyActive {
                scenario: "jitter".parse().unwrap()
            }
        );
        assert_eq!(deactivated.disarmed, [name("background-jitter")]);
        assert_eq!(armed(&rules), ["leader-flap"]);
        assert_eq!(rules.scenario(), None);
        assert_eq!(
            rules.deactivate_scenario().unwrap_err(),
            DeactivateScenarioError::NotActive
        );
    }

    #[test]
    fn a_rule_that_someone_armed_by_hand_stays_in_the_live_set_when_the_scenario_names_it() {
        let rules = armed_rules(&SteppedClock::new());
        rules.arm(resolved(&rule("background-jitter"))).unwrap();
        let chain = scenario("jitter", Mode::Chain, &["background-jitter"], &["background-jitter"]);

        let activated = rules.activate_scenario(chain).unwrap();
        let deactivated = rules.deactivate_scenario().unwrap();

        assert_eq!(activated.armed, Vec::<RuleName>::new());
        assert_eq!(deactivated.disarmed, Vec::<RuleName>::new());
        assert_eq!(armed(&rules), ["background-jitter"]);
    }

    #[test]
    fn advancing_disarms_the_rules_of_the_step_and_arms_those_of_the_next() {
        let rules = armed_rules(&SteppedClock::new());
        let churn = Step::new("leader-churn".parse().unwrap()).arming([name("refuse-with-not-leader")]);
        let recovery = Step::new("slow-recovery".parse().unwrap()).arming([name("slow-order-producers")]);
        let sequence = scenario(
            "churn",
            Mode::Sequence(vec![churn, recovery]),
            &["background-jitter"],
            &["background-jitter", "refuse-with-not-leader", "slow-order-producers"],
        );
        rules.activate_scenario(sequence).unwrap();
        assert_eq!(armed(&rules), ["refuse-with-not-leader", "background-jitter"]);

        let advanced = rules.step_scenario().unwrap();

        assert_eq!(advanced.disarmed, [name("refuse-with-not-leader")]);
        assert_eq!(advanced.armed, [name("slow-order-producers")]);
        assert_eq!(advanced.position.step.unwrap(), "slow-recovery");
        assert_eq!(armed(&rules), ["slow-order-producers", "background-jitter"]);
        // After the last step, the rules of the scenario stay armed, and no step is left to end.
        rules.step_scenario().unwrap();
        assert_eq!(armed(&rules), ["background-jitter"]);
        assert_eq!(
            rules.step_scenario().unwrap_err(),
            StepScenarioError::NoStep {
                scenario: "churn".parse().unwrap()
            }
        );
    }

    #[test]
    fn a_step_that_ends_on_firings_lets_its_rule_fire_that_often_and_ends_by_itself() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let twice = Count::new(2).unwrap();
        let churn = Step::new("leader-churn".parse().unwrap())
            .arming([name("refuse-with-not-leader")])
            .ending_on(Until::Firings {
                count: twice,
                rule: name("refuse-with-not-leader"),
            });
        let after = Step::new("back-to-normal".parse().unwrap());
        let sequence = scenario(
            "churn",
            Mode::Sequence(vec![churn, after]),
            &[],
            &["refuse-with-not-leader"],
        );
        rules.activate_scenario(sequence).unwrap();
        let arming = rules.snapshot().get(&name("refuse-with-not-leader")).unwrap().clone();
        let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1").unwrap());

        for _ in 0..twice.value() {
            let firing = arming
                .arming()
                .evaluate(clock.now(), &mut session)
                .into_firing()
                .unwrap();
            firing.confirm();
        }
        // The step allows no third firing, also before it ended.
        let third = arming.arming().evaluate(clock.now(), &mut session);
        assert!(!third.fired());
        rules.settle(clock.now());

        let taken = rules.take_steps();
        assert_eq!(taken.steps.len(), 1);
        assert_eq!(taken.steps[0].disarmed, [name("refuse-with-not-leader")]);
        assert_eq!(taken.steps[0].position.step.as_ref().unwrap(), "back-to-normal");
        assert!(taken.snapshot.is_empty());
        // Each step is taken once.
        assert_eq!(rules.take_steps().steps, Vec::<ScenarioChange>::new());
    }

    #[tokio::test]
    async fn a_step_that_ends_after_a_time_ends_when_the_clock_reaches_it() {
        let clock = SteppedClock::new();
        let rules = armed_rules(&clock);
        let mut session = rules.published();
        let ending = tokio::spawn({
            let (rules, clock) = (rules.clone(), clock.clone());
            async move { rules.end_lifetimes(&clock).await }
        });
        let twenty_seconds = TimeSpan::from_secs(20);
        let slow = Step::new("slow-recovery".parse().unwrap())
            .arming([name("slow-order-producers")])
            .ending_on(Until::After(twenty_seconds));
        let after = Step::new("back-to-normal".parse().unwrap());
        let sequence = scenario(
            "slow",
            Mode::Sequence(vec![slow, after]),
            &[],
            &["slow-order-producers"],
        );
        rules.activate_scenario(sequence).unwrap();
        tokio::task::yield_now().await;
        session.borrow_and_update();

        clock.advance(twenty_seconds.duration());
        session.changed().await.unwrap();

        assert_eq!(armed(&rules), Vec::<String>::new());
        assert_eq!(rules.scenario().unwrap().step.unwrap(), "back-to-normal");
        ending.abort();
    }
}
