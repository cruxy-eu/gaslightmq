//! Capture: what someone played live, read from the journal, as the steps of a Sequence.

use gaslightmq_clock::{Instant, TimeSpan};
use gaslightmq_journal::{Change, ChangeSurface, JournalEntry, RecordedName, Revision};
use gaslightmq_rule::{Rule, RuleName};
use gaslightmq_trigger::Seed;

use crate::{Mode, Scenario, ScenarioName, Step, StepName, StepTitle, Until};

/// A capture that runs in one environment, as sans-IO state: it reads the journal of the environment, and marks every
/// arm and disarm in the live set with its time.
///
/// Whoever runs the environment [starts](Self::start) it with the live set as it stands, hands it every entry of the
/// journal as the journal records it, and [stops](Self::stop) it. The stop gives what the capture
/// [captured](Captured): a step for the live set as the capture found it, and one step for every change after it,
/// each ending on the time until the next change, the last on the time until the stop.
///
/// | The journal says | The capture |
/// |---|---|
/// | `rule-armed`, by a request | Puts the rule in the live set, above every rule there, with the definition it is armed with. |
/// | `rule-disarmed`, of a rule of the live set | Takes the rule out of the live set, whoever disarmed it: a request, a lifetime, a reload. |
/// | `rule-armed` with the revision of a scenario that activated or advanced | Nothing: the scenario armed the rule, and no rule of a scenario is in the live set. |
/// | `rule-disarmed` of any other rule | Nothing: the rule belonged to the scenario. |
/// | Anything else | Nothing. A parameter that changed, a definition that changed, a firing: none of them arms or disarms. |
///
/// - **One change is one step.** The entries of one revision are one change, such as a reload that disarmed two
///   rules, so they make one step.
/// - **The step lists its rules with the most recently armed first**, so the Sequence resolves conflicts the way the
///   live set did.
/// - **A captured rule keeps the parameters it had when it was armed.** A live rule is captured as the definition
///   it was armed with, and a turn of an encoder on it while it is armed changes nothing of what the capture holds.
///   A rule of the workspace is captured by its name.
/// - **A live rule that arms again with another definition is another rule.** Someone disarmed it, changed it and
///   armed it again: the capture names that arming after the rule, with the next free number, as
///   `slow-order-producers-2`.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant, TimeSpan};
/// use gaslightmq_journal::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// // Someone captures while they arm a live rule at a pad, and disarm it a while later.
/// let slow = Rule::new(
///     "slow-produce".parse().expect("a rule name"),
///     Conditions::from(Condition::is(ApiKey::PRODUCE)),
///     TimingLie::new().with_delay(TimeSpan::from_secs(1)),
/// );
/// let mut journal = RecentJournal::new(RecentJournalSize::default());
/// let mut capture = Capture::start(Instant::ZERO, []);
/// let recorded: RecordedName = "slow-produce".parse()?;
/// let (armed_at, disarmed_at, stopped_at) = (Duration::from_secs(2), Duration::from_secs(5), Duration::from_secs(6));
/// let (arming, disarming) = (Revision::new(7), Revision::new(8));
///
/// let armed = Change::RuleArmed { rule: recorded.clone() };
/// let entry = journal.record(Instant::ZERO + armed_at, armed, ChangeSurface::Pad, Some(arming));
/// capture.read(&entry, |_| Some(slow.clone()));
/// let disarmed = Change::RuleDisarmed { rule: recorded };
/// let entry = journal.record(Instant::ZERO + disarmed_at, disarmed, ChangeSurface::Pad, Some(disarming));
/// capture.read(&entry, |_| None);
///
/// let captured = capture.stop(Instant::ZERO + stopped_at);
/// let arms: Vec<&[RuleName]> = captured.steps().iter().map(Step::arm).collect();
/// assert_eq!(arms, [&[][..], &[slow.name().clone()][..], &[][..]]);
/// let ends: Vec<Option<&Until>> = captured.steps().iter().map(Step::until).collect();
/// assert_eq!(
///     ends,
///     [
///         Some(&Until::After(TimeSpan::from_duration(armed_at))),
///         Some(&Until::After(TimeSpan::from_duration(disarmed_at - armed_at))),
///         Some(&Until::After(TimeSpan::from_duration(stopped_at - disarmed_at))),
///     ]
/// );
/// assert_eq!(captured.rules(), [slow]);
/// # Ok::<(), RecordedNameError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Capture {
    started_at: Instant,
    /// The live set as the capture found it, the rule armed most recently first, by the names that the capture gives.
    found: Vec<RuleName>,
    /// The live set now: each rule by its name in the environment, and the name that the capture gives its arming.
    /// The rule armed most recently comes first.
    live: Vec<Armed>,
    /// The live rules that the capture saw armed, each by the name that the capture gives it, as it was armed, with
    /// the name of the rule in its environment.
    rules: Vec<(RuleName, Rule)>,
    /// Every change of the live set since the start, the earliest first.
    changes: Vec<LiveChange>,
    /// The revision of the last change of the scenario that armed rules: an activation, or a step.
    scenario: Option<Revision>,
}

/// One rule of the live set, as the capture holds it.
#[derive(Debug, Clone)]
struct Armed {
    /// The name of the rule in its environment.
    rule: RuleName,
    /// The name that the capture gives the arming: the name of the rule, or another one when the rule armed before
    /// with another definition.
    captured: RuleName,
}

/// One change of the live set: what it armed and disarmed, and the live set after it.
#[derive(Debug, Clone)]
struct LiveChange {
    at: Instant,
    revision: Option<Revision>,
    armed: Vec<RuleName>,
    disarmed: Vec<RuleName>,
    /// The live set after the change, by the names that the capture gives, the rule armed most recently first.
    after: Vec<RuleName>,
}

/// A rule that the live set holds when a capture [starts](Capture::start): a rule of the workspace, by its name, or a
/// live rule, as it was armed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapturedRule {
    /// A rule of the workspace that someone armed by hand.
    Workspace(RuleName),
    /// A live rule, as it was armed.
    Live(Box<Rule>),
}

impl CapturedRule {
    /// The name of the rule.
    pub fn name(&self) -> &RuleName {
        match self {
            CapturedRule::Workspace(name) => name,
            CapturedRule::Live(rule) => rule.name(),
        }
    }
}

impl From<RuleName> for CapturedRule {
    fn from(name: RuleName) -> Self {
        CapturedRule::Workspace(name)
    }
}

impl From<Rule> for CapturedRule {
    fn from(rule: Rule) -> Self {
        CapturedRule::Live(Box::new(rule))
    }
}

impl Capture {
    /// A capture that starts at `at`, on the clock of the environment, with the live set as it stands: the rules that
    /// someone armed by hand, the one armed most recently first.
    pub fn start(at: Instant, live: impl IntoIterator<Item = CapturedRule>) -> Capture {
        let mut capture = Capture {
            started_at: at,
            found: Vec::new(),
            live: Vec::new(),
            rules: Vec::new(),
            changes: Vec::new(),
            scenario: None,
        };
        for rule in live {
            let armed = match rule {
                CapturedRule::Workspace(name) => Armed {
                    captured: name.clone(),
                    rule: name,
                },
                CapturedRule::Live(rule) => Armed {
                    rule: rule.name().clone(),
                    captured: capture.name_of(*rule),
                },
            };
            capture.live.push(armed);
        }
        capture.found = capture.live_names();
        capture
    }

    /// Reads one entry of the journal of the environment, as the journal records it.
    ///
    /// `armed_as` gives the definition of a live rule that the entry arms, as it is at this moment, and `None` for a
    /// rule of the workspace. The capture asks it only for a rule that arms in the live set.
    pub fn read(&mut self, entry: &JournalEntry, armed_as: impl FnOnce(&RuleName) -> Option<Rule>) {
        match &entry.change {
            Change::ScenarioActivated { .. } | Change::StepAdvanced { .. } => self.scenario = entry.revision,
            Change::RuleArmed { rule } => {
                let of_the_scenario = entry.revision.is_some() && entry.revision == self.scenario;
                let by_itself = matches!(entry.from.surface, ChangeSurface::Step | ChangeSurface::Lifetime);
                let Some(rule) = rule_name(rule) else {
                    return;
                };
                if of_the_scenario || by_itself || self.live.iter().any(|armed| armed.rule == rule) {
                    return;
                }
                let captured = match armed_as(&rule) {
                    Some(definition) => self.name_of(definition),
                    None => rule.clone(),
                };
                self.live.insert(
                    0,
                    Armed {
                        rule: rule.clone(),
                        captured,
                    },
                );
                self.changed(entry).armed.push(rule);
            }
            Change::RuleDisarmed { rule } => {
                let Some(rule) = rule_name(rule) else {
                    return;
                };
                let before = self.live.len();
                self.live.retain(|armed| armed.rule != rule);
                if self.live.len() != before {
                    self.changed(entry).disarmed.push(rule);
                }
            }
            _ => {}
        }
    }

    /// Stops the capture at `at`, on the clock of the environment, and gives what it captured.
    ///
    /// A step that a change ends lasts until that change, and the last step until `at`. Each lasts a whole number of
    /// milliseconds, counted from the start of the capture, so the steps together last as long as the capture did.
    pub fn stop(self, at: Instant) -> Captured {
        let offset = |instant: Instant| TimeSpan::from_duration(instant.duration_since(self.started_at)).as_millis();
        let mut steps = Vec::with_capacity(self.changes.len() + 1);
        let mut began = 0;
        let mut arming = self.found;
        let mut title = None;
        let ends = self.changes.iter().map(|change| change.at).chain([at]);
        let mut changes = self.changes.iter();
        for (index, ended_at) in ends.enumerate() {
            let ends = offset(ended_at).max(began);
            let name = step_name(index);
            let step = Step::new(name)
                .arming(once_each(std::mem::take(&mut arming)))
                .ending_on(Until::After(TimeSpan::from_millis(ends - began)));
            steps.push(match title.take() {
                Some(title) => step.with_title(title),
                None => step,
            });
            began = ends;
            if let Some(change) = changes.next() {
                arming = change.after.clone();
                title = change.title();
            }
        }
        Captured {
            steps,
            rules: self.rules.into_iter().map(|(_, rule)| rule).collect(),
        }
    }

    /// The change of the live set that `entry` makes: the last one, when the entry has its revision, or a new one.
    fn changed(&mut self, entry: &JournalEntry) -> &mut LiveChange {
        let after = self.live_names();
        let same = self
            .changes
            .last()
            .is_some_and(|last| last.revision.is_some() && last.revision == entry.revision);
        if !same {
            self.changes.push(LiveChange {
                at: entry.at,
                revision: entry.revision,
                armed: Vec::new(),
                disarmed: Vec::new(),
                after: Vec::new(),
            });
        }
        let last = self.changes.len() - 1;
        let change = &mut self.changes[last];
        change.after = after;
        change
    }

    /// The live set now, by the names that the capture gives.
    fn live_names(&self) -> Vec<RuleName> {
        self.live.iter().map(|armed| armed.captured.clone()).collect()
    }

    /// The name that the capture gives a live rule that arms with `definition`: the name of the rule, unless the
    /// capture holds another definition under it already. Then the arming has a name of its own, after the rule with
    /// the next free number, or the name of an earlier arming with the same definition.
    fn name_of(&mut self, definition: Rule) -> RuleName {
        let rule = definition.name().clone();
        let earlier = self
            .rules
            .iter()
            .find(|(of, captured)| *of == rule && same_but_name(captured, &definition));
        if let Some((_, captured)) = earlier {
            return captured.name().clone();
        }
        let taken = |name: &RuleName| self.rules.iter().any(|(_, captured)| captured.name() == name);
        let captured = if taken(&rule) {
            let free = (2..=u32::MAX).find_map(|number| numbered(&rule, number).filter(|name| !taken(name)));
            let name = free.unwrap_or_else(|| rule.clone());
            renamed(&definition, name)
        } else {
            definition
        };
        let name = captured.name().clone();
        self.rules.push((rule, captured));
        name
    }
}

/// What a [`Capture`] captured: the steps of a Sequence, and the live rules that they arm, as they were armed.
///
/// The first step is the live set as the capture found it, and every other step the live set after one change.
/// Each ends after the time until the next change, and the last one after the time until the stop.
/// A step names every rule it arms, the rule armed most recently first: a rule of the workspace, or one of the
/// [`rules`](Self::rules).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Captured {
    steps: Vec<Step>,
    rules: Vec<Rule>,
}

impl Captured {
    /// The steps, in the order they ran. There is at least one.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// The live rules that the steps arm, each as it was armed, in the order in which they first armed.
    /// Whoever saves the scenario keeps them in the workspace, under these names.
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// The captured steps as a Sequence scenario of this name and seed, which owns no rule and has no title.
    pub fn scenario(&self, name: ScenarioName, seed: Seed) -> Scenario {
        // The steps have names of their own, each arms a rule once, ends on its time, and there is one at least.
        match Scenario::new(name, Mode::Sequence(self.steps.clone()), seed, []) {
            Ok(scenario) => scenario,
            Err(error) => unreachable!("the steps of a capture make a Sequence: {error}"),
        }
    }
}

impl LiveChange {
    /// What the change did, in words: `Armed slow-produce, disarmed blackhole`.
    fn title(&self) -> Option<StepTitle> {
        let listed = |rules: &[RuleName]| {
            let names: Vec<&str> = rules.iter().map(RuleName::as_str).collect();
            names.join(", ")
        };
        let mut parts = Vec::new();
        if !self.armed.is_empty() {
            parts.push(format!("armed {}", listed(&self.armed)));
        }
        if !self.disarmed.is_empty() {
            parts.push(format!("disarmed {}", listed(&self.disarmed)));
        }
        let text = parts.join(", ");
        let mut letters = text.chars();
        let first = letters.next()?.to_uppercase();
        StepTitle::new(format!("{first}{}", letters.as_str())).ok()
    }
}

/// The rules, each where it comes first. A rule of the workspace can have the name that the capture gave the
/// arming of another rule, and the step arms the one armed most recently.
fn once_each(rules: Vec<RuleName>) -> Vec<RuleName> {
    let mut seen = std::collections::BTreeSet::new();
    rules.into_iter().filter(|rule| seen.insert(rule.clone())).collect()
}

/// The name of the step at `index` among the steps of a capture: `step-1` for the first.
fn step_name(index: usize) -> StepName {
    match StepName::new(format!("step-{}", index + 1)) {
        Ok(name) => name,
        Err(error) => unreachable!("a word, a hyphen and a number are a name: {error}"),
    }
}

/// The rule that an entry names, by the name it had. A recorded name follows the rule for names, as a rule name does.
fn rule_name(recorded: &RecordedName) -> Option<RuleName> {
    RuleName::new(recorded.as_str()).ok()
}

/// `name` with `-` and the number after it, shortened where the name would be too long.
fn numbered(name: &RuleName, number: u32) -> Option<RuleName> {
    let suffix = format!("-{number}");
    let room = RuleName::MAX_LENGTH.checked_sub(suffix.len())?;
    let base: String = name.as_str().chars().take(room).collect();
    RuleName::new(format!("{}{suffix}", base.trim_end_matches('-'))).ok()
}

/// Whether a live rule armed as `armed` is the rule that the capture holds as `captured`, under its name or another.
fn same_but_name(captured: &Rule, armed: &Rule) -> bool {
    if captured.name() == armed.name() {
        captured == armed
    } else {
        *captured == renamed(armed, captured.name().clone())
    }
}

/// `rule` under another name. Its die still rolls as the die of the rule did, so the arming plays as it was played.
fn renamed(rule: &Rule, name: RuleName) -> Rule {
    let renamed = Rule::new(name, rule.selector().clone(), rule.action().clone())
        .scoped(rule.scope().clone())
        .with_trigger(rule.trigger())
        .with_lifetime(rule.lifetime())
        .with_dice(rule.dice_key());
    match rule.title() {
        Some(title) => renamed.with_title(title.clone()),
        None => renamed,
    }
}
