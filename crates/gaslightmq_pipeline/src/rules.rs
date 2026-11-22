//! The rules that one call touched: their evaluations, how their lies combined, and which lies were told.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_combine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::Incident;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::{FiredAction, PassEvaluation, RuleEvaluation, RuleName};
use gaslightmq_trigger::{CounterKey, Per, SessionCounters};

use crate::explain::{UnexplainedRule, recorded, trigger_of};
use crate::untold::UntoldLie;

/// Which rule of a call a lie belongs to: the position of its line.
/// `gaslightmq_combine` names every lie by it.
pub(crate) type Slot = usize;

/// The key of the die that rolls the jitter of a delay, below the die of its rule.
const JITTER: &str = "jitter";

/// Whether the pipeline told the lie of a rule.
#[derive(Debug)]
enum Told {
    /// The stage of the lie has not come yet, or the lie lost every target.
    NotYet,
    /// The lie said what it asks for, and the pipeline has not done it yet:
    /// the request or the answer that it changed still waits for its turn.
    Asked,
    /// The lie did what it does: the pipeline followed its effect, or held the call back for it.
    Yes,
    /// GaslightMQ could not tell the lie.
    No(Failure),
}

/// One rule that the call matched.
#[derive(Debug)]
struct Line {
    evaluation: RuleEvaluation,
    told: Told,
}

/// How far the lies of the call are combined.
#[derive(Debug)]
enum Combined {
    /// The request pass ran.
    Request(RequestPass<Slot>),
    /// Both passes ran, or the call ended without a response pass.
    Call(Combination<Slot>),
}

/// The lies that act at one stage, in the order the pipeline deals with them.
#[derive(Debug, Default)]
pub(crate) struct Acting {
    /// The silence or the hang-up that ends the call at this stage.
    pub(crate) terminal: Option<Slot>,
    /// The lies to ask, each with the target it holds: rewrites, then the refusal of the whole call,
    /// then the refusals of parts. A refusal writes over what a rewrite left, and never the other way round.
    pub(crate) askable: Vec<(Slot, Target)>,
    /// The timing lies that hold the call back at this stage.
    pub(crate) modifiers: Vec<(Slot, TimingLie)>,
}

/// The rules that one call touched, across both evaluation passes.
///
/// It keeps the evaluation of every rule that matched, combines their lies,
/// notes which lies the pipeline told, settles every firing, and writes the explanation.
#[derive(Debug)]
pub(crate) struct RuleLines {
    lines: Vec<Line>,
    /// The lies of the request pass that take part in combining.
    of_request_pass: Vec<FiredLie<Slot>>,
    combined: Combined,
    /// The longest throttle of a timing lie that the pipeline told, because it also delays.
    told_throttle: TimeSpan,
    /// The rule whose lie ended the call before the truth existed, and the rules that waited for that truth.
    ended_before_truth: Option<(Slot, Vec<(RuleName, Priority)>)>,
}

impl RuleLines {
    /// The lines of a call that no rule was evaluated for.
    pub(crate) fn none() -> Self {
        Self {
            lines: Vec::new(),
            of_request_pass: Vec::new(),
            combined: Combined::Request(RequestPass::of([])),
            told_throttle: TimeSpan::ZERO,
            ended_before_truth: None,
        }
    }

    /// The lines of the request pass, with their lies combined.
    /// The incidents of the rules that fired go to `incidents`, and their firings count from now on.
    pub(crate) fn of_request_pass(pass: PassEvaluation, incidents: &mut Vec<Incident>) -> Self {
        let mut lines = Self::none();
        lines.of_request_pass = lines.take_in(pass, incidents);
        lines.combined = Combined::Request(RequestPass::of(lines.of_request_pass.clone()));
        lines
    }

    /// GaslightMQ could not tell the lie of `slot`, which was to end the call before forwarding: `failure` says why.
    ///
    /// Nothing acted on the call yet, so the lie leaves the combination as if it had not fired:
    /// the lies that it shadowed or pre-empted apply, and the rules that wait for the truth get their pass.
    /// Only the request pass can be combined again, so this changes nothing once both passes ran.
    pub(crate) fn untold_before_anything_acted(&mut self, slot: Slot, failure: impl Into<UntoldLie>) {
        self.untold(slot, failure);
        if matches!(self.combined, Combined::Request(_)) {
            self.of_request_pass.retain(|lie| *lie.id() != slot);
            self.combined = Combined::Request(RequestPass::of(self.of_request_pass.clone()));
        }
    }

    /// Adds the lines of the response pass, and combines the lies of the whole call.
    pub(crate) fn add_response_pass(&mut self, pass: PassEvaluation, incidents: &mut Vec<Incident>) {
        let fired = self.take_in(pass, incidents);
        self.combine_call(fired);
    }

    /// Combines the lies of the whole call without a response pass.
    pub(crate) fn end_without_response_pass(&mut self) {
        self.combine_call(Vec::new());
    }

    /// The lie of `ended_by` ended the call before the truth existed, and `waiting` are the rules that waited for it.
    pub(crate) fn ended_before_truth(&mut self, ended_by: Slot, waiting: Vec<(RuleName, Priority)>) {
        self.ended_before_truth = Some((ended_by, waiting));
    }

    /// Whether a lie ended the call before the truth existed.
    pub(crate) const fn has_ended_before_truth(&self) -> bool {
        self.ended_before_truth.is_some()
    }

    /// The lies that act at `stage`.
    ///
    /// Before forwarding, the fate of a lie is final once the request pass ran.
    /// After the response, it is final once both passes ran.
    pub(crate) fn acting(&self, stage: Stage) -> Acting {
        let mut acting = Acting::default();
        let mut of_call = Vec::new();
        let mut of_parts = Vec::new();
        for fate in self.fates() {
            let Some(applied) = fate.applied().filter(|_| fate.stage() == stage) else {
                continue;
            };
            let held = (*fate.id(), applied.clone());
            match fate.claim() {
                Claim::Terminal { .. } => acting.terminal = Some(*fate.id()),
                Claim::Modifier(timing) => acting.modifiers.push((*fate.id(), *timing)),
                Claim::Rewrite { .. } => acting.askable.push(held),
                Claim::Refusal { .. } if applied.is_call() => of_call.push(held),
                Claim::Refusal { .. } => of_parts.push(held),
            }
        }
        acting.askable.extend(of_call);
        acting.askable.extend(of_parts);
        acting
    }

    /// The lie of `slot` on `target`, to ask its behaviour with.
    pub(crate) fn lie_on(&self, slot: Slot, target: Target) -> Option<TargetedLie> {
        let lie = self.lines.get(slot)?.evaluation.lie()?;
        Some(TargetedLie::new(lie.lie().clone(), target))
    }

    /// The name of the rule of `slot`, for a message.
    pub(crate) fn name_of(&self, slot: Slot) -> &str {
        self.lines.get(slot).map_or("", |line| line.evaluation.name().as_str())
    }

    /// The pipeline told the lie of `slot`: what the lie asked for happened.
    pub(crate) fn told(&mut self, slot: Slot) {
        if let Some(line) = self.lines.get_mut(slot) {
            line.told = Told::Yes;
        }
    }

    /// The lie of `slot` said what it asks for, and that happens once its call is at the head of the line.
    /// Until [`took_effect`](Self::took_effect) says so, the lie is not told.
    pub(crate) fn asked(&mut self, slot: Slot) {
        if let Some(line) = self.lines.get_mut(slot) {
            line.told = Told::Asked;
        }
    }

    /// What the lies asked for so far happened: the request went on as they left it, or the answer left.
    pub(crate) fn took_effect(&mut self) {
        for line in &mut self.lines {
            if matches!(line.told, Told::Asked) {
                line.told = Told::Yes;
            }
        }
    }

    /// GaslightMQ could not tell the lie of `slot`: `failure` says why.
    pub(crate) fn untold(&mut self, slot: Slot, failure: impl Into<UntoldLie>) {
        if let Some(line) = self.lines.get_mut(slot) {
            line.told = Told::No(Failure::new(failure.into()));
        }
    }

    /// Every lie that acts at `stage` is untold, because the call has no such stage: `failure` says why.
    pub(crate) fn untold_at(&mut self, stage: Stage, failure: &UntoldLie) {
        let acting = self.acting(stage);
        let modifiers = acting.modifiers.iter().map(|(slot, _)| *slot);
        let askable = acting.askable.iter().map(|(slot, _)| *slot);
        for slot in acting.terminal.into_iter().chain(modifiers).chain(askable) {
            self.untold(slot, failure.clone());
        }
    }

    /// How long the call waits for `modifiers`, the timing lies of one stage, and notes that they are told.
    ///
    /// Each delay strays from its length by at most its jitter, and the die of its rule says how far.
    /// The die is the one of the rule, on this session when the rule counts per session,
    /// keyed once more so that the stray does not follow the roll of a chance trigger.
    /// It rolls for the match that fired the rule, so the same seed gives the same wait.
    pub(crate) fn wait(&mut self, modifiers: &[(Slot, TimingLie)], counters: &SessionCounters) -> Duration {
        let mut wait = Duration::ZERO;
        for (slot, timing) in modifiers {
            let Some(line) = self.lines.get_mut(*slot) else {
                continue;
            };
            wait = wait.saturating_add(waited(timing, &line.evaluation, counters).duration());
            let only_throttles = timing.delay() == TimeSpan::ZERO && timing.throttle() != TimeSpan::ZERO;
            line.told = if only_throttles {
                Told::No(Failure::new(UntoldLie::ThrottleUnwritten))
            } else {
                self.told_throttle = self.told_throttle.max(timing.throttle());
                Told::Yes
            };
        }
        wait
    }

    /// The throttle that the answer of the call should carry and does not, when no line of a rule says so already.
    ///
    /// A lie that only throttles is not told, and its own line says why.
    /// A lie that delays and throttles is told, because the call did wait, so the call itself has to say
    /// that its answer carries no throttle.
    pub(crate) fn unwritten_throttle(&self) -> Option<TimeSpan> {
        let throttle = match &self.combined {
            Combined::Request(_) => TimeSpan::ZERO,
            Combined::Call(call) => call.after_response().throttle(),
        };
        (throttle != TimeSpan::ZERO && self.told_throttle != TimeSpan::ZERO).then_some(throttle)
    }

    /// Settles every firing, and writes down why the call came out the way it did.
    ///
    /// A firing counts when its lie applied and the pipeline told it.
    /// Every other firing goes back to its budget.
    /// A lie whose turn never came is untold, and `unreached` says why.
    pub(crate) fn settle(&mut self, unreached: &UntoldLie) -> Result<Vec<RuleExplanation>, UnexplainedRule> {
        self.end_without_response_pass();
        let fates = self.fates();
        let fate_of = |slot: Slot| fates.iter().find(|fate| *fate.id() == slot);

        for (slot, line) in self.lines.iter_mut().enumerate() {
            let applied = fate_of(slot).is_some_and(Fate::counts);
            if applied && matches!(line.told, Told::NotYet | Told::Asked) {
                line.told = Told::No(Failure::new(unreached));
            }
            if applied && matches!(line.told, Told::Yes) {
                line.evaluation.confirm();
            } else {
                line.evaluation.withdraw();
            }
        }

        let mut explained = Vec::new();
        for (slot, line) in self.lines.iter().enumerate() {
            explained.push(self.explain(line, fate_of(slot))?);
        }
        if let Some((ended_by, waiting)) = &self.ended_before_truth {
            let ended_by = recorded(self.rule_of(*ended_by)?)?;
            for (rule, priority) in waiting {
                let line = RuleExplanation::ended_before_truth(recorded(rule)?, *priority, ended_by.clone());
                explained.push(line);
            }
        }
        explained.sort_by_key(RuleExplanation::priority);
        Ok(explained)
    }

    /// The line of one rule in the explanation.
    fn explain(&self, of_rule: &Line, fate: Option<&Fate<Slot>>) -> Result<RuleExplanation, UnexplainedRule> {
        let evaluation = &of_rule.evaluation;
        let rule = recorded(evaluation.name())?;
        let priority = evaluation.priority();
        let matched = evaluation.target().clone();
        let trigger = trigger_of(evaluation)?;

        match evaluation.fired_action() {
            None => RuleExplanation::held(rule, priority, matched, trigger),
            Some(FiredAction::Incidents(incidents)) => {
                RuleExplanation::fired_incidents(rule, priority, matched, trigger, incidents.clone())
            }
            Some(FiredAction::Lie(lie)) => {
                let fate = self.fate_in_record(lie, fate, &of_rule.told)?;
                RuleExplanation::lied(rule, priority, matched, trigger, fate)
            }
        }
        .map_err(UnexplainedRule::of)
    }

    /// What became of `lie`, in the words of a call record.
    fn fate_in_record(
        &self,
        lie: &TargetedLie,
        fate: Option<&Fate<Slot>>,
        told: &Told,
    ) -> Result<LieFate, UnexplainedRule> {
        let losses = |losses: &[Loss<Slot>]| -> Result<Vec<LostTarget>, UnexplainedRule> {
            let lost = |loss: &Loss<Slot>| Ok(LostTarget::new(loss.on().clone(), recorded(self.rule_of(*loss.to())?)?));
            losses.iter().map(lost).collect()
        };
        let recorded_fate = match fate {
            // The lie never became a claim, so it met no other lie.
            None => LieFate::all_applied(lie.clone()),
            Some(fate) => {
                let (shadowed, pre_empted) = (losses(fate.shadowed())?, losses(fate.pre_empted())?);
                LieFate::new(lie.clone(), fate.applied().cloned(), shadowed, pre_empted).map_err(UnexplainedRule::of)?
            }
        };
        Ok(match told {
            Told::No(failure) => recorded_fate.untold(failure.clone()),
            Told::Yes | Told::Asked | Told::NotYet => recorded_fate,
        })
    }

    /// The name of the rule of `slot`.
    fn rule_of(&self, slot: Slot) -> Result<&RuleName, UnexplainedRule> {
        let line = self.lines.get(slot);
        line.map(|line| line.evaluation.name())
            .ok_or_else(|| UnexplainedRule::of("a lie lost to a rule that the call did not match"))
    }

    /// Keeps the evaluations of one pass, and returns the lies that fired in it.
    /// Nothing shadows an incident, so a rule with incidents confirms its firing at once.
    fn take_in(&mut self, pass: PassEvaluation, incidents: &mut Vec<Incident>) -> Vec<FiredLie<Slot>> {
        let mut fired = Vec::new();
        for mut evaluation in pass {
            let slot = self.lines.len();
            let mut told = Told::NotYet;
            if let Some(lie) = evaluation.lie() {
                match Claim::try_from(lie) {
                    Ok(claim) => fired.push(FiredLie::new(slot, evaluation.priority(), claim)),
                    Err(error) => told = Told::No(Failure::new(error)),
                }
            } else if evaluation.fired() {
                incidents.extend(evaluation.incidents().iter().cloned());
                evaluation.confirm();
            }
            self.lines.push(Line { evaluation, told });
        }
        fired
    }

    /// Combines the whole call, with `fired` as the lies of the response pass. A call that is combined stays as it is.
    fn combine_call(&mut self, fired: Vec<FiredLie<Slot>>) {
        let request = std::mem::replace(&mut self.combined, Combined::Request(RequestPass::of([])));
        self.combined = match request {
            Combined::Request(request) => Combined::Call(request.response_pass(fired)),
            call @ Combined::Call(_) => call,
        };
    }

    /// The fate of every lie as it stands: after the request pass, or after both.
    fn fates(&self) -> Vec<Fate<Slot>> {
        match &self.combined {
            Combined::Request(request) => request.clone().without_response_pass().fates().to_vec(),
            Combined::Call(call) => call.fates().to_vec(),
        }
    }
}

/// How long the call waits for one timing lie: its delay, less or more a part of its jitter.
fn waited(timing: &TimingLie, evaluation: &RuleEvaluation, counters: &SessionCounters) -> TimeSpan {
    let (delay, jitter) = (timing.delay().as_millis(), timing.jitter().as_millis());
    let roll = (jitter > 0).then(|| jitter_roll(evaluation, counters)).flatten();
    let Some(roll) = roll else {
        return timing.delay();
    };
    // The roll is under one million, so the stray is between zero and twice the jitter.
    let stray = u128::from(jitter) * 2 * u128::from(roll) / 1_000_000;
    let wait = u128::from(delay.saturating_sub(jitter)).saturating_add(stray);
    TimeSpan::from_millis(u64::try_from(wait).unwrap_or(u64::MAX))
}

/// What the jitter die of a rule rolls for the match that fired it, in parts per million.
fn jitter_roll(evaluation: &RuleEvaluation, counters: &SessionCounters) -> Option<u32> {
    let arming = evaluation.rule().arming();
    let of_rule = match arming.trigger().per() {
        Per::Rule => arming.die(),
        Per::Session => arming.die().for_counter(counters.key()),
    };
    let die = of_rule.for_counter(&CounterKey::new(JITTER).ok()?);
    Some(die.roll(evaluation.rolled().match_number()?).ppm())
}
