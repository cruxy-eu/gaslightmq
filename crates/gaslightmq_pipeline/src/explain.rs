//! What the evaluation of a rule looked at, in the words of a call record.
//!
//! A call record depends on vocabulary only, so it has its own types for what a trigger rolled.
//! This module writes the evaluation of `gaslightmq_rule` down in them.

use std::fmt;
use std::num::NonZeroU64;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_rule::{RuleEvaluation, RuleName};
use gaslightmq_trigger::{Rolled, TriggerOutcome};

/// The name of a rule as a call record keeps it.
pub(crate) fn recorded(name: &RuleName) -> Result<RecordedRuleName, UnexplainedRule> {
    RecordedRuleName::new(name.as_str()).map_err(UnexplainedRule::of)
}

/// What the trigger of one rule looked at on one call, and what it decided.
pub(crate) fn trigger_of(evaluation: &RuleEvaluation) -> Result<TriggerExplanation, UnexplainedRule> {
    let rolled = evaluation.rolled();
    let budget = rolled.budget().map(|budget| {
        let limit = NonZeroU64::new(budget.limit().value()).unwrap_or(NonZeroU64::MIN);
        BudgetCheck::new(budget.left(), limit).map_err(UnexplainedRule::of)
    });

    let trigger = if rolled.outcome() == TriggerOutcome::LifetimeOver {
        TriggerExplanation::lifetime_over()
    } else {
        let match_number = rolled.match_number().and_then(|number| NonZeroU64::new(number.value()));
        TriggerExplanation::new(decision_of(rolled.outcome()), match_number, checks_of(rolled)?)
            .map_err(UnexplainedRule::of)?
    };
    let trigger = match budget.transpose()? {
        Some(budget) => trigger.with_budget(budget),
        None => trigger,
    };
    Ok(match evaluation.missing_subject() {
        Some(missing) => trigger.with_reason(missing),
        None => trigger,
    })
}

const fn decision_of(outcome: TriggerOutcome) -> TriggerDecision {
    match outcome {
        TriggerOutcome::Fired => TriggerDecision::Fired,
        TriggerOutcome::FiredByHand => TriggerDecision::FiredByHand,
        TriggerOutcome::Held => TriggerDecision::Held,
        TriggerOutcome::BudgetEmpty => TriggerDecision::BudgetEmpty,
        TriggerOutcome::NothingToActOn => TriggerDecision::NothingToActOn,
        TriggerOutcome::LifetimeOver => TriggerDecision::LifetimeOver,
    }
}

/// What each part of the trigger said.
fn checks_of(rolled: &Rolled) -> Result<TriggerChecks, UnexplainedRule> {
    let mut checks = TriggerChecks::new();
    if let Some(agreed) = rolled.count_agrees() {
        checks = checks.with_count(agreed);
    }
    if let (Some(roll), Some(chance)) = (rolled.roll(), rolled.chance()) {
        checks = checks.with_die(DieCheck::new(roll.ppm(), chance.ppm()).map_err(UnexplainedRule::of)?);
    }
    if let Some(window) = rolled.window() {
        let since_armed = TimeSpan::from_duration(rolled.since_armed());
        let window = WindowCheck::new(window.opens_after(), window.closes_after(), since_armed);
        checks = checks.with_window(window.map_err(UnexplainedRule::of)?);
    }
    Ok(checks)
}

/// Why the evaluation of a rule has no line in the explanation of a call: the record would contradict itself.
///
/// The evaluation and the record follow the same rules, so this only happens when one of the two is wrong.
/// The explanation then says so, and it does not guess a line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnexplainedRule {
    /// What the record said about the line it was given.
    reason: String,
}

impl UnexplainedRule {
    pub(crate) fn of(error: impl fmt::Display) -> Self {
        Self {
            reason: error.to_string(),
        }
    }
}

impl fmt::Display for UnexplainedRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a rule has no line: {}", self.reason)
    }
}

impl std::error::Error for UnexplainedRule {}
