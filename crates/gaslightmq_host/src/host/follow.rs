//! The armed rules that follow a change of the workspace: a rule whose definition changed, or what it refers to,
//! is replaced and keeps its counters, and a rule of the workspace that went disarms.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Preset, PresetSet, ResolvedRule, Rule, RuleName};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_workspace::WorkspaceContext;

use super::{Host, Running, caught_up};
use crate::revisions::RuleRevisions;

/// What a change of the workspace does to one rule that is armed in an environment.
pub(super) enum Following {
    /// The rule resolves to another rule than the one that is armed: its definition changed, or what it refers to.
    /// It is replaced, and keeps its counters and its budget.
    Retune(ResolvedRule),
    /// The rule was of the workspace, and the change took it away. It disarms.
    Disarm(RuleName),
}

/// A rule that is armed in an environment, and would not resolve after the change. `reason` says why.
pub(super) struct Unfollowed {
    pub(super) environment: EnvironmentName,
    pub(super) rule: RuleName,
    pub(super) reason: String,
}

impl<C: WorkspaceContext> Host<C> {
    /// What a change of the workspace does to every rule that is armed in an environment that runs, by environment.
    ///
    /// `rules`, `selectors` and `presets` are what the workspace holds after the change, and `gone` the rules of the
    /// workspace that the change takes away. A live rule follows the selectors and the presets, and a rule of the
    /// workspace its definition too.
    ///
    /// # Errors
    ///
    /// The first armed rule that would not resolve after the change.
    pub(super) fn followings<'a>(
        &self,
        rules: impl Iterator<Item = &'a Rule>,
        gone: &BTreeSet<RuleName>,
        selectors: impl Iterator<Item = &'a Selector>,
        presets: impl Iterator<Item = &'a Preset>,
    ) -> Result<BTreeMap<EnvironmentName, Vec<Following>>, Unfollowed> {
        let rules: BTreeMap<&RuleName, &Rule> = rules.map(|rule| (rule.name(), rule)).collect();
        let selectors = SelectorSet::new(selectors.cloned());
        let presets = PresetSet::new(presets.cloned());
        let resolve = |rule: &Rule| match (&selectors, &presets) {
            (Ok(selectors), Ok(presets)) => rule.resolve(selectors, presets).map_err(|error| error.to_string()),
            (Err(error), _) => Err(format!("the selectors are not valid: {error}")),
            (_, Err(error)) => Err(format!("the presets are not valid: {error}")),
        };

        let mut followings = BTreeMap::new();
        for (environment, running) in self.environments().iter() {
            let armed = running.engine.rules().snapshot();
            let mut here = Vec::new();
            for armed in armed.iter() {
                let name = armed.name();
                let Some(rule) = running.live.get(name).or_else(|| rules.get(name).copied()) else {
                    if gone.contains(name) {
                        here.push(Following::Disarm(name.clone()));
                    }
                    continue;
                };
                let resolved = resolve(rule).map_err(|reason| Unfollowed {
                    environment: environment.clone(),
                    rule: name.clone(),
                    reason,
                })?;
                if &resolved != armed.rule() {
                    here.push(Following::Retune(resolved));
                }
            }
            followings.insert(environment.clone(), here);
        }
        Ok(followings)
    }

    /// Replaces and disarms the armed rules of every environment that runs as `followings` say, once the workspace
    /// took the change, and hands `journal` each environment with the rules that disarmed there, to record the change.
    ///
    /// Returns every rule that whoever follows the host learns changed, with its environment: the rules of `changed`
    /// that are armed there, whose definition the change replaced, and every rule whose lifetime ended since the last
    /// look.
    pub(super) fn follow_everywhere(
        &self,
        followings: &BTreeMap<EnvironmentName, Vec<Following>>,
        changed: &BTreeSet<RuleName>,
        mut journal: impl FnMut(&Running, &BTreeSet<RuleName>),
    ) -> Vec<(EnvironmentName, RuleName)> {
        let mut following = Vec::new();
        for (environment, running) in self.environments().iter_mut() {
            let here = followings.get(environment).map_or(&[][..], Vec::as_slice);
            let (disarmed, uncounted) = caught_up(running, |running, revisions| follow(running, revisions, here));
            journal(running, &disarmed);
            let snapshot = running.engine.rules().snapshot();
            let armed = changed.iter().filter(|rule| snapshot.get(rule).is_some()).cloned();
            following.extend(
                uncounted
                    .into_iter()
                    .chain(armed)
                    .map(|rule| (environment.clone(), rule)),
            );
        }
        following
    }
}

/// Replaces and disarms the armed rules of one environment as `followings` say, and returns the names of the rules
/// that disarmed.
///
/// A rule that disarmed takes no number of its own: the change took its definition away, and the rule has no
/// revision in the environment any more. A rule that is no longer armed, because its lifetime ended or someone
/// deleted its live rule since, has nothing to follow, and is left as it is.
fn follow(running: &mut Running, revisions: &mut RuleRevisions, followings: &[Following]) -> BTreeSet<RuleName> {
    let rules = running.engine.rules();
    let mut disarmed = BTreeSet::new();
    for following in followings {
        match following {
            Following::Retune(resolved) => {
                let _ = rules.retune(resolved.clone());
            }
            Following::Disarm(rule) => {
                if rules.disarm(rule).is_ok() {
                    revisions.forget(rule);
                    disarmed.insert(rule.clone());
                }
            }
        }
    }
    disarmed
}
