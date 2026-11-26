//! A reload of the workspace: what the files hold now, taken in as one change, and the armed rules that follow it.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::{Change, ChangeSurface};
use gaslightmq_rule::{Preset, PresetSet, ResolvedRule, Rule, RuleName};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_workspace::{DefinitionChange, DefinitionName, Reload, WorkspaceContext};

use super::{Host, Running, caught_up};
use crate::revisions::RuleRevisions;
use crate::{HostChange, ReloadWorkspaceError, Reloaded, journaling};

/// What a reload does to one rule that is armed in an environment.
enum Following {
    /// The rule resolves to another rule than the one that is armed: its definition changed, or what it refers to.
    /// It is replaced, and keeps its counters and its budget.
    Retune(ResolvedRule),
    /// The rule was of the workspace, and the reload took it away. It disarms.
    Disarm(RuleName),
}

impl<C: WorkspaceContext> Host<C> {
    /// Reads the files of the workspace again, and takes what changed in as one change.
    /// See [`Workspace::reload`](gaslightmq_workspace::Workspace::reload).
    ///
    /// The reload compares what the stores read with what the workspace holds, by name.
    /// It is one change: every definition that it creates or updates has its revision, and every other one keeps its
    /// own. A reload that changes nothing takes a number too. A store in memory has nothing to read again, so a
    /// reload of a workspace in memory changes nothing.
    ///
    /// The host refuses what only it knows of before the workspace takes the reload in:
    ///
    /// - **No rule of the files has the name of a live rule.** A rule of the workspace is in every environment, and
    ///   rule names are unique among all rules, so a rule of the files, or one that a scenario of the files owns,
    ///   never takes the name of a live rule of an environment that runs.
    /// - **Every armed rule follows.** The host resolves every rule that is armed in an environment that runs against
    ///   what the files hold. A rule that resolves to another rule than the one that is armed is replaced, and stays
    ///   armed with its counters and its budget, as after a change of a parameter. A rule of the workspace that the
    ///   reload takes away disarms. A live rule that would not resolve any more refuses the reload.
    ///
    /// A reload has no source: whoever asks for it, the changes that it takes in come from the files.
    /// The journal of every environment that runs records each with the surface `file` and the revision of the
    /// reload: a definition that was created, changed or deleted, and a rule that disarmed because its definition
    /// went. Whoever follows the host learns of every definition that changed or went, and of every armed rule of
    /// the workspace whose definition changed.
    ///
    /// # Errors
    ///
    /// - [`ReloadWorkspaceError::NameTaken`] when a rule of the files has the name of a live rule of an environment
    ///   that runs.
    /// - [`ReloadWorkspaceError::Unresolved`] when a rule that is armed in an environment would not resolve against
    ///   what the files hold.
    /// - [`ReloadWorkspaceError::NotReloaded`] with the [`ReloadError`](gaslightmq_workspace::ReloadError) of the
    ///   workspace: a store could not read again, or what the files hold does not fit together.
    ///
    /// A reload that fails changes nothing: the workspace and every armed rule stay as they are.
    pub async fn reload(&self) -> Result<Reloaded, ReloadWorkspaceError> {
        // The workspace stays taken from the reading of the files until every armed rule follows it.
        let mut workspace = self.workspace.lock().await;
        let reload = workspace
            .reload()
            .await
            .map_err(|error| ReloadWorkspaceError::NotReloaded { error })?;
        self.check_rule_names(&reload)?;
        let followings = self.followings(&reload)?;

        let changes = reload.changes().to_vec();
        let revision = reload.take().await;
        let changed_rules: BTreeSet<&RuleName> = changes
            .iter()
            .filter_map(|change| match change {
                DefinitionChange::Updated(DefinitionName::Rule(rule)) => Some(rule),
                _ => None,
            })
            .collect();
        let mut following = Vec::new();
        for (environment, running) in self.environments().iter_mut() {
            let followings = followings.get(environment).map_or(&[][..], Vec::as_slice);
            let (disarmed, uncounted) = caught_up(running, |running, revisions| follow(running, revisions, followings));
            for change in &changes {
                let definition = journaling::definition(change.name());
                let entry = match change {
                    DefinitionChange::Created(_) => Change::DefinitionCreated { definition },
                    DefinitionChange::Updated(_) => Change::DefinitionChanged { definition },
                    DefinitionChange::Deleted(name) => {
                        if let DefinitionName::Rule(rule) = name
                            && disarmed.contains(rule)
                        {
                            // Every arming in a journal ends with a disarm, also the one of a rule that goes.
                            let rule = journaling::rule(rule);
                            running.record(Change::RuleDisarmed { rule }, ChangeSurface::File, revision);
                        }
                        Change::DefinitionDeleted { definition }
                    }
                };
                running.record(entry, ChangeSurface::File, revision);
            }
            let snapshot = running.engine.rules().snapshot();
            let armed = changed_rules.iter().filter(|rule| snapshot.get(rule).is_some());
            let armed = armed.map(|rule| (*rule).clone());
            following.extend(
                uncounted
                    .into_iter()
                    .chain(armed)
                    .map(|rule| (environment.clone(), rule)),
            );
        }
        drop(workspace);

        for change in &changes {
            self.publish(match change {
                DefinitionChange::Created(name) | DefinitionChange::Updated(name) => {
                    HostChange::DefinitionChanged { name: name.clone() }
                }
                DefinitionChange::Deleted(name) => HostChange::DefinitionDeleted { name: name.clone() },
            });
        }
        for (environment, rule) in following {
            self.publish(HostChange::RuleChanged { environment, rule });
        }
        Ok(Reloaded { revision, changes })
    }

    /// Refuses a reload that brings a rule with the name of a live rule of an environment that runs:
    /// a rule of the workspace, or a rule that a scenario owns.
    ///
    /// No rule that the workspace held had such a name, so checking every rule of the files checks the new ones.
    fn check_rule_names(&self, reload: &Reload<'_, C>) -> Result<(), ReloadWorkspaceError> {
        let owned = reload.definitions::<Scenario>().flat_map(Scenario::owned_rules);
        for rule in reload.definitions::<Rule>().chain(owned) {
            if let Some(environment) = self.live_rule_of_another(rule.name(), None) {
                return Err(ReloadWorkspaceError::NameTaken {
                    rule: rule.name().clone(),
                    environment,
                });
            }
        }
        Ok(())
    }

    /// What the reload does to every rule that is armed in an environment that runs, by environment.
    ///
    /// # Errors
    ///
    /// [`ReloadWorkspaceError::Unresolved`] for the first armed rule that would not resolve against what the files
    /// hold.
    fn followings(
        &self,
        reload: &Reload<'_, C>,
    ) -> Result<BTreeMap<EnvironmentName, Vec<Following>>, ReloadWorkspaceError> {
        let rules: BTreeMap<&RuleName, &Rule> = reload.definitions::<Rule>().map(|rule| (rule.name(), rule)).collect();
        let gone: BTreeSet<&RuleName> = reload
            .changes()
            .iter()
            .filter_map(|change| match change {
                DefinitionChange::Deleted(DefinitionName::Rule(rule)) => Some(rule),
                _ => None,
            })
            .collect();
        let selectors = SelectorSet::new(reload.definitions::<Selector>().cloned());
        let presets = PresetSet::new(reload.definitions::<Preset>().cloned());
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
                let resolved = resolve(rule).map_err(|reason| ReloadWorkspaceError::Unresolved {
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
}

/// Replaces and disarms the armed rules of one environment as the reload says, and returns the names of the rules
/// that disarmed.
///
/// A rule that disarmed takes no number of its own: the reload took its definition away, and the rule has no
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
