//! A reload of the workspace: what the files hold now, taken in as one change, and the armed rules that follow it.

use std::collections::BTreeSet;

use gaslightmq_journal::{Change, ChangeSurface};
use gaslightmq_rule::{Preset, Rule, RuleName};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::Selector;
use gaslightmq_workspace::{DefinitionChange, DefinitionName, Reload, WorkspaceContext};

use super::Host;
use super::follow::Unfollowed;
use crate::{HostChange, ReloadWorkspaceError, Reloaded, journaling};

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
        let gone: BTreeSet<RuleName> = reload
            .changes()
            .iter()
            .filter_map(|change| match change {
                DefinitionChange::Deleted(DefinitionName::Rule(rule)) => Some(rule.clone()),
                _ => None,
            })
            .collect();
        let followings = self
            .followings(
                reload.definitions::<Rule>(),
                &gone,
                reload.definitions::<Selector>(),
                reload.definitions::<Preset>(),
            )
            .map_err(
                |Unfollowed {
                     environment,
                     rule,
                     reason,
                 }| ReloadWorkspaceError::Unresolved {
                    environment,
                    rule,
                    reason,
                },
            )?;

        let changes = reload.changes().to_vec();
        let revision = reload.take().await;
        let changed_rules: BTreeSet<RuleName> = changes
            .iter()
            .filter_map(|change| match change {
                DefinitionChange::Updated(DefinitionName::Rule(rule)) => Some(rule.clone()),
                _ => None,
            })
            .collect();
        let following = self.follow_everywhere(&followings, &changed_rules, |running, disarmed| {
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
        });
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
}
