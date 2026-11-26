//! The rules of an environment: those of the workspace with their state there, and the live rules.

use gaslightmq_definition_api::{Store, StoreReadError};
use gaslightmq_engine::ArmError;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::{Change, ChangeSource, Parameter, RecordedDefinition};
use gaslightmq_rule::{Rule, RuleName, RuleState};
use gaslightmq_scenario::{Scenario, ScenarioName};
use gaslightmq_workspace::{Revision, Stores, Workspace, WorkspaceContext};

use super::{Host, Unresolvable, resolve, unread};
use crate::{
    CreateLiveRuleError, DeleteLiveRuleError, EnvironmentRule, EnvironmentRulesError, Expects, HostChange,
    KeepRuleError, RuleHolder, UpdateLiveRuleError, journaling,
};

/// What changes about a live rule when its definition is replaced, as the journal records it.
pub(super) enum LiveRuleChange {
    /// Someone replaced the definition.
    Definition,
    /// A turn of an encoder changed one parameter, from the value `old` to the value `new`.
    Parameter {
        parameter: Parameter,
        old: String,
        new: String,
    },
}

impl<C: WorkspaceContext> Host<C> {
    /// The rules of an environment, each with its state and its revision there: the rules of the workspace in the
    /// order of their names, and then the live rules of the environment in the order of theirs.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentRulesError::NotRunning`] when no environment of that name runs.
    /// - [`EnvironmentRulesError::StoreFailed`] when the store of the rules could not be read.
    pub async fn rules_of(&self, environment: &EnvironmentName) -> Result<Vec<EnvironmentRule>, EnvironmentRulesError> {
        self.check_running(environment)
            .map_err(|environment| EnvironmentRulesError::NotRunning { environment })?;
        // The workspace stays as it is until the rules are read: each one has the revision of the definition read.
        let workspace = self.workspace.lock().await;
        let workspace_rules = workspace.stores().rules().list().await;
        let workspace_rules = workspace_rules.map_err(|error| EnvironmentRulesError::StoreFailed {
            environment: environment.clone(),
            reason: unread::<Rule>(error),
        })?;
        let not_running = |environment| EnvironmentRulesError::NotRunning { environment };
        self.change_rules(environment, not_running, |running, revisions| {
            let armed = running.engine.rules().snapshot();
            let state = |name: &RuleName| {
                if armed.get(name).is_some() {
                    RuleState::Armed
                } else {
                    RuleState::Disarmed
                }
            };
            let of_the_workspace = workspace_rules.into_iter().map(|rule| EnvironmentRule {
                state: state(rule.name()),
                // The later of the two: the last change of the definition, and the last change of the state here.
                revision: revisions.of(rule.name()).max(workspace.revision(rule.name().clone())),
                rule,
                live: false,
            });
            let live = running.live.values().map(|rule| EnvironmentRule {
                state: state(rule.name()),
                revision: revisions.of(rule.name()),
                rule: rule.clone(),
                live: true,
            });
            Ok(of_the_workspace.chain(live).collect())
        })
    }

    /// The rule of an environment that has the name, with its state and its revision there, or `None` when neither
    /// the workspace nor the environment has a rule of that name.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentRulesError::NotRunning`] when no environment of that name runs.
    /// - [`EnvironmentRulesError::StoreFailed`] when the store of the rules could not be read.
    pub async fn rule_of(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
    ) -> Result<Option<EnvironmentRule>, EnvironmentRulesError> {
        let rules = self.rules_of(environment).await?;
        Ok(rules.into_iter().find(|rule| rule.rule.name() == name))
    }

    /// Adds a live rule to an environment, disarmed, and returns the revision it has there.
    ///
    /// The rule stays in the memory of the environment, and never in the workspace, until someone
    /// [keeps](Self::keep) it. It resolves against the workspace first, so a rule that could never work is no live
    /// rule. A create expects nothing: it refuses a name that is taken. `from` says where the change comes from,
    /// and the journal of the environment records it. Whoever follows the host learns of it.
    ///
    /// # Errors
    ///
    /// - [`CreateLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`CreateLiveRuleError::NameTaken`] when a rule of the workspace, a live rule of the environment or a rule
    ///   that a scenario owns has the name. Rule names are unique among all rules.
    /// - [`CreateLiveRuleError::StoreFailed`], [`CreateLiveRuleError::WorkspaceInvalid`] and
    ///   [`CreateLiveRuleError::Unresolved`] when the rule does not resolve against the workspace, as on
    ///   [`arm`](Self::arm).
    pub async fn create_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, CreateLiveRuleError> {
        self.add_live_rule(environment, rule, false, from.into()).await
    }

    /// Adds a live rule to an environment and arms it, as one step, and returns the revision it has there.
    ///
    /// The rule is a live rule of the environment and armed, or neither: a rule that does not arm is not added.
    /// The two are one change, which takes one number. The journal records it as two entries with that revision:
    /// the rule was made, and it armed.
    /// Everything else is as on [`create_live_rule`](Self::create_live_rule), and the rule arms as on
    /// [`arm`](Self::arm), above every rule that is armed in the environment.
    ///
    /// # Errors
    ///
    /// The errors of [`create_live_rule`](Self::create_live_rule).
    /// [`CreateLiveRuleError::NameTaken`] also comes when a rule of that name is armed in the environment already.
    pub async fn create_live_rule_armed(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, CreateLiveRuleError> {
        self.add_live_rule(environment, rule, true, from.into()).await
    }

    /// Resolves a rule against the workspace, puts it among the live rules of an environment, armed when `armed`
    /// says so, and tells whoever follows the host. Returns the revision that the rule has there.
    ///
    /// The rule arms while nobody else reads or changes the environments, so it is armed and a live rule from the
    /// same moment. When it does not arm, it is no live rule.
    ///
    /// The workspace stays taken from the check of the name until the live rule is in, so no rule of the workspace
    /// gets its name in between. Creating a rule of the workspace takes it too, and checks the live rules.
    async fn add_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
        armed: bool,
        from: ChangeSource,
    ) -> Result<Revision, CreateLiveRuleError> {
        let name = rule.name().clone();
        let taken = |holder: Option<RuleHolder>| CreateLiveRuleError::NameTaken {
            environment: environment.clone(),
            rule: name.clone(),
            holder,
        };
        self.check_running(environment)
            .map_err(|environment| CreateLiveRuleError::NotRunning { environment })?;
        // The workspace stays as it is until the environment holds the rule: nobody creates a rule of the workspace
        // under the name in between.
        let workspace = self.workspace.lock().await;
        match workspace.stores().rules().get(&name).await {
            Ok(None) => {}
            Ok(Some(_)) => return Err(taken(None)),
            Err(error) => {
                return Err(CreateLiveRuleError::StoreFailed {
                    rule: name.clone(),
                    reason: unread::<Rule>(error),
                });
            }
        }
        match scenario_owning(&workspace, &name).await {
            Ok(None) => {}
            Ok(Some(scenario)) => return Err(taken(Some(RuleHolder::Scenario(scenario)))),
            Err(error) => {
                return Err(CreateLiveRuleError::StoreFailed {
                    rule: name.clone(),
                    reason: unread::<Scenario>(error),
                });
            }
        }
        let resolved = resolve(&workspace, &rule)
            .await
            .map_err(|unresolvable| match unresolvable {
                Unresolvable::StoreFailed { reason } => CreateLiveRuleError::StoreFailed {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::WorkspaceInvalid { reason } => CreateLiveRuleError::WorkspaceInvalid {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::Unresolved { error } => CreateLiveRuleError::Unresolved { error },
            })?;
        let not_running = |environment| CreateLiveRuleError::NotRunning { environment };
        let revision = self.change_rules(environment, not_running, |running, revisions| {
            let here = || taken(Some(RuleHolder::Environment(environment.clone())));
            if running.live.contains_key(&name) {
                return Err(here());
            }
            let rules = running.engine.rules();
            if armed {
                rules.arm(resolved).map_err(|ArmError::AlreadyArmed { .. }| here())?;
            }
            running.live.insert(name.clone(), rule);
            // Making the rule and arming it are one change, and take one number.
            let revision = revisions.count(&name, &rules.snapshot());
            let rule = journaling::rule(&name);
            running.record(Change::LiveRuleMade { rule: rule.clone() }, from, revision);
            if armed {
                running.record(Change::RuleArmed { rule }, from, revision);
            }
            Ok(revision)
        })?;
        drop(workspace);
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name,
        });
        Ok(revision)
    }

    /// Replaces the live rule of an environment that has the name of `rule`, and returns the revision it has there
    /// from then on.
    ///
    /// Everything but the name changes. A rule that is armed stays armed with the change, and none of its counters
    /// resets. With `expects`, the rule only changes while it has the revision that its sender saw.
    /// `from` says where the change comes from, and the journal of the environment records that the live rule
    /// changed. Whoever follows the host learns of the change.
    ///
    /// # Errors
    ///
    /// - [`UpdateLiveRuleError::OtherRun`] when the change expects a revision of another run.
    /// - [`UpdateLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`UpdateLiveRuleError::NotFound`] when the environment has no live rule of that name.
    /// - [`UpdateLiveRuleError::StoreFailed`], [`UpdateLiveRuleError::WorkspaceInvalid`] and
    ///   [`UpdateLiveRuleError::Unresolved`] when the rule does not resolve against the workspace.
    /// - [`UpdateLiveRuleError::Changed`] when the live rule has another revision than the change expects.
    pub async fn update_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, UpdateLiveRuleError> {
        let change = LiveRuleChange::Definition;
        self.change_live_rule(environment, rule, expects, from.into(), change)
            .await
    }

    /// Replaces the live rule of an environment as [`update_live_rule`](Self::update_live_rule) does,
    /// and records in the journal what `change` says changed about it.
    pub(super) async fn change_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
        expects: Option<Expects>,
        from: ChangeSource,
        change: LiveRuleChange,
    ) -> Result<Revision, UpdateLiveRuleError> {
        let expects = self
            .expected(expects)
            .map_err(|run| UpdateLiveRuleError::OtherRun { run })?;
        let name = rule.name().clone();
        let not_found = || UpdateLiveRuleError::NotFound {
            environment: environment.clone(),
            rule: name.clone(),
        };
        {
            let environments = self.environments();
            let Some(running) = environments.get(environment) else {
                return Err(UpdateLiveRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            if !running.live.contains_key(&name) {
                return Err(not_found());
            }
        }
        // The workspace stays as it is until the rule changed: what it resolved against is what it runs against.
        let workspace = self.workspace.lock().await;
        let resolved = resolve(&workspace, &rule)
            .await
            .map_err(|unresolvable| match unresolvable {
                Unresolvable::StoreFailed { reason } => UpdateLiveRuleError::StoreFailed {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::WorkspaceInvalid { reason } => UpdateLiveRuleError::WorkspaceInvalid {
                    rule: name.clone(),
                    reason,
                },
                Unresolvable::Unresolved { error } => UpdateLiveRuleError::Unresolved { error },
            })?;
        let not_running = |environment| UpdateLiveRuleError::NotRunning { environment };
        let revision = self.change_rules(environment, not_running, |running, revisions| {
            let Some(live) = running.live.get_mut(&name) else {
                return Err(not_found());
            };
            let revision = revisions.of(&name);
            if expects.is_some_and(|expected| expected != revision) {
                return Err(UpdateLiveRuleError::Changed {
                    environment: environment.clone(),
                    rule: name.clone(),
                    revision,
                });
            }
            *live = rule;
            let armed = running.engine.rules();
            // A rule that is not armed has no arming to keep, and arms with the change when someone arms it.
            let _ = armed.retune(resolved);
            let revision = revisions.count(&name, &armed.snapshot());
            let rule = journaling::rule(&name);
            let changed = match change {
                LiveRuleChange::Definition => Change::LiveRuleChanged { rule },
                LiveRuleChange::Parameter { parameter, old, new } => Change::ParameterChanged {
                    rule,
                    parameter,
                    old,
                    new,
                },
            };
            running.record(changed, from, revision);
            Ok(revision)
        })?;
        drop(workspace);
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name,
        });
        Ok(revision)
    }

    /// Removes a live rule from an environment, armed or not, and returns the revision of the delete.
    ///
    /// With `expects`, the rule only goes while it has the revision that its sender saw.
    /// Nothing has the revision of the delete afterwards: a live rule that someone makes again under the name has a
    /// higher one. `from` says where the change comes from, and the journal of the environment records it:
    /// that the rule disarmed, when it was armed, and that it was deleted, both with the revision of the delete.
    /// Whoever follows the host learns of the change.
    ///
    /// # Errors
    ///
    /// - [`DeleteLiveRuleError::OtherRun`] when the change expects a revision of another run.
    /// - [`DeleteLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`DeleteLiveRuleError::NotFound`] when the environment has no live rule of that name.
    /// - [`DeleteLiveRuleError::Changed`] when the live rule has another revision than the change expects.
    pub fn delete_live_rule(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, DeleteLiveRuleError> {
        let from = from.into();
        let expects = self
            .expected(expects)
            .map_err(|run| DeleteLiveRuleError::OtherRun { run })?;
        let not_running = |environment| DeleteLiveRuleError::NotRunning { environment };
        let revision = self.change_rules(environment, not_running, |running, revisions| {
            if !running.live.contains_key(name) {
                return Err(DeleteLiveRuleError::NotFound {
                    environment: environment.clone(),
                    rule: name.clone(),
                });
            }
            let revision = revisions.of(name);
            if expects.is_some_and(|expected| expected != revision) {
                return Err(DeleteLiveRuleError::Changed {
                    environment: environment.clone(),
                    rule: name.clone(),
                    revision,
                });
            }
            running.live.remove(name);
            // A rule that is not armed has nothing to disarm.
            let _ = running.engine.rules().disarm(name);
            let was_armed = revisions.was_armed(name);
            let revision = revisions.count_removed(name);
            let rule = journaling::rule(name);
            if was_armed {
                // Every arming in a journal ends with a disarm, also the one of a rule that goes.
                running.record(Change::RuleDisarmed { rule: rule.clone() }, from, revision);
            }
            running.record(Change::LiveRuleDeleted { rule }, from, revision);
            Ok(revision)
        })?;
        self.publish(HostChange::RuleRemoved {
            environment: environment.clone(),
            rule: name.clone(),
        });
        Ok(revision)
    }

    /// Keeps a live rule of an environment in the workspace, under its name, and returns the revision that the rule
    /// has from then on: in the workspace, and in the environment.
    ///
    /// The rule stays in the state it has in the environment, and every other environment holds it disarmed,
    /// as every rule of the workspace. Whoever follows the host learns that the live rule left the environment,
    /// that the workspace has the rule, and the state of the rule in the environment, in that order.
    ///
    /// With `expects`, the workspace only gets the live rule that its sender saw: a rule that someone changed since,
    /// as with a turn of an encoder, is not kept.
    ///
    /// `from` says where the change comes from. The journal of the environment records that the live rule was kept,
    /// and the journal of every other environment that runs records that the workspace has a new rule.
    ///
    /// # Errors
    ///
    /// - [`KeepRuleError::OtherRun`] when the change expects a revision of another run.
    /// - [`KeepRuleError::NotRunning`] when no environment of that name runs.
    /// - [`KeepRuleError::NotFound`] when the environment has no live rule of that name.
    /// - [`KeepRuleError::Changed`] when the live rule has another revision than the change expects.
    /// - [`KeepRuleError::NameTaken`] when a live rule of another environment that runs, or a rule that a scenario
    ///   owns, has the name.
    ///   Whoever wants the rule in the workspace deletes that live rule first.
    /// - [`KeepRuleError::NotKept`] when the workspace did not take the rule.
    pub async fn keep(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, KeepRuleError> {
        let from = from.into();
        let expects = self.expected(expects).map_err(|run| KeepRuleError::OtherRun { run })?;
        // Whoever changes a live rule resolves it against the workspace first, so the rule stays as it is read here
        // until the workspace holds it. The workspace also stays taken from the check of the name to the write,
        // as it does while a live rule goes in.
        let mut workspace = self.workspace.lock().await;
        let not_running = |environment| KeepRuleError::NotRunning { environment };
        let rule = self.change_rules(environment, not_running, |running, revisions| {
            let Some(rule) = running.live.get(name) else {
                return Err(KeepRuleError::NotFound {
                    environment: environment.clone(),
                    rule: name.clone(),
                });
            };
            let revision = revisions.of(name);
            if expects.is_some_and(|expected| expected != revision) {
                return Err(KeepRuleError::Changed {
                    environment: environment.clone(),
                    rule: name.clone(),
                    revision,
                });
            }
            Ok(rule.clone())
        })?;
        if let Some(holder) = self.holder_of(&workspace, name, Some(environment)).await {
            return Err(KeepRuleError::NameTaken {
                rule: name.clone(),
                holder,
            });
        }
        // The keep is one change: the rule has its number in the workspace, and in the environment that held it.
        let revision = workspace.create(rule).await.map_err(|error| KeepRuleError::NotKept {
            rule: name.clone(),
            error,
        })?;
        for (holder, running) in self.environments().iter_mut() {
            let rule = journaling::rule(name);
            // The rule goes on in the environment that held it, and it is a new definition for every other one.
            let change = if holder == environment {
                running.live.remove(name);
                Change::LiveRuleKept { rule }
            } else {
                let definition = RecordedDefinition::Rule(rule);
                Change::DefinitionCreated { definition }
            };
            running.record(change, from, revision);
        }
        drop(workspace);
        self.publish(HostChange::RuleRemoved {
            environment: environment.clone(),
            rule: name.clone(),
        });
        self.publish(HostChange::DefinitionChanged {
            name: name.clone().into(),
        });
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name.clone(),
        });
        self.publish_unsaved().await;
        Ok(revision)
    }

    /// The environment that runs and holds a live rule of that name, other than `except`.
    /// What holds a rule of this name outside the workspace: a live rule of an environment that runs, other than
    /// `except`, or a rule that a scenario of the workspace owns. `None` when nothing does.
    ///
    /// A store of scenarios that cannot be read holds nothing here: the workspace reads it again for the change that
    /// asks, and fails with what the store said.
    pub(super) async fn holder_of(
        &self,
        workspace: &Workspace<C>,
        name: &RuleName,
        except: Option<&EnvironmentName>,
    ) -> Option<RuleHolder> {
        if let Some(environment) = self.live_rule_of_another(name, except) {
            return Some(RuleHolder::Environment(environment));
        }
        scenario_owning(workspace, name)
            .await
            .ok()
            .flatten()
            .map(RuleHolder::Scenario)
    }

    pub(super) fn live_rule_of_another(
        &self,
        name: &RuleName,
        except: Option<&EnvironmentName>,
    ) -> Option<EnvironmentName> {
        self.environments()
            .iter()
            .find(|(environment, running)| Some(*environment) != except && running.live.contains_key(name))
            .map(|(environment, _)| environment.clone())
    }

    /// Whether an environment of that name runs. Returns its name when it does not.
    fn check_running(&self, environment: &EnvironmentName) -> Result<(), EnvironmentName> {
        if self.environments().contains_key(environment) {
            Ok(())
        } else {
            Err(environment.clone())
        }
    }
}

/// The scenario of the workspace that owns a rule of this name, or `None` when no scenario does.
///
/// # Errors
///
/// What the store of the scenarios said when it could not read.
async fn scenario_owning<C: WorkspaceContext>(
    workspace: &Workspace<C>,
    name: &RuleName,
) -> Result<Option<ScenarioName>, StoreReadError> {
    let scenarios = workspace.stores().scenarios().list().await?;
    Ok(scenarios
        .into_iter()
        .find(|scenario| scenario.owned_rules().any(|rule| rule.name() == name))
        .map(|scenario| scenario.name().clone()))
}
