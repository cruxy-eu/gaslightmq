//! The rules of an environment: those of the workspace with their state there, and the live rules.

use gaslightmq_definition_api::Store;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Rule, RuleName, RuleState};
use gaslightmq_workspace::{Stores, WorkspaceContext};

use super::{Host, LiveRule, Unresolvable, unread};
use crate::{
    CreateLiveRuleError, DeleteLiveRuleError, EnvironmentRule, EnvironmentRulesError, HostChange, KeepRuleError,
    UpdateLiveRuleError,
};

impl<C: WorkspaceContext> Host<C> {
    /// The rules of an environment, each with its state there: the rules of the workspace in the order of their
    /// names, and then the live rules of the environment in the order of theirs.
    ///
    /// # Errors
    ///
    /// - [`EnvironmentRulesError::NotRunning`] when no environment of that name runs.
    /// - [`EnvironmentRulesError::StoreFailed`] when the store of the rules could not be read.
    pub async fn rules_of(&self, environment: &EnvironmentName) -> Result<Vec<EnvironmentRule>, EnvironmentRulesError> {
        self.check_running(environment)
            .map_err(|environment| EnvironmentRulesError::NotRunning { environment })?;
        let workspace_rules = {
            let workspace = self.workspace.lock().await;
            workspace.stores().rules().list().await
        };
        let workspace_rules = workspace_rules.map_err(|error| EnvironmentRulesError::StoreFailed {
            environment: environment.clone(),
            reason: unread::<Rule>(error),
        })?;
        let environments = self.environments();
        let Some(running) = environments.get(environment) else {
            return Err(EnvironmentRulesError::NotRunning {
                environment: environment.clone(),
            });
        };
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
            rule,
            live: false,
        });
        let live = running.live.values().map(|live| EnvironmentRule {
            state: state(live.rule.name()),
            rule: live.rule.clone(),
            live: true,
        });
        Ok(of_the_workspace.chain(live).collect())
    }

    /// The rule of an environment that has the name, with its state there, or `None` when neither the workspace
    /// nor the environment has a rule of that name.
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

    /// Adds a live rule to an environment, disarmed, and returns it.
    ///
    /// The rule stays in the memory of the environment, and never in the workspace, until someone
    /// [keeps](Self::keep) it. It resolves against the workspace first, so a rule that could never work is no live
    /// rule. It gets its UUID from the workspace then, and the environment keeps that UUID next to it.
    /// Whoever follows the host learns of it.
    ///
    /// # Errors
    ///
    /// - [`CreateLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`CreateLiveRuleError::NameTaken`] when a rule of the workspace or a live rule of the environment has the
    ///   name.
    /// - [`CreateLiveRuleError::StoreFailed`], [`CreateLiveRuleError::WorkspaceInvalid`] and
    ///   [`CreateLiveRuleError::Unresolved`] when the rule does not resolve against the workspace, as on
    ///   [`arm`](Self::arm).
    pub async fn create_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
    ) -> Result<Rule, CreateLiveRuleError> {
        let name = rule.name().clone();
        let taken = || CreateLiveRuleError::NameTaken {
            environment: environment.clone(),
            rule: name.clone(),
        };
        self.check_running(environment)
            .map_err(|environment| CreateLiveRuleError::NotRunning { environment })?;
        let of_the_workspace = {
            let workspace = self.workspace.lock().await;
            workspace.stores().rules().get(&name).await
        };
        match of_the_workspace {
            Ok(None) => {}
            Ok(Some(_)) => return Err(taken()),
            Err(error) => {
                return Err(CreateLiveRuleError::StoreFailed {
                    rule: name.clone(),
                    reason: unread::<Rule>(error),
                });
            }
        }
        self.resolve(&rule).await.map_err(|unresolvable| match unresolvable {
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
        let uuid = self.workspace.lock().await.live_rule_uuid();
        {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(CreateLiveRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            if running.live.contains_key(&name) {
                return Err(taken());
            }
            running.live.insert(
                name.clone(),
                LiveRule {
                    rule: rule.clone(),
                    uuid,
                },
            );
        }
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name,
        });
        Ok(rule)
    }

    /// Replaces the live rule of an environment that has the name of `rule`, and returns it as the environment holds
    /// it now. It keeps the UUID it had.
    ///
    /// Everything but the name changes. A rule that is armed stays armed with the change, and none of its counters
    /// resets. Whoever follows the host learns of it.
    ///
    /// # Errors
    ///
    /// - [`UpdateLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`UpdateLiveRuleError::NotFound`] when the environment has no live rule of that name.
    /// - [`UpdateLiveRuleError::StoreFailed`], [`UpdateLiveRuleError::WorkspaceInvalid`] and
    ///   [`UpdateLiveRuleError::Unresolved`] when the rule does not resolve against the workspace.
    pub async fn update_live_rule(
        &self,
        environment: &EnvironmentName,
        rule: Rule,
    ) -> Result<Rule, UpdateLiveRuleError> {
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
        let resolved = self.resolve(&rule).await.map_err(|unresolvable| match unresolvable {
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
        {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(UpdateLiveRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            let Some(live) = running.live.get_mut(&name) else {
                return Err(not_found());
            };
            live.rule = rule.clone();
            // A rule that is not armed has no arming to keep, and arms with the change when someone arms it.
            let _ = running.engine.rules().retune(resolved);
        }
        self.publish(HostChange::RuleChanged {
            environment: environment.clone(),
            rule: name,
        });
        Ok(rule)
    }

    /// Removes a live rule from an environment, armed or not. Whoever follows the host learns of it.
    ///
    /// # Errors
    ///
    /// - [`DeleteLiveRuleError::NotRunning`] when no environment of that name runs.
    /// - [`DeleteLiveRuleError::NotFound`] when the environment has no live rule of that name.
    pub fn delete_live_rule(&self, environment: &EnvironmentName, name: &RuleName) -> Result<(), DeleteLiveRuleError> {
        {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(DeleteLiveRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            if running.live.remove(name).is_none() {
                return Err(DeleteLiveRuleError::NotFound {
                    environment: environment.clone(),
                    rule: name.clone(),
                });
            }
            // A rule that is not armed has nothing to disarm.
            let _ = running.engine.rules().disarm(name);
        }
        self.publish(HostChange::RuleRemoved {
            environment: environment.clone(),
            rule: name.clone(),
        });
        Ok(())
    }

    /// Keeps a live rule of an environment in the workspace, under its name and its UUID, and returns it as the
    /// workspace holds it.
    ///
    /// The rule stays in the state it has in the environment, and every other environment holds it disarmed,
    /// as every rule of the workspace. Whoever follows the host learns that the live rule left the environment,
    /// that the workspace has the rule, and the state of the rule in the environment, in that order.
    ///
    /// # Errors
    ///
    /// - [`KeepRuleError::NotRunning`] when no environment of that name runs.
    /// - [`KeepRuleError::NotFound`] when the environment has no live rule of that name.
    /// - [`KeepRuleError::NotKept`] when the workspace did not take the rule.
    pub async fn keep(&self, environment: &EnvironmentName, name: &RuleName) -> Result<Rule, KeepRuleError> {
        let live = {
            let environments = self.environments();
            let Some(running) = environments.get(environment) else {
                return Err(KeepRuleError::NotRunning {
                    environment: environment.clone(),
                });
            };
            running.live.get(name).map(|live| (live.rule.clone(), live.uuid))
        };
        let Some((live, uuid)) = live else {
            return Err(KeepRuleError::NotFound {
                environment: environment.clone(),
                rule: name.clone(),
            });
        };
        self.workspace
            .lock()
            .await
            .keep(live.clone(), uuid)
            .await
            .map_err(|error| KeepRuleError::NotKept {
                rule: name.clone(),
                error,
            })?;
        if let Some(running) = self.environments().get_mut(environment) {
            running.live.remove(name);
        }
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
        Ok(live)
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
