//! What someone does at a pad: the intent of a surface, acted on in one environment.

use std::sync::atomic::Ordering;

use gaslightmq_clock::Clock;
use gaslightmq_definition_api::Store;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{ArmedRule, Preset, PresetSet, Rule, RuleName};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_surface::{Hold, Intent, LayoutName, Pad, PadLayout};
use gaslightmq_workspace::{Stores, WorkspaceContext};

use super::{Host, Running, unread};
use crate::{
    ArmRuleError, CreateLiveRuleError, EnvironmentRule, EnvironmentRulesError, IntentEffect, IntentError,
    UpdateLiveRuleError,
};

impl<C: WorkspaceContext> Host<C> {
    /// Acts on what someone did at a pad of a layout, in one environment, and answers with what that did.
    ///
    /// The pad points at the rule that has [the name of its position](PadLayout::rule_name): a rule of the
    /// workspace, or a live rule of the environment.
    /// An intent says the state it wants, and one that finds that state there changes nothing.
    ///
    /// | Intent | The host |
    /// |---|---|
    /// | [`Arm`](Intent::Arm) | Arms the rule of the pad, unless it is armed. A pad without a rule gets [the rule of its position](PadLayout::rule) as a live rule, made and armed in one step. |
    /// | [`Disarm`](Intent::Disarm) | Disarms the rule of the pad, unless it is not armed or the pad has none. |
    /// | [`Hold`](Intent::Hold) | Arms the rule of the pad as `Arm` does, and adds a [`Hold`] to it, which the answer holds. |
    /// | [`Release`](Intent::Release) | Takes the hold away, and disarms the rule when that was its last hold and holds armed it. |
    /// | [`Turn`](Intent::Turn) | Changes the live rule of the pad as the [encoders](crate::HostConfiguration::encoders) say. An armed rule keeps its counters. |
    ///
    /// The host counts the holds of every rule, and a rule stays armed while anyone holds its pad.
    /// A release never disarms a rule that something other than holds armed: a press in latch, a command, a scenario
    /// step or a create armed. A hold of such a rule still answers with its hold, and its release changes nothing.
    /// A disarm ends every hold of the rule, whatever disarms it: a press in latch, a command, a lifetime,
    /// or deleting the rule. A release of a hold that a disarm ended changes nothing,
    /// also when someone armed the rule again since.
    ///
    /// A turn changes a live rule, and never a rule of the workspace, which changes in the workspace.
    /// Whoever follows the host learns what the rule became.
    ///
    /// # Errors
    ///
    /// - [`IntentError::NotRunning`] when no environment of that name runs.
    /// - [`IntentError::UnknownLayout`] when the workspace has no layout of the pad.
    /// - [`IntentError::StoreFailed`] when a store of the workspace could not be read.
    /// - [`IntentError::Refused`] when the host cannot act on the intent: the pad lies outside its layout, its rule
    ///   cannot be made or armed, it has no rule to turn, or the turn cannot change the rule.
    pub async fn send_intent(
        &self,
        environment: &EnvironmentName,
        intent: &Intent,
    ) -> Result<IntentEffect, IntentError> {
        let refused = |reason: String| IntentError::Refused { reason };
        if !self.environments().contains_key(environment) {
            return Err(IntentError::NotRunning {
                environment: environment.clone(),
            });
        }
        let pad = intent.pad();
        let (layout, selectors, presets) = self.played(pad.layout()).await?;
        let Some(name) = layout.rule_name(pad) else {
            return Err(refused(format!("the {pad} lies outside its layout")));
        };
        let existing = self.rule_at(environment, &name).await?;
        let mut made = false;
        let mut hold = None;
        let changed = match intent {
            Intent::Arm { .. } | Intent::Hold { .. } => {
                let armed = if let Some(existing) = existing {
                    self.arm_unless_armed(environment, &existing.rule).await?
                } else if let Some(made_by_another) = self
                    .make_armed(environment, &name, pad, &layout, &selectors, &presets)
                    .await?
                {
                    self.arm_unless_armed(environment, &made_by_another).await?
                } else {
                    made = true;
                    true
                };
                if matches!(intent, Intent::Hold { .. }) {
                    hold = self.add_hold(environment, &name, armed)?;
                }
                armed
            }
            Intent::Disarm { .. } => existing.is_some() && self.disarm_if_armed(environment, &name)?,
            Intent::Release { hold: released, .. } => self.release_hold(environment, &name, *released)?,
            Intent::Turn { turn, .. } => {
                let Some(existing) = existing else {
                    return Err(refused(format!("the {pad} has no rule to turn")));
                };
                if !existing.live {
                    return Err(refused(format!(
                        "rule {:?} is a rule of the workspace, which a turn does not change",
                        name.as_str()
                    )));
                }
                let turned = self
                    .encoders
                    .apply(*turn, &existing.rule, &presets)
                    .map_err(|error| refused(error.to_string()))?;
                let changed = turned != existing.rule;
                if changed {
                    self.update_live_rule(environment, turned)
                        .await
                        .map_err(|error| match error {
                            UpdateLiveRuleError::NotRunning { environment } => IntentError::NotRunning { environment },
                            UpdateLiveRuleError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
                            other => refused(other.to_string()),
                        })?;
                }
                changed
            }
        };
        let rule = self.rule_at(environment, &name).await?;
        Ok(IntentEffect {
            rule,
            made,
            changed,
            hold,
        })
    }

    /// The rule of the environment that has the name, with its state there.
    async fn rule_at(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
    ) -> Result<Option<EnvironmentRule>, IntentError> {
        self.rule_of(environment, name).await.map_err(|error| match error {
            EnvironmentRulesError::NotRunning { environment } => IntentError::NotRunning { environment },
            EnvironmentRulesError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
        })
    }

    /// Makes the live rule of the position of `pad`, which has the name `name`, and arms it, in one step.
    ///
    /// Returns `None` when this call made it. When another press made it in the meantime, it returns that rule, which
    /// this call neither made nor armed.
    async fn make_armed(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        pad: &Pad,
        layout: &PadLayout,
        selectors: &SelectorSet,
        presets: &PresetSet,
    ) -> Result<Option<Rule>, IntentError> {
        let refused = |reason: String| IntentError::Refused { reason };
        let Some(rule) = layout.rule(pad, selectors, presets) else {
            return Err(refused(format!("the {pad} has no rule to make")));
        };
        match self.create_live_rule_armed(environment, rule).await {
            Ok(_) => Ok(None),
            Err(CreateLiveRuleError::NotRunning { environment }) => Err(IntentError::NotRunning { environment }),
            Err(CreateLiveRuleError::StoreFailed { reason, .. }) => Err(IntentError::StoreFailed { reason }),
            Err(taken @ CreateLiveRuleError::NameTaken { .. }) => match self.rule_at(environment, name).await? {
                // Another press made the rule of the pad first.
                Some(made) => Ok(Some(made.rule)),
                None => Err(refused(taken.to_string())),
            },
            Err(other) => Err(refused(other.to_string())),
        }
    }

    /// Arms `rule` in the environment, and returns whether it was not armed. A rule that is armed stays as it is.
    async fn arm_unless_armed(&self, environment: &EnvironmentName, rule: &Rule) -> Result<bool, IntentError> {
        match self.arm(environment, rule).await {
            Ok(_) => Ok(true),
            Err(ArmRuleError::AlreadyArmed { .. }) => Ok(false),
            Err(ArmRuleError::NotRunning { environment }) => Err(IntentError::NotRunning { environment }),
            Err(ArmRuleError::StoreFailed { reason, .. }) => Err(IntentError::StoreFailed { reason }),
            Err(other) => Err(IntentError::Refused {
                reason: other.to_string(),
            }),
        }
    }

    /// Disarms the rule `name` in the environment, and returns whether it was armed.
    fn disarm_if_armed(&self, environment: &EnvironmentName, name: &RuleName) -> Result<bool, IntentError> {
        self.with_running_rule(environment, |running| running.engine.rules().disarm(name).is_ok())
    }

    /// Adds a hold to the rule `name`, which is armed in the environment. Returns `None` when someone disarmed the
    /// rule since it was armed, so there is nothing to hold.
    ///
    /// `armed_it` says whether the press of the hold armed the rule. Only a rule that holds armed disarms when its last
    /// hold goes.
    fn add_hold(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        armed_it: bool,
    ) -> Result<Option<Hold>, IntentError> {
        self.with_running_rule(environment, |running| {
            let armed = armed_now(running, name)?;
            let hold = Hold::new(self.holds_given.fetch_add(1, Ordering::Relaxed) + 1);
            running.holds.add(&armed, hold, armed_it);
            Some(hold)
        })
    }

    /// Takes `hold` away from the rule `name`, and disarms the rule when that was its last hold and holds armed it.
    /// Returns whether the rule disarmed.
    fn release_hold(&self, environment: &EnvironmentName, name: &RuleName, hold: Hold) -> Result<bool, IntentError> {
        self.with_running_rule(environment, |running| {
            let armed = armed_now(running, name);
            running.holds.release(name, hold, armed.as_ref()) && running.engine.rules().disarm(name).is_ok()
        })
    }

    /// Runs `act` on the environment that runs, which holds its lock, so what it reads and changes is one step.
    fn with_running_rule<T>(
        &self,
        environment: &EnvironmentName,
        act: impl FnOnce(&mut Running) -> T,
    ) -> Result<T, IntentError> {
        let mut environments = self.environments();
        let Some(running) = environments.get_mut(environment) else {
            return Err(IntentError::NotRunning {
                environment: environment.clone(),
            });
        };
        Ok(act(running))
    }

    /// The layout of that name, with the selectors and the presets that its pads stand on.
    async fn played(&self, layout: &LayoutName) -> Result<(PadLayout, SelectorSet, PresetSet), IntentError> {
        let workspace = self.workspace.lock().await;
        let stores = workspace.stores();
        let failed = |reason: String| IntentError::StoreFailed { reason };
        let found = stores
            .layouts()
            .get(layout)
            .await
            .map_err(|error| failed(unread::<PadLayout>(error)))?;
        let Some(found) = found else {
            return Err(IntentError::UnknownLayout { layout: layout.clone() });
        };
        let selectors = stores
            .selectors()
            .list()
            .await
            .map_err(|error| failed(unread::<Selector>(error)))?;
        let presets = stores
            .presets()
            .list()
            .await
            .map_err(|error| failed(unread::<Preset>(error)))?;
        let invalid = |reason: String| IntentError::Refused {
            reason: format!("the workspace is not valid: {reason}"),
        };
        let selectors = SelectorSet::new(selectors).map_err(|error| invalid(error.to_string()))?;
        let presets = PresetSet::new(presets).map_err(|error| invalid(error.to_string()))?;
        Ok((found, selectors, presets))
    }
}

/// The arming of the rule `name` in the environment, unless the rule is not armed or its lifetime ended.
fn armed_now(running: &Running, name: &RuleName) -> Option<ArmedRule> {
    let now = running.engine.clock().now();
    running
        .engine
        .rules()
        .snapshot()
        .get(name)
        .filter(|armed| !armed.arming().has_ended(now))
        .cloned()
}
