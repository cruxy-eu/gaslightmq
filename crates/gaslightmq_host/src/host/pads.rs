//! What someone does at a pad: the intent of a surface, acted on in one environment, and the presences whose
//! holds end with them.

use std::sync::atomic::Ordering;

use gaslightmq_clock::Clock;
use gaslightmq_definition_api::Store;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{ArmedRule, Preset, PresetSet, Rule, RuleName};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_surface::{Hold, Intent, LayoutName, Pad, PadLayout, PresenceId};
use gaslightmq_workspace::{Stores, WorkspaceContext};

use super::{Host, Running, caught_up, unread};
use crate::{
    ArmRuleError, CreateLiveRuleError, EnvironmentRule, EnvironmentRulesError, HostChange, IntentEffect, IntentError,
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
    /// An intent expects no revision: it acts on the rule as the rule is, and says the state it wants.
    /// The rule of the answer has the revision that the rule has in the environment now.
    /// An intent that changed the rule took one number for it, also where it made the rule and armed it.
    ///
    /// The intent comes from no [presence](Self::open_presence): from `gmq`, a test or another program.
    /// A hold that it adds ends by its release or by a disarm, and by nothing else.
    /// An intent of a client that can go away without a word comes [from its presence](Self::send_intent_from).
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
        self.act_on(None, environment, intent).await
    }

    /// Acts on what the client of `presence` did at a pad, as [`send_intent`](Self::send_intent) does.
    ///
    /// A hold that the intent adds belongs to the presence, and ends with it: see
    /// [`end_presence`](Self::end_presence). Any other intent does what it does from anyone.
    ///
    /// # Errors
    ///
    /// The errors of [`send_intent`](Self::send_intent), and [`IntentError::Refused`] for a hold of a presence that
    /// is not open: its client is gone, so nothing would end the hold. The rule stays as the press found it.
    pub async fn send_intent_from(
        &self,
        presence: PresenceId,
        environment: &EnvironmentName,
        intent: &Intent,
    ) -> Result<IntentEffect, IntentError> {
        self.act_on(Some(presence), environment, intent).await
    }

    /// Opens `presence`: a client of a control surface is there, such as a tab of the UI, and its holds end with it.
    ///
    /// Whoever lets clients connect opens a presence for a client that came, and [ends](Self::end_presence) it
    /// once the client is gone. The host waits for nothing itself: it knows no connection, and no time to wait
    /// for a client that may come back. A presence that is open already stays as it is.
    pub fn open_presence(&self, presence: PresenceId) {
        self.presences().insert(presence);
    }

    /// Ends `presence`: its client is gone, and every hold that came from it ends, in every environment.
    ///
    /// A client that is gone never sends its release.
    /// A rule disarms when that was its last hold and holds armed it, so it stays armed while another presence
    /// still holds its pad, and when something else armed it: a press in latch, a command, a scenario step or a
    /// create. A rule that disarms takes the next number, as with a release, and whoever follows the host learns
    /// of it.
    ///
    /// No press adds a hold to the presence from here on: see [`send_intent_from`](Self::send_intent_from).
    /// A presence that is not open has no holds, and ending it changes nothing.
    pub fn end_presence(&self, presence: PresenceId) {
        let mut changed = Vec::new();
        {
            // The environments stay taken from the moment the presence is no longer open until its holds went,
            // so no press adds a hold in between that nothing would end.
            let mut environments = self.environments();
            if !self.presences().remove(&presence) {
                return;
            }
            for (environment, running) in environments.iter_mut() {
                let (disarmed, uncounted) = caught_up(running, |running, revisions| {
                    let rules = running.engine.rules();
                    let (armed, now) = (rules.snapshot(), running.engine.clock().now());
                    let armed_now =
                        |name: &RuleName| armed.get(name).filter(|armed| !armed.arming().has_ended(now)).cloned();
                    let mut disarmed = running.holds.end_of(presence, armed_now);
                    // A rule that someone disarmed in this moment is disarmed, and that disarm took its number.
                    disarmed.retain(|rule| rules.disarm(rule).is_ok());
                    for rule in &disarmed {
                        revisions.count(rule, &rules.snapshot());
                    }
                    disarmed
                });
                let rules = disarmed.into_iter().chain(uncounted);
                changed.extend(rules.map(|rule| (environment.clone(), rule)));
            }
        }
        for (environment, rule) in changed {
            self.publish(HostChange::RuleChanged { environment, rule });
        }
    }

    /// Acts on an intent that comes from `from`, or from no presence.
    async fn act_on(
        &self,
        from: Option<PresenceId>,
        environment: &EnvironmentName,
        intent: &Intent,
    ) -> Result<IntentEffect, IntentError> {
        let refused = |reason: String| IntentError::Refused { reason };
        if !self.environments().contains_key(environment) {
            return Err(IntentError::NotRunning {
                environment: environment.clone(),
            });
        }
        if let (Intent::Hold { .. }, Some(presence)) = (intent, from)
            && !self.presences().contains(&presence)
        {
            return Err(not_open(presence));
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
                    hold = self.add_hold(environment, &name, armed, from)?;
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
                    self.update_live_rule(environment, turned, None)
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
        // An intent expects no revision: it acts on the rule as the rule is.
        match self.arm(environment, rule, None).await {
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
        self.disarm_when(environment, name, |_| true)
    }

    /// Disarms the rule `name` in the environment when `wanted` says so, and returns whether the rule disarmed.
    ///
    /// `wanted` and the disarm are one step: nobody else reads or changes the environment in between.
    /// A rule that disarmed takes the next number, and whoever follows the host learns of it.
    fn disarm_when(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        wanted: impl FnOnce(&mut Running) -> bool,
    ) -> Result<bool, IntentError> {
        let not_running = |environment| IntentError::NotRunning { environment };
        let disarmed = self.change_rules(environment, not_running, |running, revisions| {
            let disarmed = wanted(running) && running.engine.rules().disarm(name).is_ok();
            if disarmed {
                revisions.count(name, &running.engine.rules().snapshot());
            }
            Ok(disarmed)
        })?;
        if disarmed {
            self.publish(HostChange::RuleChanged {
                environment: environment.clone(),
                rule: name.clone(),
            });
        }
        Ok(disarmed)
    }

    /// Adds a hold to the rule `name`, which is armed in the environment. Returns `None` when someone disarmed the
    /// rule since it was armed, so there is nothing to hold.
    ///
    /// `armed_it` says whether the press of the hold armed the rule. Only a rule that holds armed disarms when its last
    /// hold goes. `of` names the presence that the press came from, when it came from one.
    ///
    /// # Errors
    ///
    /// [`IntentError::Refused`] when the presence ended since the press came. The rule is disarmed again when the
    /// press armed it, as one more change of the rule: nothing would end a hold of a client that is gone.
    fn add_hold(
        &self,
        environment: &EnvironmentName,
        name: &RuleName,
        armed_it: bool,
        of: Option<PresenceId>,
    ) -> Result<Option<Hold>, IntentError> {
        let mut added = Ok(None);
        // Adding the hold, or disarming the rule that nobody holds after all, is one step with the look at the presence.
        self.disarm_when(environment, name, |running| {
            if let Some(presence) = of
                && !self.presences().contains(&presence)
            {
                added = Err(not_open(presence));
                return armed_it;
            }
            if let Some(armed) = armed_now(running, name) {
                let hold = Hold::new(self.holds_given.fetch_add(1, Ordering::Relaxed) + 1);
                running.holds.add(&armed, hold, armed_it, of);
                added = Ok(Some(hold));
            }
            false
        })?;
        added
    }

    /// Takes `hold` away from the rule `name`, and disarms the rule when that was its last hold and holds armed it.
    /// Returns whether the rule disarmed.
    fn release_hold(&self, environment: &EnvironmentName, name: &RuleName, hold: Hold) -> Result<bool, IntentError> {
        self.disarm_when(environment, name, |running| {
            let armed = armed_now(running, name);
            running.holds.release(name, hold, armed.as_ref())
        })
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

/// A hold of `presence` is refused: the presence is not open, so nothing would end the hold.
fn not_open(presence: PresenceId) -> IntentError {
    IntentError::Refused {
        reason: format!("presence {presence} is not open, so nothing would end its hold"),
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
