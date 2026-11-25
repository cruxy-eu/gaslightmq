//! What someone does at a pad: the intent of a surface, acted on in one environment.

use gaslightmq_definition_api::Store;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{Preset, PresetSet};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_surface::{Intent, LayoutName, PadLayout};
use gaslightmq_workspace::{Stores, WorkspaceContext};

use super::{Host, unread};
use crate::{ArmRuleError, CreateLiveRuleError, EnvironmentRulesError, IntentError, UpdateLiveRuleError};

impl<C: WorkspaceContext> Host<C> {
    /// Acts on what someone did at a pad of a layout, in one environment.
    ///
    /// The pad points at the rule that has [the name of its position](PadLayout::rule_name): a rule of the
    /// workspace, or a live rule of the environment.
    ///
    /// | Intent | The host |
    /// |---|---|
    /// | [`Arm`](Intent::Arm) | Arms the rule of the pad. A pad without a rule gets [the rule of its position](PadLayout::rule) first, as a live rule. |
    /// | [`Disarm`](Intent::Disarm) | Disarms the rule of the pad. |
    /// | [`Turn`](Intent::Turn) | Changes the live rule of the pad as the [encoders](crate::HostConfiguration::encoders) say. An armed rule keeps its counters. |
    ///
    /// A turn changes a live rule, and never a rule of the workspace, which changes in the workspace.
    /// Whoever follows the host learns what the rule became.
    ///
    /// # Errors
    ///
    /// - [`IntentError::NotRunning`] when no environment of that name runs.
    /// - [`IntentError::UnknownLayout`] when the workspace has no layout of the pad.
    /// - [`IntentError::StoreFailed`] when a store of the workspace could not be read.
    /// - [`IntentError::Refused`] when the intent changed nothing: the pad lies outside its layout, it has no rule to
    ///   disarm or to turn, its rule cannot be made, armed or disarmed, or the turn has nothing to change.
    pub async fn send_intent(&self, environment: &EnvironmentName, intent: &Intent) -> Result<(), IntentError> {
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
        let existing = self.rule_of(environment, &name).await.map_err(|error| match error {
            EnvironmentRulesError::NotRunning { environment } => IntentError::NotRunning { environment },
            EnvironmentRulesError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
        })?;
        match intent {
            Intent::Arm { .. } => {
                let rule = if let Some(existing) = existing {
                    existing.rule
                } else {
                    // The pad has no rule yet, so it gets the live rule of its position.
                    let Some(rule) = layout.rule(pad, &selectors, &presets) else {
                        return Err(refused(format!("the {pad} has no rule to make")));
                    };
                    self.create_live_rule(environment, rule)
                        .await
                        .map_err(|error| match error {
                            CreateLiveRuleError::NotRunning { environment } => IntentError::NotRunning { environment },
                            CreateLiveRuleError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
                            other => refused(other.to_string()),
                        })?
                };
                self.arm(environment, &rule).await.map_err(|error| match error {
                    ArmRuleError::NotRunning { environment } => IntentError::NotRunning { environment },
                    ArmRuleError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
                    other => refused(other.to_string()),
                })?;
            }
            Intent::Disarm { .. } => {
                if existing.is_none() {
                    return Err(refused(format!("the {pad} has no rule to disarm")));
                }
                let armed = self.rules(environment).map_err(|error| IntentError::NotRunning {
                    environment: error.name,
                })?;
                armed.disarm(&name).map_err(|error| refused(error.to_string()))?;
            }
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
                self.update_live_rule(environment, turned)
                    .await
                    .map_err(|error| match error {
                        UpdateLiveRuleError::NotRunning { environment } => IntentError::NotRunning { environment },
                        UpdateLiveRuleError::StoreFailed { reason, .. } => IntentError::StoreFailed { reason },
                        other => refused(other.to_string()),
                    })?;
            }
        }
        Ok(())
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
