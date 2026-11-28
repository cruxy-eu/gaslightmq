//! The scenario of an environment: activating, deactivating and advancing it.

use gaslightmq_backend::Backend;
use gaslightmq_definition_api::Store;
use gaslightmq_engine::{ActivateScenarioError, DeactivateScenarioError, ScenarioChange, StepScenarioError};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::{Change, ChangeSource};
use gaslightmq_rule::{Preset, PresetSet, Rule, RuleSet};
use gaslightmq_scenario::{ResolvedScenario, Scenario, ScenarioName, ScenarioPosition};
use gaslightmq_selector::{Selector, SelectorSet};
use gaslightmq_workspace::{Revision, Stores, Workspace, WorkspaceContext};

use super::{Host, unread};
use crate::journaling::{self, recorded};
use crate::{
    EnvironmentScenario, Expects, HostChange, NotRunningError, ScenarioActivationError, ScenarioDeactivationError,
    ScenarioStepError,
};

impl<C: WorkspaceContext> Host<C> {
    /// The scenario of an environment: where the active one stands, and the revision of the scenario there.
    ///
    /// A step that advanced by itself since the last look takes its number first, so the revision is the one that a
    /// change of the scenario expects.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn scenario_of(&self, environment: &EnvironmentName) -> Result<EnvironmentScenario, NotRunningError> {
        let not_running = |name| NotRunningError { name };
        self.change_rules(environment, not_running, |running, revisions| {
            Ok(EnvironmentScenario {
                active: running.engine.rules().scenario(),
                revision: revisions.of_the_scenario(),
            })
        })
    }

    /// Activates the scenario of that name in an environment, and returns the revision that the scenario of the
    /// environment has from then on.
    ///
    /// The scenario resolves against the workspace, and the backend of the environment says whether it supports the
    /// incidents that its rules fire. Then it arms its rules, and the rules of its first step when it is a Sequence,
    /// below the rules that someone armed by hand: see [`ArmedRules`](gaslightmq_engine::ArmedRules).
    /// The scenario runs the definition that it had when it was activated, until someone deactivates it.
    ///
    /// The activation is one change: the scenario and every rule that it armed take one number. The journal of the
    /// environment records the activation with the step it starts at, and every rule that armed, all with that
    /// revision and with `from`. Whoever follows the host learns that the scenario changed, and of every rule.
    ///
    /// With `expects`, the scenario only activates while the scenario of the environment has the revision that its
    /// sender saw.
    ///
    /// # Errors
    ///
    /// - [`ScenarioActivationError::OtherRun`] when the change expects a revision of another run.
    /// - [`ScenarioActivationError::NotRunning`] when no environment of that name runs.
    /// - [`ScenarioActivationError::NotFound`] when the workspace has no scenario of that name.
    /// - [`ScenarioActivationError::StoreFailed`] when a store of the workspace could not be read.
    /// - [`ScenarioActivationError::DoesNotFit`] when the scenario does not resolve against the rules, the selectors and the presets
    ///   of the workspace.
    /// - [`ScenarioActivationError::Changed`] when the scenario of the environment has another revision than the change
    ///   expects.
    /// - [`ScenarioActivationError::AlreadyActive`] when a scenario is active in the environment.
    /// - [`ScenarioActivationError::Unsupported`] when the backend can neither apply nor emulate an incident of the
    ///   scenario, with every such incident.
    pub async fn activate_scenario(
        &self,
        environment: &EnvironmentName,
        scenario: &ScenarioName,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, ScenarioActivationError> {
        let from = from.into();
        let expects = self
            .expected(expects)
            .map_err(|run| ScenarioActivationError::OtherRun { run })?;
        if !self.environments().contains_key(environment) {
            return Err(ScenarioActivationError::NotRunning {
                environment: environment.clone(),
            });
        }
        // The workspace stays as it is until the scenario is active: nobody changes what it resolved against.
        let workspace = self.workspace.lock().await;
        let definition = workspace.stores().scenarios().get(scenario).await;
        let definition = definition
            .map_err(|error| ScenarioActivationError::StoreFailed {
                scenario: scenario.clone(),
                reason: unread::<Scenario>(error),
            })?
            .ok_or_else(|| ScenarioActivationError::NotFound {
                scenario: scenario.clone(),
            })?;
        let resolved = resolve(&workspace, &definition).await?;
        let requirements = resolved.requirements();

        let not_running = |environment| ScenarioActivationError::NotRunning { environment };
        let (revision, change) = self.change_rules(environment, not_running, |running, revisions| {
            let revision = revisions.of_the_scenario();
            if expects.is_some_and(|expected| expected != revision) {
                return Err(ScenarioActivationError::Changed {
                    environment: environment.clone(),
                    revision,
                });
            }
            let already_active = |active: ScenarioName| ScenarioActivationError::AlreadyActive {
                environment: environment.clone(),
                scenario: active,
            };
            if let Some(active) = running.engine.rules().scenario() {
                return Err(already_active(active.scenario));
            }
            let support = running.backend.incident_support();
            support
                .check(&requirements)
                .map_err(|error| ScenarioActivationError::Unsupported {
                    environment: environment.clone(),
                    scenario: scenario.clone(),
                    error,
                })?;
            let change = running
                .engine
                .rules()
                .activate_scenario(resolved)
                .map_err(|ActivateScenarioError::AlreadyActive { scenario }| already_active(scenario))?;
            let revision = revisions.count_scenario(&change.disarmed, &change.armed);
            let activated = Change::ScenarioActivated {
                scenario: recorded(scenario.as_str()),
                step: step_of(&change.position),
            };
            journaling::record_scenario_change(running.engine.journal(), &change, activated, from, revision);
            running.scenario = Some(definition);
            Ok((revision, change))
        })?;
        drop(workspace);
        self.publish_scenario_change(environment, change);
        Ok(revision)
    }

    /// Deactivates the scenario of an environment, and returns the revision that the scenario of the environment has
    /// from then on.
    ///
    /// Every rule that the scenario armed disarms, and the rules that someone armed by hand stay. The deactivation is
    /// one change: the scenario and every rule that disarmed take one number. The journal of the environment records
    /// every rule that disarmed, and then the deactivation with the step the scenario stood at.
    /// Whoever follows the host learns that the scenario changed, and of every rule.
    ///
    /// With `expects`, the scenario only deactivates while it has the revision that its sender saw.
    ///
    /// # Errors
    ///
    /// - [`ScenarioDeactivationError::OtherRun`] when the change expects a revision of another run.
    /// - [`ScenarioDeactivationError::NotRunning`] when no environment of that name runs.
    /// - [`ScenarioDeactivationError::Changed`] when the scenario has another revision than the change expects.
    /// - [`ScenarioDeactivationError::NotActive`] when no scenario is active in the environment.
    pub fn deactivate_scenario(
        &self,
        environment: &EnvironmentName,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, ScenarioDeactivationError> {
        let from = from.into();
        let expects = self
            .expected(expects)
            .map_err(|run| ScenarioDeactivationError::OtherRun { run })?;
        let not_running = |environment| ScenarioDeactivationError::NotRunning { environment };
        let (revision, change) = self.change_rules(environment, not_running, |running, revisions| {
            let revision = revisions.of_the_scenario();
            if expects.is_some_and(|expected| expected != revision) {
                return Err(ScenarioDeactivationError::Changed {
                    environment: environment.clone(),
                    revision,
                });
            }
            let change =
                running
                    .engine
                    .rules()
                    .deactivate_scenario()
                    .map_err(
                        |DeactivateScenarioError::NotActive| ScenarioDeactivationError::NotActive {
                            environment: environment.clone(),
                        },
                    )?;
            let revision = revisions.count_scenario(&change.disarmed, &change.armed);
            let deactivated = Change::ScenarioDeactivated {
                scenario: recorded(change.position.scenario.as_str()),
                step: step_of(&change.position),
            };
            journaling::record_scenario_change(running.engine.journal(), &change, deactivated, from, revision);
            running.scenario = None;
            Ok((revision, change))
        })?;
        self.publish_scenario_change(environment, change);
        Ok(revision)
    }

    /// Ends the step of the scenario of an environment, whatever its condition says, starts the next one, and returns
    /// the revision that the scenario of the environment has from then on.
    ///
    /// The rules of the step disarm, and those of the next step arm. After the last step no step runs, and the rules
    /// of the scenario stay armed until someone deactivates it. The step is one change: the scenario and every rule
    /// that it disarmed and armed take one number. The journal of the environment records the rules that disarmed,
    /// the step that started, and the rules that armed, with `from`. Whoever follows the host learns that the
    /// scenario changed, and of every rule.
    ///
    /// A step expects the scenario as its sender saw it: two clients that both advance from one step advance the
    /// scenario once, and the second one gets [`ScenarioStepError::Changed`]. A step has no inverse.
    ///
    /// # Errors
    ///
    /// - [`ScenarioStepError::OtherRun`] when the change expects a revision of another run.
    /// - [`ScenarioStepError::NotRunning`] when no environment of that name runs.
    /// - [`ScenarioStepError::Changed`] when the scenario has another revision than the change expects.
    /// - [`ScenarioStepError::NotActive`] when no scenario is active in the environment.
    /// - [`ScenarioStepError::NoStep`] when the scenario runs no step: it is no Sequence, or it ran its last step.
    pub fn step_scenario(
        &self,
        environment: &EnvironmentName,
        expects: Option<Expects>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, ScenarioStepError> {
        let from = from.into();
        let expects = self
            .expected(expects)
            .map_err(|run| ScenarioStepError::OtherRun { run })?;
        let not_running = |environment| ScenarioStepError::NotRunning { environment };
        let (revision, change) = self.change_rules(environment, not_running, |running, revisions| {
            let revision = revisions.of_the_scenario();
            if expects.is_some_and(|expected| expected != revision) {
                return Err(ScenarioStepError::Changed {
                    environment: environment.clone(),
                    revision,
                });
            }
            let change = running.engine.rules().step_scenario().map_err(|error| match error {
                StepScenarioError::NotActive => ScenarioStepError::NotActive {
                    environment: environment.clone(),
                },
                StepScenarioError::NoStep { scenario } => ScenarioStepError::NoStep {
                    environment: environment.clone(),
                    scenario,
                },
            })?;
            let revision = revisions.count_scenario(&change.disarmed, &change.armed);
            let advanced = Change::StepAdvanced {
                scenario: recorded(change.position.scenario.as_str()),
                step: step_of(&change.position),
            };
            journaling::record_scenario_change(running.engine.journal(), &change, advanced, from, revision);
            Ok((revision, change))
        })?;
        self.publish_scenario_change(environment, change);
        Ok(revision)
    }

    /// Tells whoever follows the host that the scenario of the environment changed, and so did every rule that the
    /// change disarmed or armed.
    fn publish_scenario_change(&self, environment: &EnvironmentName, change: ScenarioChange) {
        self.publish(HostChange::ScenarioChanged {
            environment: environment.clone(),
        });
        for rule in change.disarmed.into_iter().chain(change.armed) {
            self.publish(HostChange::RuleChanged {
                environment: environment.clone(),
                rule,
            });
        }
    }
}

/// The step that runs, as an entry records it.
fn step_of(position: &ScenarioPosition) -> Option<gaslightmq_journal::RecordedName> {
    position.step.as_ref().map(|step| recorded(step.as_str()))
}

/// Resolves `scenario` against the rules, the selectors and the presets of `workspace`.
async fn resolve<C: WorkspaceContext>(
    workspace: &Workspace<C>,
    scenario: &Scenario,
) -> Result<ResolvedScenario, ScenarioActivationError> {
    let stores = workspace.stores();
    let failed = |reason| ScenarioActivationError::StoreFailed {
        scenario: scenario.name().clone(),
        reason,
    };
    let rules = stores
        .rules()
        .list()
        .await
        .map_err(|error| failed(unread::<Rule>(error)))?;
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
    let does_not_fit = |reason: String| ScenarioActivationError::DoesNotFit {
        scenario: scenario.name().clone(),
        reason,
    };
    let rules = RuleSet::new(rules).map_err(|error| does_not_fit(error.to_string()))?;
    let selectors = SelectorSet::new(selectors).map_err(|error| does_not_fit(error.to_string()))?;
    let presets = PresetSet::new(presets).map_err(|error| does_not_fit(error.to_string()))?;
    scenario
        .resolve(&rules, &selectors, &presets)
        .map_err(|error| does_not_fit(error.to_string()))
}
