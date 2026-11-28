//! The capture of an environment: starting and stopping it, and saving what it captured as a scenario.

use gaslightmq_definition_api::Store;
use gaslightmq_engine::{StartCaptureError, StopCaptureError};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::ChangeSource;
use gaslightmq_rule::{Rule, RuleName};
use gaslightmq_scenario::{Captured, CapturedRule, Scenario, ScenarioName, ScenarioTitle};
use gaslightmq_workspace::{CreateError, DefinitionName, Revision, Stores, Workspace, WorkspaceContext};

use super::live::{Kept, Unkept};
use super::{Host, unread};
use crate::{CaptureSaveError, CaptureStartError, CaptureStopError, CreateDefinitionError, HostChange};

impl<C: WorkspaceContext> Host<C> {
    /// Starts a capture of an environment: from here on, its journal marks every arm and disarm of the live set,
    /// with its time.
    ///
    /// The capture starts with the live set as it stands: the rules that someone armed by hand, the one armed most
    /// recently first, each live rule as it is defined now. The journal of the environment records that the capture
    /// started, with `from`. A start has no inverse and no revision. What the last capture captured goes.
    ///
    /// # Errors
    ///
    /// - [`CaptureStartError::NotRunning`] when no environment of that name runs.
    /// - [`CaptureStartError::AlreadyCapturing`] when the environment captures already.
    pub fn start_capture(
        &self,
        environment: &EnvironmentName,
        from: impl Into<ChangeSource>,
    ) -> Result<(), CaptureStartError> {
        let from = from.into();
        let not_running = |environment| CaptureStartError::NotRunning { environment };
        // The armed rules are caught up first, so the live set holds no rule whose lifetime ended uncounted.
        self.change_rules(environment, not_running, |running, _| {
            let live = running
                .engine
                .rules()
                .live_set()
                .into_iter()
                .map(|rule| match running.live.get(&rule) {
                    Some(definition) => CapturedRule::from(definition.clone()),
                    None => CapturedRule::from(rule),
                });
            let live: Vec<CapturedRule> = live.collect();
            running
                .engine
                .journal()
                .start_capture(live, from)
                .map_err(
                    |StartCaptureError::AlreadyCapturing| CaptureStartError::AlreadyCapturing {
                        environment: environment.clone(),
                    },
                )?;
            running.captured = None;
            Ok(())
        })
    }

    /// Stops the capture of an environment, and returns what it captured: a step for the live set as the capture
    /// found it, and one for every change after it, each ending on the time until the next change, and the live rules
    /// that the steps arm, as they were armed.
    ///
    /// The journal of the environment records that the capture stopped, with `from`. The host keeps what the capture
    /// captured, so whoever wants it [saves it as a scenario](Self::save_capture).
    ///
    /// # Errors
    ///
    /// - [`CaptureStopError::NotRunning`] when no environment of that name runs.
    /// - [`CaptureStopError::NotCapturing`] when the environment captures nothing.
    pub fn stop_capture(
        &self,
        environment: &EnvironmentName,
        from: impl Into<ChangeSource>,
    ) -> Result<Captured, CaptureStopError> {
        let from = from.into();
        let not_running = |environment| CaptureStopError::NotRunning { environment };
        // The armed rules are caught up first, so a lifetime that ended before the stop is a change of the capture.
        self.change_rules(environment, not_running, |running, _| {
            let captured = running
                .engine
                .journal()
                .stop_capture(from)
                .map_err(|StopCaptureError::NotCapturing| CaptureStopError::NotCapturing {
                    environment: environment.clone(),
                })?;
            running.captured = Some(captured.clone());
            Ok(captured)
        })
    }

    /// Saves what the last capture of an environment captured as a Sequence scenario of that name and title, with the
    /// seed that the proxy of the environment arms a rule by hand with, and returns the revision of the scenario.
    ///
    /// The save keeps the live rules that the scenario uses, as they were armed:
    ///
    /// | The rule of the capture | The save |
    /// |---|---|
    /// | A live rule of the environment, defined as it was armed | [Keeps it](Self::keep): it goes on as a rule of the workspace. |
    /// | A rule of the workspace, defined as it was armed | Leaves it: someone kept the live rule since. |
    /// | No rule has its name | Creates it in the workspace: someone deleted the live rule since, or it armed again with another definition. |
    /// | A live rule that someone changed since it armed | Fails with [`CaptureSaveError::RuleChanged`]. |
    ///
    /// Then it creates the scenario. The workspace stays taken from the first check to the last write. Every keep and
    /// every create records in the journals as on its own, with `from`, and whoever follows the host learns of each.
    /// What the host keeps of the capture stays, so the capture can be saved under another name too.
    ///
    /// # Errors
    ///
    /// - [`CaptureSaveError::NotRunning`] when no environment of that name runs.
    /// - [`CaptureSaveError::NothingCaptured`] when no capture stopped there since the last one started.
    /// - [`CaptureSaveError::RuleChanged`] when a live rule that the capture armed changed since.
    /// - [`CaptureSaveError::NameTaken`] when a live rule of another environment, or a rule that a scenario owns, has
    ///   the name of a rule of the capture.
    /// - [`CaptureSaveError::NotCreated`] when the workspace did not take the scenario or a rule: the workspace has a
    ///   scenario of the name, the scenario does not fit, or a store failed. A taken name of the scenario keeps no rule.
    pub async fn save_capture(
        &self,
        environment: &EnvironmentName,
        scenario: ScenarioName,
        title: Option<ScenarioTitle>,
        from: impl Into<ChangeSource>,
    ) -> Result<Revision, CaptureSaveError> {
        let from = from.into();
        let held = self.with_running(environment, |running| (running.captured.clone(), running.seed));
        let (captured, seed) = held.map_err(|_| CaptureSaveError::NotRunning {
            environment: environment.clone(),
        })?;
        let captured = captured.ok_or_else(|| CaptureSaveError::NothingCaptured {
            environment: environment.clone(),
        })?;
        let definition = captured.scenario(scenario, seed);
        let definition = match title {
            Some(title) => definition.with_title(title),
            None => definition,
        };
        let mut saved = Saved::default();
        let outcome = {
            let mut workspace = self.workspace.lock().await;
            self.save_in(&mut workspace, environment, &captured, definition, from, &mut saved)
                .await
        };
        for rule in &saved.kept {
            self.publish_kept(environment, rule);
        }
        for name in saved.created {
            self.publish(HostChange::DefinitionChanged { name });
        }
        self.publish_unsaved().await;
        outcome
    }

    /// Keeps the rules of `captured` in `workspace` and creates `scenario` there, and notes in `saved` what changed,
    /// also when a later part fails.
    async fn save_in(
        &self,
        workspace: &mut Workspace<C>,
        environment: &EnvironmentName,
        captured: &Captured,
        scenario: Scenario,
        from: ChangeSource,
        saved: &mut Saved,
    ) -> Result<Revision, CaptureSaveError> {
        let not_created = |error| CaptureSaveError::NotCreated { error };
        let name: DefinitionName = scenario.name().clone().into();
        match workspace.stores().scenarios().get(scenario.name()).await {
            Ok(None) => {}
            Ok(Some(_)) => return Err(not_created(CreateError::NameTaken { name })),
            Err(error) => {
                let reason = unread::<Scenario>(error);
                return Err(not_created(CreateError::StoreFailed { name, reason }));
            }
        }
        for rule in captured.rules() {
            let live = self.with_running(environment, |running| running.live.contains_key(rule.name()));
            let live = live.map_err(|_| CaptureSaveError::NotRunning {
                environment: environment.clone(),
            })?;
            if live {
                let kept = Kept::Definition(rule);
                let keep = self.keep_in(workspace, environment, rule.name(), kept, from).await;
                keep.map_err(|error| kept_error(environment, rule.name(), error))?;
                saved.kept.push(rule.name().clone());
                continue;
            }
            match workspace.stores().rules().get(rule.name()).await {
                // Someone kept the live rule since the capture armed it.
                Ok(Some(kept)) if kept == *rule => continue,
                Ok(_) => {}
                Err(error) => {
                    let reason = unread::<Rule>(error);
                    let name = rule.name().clone().into();
                    return Err(not_created(CreateError::StoreFailed { name, reason }));
                }
            }
            let created = self.create_in(workspace, rule.clone(), from).await;
            created.map_err(created_error)?;
            saved.created.push(rule.name().clone().into());
        }
        let revision = self.create_in(workspace, scenario, from).await.map_err(created_error)?;
        saved.created.push(name);
        Ok(revision)
    }
}

/// What a save changed in the workspace, for whoever follows the host.
#[derive(Default)]
struct Saved {
    /// The live rules of the environment that went on as rules of the workspace.
    kept: Vec<RuleName>,
    /// The definitions that the save created: rules, and the scenario last.
    created: Vec<DefinitionName>,
}

/// The error of a save that a keep of `rule` stopped.
fn kept_error(environment: &EnvironmentName, rule: &RuleName, unkept: Unkept) -> CaptureSaveError {
    match unkept {
        Unkept::NotRunning => CaptureSaveError::NotRunning {
            environment: environment.clone(),
        },
        // The live rule went, or someone changed it, since the save looked at it.
        Unkept::NotFound | Unkept::Changed { .. } => CaptureSaveError::RuleChanged {
            environment: environment.clone(),
            rule: rule.clone(),
        },
        Unkept::NameTaken { holder } => CaptureSaveError::NameTaken {
            rule: rule.clone(),
            holder,
        },
        Unkept::NotKept { error } => CaptureSaveError::NotCreated { error },
    }
}

/// The error of a save that the create of a rule or of the scenario stopped.
fn created_error(error: CreateDefinitionError) -> CaptureSaveError {
    match error {
        CreateDefinitionError::NameTaken { rule, holder } => CaptureSaveError::NameTaken { rule, holder },
        CreateDefinitionError::NotCreated { error } => CaptureSaveError::NotCreated { error },
    }
}
