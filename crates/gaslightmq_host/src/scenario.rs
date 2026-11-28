//! The scenario of an environment, as the host holds it.

use gaslightmq_scenario::ScenarioPosition;
use gaslightmq_workspace::Revision;

/// The scenario of an environment: where the active one stands, and the revision of the scenario there.
///
/// At most one scenario is active in an environment. Its revision is the number of the last change of the scenario
/// there: the start of the environment, an activation, a deactivation, or a step that advanced, by itself or because
/// someone advanced it. A change of the scenario [expects](crate::Expects) it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentScenario {
    /// The scenario that is active, and the step that runs. `None` while no scenario is active.
    pub active: Option<ScenarioPosition>,
    /// The revision of the scenario of the environment.
    pub revision: Revision,
}
