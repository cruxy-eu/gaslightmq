//! How the rules of a scenario combine: the mode, with the steps of a Sequence.

use crate::Step;

/// How the rules of a scenario combine: a conflict strategy, with or without steps.
///
/// Lies that do not conflict combine on their own, in every mode.
/// The mode only decides between lies that conflict.
///
/// | Mode | A conflict goes to | Steps |
/// |---|---|---|
/// | [`Live`](Self::Live) | The rule that was armed most recently. | No |
/// | [`Chain`](Self::Chain) | The rule that comes first in the scenario's order. | No |
/// | [`Sequence`](Self::Sequence) | The rule that comes first in the step's order, and then in the scenario's. | Yes |
///
/// Only a Sequence has steps, so only its variant holds them.
/// The [`Scenario`](crate::Scenario) checks that a Sequence has at least one step, and that their names are unique.
///
/// ```
/// use gaslightmq_scenario::prelude::*;
///
/// let sequence = Mode::Sequence(vec![Step::new("warm-up".parse()?), Step::new("back-to-normal".parse()?)]);
///
/// assert_eq!(sequence.name(), "sequence");
/// assert_eq!(sequence.steps().len(), 2);
/// assert_eq!(Mode::Chain.name(), "chain");
/// assert!(Mode::Chain.steps().is_empty());
/// # Ok::<(), StepNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Mode {
    /// For exploring: the rule that was armed most recently wins a conflict.
    /// Activating the scenario arms its rules in the order it lists them, so a rule further down counts as armed
    /// more recently.
    Live,
    /// For steady background behaviour with priorities: the rule that comes first in the scenario wins a conflict.
    Chain,
    /// For exact, reproducible test cases: the steps, in the order they run.
    Sequence(Vec<Step>),
}

impl Mode {
    /// The word for the mode, as a file writes it: `live`, `chain` or `sequence`.
    pub const fn name(&self) -> &'static str {
        match self {
            Mode::Live => "live",
            Mode::Chain => "chain",
            Mode::Sequence(_) => "sequence",
        }
    }

    /// The steps of a Sequence, in the order they run. A Live and a Chain scenario have none.
    pub fn steps(&self) -> &[Step] {
        match self {
            Mode::Sequence(steps) => steps,
            Mode::Live | Mode::Chain => &[],
        }
    }
}
