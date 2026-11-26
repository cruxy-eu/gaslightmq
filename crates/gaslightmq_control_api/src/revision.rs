//! Revisions and runs: what a change expects of what it changes, and what it leaves.
//!
//! The revision is the one that an entry of the journal holds, so the control plane has one type for both:
//! what a client reads beside a rule, and what the journal says a change left.

use std::fmt;

use serde::{Deserialize, Serialize};

pub use gaslightmq_journal::Revision;

gaslightmq_resource::random_id! {
    /// A run is one life of a process, from its start to its stop.
    /// The process makes the ID once, at the start, and no file holds it.
    ///
    /// The [snapshot](crate::ProcessState) names the run of the process, so a client learns it when it connects,
    /// and learns of a restart when the next snapshot names another one.
    /// A change that names the revision it [expects](Expects) names the run that gave it.
    ///
    /// ```
    /// use gaslightmq_control_api::prelude::*;
    ///
    /// let run: RunId = serde_json::from_str("\"4be0643f1d98573b97cdca98a65347dd\"")?;
    /// assert_eq!(run.to_string(), "4be0643f1d98573b97cdca98a65347dd");
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    RunId, RunIdError, "run ID", serde
}

/// What a change expects of what it changes: the run that its sender read from, and the revision that it saw.
///
/// Several clients change one process: two tabs of the UI, `gmq`, a lifetime that ends.
/// A change that carries what its sender saw never overwrites what its sender has not seen.
///
/// - **A change that expects another revision changes nothing.**
///   Its error is `changed`, with the revision that the thing has now.
/// - **A change that expects another run changes nothing.**
///   Its error is `other-run`, with the run of the process: the process started again since the client read,
///   so the client reads the snapshot again.
/// - **Without it, the last change wins.** `gmq` and a test that has its process to itself leave it out.
/// - **A create expects nothing.** It refuses a name that is taken, so it never overwrites.
///
/// | An operation | Expects the revision of |
/// |---|---|
/// | [`UpdateDefinition`](crate::UpdateDefinition), [`DeleteDefinition`](crate::DeleteDefinition) | The definition. |
/// | [`ArmRule`](crate::ArmRule), [`DisarmRule`](crate::DisarmRule) | The rule in its environment. |
/// | [`UpdateLiveRule`](crate::UpdateLiveRule), [`DeleteLiveRule`](crate::DeleteLiveRule), [`KeepRule`](crate::KeepRule) | The rule in its environment. |
///
/// With `serde`, it is a map of its fields, next to the keys of its operation:
/// `"expects": { "run": "4be0643f1d98573b97cdca98a65347dd", "revision": 7 }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// // What the UI read: the run of the snapshot, and the revision beside the rule.
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().expect("a run ID");
/// let seen = Expects { run, revision: Revision::new(7) };
///
/// let arm = ArmRule {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     expects: Some(seen),
/// };
/// let request = serde_json::to_value(OperationRequest::new(RequestSurface::Ui, arm)).expect("a request has a form");
/// assert_eq!(
///     request,
///     serde_json::json!({
///         "from": { "surface": "ui" },
///         "operation": "arm-rule",
///         "environment": "orders-sim",
///         "rule": "slow-order-producers",
///         "expects": { "run": "4be0643f1d98573b97cdca98a65347dd", "revision": 7 },
///     }),
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expects {
    /// The run that the sender read from.
    pub run: RunId,
    /// The revision that the sender saw.
    pub revision: Revision,
}

/// Writes the revision and its run: `revision 7 of run 4be0643f1d98573b97cdca98a65347dd`.
impl fmt::Display for Expects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "revision {} of run {}", self.revision, self.run)
    }
}

/// What a change answers with: the revision it left.
///
/// A client that changes one thing twice in a row waits for no message of the live stream in between:
/// the second change [expects](Expects) the revision that the first one answered with.
/// The revision that a delete answers with is the number of the delete, and nothing has it afterwards.
///
/// With `serde`, it is a map of its field: `{ "revision": 8 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revised {
    /// The revision that the changed thing has from here on.
    pub revision: Revision,
}
