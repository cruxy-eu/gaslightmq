//! The UUID of a live rule, which only a workspace issues, and why keeping a live rule failed.

use gaslightmq_rule::RuleUuid;

use crate::CreateError;

/// The UUID that a workspace gave a live rule when someone made it.
///
/// A live rule lives in one environment until someone keeps it, and the environment holds this next to the rule.
/// Only a [`Workspace`](crate::Workspace) makes one, with [`live_rule_uuid`](crate::Workspace::live_rule_uuid),
/// and [`keep`](crate::Workspace::keep) takes it back. So no rule of the workspace gets a UUID that someone made up.
///
/// It has no clone: one UUID belongs to one live rule, and keeping the rule spends it.
/// [`uuid`](Self::uuid) gives the UUID itself, to name the rule by in the journal.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
///
/// let live = workspace.live_rule_uuid();
/// assert_eq!(String::from(live.uuid()), KnownUuids::nth(1));
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct LiveRuleUuid {
    uuid: RuleUuid,
}

impl LiveRuleUuid {
    /// The UUID that the workspace issued.
    pub(crate) fn new(uuid: RuleUuid) -> Self {
        Self { uuid }
    }

    /// The UUID of the live rule.
    pub fn uuid(&self) -> RuleUuid {
        self.uuid
    }
}

/// Why a workspace did not [keep](crate::Workspace::keep) a live rule, with the UUID of the live rule.
///
/// Keeping fails in the ways that creating does, and `error` says which.
/// The live rule stays live, so the error hands its UUID back: whoever holds the live rule holds it again.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
/// let name: RuleName = "slow-order-producers".parse()?;
///
/// let error = KeepError {
///     error: CreateError::NameTaken { name: name.into() },
///     uuid: workspace.live_rule_uuid(),
/// };
/// assert_eq!(error.to_string(), "rule \"slow-order-producers\" exists already");
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{error}")]
pub struct KeepError {
    /// Why the workspace did not take the rule.
    pub error: CreateError,
    /// The UUID of the live rule, which it keeps while it stays live.
    pub uuid: LiveRuleUuid,
}
