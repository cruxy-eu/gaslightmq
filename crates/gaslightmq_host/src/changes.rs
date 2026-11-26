//! What changes in a host, as whoever follows it sees it: the control plane, a recorder of the journal.

use gaslightmq_engine::SessionEvent;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::JournalEntry;
use gaslightmq_rule::RuleName;
use gaslightmq_workspace::DefinitionName;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// One change of a host, whoever made it.
///
/// A change names what changed, and no more: whoever follows the host asks it for the state of what it names.
/// A change can therefore come after a later one changed the same thing again,
/// and the state it finds is the newest. Applying it twice changes nothing.
/// What happened is no state, so it comes whole: what happened to a session, and an entry of a journal.
///
/// | Change | Comes when |
/// |---|---|
/// | [`EnvironmentStarted`](Self::EnvironmentStarted), [`EnvironmentStopped`](Self::EnvironmentStopped) | An environment started, or stopped and every change of its sessions came before. |
/// | [`DefinitionChanged`](Self::DefinitionChanged), [`DefinitionDeleted`](Self::DefinitionDeleted) | Someone changed a definition through the host. |
/// | [`RuleChanged`](Self::RuleChanged) | A rule of an environment armed or disarmed, by a command, a pad, a firing that spent its budget or a lifetime that ended, or a live rule was made or changed. |
/// | [`RuleRemoved`](Self::RuleRemoved) | A live rule left its environment: someone deleted or kept it. |
/// | [`Session`](Self::Session) | A session of an environment opened, said who its client is, or closed. |
/// | [`Journaled`](Self::Journaled) | The journal of an environment got an entry: something changed there, or a definition of the workspace changed. |
/// | [`WorkspaceChanged`](Self::WorkspaceChanged) | What the workspace holds unsaved may be other: a definition changed in a workspace that writes on save, someone saved, or a reload let go of what nobody saved. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostChange {
    /// The environment started.
    EnvironmentStarted { environment: EnvironmentName },
    /// The environment stopped, and everything live in it with it.
    EnvironmentStopped { environment: EnvironmentName },
    /// Someone created or updated the definition of that name.
    DefinitionChanged { name: DefinitionName },
    /// Someone deleted the definition of that name.
    DefinitionDeleted { name: DefinitionName },
    /// The rule of that name changed its state in the environment, or the live rule of that name was made or
    /// changed its definition.
    RuleChanged {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// The live rule of that name left the environment: someone deleted it, or kept it in the workspace.
    RuleRemoved {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// Something happened to a session of the environment.
    Session {
        environment: EnvironmentName,
        event: SessionEvent,
    },
    /// The journal of the environment got this entry. The entries of one environment come in the order of their IDs.
    Journaled {
        environment: EnvironmentName,
        entry: JournalEntry,
    },
    /// What the workspace holds unsaved may be other than it was: a definition changed in a workspace that writes
    /// on save, someone saved, or a reload let go of what nobody saved.
    /// It comes after the change of the definition, and only from a workspace that writes on save.
    WorkspaceChanged,
}

/// The changes of a host from the moment it was made, for one follower.
///
/// [`Host::changes`](crate::Host::changes) makes one. No change waits for a follower:
/// the host keeps a [backlog](crate::HostConfiguration::change_backlog) for each, and a follower that falls further
/// behind misses the oldest changes, and learns how many.
#[derive(Debug)]
pub struct HostChanges {
    changes: broadcast::Receiver<HostChange>,
}

impl HostChanges {
    pub(crate) const fn new(changes: broadcast::Receiver<HostChange>) -> Self {
        Self { changes }
    }

    /// Waits for the next change.
    ///
    /// Giving up the wait loses nothing.
    ///
    /// # Errors
    ///
    /// - [`HostChangesError::Missed`] when the follower fell behind. It says how many changes are gone,
    ///   and the next call returns the oldest change that the host still keeps.
    /// - [`HostChangesError::HostGone`] when the host is gone, and the follower took every change it kept.
    pub async fn next(&mut self) -> Result<HostChange, HostChangesError> {
        match self.changes.recv().await {
            Ok(change) => Ok(change),
            Err(RecvError::Lagged(changes)) => Err(HostChangesError::Missed { changes }),
            Err(RecvError::Closed) => Err(HostChangesError::HostGone),
        }
    }
}

/// Why a follower of the [changes](HostChanges) of a host has no next change.
///
/// ```
/// use gaslightmq_host::prelude::*;
///
/// let error = HostChangesError::Missed { changes: 3 };
/// assert_eq!(error.to_string(), "the follower fell behind, and missed 3 changes of the host");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostChangesError {
    /// The follower fell behind, and the host let go of the oldest changes it kept for it.
    /// The follower no longer knows what the host holds, so it asks again.
    #[error("the follower fell behind, and missed {changes} changes of the host")]
    Missed { changes: u64 },
    /// The host is gone, and no change comes any more.
    #[error("the host is gone")]
    HostGone,
}
