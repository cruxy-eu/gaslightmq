//! What a reload of the workspace did.

use gaslightmq_workspace::{DefinitionChange, Revision};

/// What a [reload](crate::Host::reload) took in from the files of the workspace, as one change.
///
/// ```
/// use gaslightmq_host::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ReloadWorkspaceError> {
/// let run: RunId = "4be0643f1d98573b97cdca98a65347dd".parse().expect("a run ID");
/// let host = Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build(), run);
///
/// // A workspace in memory has no files to read again, so its reload changes nothing, and takes a number.
/// let reloaded = host.reload().await?;
/// assert!(reloaded.changes.is_empty());
/// assert!(reloaded.revision > host.workspace().await.loaded());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reloaded {
    /// The revision of the reload, which every definition that it created or updated has.
    pub revision: Revision,
    /// What the reload changed, by the kind and the name of each definition, in that order.
    /// A definition that the files hold as the workspace held it is not among them.
    pub changes: Vec<DefinitionChange>,
}
