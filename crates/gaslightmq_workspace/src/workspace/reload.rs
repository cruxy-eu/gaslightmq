//! A reload: what the stores read again, compared with what they hold, and taken in as one change.

use std::fmt;

use gaslightmq_definition_api::{Store, StoreReadError};

use super::{Unread, Workspace};
use crate::contents::Contents;
use crate::{DefinitionName, ReloadError, Revision, Stores, WorkspaceContext, WorkspaceDefinition};

/// What a [reload](Workspace::reload) changes in one definition, which it names with its kind.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let name: RuleName = "slow-order-producers".parse()?;
/// let change = DefinitionChange::Updated(name.into());
/// assert_eq!(change.to_string(), "rule \"slow-order-producers\" changed");
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionChange {
    /// The stores hold a definition of a name that the workspace did not have.
    Created(DefinitionName),
    /// The stores hold another definition under a name that the workspace had.
    Updated(DefinitionName),
    /// The stores no longer hold a definition of a name that the workspace had.
    Deleted(DefinitionName),
}

impl DefinitionChange {
    /// The name of the definition that changed, with its kind.
    pub fn name(&self) -> &DefinitionName {
        match self {
            Self::Created(name) | Self::Updated(name) | Self::Deleted(name) => name,
        }
    }
}

/// Writes the name of the definition, and what happened to it: `rule "slow-order-producers" changed`.
impl fmt::Display for DefinitionChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created(name) => write!(f, "{name} is new"),
            Self::Updated(name) => write!(f, "{name} changed"),
            Self::Deleted(name) => write!(f, "{name} is gone"),
        }
    }
}

/// A reload that fits, and that the workspace has not taken in yet.
///
/// [`Workspace::reload`] reads every store again and checks what they read, and this is what it found.
/// [`changes`](Self::changes) says what the reload would change, by name.
/// Whoever reloads can still refuse it, for what only it knows: it drops the reload, and nothing changed.
/// [`take`](Self::take) takes the changes in, as one change.
///
/// The reload holds the workspace until then, so no other change comes between what it read and what it takes.
#[derive(Debug)]
#[must_use = "a reload changes nothing until it is taken"]
pub struct Reload<'w, C: WorkspaceContext> {
    workspace: &'w mut Workspace<C>,
    /// In the order of the kinds and names of their definitions.
    changes: Vec<DefinitionChange>,
}

impl<C: WorkspaceContext> Reload<'_, C> {
    /// What the reload changes, in the order of the kinds and the names of the definitions.
    ///
    /// A definition that the stores hold as the workspace held it is not among them.
    pub fn changes(&self) -> &[DefinitionChange] {
        &self.changes
    }

    /// Takes the changes in, as one change, and returns its revision.
    ///
    /// Every store takes what it read, and lists it from now on.
    /// Every definition that the reload created or updated has the revision of the reload,
    /// the workspace forgets the revision of every definition that went,
    /// and every other definition keeps the revision it had.
    /// A reload that changes nothing is a change too, and takes a number.
    pub async fn take(self) -> Revision {
        let stores = self.workspace.context.stores();
        stores.selectors().take_reading().await;
        stores.presets().take_reading().await;
        stores.rules().take_reading().await;
        stores.scenarios().take_reading().await;
        stores.layouts().take_reading().await;
        stores.environments().take_reading().await;

        let revision = self.workspace.counter.next_revision();
        for change in self.changes {
            match change {
                DefinitionChange::Created(name) | DefinitionChange::Updated(name) => {
                    self.workspace.changed.insert(name, revision);
                }
                DefinitionChange::Deleted(name) => {
                    self.workspace.changed.remove(&name);
                }
            }
        }
        revision
    }
}

impl<C: WorkspaceContext> Workspace<C> {
    /// Reads every store again, compares what they read with what they hold, by name, and checks the result.
    ///
    /// A store whose definitions someone else can change, as the files of a workspace, reads them again.
    /// A store that nobody else changes, as a store in memory, has nothing to read, and changes nothing.
    /// The reload holds what they read aside: the stores go on holding what they held,
    /// until whoever reloads [takes](Reload::take) it.
    ///
    /// What the stores read has to fit together as a whole, whatever fitted before:
    /// the workspace only ever holds definitions that fit.
    ///
    /// # Errors
    ///
    /// - [`ReloadError::StoreFailed`] when a store could not read again. It names every store that failed.
    /// - [`ReloadError::Invalid`] with everything that is wrong, when what the stores read does not fit together.
    ///
    /// A reload that fails changes nothing.
    pub async fn reload(&mut self) -> Result<Reload<'_, C>, ReloadError> {
        let before = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(ReloadError::StoreFailed { reason }),
        };

        let stores = self.context.stores();
        let mut failures = Vec::new();
        let after = Contents::new(
            read_again(stores.selectors(), &mut failures).await,
            read_again(stores.presets(), &mut failures).await,
            read_again(stores.rules(), &mut failures).await,
            read_again(stores.scenarios(), &mut failures).await,
            read_again(stores.layouts(), &mut failures).await,
            read_again(stores.environments(), &mut failures).await,
        );
        if !failures.is_empty() {
            return Err(ReloadError::StoreFailed {
                reason: failures.join("; "),
            });
        }

        let errors = after.errors();
        if !errors.is_empty() {
            return Err(ReloadError::Invalid { errors });
        }

        let changes = before.changes_to(&after);
        Ok(Reload {
            workspace: self,
            changes,
        })
    }
}

/// What a store reads again, or what it holds when it has nothing to read again.
///
/// A store that fails adds what it said to `failures`, after the word for its kind, and gives nothing.
async fn read_again<D, S>(store: &S, failures: &mut Vec<String>) -> Vec<D>
where
    D: WorkspaceDefinition,
    S: Store<D>,
{
    let read = match store.read_again().await {
        Ok(Some(read)) => return read,
        Ok(None) => store.list().await,
        Err(StoreReadError::Failed { reason }) => {
            failures.push(format!("the {} store could not be read again: {reason}", D::LABEL));
            return Vec::new();
        }
    };
    read.unwrap_or_else(|error| {
        failures.push(Unread::by::<D>(error).reason);
        Vec::new()
    })
}
