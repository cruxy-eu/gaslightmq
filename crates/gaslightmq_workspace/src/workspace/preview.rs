//! What the stores would hold after a change, for whoever checks what only it knows before it asks for the change.

use super::{Unread, Workspace};
use crate::contents::Contents;
use crate::{DefinitionName, DeleteError, Revision, UpdateError, WorkspaceContext, WorkspaceDefinition};

/// What the stores of a workspace would hold after one change that the workspace checked, and nobody asked for yet.
///
/// [`check_update`](Workspace::check_update) and [`check_delete`](Workspace::check_delete) give one, once the change
/// passes every check of the workspace. Whoever holds what the workspace does not know reads it, before it asks for
/// the change: a host checks that every rule that is armed would still resolve. The preview changes nothing: the
/// change goes through [`update`](Workspace::update) or [`delete`](Workspace::delete), which check it again.
///
/// ```
/// use futures::executor::block_on;
/// use gaslightmq_definition_api::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut workspace = Workspace::new(MemoryContext::new());
/// let blackhole = Preset::new("blackhole".parse().expect("a preset name"), LieDefinition::Silence);
/// block_on(async {
///     workspace.create(blackhole.clone()).await?;
///     let after = workspace.check_delete(blackhole.name().clone(), None).await.expect("nothing uses the preset");
///     assert_eq!(after.definitions::<Preset>().count(), 0);
///     // Nothing changed yet.
///     assert_eq!(workspace.stores().presets().list().await.expect("a store in memory reads").len(), 1);
///     Ok::<(), CreateError>(())
/// })?;
/// # Ok::<(), CreateError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Preview {
    contents: Contents,
}

impl Preview {
    /// Every definition of the kind `D` that the stores would hold after the change, in the order of their names.
    pub fn definitions<D: WorkspaceDefinition>(&self) -> impl Iterator<Item = &D> {
        self.contents.of::<D>()
    }
}

impl<C: WorkspaceContext> Workspace<C> {
    /// Checks an [update](Self::update) as the update checks it, without asking a store for it, and gives what the
    /// stores would hold after it.
    ///
    /// Whoever holds what the workspace does not know checks that next, before it asks for the update:
    /// a stale update is refused for being stale before anything else is asked of it.
    ///
    /// # Errors
    ///
    /// The [`UpdateError`] that the update would fail with before it asks its store: `NotFound`, `Changed`,
    /// `DoesNotFit`, or `StoreFailed` when a store could not read.
    pub async fn check_update<D: WorkspaceDefinition>(
        &self,
        definition: D,
        expects: Option<Revision>,
    ) -> Result<Preview, UpdateError> {
        let name: DefinitionName = definition.name().clone().into();
        let contents = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(UpdateError::StoreFailed { name, reason }),
        };
        if contents.get(&name).is_none() {
            return Err(UpdateError::NotFound { name });
        }
        if let Err(revision) = self.check(&name, expects) {
            return Err(UpdateError::Changed { name, revision });
        }
        let errors = contents.errors_with(definition.clone().into_any());
        if !errors.is_empty() {
            return Err(UpdateError::DoesNotFit { name, errors });
        }
        Ok(Preview {
            contents: contents.with(definition.into_any()),
        })
    }

    /// Checks a [delete](Self::delete) as the delete checks it, without asking a store for it, and gives what the
    /// stores would hold after it.
    ///
    /// # Errors
    ///
    /// The [`DeleteError`] that the delete would fail with before it asks its store: `NotFound`, `Changed`,
    /// `InUse`, or `StoreFailed` when a store could not read.
    pub async fn check_delete(
        &self,
        name: impl Into<DefinitionName>,
        expects: Option<Revision>,
    ) -> Result<Preview, DeleteError> {
        let name = name.into();
        match self.holds(&name).await {
            Ok(true) => {}
            Ok(false) => return Err(DeleteError::NotFound { name }),
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
        }
        if let Err(revision) = self.check(&name, expects) {
            return Err(DeleteError::Changed { name, revision });
        }
        let users = match self.users_of(&name).await {
            Ok(users) => users,
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
        };
        if !users.is_empty() {
            return Err(DeleteError::InUse { name, users });
        }
        let contents = match self.contents().await {
            Ok(contents) => contents,
            Err(Unread { reason }) => return Err(DeleteError::StoreFailed { name, reason }),
        };
        Ok(Preview {
            contents: contents.without(&name),
        })
    }
}
