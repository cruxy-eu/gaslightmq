//! What a workspace works with: its stores.

use crate::Stores;

/// Everything that a [`Workspace`](crate::Workspace) is handed, as one type.
///
/// A workspace holds no definition: it works with what its context brings.
/// Whoever puts a process together writes the context once, and names the concrete stores there:
/// the stores of files, or stores in memory.
/// Everything else names the context by this trait, so it cannot tell which stores it works with.
///
/// | A context brings | Through |
/// |---|---|
/// | The stores of the definitions, one for each kind. | [`stores`](Self::stores) |
///
/// Whoever holds a workspace hands its changes to tasks, so a context is `Send` and `Sync`.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// /// The context of a process: the stores it was started with.
/// struct Started<S> {
///     stores: S,
/// }
///
/// impl<S: Stores> WorkspaceContext for Started<S> {
///     type Stores = S;
///
///     fn stores(&self) -> &S {
///         &self.stores
///     }
/// }
/// ```
pub trait WorkspaceContext: Send + Sync {
    /// The stores of the definitions.
    type Stores: Stores;

    /// The stores of the definitions, one for each kind.
    fn stores(&self) -> &Self::Stores;
}
