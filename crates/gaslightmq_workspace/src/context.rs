//! What a workspace works with: its stores, and the source of its UUIDs.

use crate::{Stores, UuidSource};

/// Everything that a [`Workspace`](crate::Workspace) is handed, as one type.
///
/// A workspace holds no definition and reads no clock: it works with what its context brings.
/// Whoever puts a process together writes the context once, and names the concrete parts there:
/// the stores of files with the clock and the randomness of the system, or stores in memory with UUIDs
/// that a test knows.
/// Everything else names the context by this trait, so it cannot tell which parts it works with.
///
/// | A context brings | Through |
/// |---|---|
/// | The stores of the definitions, one for each kind. | [`stores`](Self::stores) |
/// | The seeds of new UUIDs. | [`uuids`](Self::uuids) |
///
/// Whoever holds a workspace hands its changes to tasks, so a context is `Send` and `Sync`.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// /// The context of a process: the stores it was started with, and the source of its UUIDs.
/// struct Started<S, U> {
///     stores: S,
///     uuids: U,
/// }
///
/// impl<S: Stores, U: UuidSource + Send + Sync> WorkspaceContext for Started<S, U> {
///     type Stores = S;
///     type Uuids = U;
///
///     fn stores(&self) -> &S {
///         &self.stores
///     }
///
///     fn uuids(&mut self) -> &mut U {
///         &mut self.uuids
///     }
/// }
/// ```
pub trait WorkspaceContext: Send + Sync {
    /// The stores of the definitions.
    type Stores: Stores;
    /// Where the seeds of new UUIDs come from.
    type Uuids: UuidSource;

    /// The stores of the definitions, one for each kind.
    fn stores(&self) -> &Self::Stores;

    /// The source of the seeds of new UUIDs.
    fn uuids(&mut self) -> &mut Self::Uuids;
}
