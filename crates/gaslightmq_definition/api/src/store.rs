//! The one trait through which the definitions of one kind are kept.

use crate::{Definition, StoreChangeError, StoreCreateError, StoreReadError};

/// What keeps the definitions of one kind: the files of a workspace, memory, or anything else.
///
/// The contract is written once, for every kind of [`Definition`].
/// The crate of a kind names its store after it, and asks there what only that kind is asked:
/// a store of rules also says which rules use a selector.
/// Whoever holds a store shares it between its tasks, so a store is `Send` and `Sync`,
/// and so is every future it returns.
///
/// | A store | Through |
/// |---|---|
/// | Gives every definition it holds. | [`list`](Self::list) |
/// | Gives the definition of one name. | [`get`](Self::get) |
/// | Takes a new definition. | [`create`](Self::create) |
/// | Replaces a definition it holds. | [`update`](Self::update) |
/// | Lets go of a definition it holds. | [`delete`](Self::delete) |
///
/// # What every store keeps to
///
/// - **A name is there once.**
///   A store holds at most one definition of a name, and the name is how every call finds it.
/// - **Nothing overwrites another definition.**
///   [`create`](Self::create) refuses a name that is taken,
///   and [`update`](Self::update) and [`delete`](Self::delete) refuse a name that the store does not hold.
/// - **A definition comes back as it went in.**
///   A store keeps every part of a definition, and changes none.
/// - **A change happens as a whole, or not at all.**
///   A change that fails changed nothing.
///   The same holds for a change whose future is dropped after it started, and its caller does not learn which.
///   A change starts when its future is polled for the first time.
/// - **A store checks nothing else.**
///   It knows one kind, so it cannot tell whether a definition names another one that exists.
///   Whoever holds the stores of every kind checks that, before it asks a store for a change.
///
/// The trait is a signature, and it has no logic.
/// It names no runtime: a store waits however it likes, and whoever calls it drives the futures.
/// An implementation can write each method as an `async fn`.
pub trait Store<D: Definition>: Send + Sync {
    /// Every definition that the store holds, in the order of their names.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn list(&self) -> impl Future<Output = Result<Vec<D>, StoreReadError>> + Send;

    /// The definition of this name, or `None` when the store holds none.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn get(&self, name: &D::Name) -> impl Future<Output = Result<Option<D>, StoreReadError>> + Send;

    /// Takes a definition that the store does not hold yet.
    ///
    /// # Errors
    ///
    /// - [`StoreCreateError::NameTaken`] when the store holds a definition of that name. It stays as it is.
    /// - [`StoreCreateError::Failed`] when the store could not keep the definition.
    fn create(&self, definition: D) -> impl Future<Output = Result<(), StoreCreateError>> + Send;

    /// Replaces the definition of the same name by `definition`, as a whole.
    ///
    /// # Errors
    ///
    /// - [`StoreChangeError::NotFound`] when the store holds no definition of that name. Nothing is created.
    /// - [`StoreChangeError::Failed`] when the store could not keep the change.
    fn update(&self, definition: D) -> impl Future<Output = Result<(), StoreChangeError>> + Send;

    /// Lets go of the definition of this name.
    ///
    /// # Errors
    ///
    /// - [`StoreChangeError::NotFound`] when the store holds no definition of that name.
    /// - [`StoreChangeError::Failed`] when the store could not remove the definition.
    fn delete(&self, name: &D::Name) -> impl Future<Output = Result<(), StoreChangeError>> + Send;
}
