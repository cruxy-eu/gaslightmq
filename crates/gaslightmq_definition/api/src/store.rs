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
/// | Says whether a write of a definition loses a comment of its text. | [`would_lose_comment`](Self::would_lose_comment) |
/// | Reads again what someone else can change, and holds it once it is taken. | [`read_again`](Self::read_again), [`take_reading`](Self::take_reading) |
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
/// # What someone else changes
///
/// Some stores hold definitions that someone else can change too, as a file that someone edits.
/// Such a store keeps to three things more:
///
/// - **It never writes over a change that it has not read.**
///   A create, an update or a delete that would fails with `ChangedOutside`, and changes nothing.
/// - **It says whether a write loses a comment.**
///   It holds the text that someone wrote, and a write puts the definition in its own written form.
/// - **It reads again only when it is asked, and holds what it read only once it is told to.**
///   [`read_again`](Self::read_again) gives what it reads, and [`take_reading`](Self::take_reading)
///   makes that what it holds. Whoever asks compares the two. The store tells nobody of a change on its own.
///
/// A store whose definitions only change through it, such as a store in memory, keeps the methods that
/// say so as the trait writes them: it never loses a comment, and it has nothing to read again.
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
    /// - [`StoreCreateError::ChangedOutside`] when someone else changed where the definition would go,
    ///   since the store read it.
    /// - [`StoreCreateError::Failed`] when the store could not keep the definition.
    fn create(&self, definition: D) -> impl Future<Output = Result<(), StoreCreateError>> + Send;

    /// Replaces the definition of the same name by `definition`, as a whole.
    ///
    /// # Errors
    ///
    /// - [`StoreChangeError::NotFound`] when the store holds no definition of that name. Nothing is created.
    /// - [`StoreChangeError::ChangedOutside`] when someone else changed where the definition is,
    ///   since the store read it.
    /// - [`StoreChangeError::Failed`] when the store could not keep the change.
    fn update(&self, definition: D) -> impl Future<Output = Result<(), StoreChangeError>> + Send;

    /// Lets go of the definition of this name.
    ///
    /// # Errors
    ///
    /// - [`StoreChangeError::NotFound`] when the store holds no definition of that name.
    /// - [`StoreChangeError::ChangedOutside`] when someone else changed where the definition is,
    ///   since the store read it.
    /// - [`StoreChangeError::Failed`] when the store could not remove the definition.
    fn delete(&self, name: &D::Name) -> impl Future<Output = Result<(), StoreChangeError>> + Send;

    /// Whether the text that the store holds for the definition of this name has a comment that a write would lose.
    ///
    /// A store writes a definition in its own form, so a comment that someone wrote inside it goes with the write.
    /// Whoever edits the definition can warn first.
    /// A name that the store does not hold loses nothing.
    /// A store that holds no text, such as a store in memory, keeps this method as the trait writes it,
    /// and loses nothing.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when the store could not read what it holds.
    fn would_lose_comment(&self, name: &D::Name) -> impl Future<Output = Result<bool, StoreReadError>> + Send {
        async move {
            let _ = name;
            Ok(false)
        }
    }

    /// Reads again what someone else can change, and gives every definition it read, in the order of their names.
    ///
    /// The store goes on holding what it held before, until [`take_reading`](Self::take_reading).
    /// Whoever asks compares what it read with what the store [lists](Self::list), and takes it or leaves it.
    /// A reading that nobody takes is replaced by the next one.
    ///
    /// `None` says that the store has nothing to read again: nobody else changes what it holds.
    /// A store in memory keeps this method as the trait writes it, and answers that.
    ///
    /// # Errors
    ///
    /// [`StoreReadError`] when what the store reads does not read. Its reason says everything that is wrong.
    fn read_again(&self) -> impl Future<Output = Result<Option<Vec<D>>, StoreReadError>> + Send {
        async { Ok(None) }
    }

    /// Makes what the last [`read_again`](Self::read_again) read what the store holds, and lists, from now on.
    ///
    /// It cannot fail, so whoever takes the readings of several stores takes all of them or none.
    /// Without a reading to take, nothing changes.
    /// Whoever reads again takes the reading before it asks the store for a change, or leaves it.
    /// A store that has nothing to read again keeps this method as the trait writes it.
    fn take_reading(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}
