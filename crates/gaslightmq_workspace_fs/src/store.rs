//! The store of one kind of definition in the files of a workspace.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_definition_api::{Store, StoreChangeError, StoreCreateError, StoreReadError};
use gaslightmq_environment::{Environment, EnvironmentStore};
use gaslightmq_rule::{Preset, PresetStore, Rule, RuleStore};
use gaslightmq_scenario::{Scenario, ScenarioStore};
use gaslightmq_selector::{Selector, SelectorStore};
use gaslightmq_surface::{LayoutStore, PadLayout};

use crate::held::{self, After, Edit, Held};
use crate::{Access, FileDefinition, FileError, disk, error};

/// The definitions of one kind, kept in the files of a workspace.
///
/// The store reads the files of its kind when it is [opened](crate::FileStores::open), and again when someone
/// [reads it again](Store::read_again). In between, it answers from what it read, and never looks at the files:
/// an edit of a file changes nothing until someone reloads.
///
/// # Writing back
///
/// - **A write changes one definition.** It replaces the text of the definition, adds it after the last one of
///   its file, or takes it away, and leaves every other byte of the file as it was.
/// - **A definition has one written form**, with the keys in the order of its type.
///   A definition that the store wrote once is written the same way again, so it only changes in a diff when its
///   content changes.
/// - **A write loses the comments inside the definition.** [`would_lose_comment`](Store::would_lose_comment)
///   says which definition holds one.
/// - **The store checks each file before it writes it.** It keeps a hash of the bytes of every file it read and
///   wrote, and reads the file again before a write. When the bytes differ, or a file is gone or appeared,
///   someone changed it since, and the write fails with `ChangedOutside` and changes nothing.
/// - **A write replaces the file in one step**: through a temporary file next to it, renamed over it.
/// - **A change runs as a whole.** It runs on the blocking pool of the runtime, and runs to its end even when
///   whoever asked for it stops waiting.
///
/// A store of [`Access::ReadOnly`] refuses every write, and reads as any other.
///
/// Whoever shares the store shares one set of definitions: put it behind an `Arc`, or lend it out.
/// The store needs a Tokio runtime for every method that touches the files.
pub struct FileStore<D: FileDefinition> {
    shared: Arc<Shared<D>>,
}

/// What every handle of a store shares.
struct Shared<D: FileDefinition> {
    directory: PathBuf,
    access: Access,
    /// Held by a change or a reading from its first look at the files to its last, so that none overlap.
    files: Mutex<()>,
    /// What the store holds, and lists.
    held: Mutex<Held<D>>,
    /// What the last reading read, until the store takes it.
    reading: Mutex<Option<Held<D>>>,
}

impl<D: FileDefinition> FileStore<D> {
    /// A store of the files of the workspace in `directory`, which has read nothing yet.
    ///
    /// It holds no definition, and takes every file as not there, until it [reads again](Store::read_again)
    /// and [takes the reading](Store::take_reading).
    /// A store of a new, empty directory holds what the directory holds from the start.
    /// [`FileStores::open`](crate::FileStores::open) opens the stores of every kind, and reads them.
    pub fn new(directory: impl Into<PathBuf>, access: Access) -> Self {
        Self {
            shared: Arc::new(Shared {
                directory: directory.into(),
                access,
                files: Mutex::new(()),
                held: Mutex::new(Held::new()),
                reading: Mutex::new(None),
            }),
        }
    }

    /// The directory of the workspace.
    pub fn directory(&self) -> &Path {
        &self.shared.directory
    }

    /// Whether the store writes back to the files.
    pub fn access(&self) -> Access {
        self.shared.access
    }

    /// Reads the files of the kind, and holds what they hold.
    ///
    /// # Errors
    ///
    /// Every file that does not read. The store then holds what it held before.
    pub(crate) async fn load(&self) -> Result<(), Vec<FileError>> {
        self.read(|shared, held| *lock(&shared.held) = held).await
    }

    /// Reads the files of the kind on the blocking pool, and hands what they hold to `then`, before any change
    /// can look at the files again.
    async fn read<R: Send + 'static>(
        &self,
        then: impl FnOnce(&Shared<D>, Held<D>) -> R + Send + 'static,
    ) -> Result<R, Vec<FileError>> {
        let shared = Arc::clone(&self.shared);
        let read = tokio::task::spawn_blocking(move || {
            let _files = lock(&shared.files);
            let held = Held::read(disk::find(&shared.directory, D::PLACE)?)?;
            Ok(then(&shared, held))
        });
        read.await.unwrap_or_else(|stopped| {
            Err(vec![FileError {
                file: PathBuf::from(D::PLACE.to_string()),
                reason: format!("the reading stopped before it ended: {stopped}"),
            }])
        })
    }

    /// Makes one change to one file, as a whole, on the blocking pool.
    ///
    /// `place` says which file the change goes to, or why it goes nowhere.
    async fn change<E: Refusal>(
        &self,
        edit: Edit<D>,
        place: impl FnOnce(&Held<D>) -> Result<PathBuf, E> + Send + 'static,
    ) -> Result<(), E> {
        let shared = Arc::clone(&self.shared);
        let changed = tokio::task::spawn_blocking(move || shared.change(&edit, place));
        changed
            .await
            .unwrap_or_else(|stopped| Err(E::failed(format!("the change stopped before it ended: {stopped}"))))
    }
}

impl<D: FileDefinition> Shared<D> {
    /// Checks the file of the change, writes it, and holds what it holds after.
    fn change<E: Refusal>(&self, edit: &Edit<D>, place: impl FnOnce(&Held<D>) -> Result<PathBuf, E>) -> Result<(), E> {
        let _files = lock(&self.files);
        let (file, expected) = {
            let held = lock(&self.held);
            let file = place(&held)?;
            let expected = held.fingerprint(&file);
            (file, expected)
        };
        if self.access == Access::ReadOnly {
            return Err(E::failed("the workspace is read-only".to_owned()));
        }

        let found =
            disk::read(&self.directory, &file).map_err(|reason| E::failed(format!("{}: {reason}", file.display())))?;
        if let Some(reason) = held::changed_outside(&file, expected, found.as_deref()) {
            return Err(E::changed_outside(reason));
        }
        let text = match &found {
            None => "",
            Some(bytes) => {
                std::str::from_utf8(bytes).map_err(|_| E::failed(format!("{} is no UTF-8 text", file.display())))?
            }
        };
        let after = lock(&self.held).after(&file, text, edit).map_err(E::failed)?;

        let written = match &after {
            After::Text { text: new, .. } if new.as_str() == text && found.is_some() => Ok(()),
            After::Text { text: new, .. } => disk::replace(&self.directory, &file, new),
            After::Gone => disk::remove(&self.directory, &file),
        };
        written.map_err(|reason| E::failed(format!("{}: {reason}", file.display())))?;
        lock(&self.held).take(file, after);
        Ok(())
    }
}

/// What a create and a change of a store of files refuse with, in the words of their own error.
trait Refusal: Send + 'static {
    fn changed_outside(reason: String) -> Self;
    fn failed(reason: String) -> Self;
}

impl Refusal for StoreCreateError {
    fn changed_outside(reason: String) -> Self {
        Self::ChangedOutside { reason }
    }

    fn failed(reason: String) -> Self {
        Self::Failed { reason }
    }
}

impl Refusal for StoreChangeError {
    fn changed_outside(reason: String) -> Self {
        Self::ChangedOutside { reason }
    }

    fn failed(reason: String) -> Self {
        Self::Failed { reason }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the contract is async, and a store answers a question from what it read without waiting"
)]
impl<D: FileDefinition> Store<D> for FileStore<D> {
    async fn list(&self) -> Result<Vec<D>, StoreReadError> {
        Ok(lock(&self.shared.held).list())
    }

    async fn get(&self, name: &D::Name) -> Result<Option<D>, StoreReadError> {
        Ok(lock(&self.shared.held).get(name))
    }

    async fn create(&self, definition: D) -> Result<(), StoreCreateError> {
        let placed = definition.clone();
        self.change(Edit::Create(definition), move |held| held.file_for(&placed))
            .await
    }

    async fn update(&self, definition: D) -> Result<(), StoreChangeError> {
        let name = definition.name().clone();
        self.change(Edit::Update(definition), move |held| held.file_holding(&name))
            .await
    }

    async fn delete(&self, name: &D::Name) -> Result<(), StoreChangeError> {
        let held_name = name.clone();
        self.change(Edit::Delete(name.clone()), move |held| held.file_holding(&held_name))
            .await
    }

    async fn would_lose_comment(&self, name: &D::Name) -> Result<bool, StoreReadError> {
        Ok(lock(&self.shared.held).would_lose_comment(name))
    }

    fn read_only(&self) -> bool {
        self.shared.access == Access::ReadOnly
    }

    async fn read_again(&self) -> Result<Option<Vec<D>>, StoreReadError> {
        let read = self.read(|shared, reading| {
            let definitions = reading.list();
            *lock(&shared.reading) = Some(reading);
            definitions
        });
        match read.await {
            Ok(definitions) => Ok(Some(definitions)),
            Err(errors) => Err(StoreReadError::Failed {
                reason: error::each(&errors),
            }),
        }
    }

    async fn take_reading(&self) {
        let reading = lock(&self.shared.reading).take();
        if let Some(reading) = reading {
            let _files = lock(&self.shared.files);
            *lock(&self.shared.held) = reading;
        }
    }
}

/// A store of selectors in the files is a [`SelectorStore`], with the answers of the trait.
impl SelectorStore for FileStore<Selector> {}

/// A store of presets in the files is a [`PresetStore`].
impl PresetStore for FileStore<Preset> {}

/// A store of rules in the files is a [`RuleStore`], with the answers of the trait.
impl RuleStore for FileStore<Rule> {}

/// A store of scenarios in the files is a [`ScenarioStore`], with the answers of the trait.
impl ScenarioStore for FileStore<Scenario> {}

/// A store of pad layouts in the files is a [`LayoutStore`], with the answers of the trait.
impl LayoutStore for FileStore<PadLayout> {}

/// A store of environments in the files is an [`EnvironmentStore`].
impl EnvironmentStore for FileStore<Environment> {}

/// Writes the word for the kind of the store, its directory, and the names it holds.
impl<D: FileDefinition<Name: fmt::Debug>> fmt::Debug for FileStore<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<D::Name> = lock(&self.shared.held)
            .list()
            .iter()
            .map(|definition| definition.name().clone())
            .collect();
        f.debug_struct("FileStore")
            .field("of", &D::LABEL)
            .field("directory", &self.shared.directory)
            .field("access", &self.shared.access)
            .field("names", &names)
            .finish()
    }
}

/// What a mutex holds, for one step.
///
/// A task that panicked while it held it left it whole: every step changes what it holds once, at its end.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
