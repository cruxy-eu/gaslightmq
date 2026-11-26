//! The store of one kind of definition in the files of a workspace.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_definition_api::{Store, StoreChangeError, StoreCreateError, StoreReadError, StoreSaveError, WriteBack};
use gaslightmq_environment::{Environment, EnvironmentStore};
use gaslightmq_rule::{Preset, PresetStore, Rule, RuleStore};
use gaslightmq_scenario::{Scenario, ScenarioStore};
use gaslightmq_selector::{Selector, SelectorStore};
use gaslightmq_surface::{LayoutStore, PadLayout};

use crate::held::{self, After, Edit, Held, Pending};
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
/// # Writing on save
///
/// A store of [`Access::WriteOnSave`] makes every change to the text that it holds, and lists the change at once,
/// but writes no file until someone saves.
///
/// - **A change waits in memory.** The store keeps the text of every file after the change, and the hash of the bytes
///   on the disk from when it last read or wrote the file.
/// - **The store names what it has not written**: see [`unsaved`](Store::unsaved).
/// - **A save checks every file before it writes one.** When the bytes of one file differ from what the store
///   expects, or a file is gone or appeared, the stage fails with `ChangedOutside`, names every such file, and
///   writes nothing.
/// - **A stage writes every file beside its place.** The text of each file goes to a temporary file next to it,
///   whose bytes are on the disk before the stage returns. A stage that fails leaves no temporary file.
/// - **A commit renames them over their files, one at a time**, and asks the system to keep each rename through a
///   crash. A rename that fails stops the commit: every file before it is saved, and it and every file after it
///   still wait.
/// - **Reading again lets go of what was not saved.** The store holds what it read once the reading is taken.
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
    /// What the last stage of a save wrote beside the files, until the save commits it or drops it.
    staged: Mutex<Vec<Staged>>,
}

/// One file that a stage of a save prepared.
struct Staged {
    /// The file, below the directory of the workspace.
    file: PathBuf,
    /// The temporary file next to it that holds its text after the save, or `None` when the file goes.
    temporary: Option<PathBuf>,
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
                staged: Mutex::new(Vec::new()),
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
    /// Every file that a save would write and that someone changed since the store read or wrote it, with how,
    /// in one line. `None` when nobody changed one.
    ///
    /// # Errors
    ///
    /// What the system said about a file that it could not read.
    fn changed_outside(&self, pending: &[Pending]) -> Result<Option<String>, String> {
        let mut changed = Vec::new();
        for Pending { file, on_disk, .. } in pending {
            let found = disk::read(&self.directory, file).map_err(|reason| format!("{}: {reason}", file.display()))?;
            changed.extend(held::changed_outside(file, *on_disk, found.as_deref()));
        }
        Ok((!changed.is_empty()).then(|| changed.join("; ")))
    }

    /// Checks every file that a save would write, and writes the text of each to a temporary file next to it,
    /// when nobody changed one since. A file that goes needs no temporary file.
    fn stage(&self) -> Result<(), StoreSaveError> {
        let _files = lock(&self.files);
        drop_all(std::mem::take(&mut *lock(&self.staged)));
        let pending = lock(&self.held).pending();
        match self.changed_outside(&pending) {
            Ok(None) => {}
            Ok(Some(reason)) => return Err(StoreSaveError::ChangedOutside { reason }),
            Err(reason) => return Err(StoreSaveError::Failed { reason }),
        }
        let mut staged = Vec::with_capacity(pending.len());
        for Pending { file, text, .. } in pending {
            let temporary = match &text {
                Some(text) => match disk::write_beside(&self.directory, &file, text) {
                    Ok(temporary) => Some(temporary),
                    Err(reason) => {
                        drop_all(staged);
                        return Err(StoreSaveError::Failed {
                            reason: format!("{}: {reason}", file.display()),
                        });
                    }
                },
                None => None,
            };
            staged.push(Staged { file, temporary });
        }
        *lock(&self.staged) = staged;
        Ok(())
    }

    /// Puts every file that the last stage wrote in its place, one at a time, and holds each one saved once it is.
    fn commit(&self) -> Result<(), StoreSaveError> {
        let _files = lock(&self.files);
        let mut staged = std::mem::take(&mut *lock(&self.staged)).into_iter();
        while let Some(Staged { file, temporary }) = staged.next() {
            let put = match &temporary {
                Some(temporary) => disk::put_in_place(&self.directory, temporary, &file),
                None => match disk::read(&self.directory, &file) {
                    Ok(None) => Ok(()),
                    Ok(Some(_)) => disk::remove(&self.directory, &file),
                    Err(reason) => Err(reason),
                },
            };
            if let Err(reason) = put {
                drop_all(staged.collect());
                return Err(StoreSaveError::Failed {
                    reason: format!("{}: {reason}", file.display()),
                });
            }
            lock(&self.held).saved(&file);
        }
        Ok(())
    }

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
        if self.access == Access::WriteOnSave {
            let mut held = lock(&self.held);
            let text = held.text(&file).to_owned();
            let after = held.after(&file, &text, edit).map_err(E::failed)?;
            held.hold(file, after, edit.name().clone());
            return Ok(());
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

    fn write_back(&self) -> WriteBack {
        match self.shared.access {
            Access::WriteOnSave => WriteBack::OnSave,
            Access::ReadWrite | Access::ReadOnly => WriteBack::EachChange,
        }
    }

    async fn unsaved(&self) -> Vec<D::Name> {
        lock(&self.shared.held).unsaved_names()
    }

    async fn check_unsaved(&self) -> Result<(), StoreSaveError> {
        let shared = Arc::clone(&self.shared);
        let checked = tokio::task::spawn_blocking(move || {
            let _files = lock(&shared.files);
            let pending = lock(&shared.held).pending();
            match shared.changed_outside(&pending) {
                Ok(None) => Ok(()),
                Ok(Some(reason)) => Err(StoreSaveError::ChangedOutside { reason }),
                Err(reason) => Err(StoreSaveError::Failed { reason }),
            }
        });
        checked.await.unwrap_or_else(|stopped| {
            Err(StoreSaveError::Failed {
                reason: format!("the check stopped before it ended: {stopped}"),
            })
        })
    }

    async fn stage_unsaved(&self) -> Result<(), StoreSaveError> {
        let shared = Arc::clone(&self.shared);
        let staged = tokio::task::spawn_blocking(move || shared.stage());
        staged.await.unwrap_or_else(|stopped| {
            Err(StoreSaveError::Failed {
                reason: format!("the stage stopped before it ended: {stopped}"),
            })
        })
    }

    async fn commit_staged(&self) -> Result<(), StoreSaveError> {
        let shared = Arc::clone(&self.shared);
        let committed = tokio::task::spawn_blocking(move || shared.commit());
        committed.await.unwrap_or_else(|stopped| {
            Err(StoreSaveError::Failed {
                reason: format!("the commit stopped before it ended: {stopped}"),
            })
        })
    }

    async fn drop_staged(&self) {
        let shared = Arc::clone(&self.shared);
        let dropped = tokio::task::spawn_blocking(move || {
            let _files = lock(&shared.files);
            drop_all(std::mem::take(&mut *lock(&shared.staged)));
        });
        // A drop that stopped left temporary files whose names start with a `.`, which nobody reads.
        let _ = dropped.await;
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

/// Takes away the temporary files of what a stage prepared, which nobody puts in place.
fn drop_all(staged: Vec<Staged>) {
    for Staged { temporary, .. } in staged {
        if let Some(temporary) = temporary {
            disk::throw_away(&temporary);
        }
    }
}

/// What a mutex holds, for one step.
///
/// A task that panicked while it held it left it whole: every step changes what it holds once, at its end.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
