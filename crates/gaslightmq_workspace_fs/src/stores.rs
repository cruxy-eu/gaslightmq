//! The stores of every kind in the files of one workspace, and the context of a workspace over them.

use std::path::PathBuf;

use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;
use gaslightmq_workspace::{Stores, WorkspaceContext};

use crate::{Access, FileStore, OpenError};

/// The stores of a workspace in its files: a [`FileStore`] for each kind of definition, over one directory.
///
/// [`open`](Self::open) reads every file of every kind once, and a process does not start on a workspace whose
/// files do not read. From there the stores answer from what they read, until someone reloads the workspace.
#[derive(Debug)]
pub struct FileStores {
    selectors: FileStore<Selector>,
    presets: FileStore<Preset>,
    rules: FileStore<Rule>,
    scenarios: FileStore<Scenario>,
    layouts: FileStore<PadLayout>,
    environments: FileStore<Environment>,
}

impl FileStores {
    /// Opens the stores of the workspace in `directory`, and reads every file of every kind.
    ///
    /// A directory without the files of a kind holds no definition of that kind, so an empty directory is an
    /// empty workspace.
    /// The worlds, the templates, the schemas and the data files of the workspace are not read here.
    ///
    /// # Errors
    ///
    /// [`OpenError`] with every file that does not read, of every kind.
    pub async fn open(directory: impl Into<PathBuf>, access: Access) -> Result<Self, OpenError> {
        let directory = directory.into();
        let stores = Self {
            selectors: FileStore::new(directory.clone(), access),
            presets: FileStore::new(directory.clone(), access),
            rules: FileStore::new(directory.clone(), access),
            scenarios: FileStore::new(directory.clone(), access),
            layouts: FileStore::new(directory.clone(), access),
            environments: FileStore::new(directory, access),
        };
        let loads = [
            stores.selectors.load().await,
            stores.presets.load().await,
            stores.rules.load().await,
            stores.scenarios.load().await,
            stores.layouts.load().await,
            stores.environments.load().await,
        ];
        let errors: Vec<_> = loads.into_iter().filter_map(Result::err).flatten().collect();
        if errors.is_empty() {
            Ok(stores)
        } else {
            Err(OpenError { errors })
        }
    }
}

impl Stores for FileStores {
    type Selectors = FileStore<Selector>;
    type Presets = FileStore<Preset>;
    type Rules = FileStore<Rule>;
    type Scenarios = FileStore<Scenario>;
    type Layouts = FileStore<PadLayout>;
    type Environments = FileStore<Environment>;

    fn selectors(&self) -> &FileStore<Selector> {
        &self.selectors
    }

    fn presets(&self) -> &FileStore<Preset> {
        &self.presets
    }

    fn rules(&self) -> &FileStore<Rule> {
        &self.rules
    }

    fn scenarios(&self) -> &FileStore<Scenario> {
        &self.scenarios
    }

    fn layouts(&self) -> &FileStore<PadLayout> {
        &self.layouts
    }

    fn environments(&self) -> &FileStore<Environment> {
        &self.environments
    }
}

/// The context of a workspace over its files: the [`FileStores`] of one directory.
///
/// ```no_run
/// use gaslightmq_workspace::prelude::*;
/// use gaslightmq_workspace_fs::prelude::*;
///
/// # async fn started() -> Result<(), OpenError> {
/// let stores = FileStores::open("gaslight", Access::ReadWrite).await?;
/// let workspace = Workspace::new(FileContext::new(stores));
/// assert!(workspace.validate().await.is_ok());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FileContext {
    stores: FileStores,
}

impl FileContext {
    /// The context over the stores of one workspace.
    pub fn new(stores: FileStores) -> Self {
        Self { stores }
    }
}

impl WorkspaceContext for FileContext {
    type Stores = FileStores;

    fn stores(&self) -> &FileStores {
        &self.stores
    }
}
