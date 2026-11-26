//! The stores that keep definitions in the files of a workspace.
//!
//! A workspace is a directory of plain-text files, which people also edit by hand and review in git.
//! This crate reads them, and writes the edits of the UI back to them, so that a diff shows what changed and nothing
//! else. It keeps the [store contract](gaslightmq_definition_api::Store), and the store trait of every kind, so a
//! [`Workspace`](gaslightmq_workspace::Workspace) over these stores cannot tell them from stores in memory.
//!
//! | Type | Is |
//! |---|---|
//! | [`FileStores`] | The stores of every kind over one directory. Opening them reads every file once. |
//! | [`FileContext`] | The context of a workspace over those stores. |
//! | [`FileStore`] | The store of one kind of definition in the files. |
//! | [`FileDefinition`] | A kind of definition that the files hold, and where its files are. |
//! | [`Access`] | Whether the stores write back, or the workspace is read-only. |
//! | [`OpenError`], [`FileError`] | Why the files of a workspace do not read, file by file. |
//!
//! The [`prelude`] holds all of them.
//!
//! # The layout
//!
//! ```text
//! gaslight/
//! ├── environments/
//! │   └── orders-sim.yaml      # one environment
//! ├── selectors.yaml           # every selector, as a list
//! ├── presets.yaml             # every preset, as a list
//! ├── rules/
//! │   └── background-noise.yaml  # a list of rules
//! └── layouts/
//!     └── order-desk.yaml      # one pad layout
//! ```
//!
//! A file of one definition carries the name of its definition, and a new rule goes to a file of its name.
//! The name inside the file is the one that counts. A directory reads its files that end in `.yaml`, and none
//! whose name starts with a `.`. A file or a directory that is not there holds nothing.
//! The settings in `gaslight.yaml`, the scenarios, the worlds, the templates, the schemas and the data files
//! are not read here.
//!
//! # Reading
//!
//! The stores read every file once, when they open, and again only when someone reloads the workspace.
//! They do not watch the files: an edit by hand changes nothing until then.
//! A file holds what a person writes and nothing that GaslightMQ keeps, so a key that a definition does not
//! have, such as `uuid`, `revision` or `armed`, does not read.
//!
//! A file that does not read is a [`FileError`]: the file, the path of the key that failed, and what the type of
//! the value said there. An item of a list starts its path with its place:
//!
//! ```text
//! rules/orders.yaml: [2].trigger.every: a count is at least one
//! ```
//!
//! # Writing back
//!
//! A write changes the one definition that changed, in its one written form, and leaves every other byte of
//! its file as it was. It loses the comments inside that definition, and the store says beforehand which
//! definitions hold one. Before a write, the store checks that nobody changed the file since it read it, and it
//! replaces the file in one step. See [`FileStore`].
//!
//! ```no_run
//! use gaslightmq_definition_api::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_workspace::prelude::*;
//! use gaslightmq_workspace_fs::prelude::*;
//!
//! # async fn edited() -> anyhow::Result<()> {
//! let stores = FileStores::open("gaslight", Access::ReadWrite).await?;
//! let fetches: SelectorName = "order-fetches".parse()?;
//!
//! // The UI asks before it saves an edit that loses a comment.
//! let loses = stores.selectors().would_lose_comment(&fetches).await?;
//! # let _ = loses;
//!
//! let mut workspace = Workspace::new(FileContext::new(stores));
//! let selector = workspace.stores().selectors().get(&fetches).await?.expect("the selector");
//! match workspace.update(selector.with_title("Order fetches".parse()?), None).await {
//!     // Someone edited `selectors.yaml` since the store read it, and the edit stays as it is.
//!     Err(UpdateError::ChangedOutside { .. }) => {
//!         let reload = workspace.reload().await?;
//!         reload.take().await;
//!     }
//!     other => {
//!         other?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # IO
//!
//! The crate does IO: it reads and writes the files of a workspace, on the blocking pool of a Tokio runtime.
//! What a file holds and the text after a change are plain functions over text, and do no IO.

mod access;
mod disk;
mod document;
mod error;
mod held;
mod kind;
mod store;
mod stores;
mod text;

pub use access::Access;
pub use error::{FileError, OpenError};
pub use kind::FileDefinition;
pub use store::FileStore;
pub use stores::{FileContext, FileStores};

/// Every item of this crate.
///
/// Import it whole wherever code opens the files of a workspace, next to the preludes of the workspace and of the
/// store contract. No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_workspace_fs::prelude::*;
///
/// let error = FileError { file: "selectors.yaml".into(), reason: "the file is no UTF-8 text".to_owned() };
/// assert_eq!(error.to_string(), "selectors.yaml: the file is no UTF-8 text");
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Access, FileContext, FileDefinition, FileError, FileStore, FileStores, OpenError};
}
