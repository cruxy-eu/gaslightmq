//! The store contract: the one trait through which the definitions of one kind are kept, and its errors.
//!
//! A definition is something that someone defined under a name, such as a selector or a rule.
//! Each kind of definition has a store of its own, and a store can be anything:
//! files, memory, or a service somewhere else.
//! Whoever works with definitions reaches them through this contract, and cannot tell which store it is.
//!
//! | Type | Is |
//! |---|---|
//! | [`Store`] | The trait: list, get, create, update and delete the definitions of one kind, and read again what someone else changed. |
//! | [`Definition`] | What a store holds: something with a name, and a word for its kind. |
//! | [`StoreReadError`] | Why a store has no answer to a list, a get or another question. |
//! | [`StoreCreateError`] | Why a store did not create a definition. |
//! | [`StoreChangeError`] | Why a store did not update or delete a definition. |
//! | [`WriteBack`] | When a store writes a change: as it happens, or with the next save. |
//! | [`StoreSaveError`] | Why a store did not save the changes it holds. |
//!
//! The [`prelude`] holds every type of the table.
//!
//! # No kind of definition
//!
//! The crate knows no kind of definition, and depends on none of their crates.
//! The crate of a kind depends on this one.
//! It implements [`Definition`] for its type, and names the store of its kind in a trait of its own on top of
//! [`Store`]: the store of the rules is a `RuleStore` in the crate of the rules.
//! Such a trait is also where a kind asks its store what [`Store`] does not ask of every kind.
//!
//! # A store
//!
//! The contract is written once, and a store implements it for the kind it holds.
//! This one holds the greetings that a build brings along, and nobody changes those:
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_api::prelude::*;
//!
//! /// What a team says when it starts its day, under a name.
//! #[derive(Debug, Clone, PartialEq)]
//! struct Greeting {
//!     name: String,
//!     text: String,
//! }
//!
//! impl Definition for Greeting {
//!     type Name = String;
//!
//!     const LABEL: &'static str = "greeting";
//!
//!     fn name(&self) -> &String {
//!         &self.name
//!     }
//! }
//!
//! /// The greetings of a build, in the order of their names.
//! struct BuiltIn(Vec<Greeting>);
//!
//! impl BuiltIn {
//!     const FIXED: &str = "the greetings of a build do not change";
//! }
//!
//! impl Store<Greeting> for BuiltIn {
//!     async fn list(&self) -> Result<Vec<Greeting>, StoreReadError> {
//!         Ok(self.0.clone())
//!     }
//!
//!     async fn get(&self, name: &String) -> Result<Option<Greeting>, StoreReadError> {
//!         Ok(self.0.iter().find(|greeting| greeting.name() == name).cloned())
//!     }
//!
//!     async fn create(&self, greeting: Greeting) -> Result<(), StoreCreateError> {
//!         match self.get(greeting.name()).await {
//!             Ok(Some(_)) => Err(StoreCreateError::NameTaken),
//!             _ => Err(StoreCreateError::Failed { reason: Self::FIXED.to_owned() }),
//!         }
//!     }
//!
//!     async fn update(&self, greeting: Greeting) -> Result<(), StoreChangeError> {
//!         self.delete(greeting.name()).await
//!     }
//!
//!     async fn delete(&self, name: &String) -> Result<(), StoreChangeError> {
//!         match self.get(name).await {
//!             Ok(Some(_)) => Err(StoreChangeError::Failed { reason: Self::FIXED.to_owned() }),
//!             _ => Err(StoreChangeError::NotFound),
//!         }
//!     }
//! }
//!
//! let morning = Greeting { name: "morning".to_owned(), text: "Good morning".to_owned() };
//! let store = BuiltIn(vec![morning.clone()]);
//!
//! // Any executor drives a store. A host has its runtime, and this example has the smallest one.
//! assert_eq!(block_on(store.list())?, [morning.clone()]);
//! assert_eq!(block_on(store.get(morning.name()))?, Some(morning.clone()));
//!
//! // Nothing overwrites a definition, and this store keeps no new one either.
//! assert_eq!(block_on(store.create(morning)), Err(StoreCreateError::NameTaken));
//!
//! let evening = Greeting { name: "evening".to_owned(), text: "Good evening".to_owned() };
//! let fixed = block_on(store.create(evening)).unwrap_err();
//! assert_eq!(
//!     fixed.to_string(),
//!     "the store could not keep the definition: the greetings of a build do not change"
//! );
//! # Ok::<(), StoreReadError>(())
//! ```
//!
//! # Errors
//!
//! No type of this crate checks a value, so none has an error of its own.
//! The contract has an error for each thing that someone asks of a store.
//! Operations that fail in the same ways share one.
//!
//! | Error | The store | Its caller |
//! |---|---|---|
//! | [`StoreReadError`] | Could not read what it holds, for a [list](Store::list), a [get](Store::get) or a question of its kind. | Keeps what it read before, and reports the error. |
//! | [`StoreCreateError`] | Did not [create](Store::create) a definition: its name is taken, someone else changed where it would go, or the store could not keep it. | Tells whoever asked that the name is taken, or that the store has to read again first, or reports the error. |
//! | [`StoreChangeError`] | Did not [update](Store::update) or [delete](Store::delete) a definition: no definition has the name, someone else changed where it is, or the store could not keep the change. | Tells whoever asked that there is no such definition, or that the store has to read again first, or reports the error. |
//! | [`StoreSaveError`] | Did not [stage](Store::stage_unsaved) or [commit](Store::commit_staged) the changes it holds: someone else changed where they go, or the store could not write them. | Tells whoever asked that the store has to read again first, or reports the error. |
//!
//! Every store returns these errors and no other, whatever stands behind it.
//! An error names no definition and no kind: whoever asked knows both, and says them in its own error.
//! Each says everything in one message, and has no [`source`](std::error::Error::source).
//! What a file, a library or the system said is text, in a field `reason`:
//! a store of files puts the path of the file and of the key that failed there.
//!
//! # No runtime
//!
//! The crate has no logic and no IO: no socket, no file, no clock and no randomness.
//! The trait returns futures, because a store of files and a store behind a network wait,
//! and nothing in the crate spawns, sleeps or polls one.
//! A store waits on whatever its own crate brings, and whoever calls it drives the futures on its runtime.
//! The crate compiles to WebAssembly, because the crates of the definitions do.

mod definition;
mod error;
mod store;

pub use definition::Definition;
pub use error::{StoreChangeError, StoreCreateError, StoreReadError, StoreSaveError};
pub use store::{Store, WriteBack};

/// Every item of this crate.
///
/// Import it whole wherever code implements a store or works over one, next to the preludes of the definitions.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// let error = StoreReadError::Failed { reason: "permission denied".to_owned() };
/// assert_eq!(error.to_string(), "the store could not read its definitions: permission denied");
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Definition, Store, StoreChangeError, StoreCreateError, StoreReadError, StoreSaveError, WriteBack};
}
