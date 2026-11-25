//! The store that keeps definitions in memory, and nowhere else.
//!
//! A [`MemoryStore`] keeps the [store contract](gaslightmq_definition_api::Store) for any kind of definition.
//! It is the store of a test, and of an engine that runs without the files of a workspace:
//! everything it holds is gone when it is.
//!
//! The crate knows no kind of definition.
//! The crate of a kind implements its store trait for the `MemoryStore` of its kind,
//! so a store of rules in memory is also what the crate of the rules asks a store of rules to be.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_api::prelude::*;
//! use gaslightmq_definition_memory::MemoryStore;
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
//! let greetings = MemoryStore::new();
//! let morning = Greeting { name: "morning".to_owned(), text: "Good morning".to_owned() };
//!
//! block_on(greetings.create(morning.clone()))?;
//! assert_eq!(block_on(greetings.get(morning.name())).expect("a store in memory reads"), Some(morning.clone()));
//!
//! // Nothing overwrites a definition: the name is taken.
//! assert_eq!(block_on(greetings.create(morning)), Err(StoreCreateError::NameTaken));
//! # Ok::<(), StoreCreateError>(())
//! ```
//!
//! # No IO
//!
//! The crate depends on the store contract alone.
//! It has no IO, no system clock and no global randomness.
//! Its methods are async because the contract is, and none of them waits: every future is ready when it is first polled.

mod store;

pub use store::MemoryStore;
