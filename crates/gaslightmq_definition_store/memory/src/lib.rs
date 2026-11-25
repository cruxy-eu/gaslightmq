//! Definitions kept in memory, and nowhere else: the stores, and the context of a workspace over them.
//!
//! | Type | Is |
//! |---|---|
//! | [`MemoryStore`] | The store of one kind of definition. It keeps the store contract for any kind, and the store trait of each kind of a workspace. |
//! | [`MemoryStores`] | The stores of a workspace: a [`MemoryStore`] for each kind. |
//! | [`MemoryContext`] | The context of a workspace over those stores, with a source of UUIDs. |
//! | [`KnownUuids`] | A source whose UUIDs a test knows before they are made. |
//!
//! These are the stores of a test, and of an engine that runs without the files of a workspace:
//! everything they hold is gone when they are.
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_definition_store_api::prelude::*;
//! use gaslightmq_definition_store_memory::{KnownUuids, MemoryContext};
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_workspace::prelude::*;
//!
//! // The workspace of a test: nothing is written anywhere, and every UUID is known.
//! let mut workspace = Workspace::new(MemoryContext::new());
//!
//! let metadata: SelectorName = "metadata".parse().expect("a selector name");
//! let selector = Selector::new(metadata.clone(), Conditions::new().with(Condition::is(ApiKey::METADATA)));
//! let rule = Rule::new("blackhole-metadata".parse().expect("a rule name"), metadata.clone(), LieDefinition::Silence);
//!
//! block_on(async {
//!     workspace.create(selector).await?;
//!     let rule = workspace.create(rule).await?;
//!     assert_eq!(rule.uuid().map(String::from), Some(KnownUuids::nth(2)));
//!
//!     // The store of the rules says who uses the selector, without anyone reading every rule.
//!     let using = workspace.stores().rules().using_selector(&metadata).await;
//!     assert_eq!(using, Ok(vec![rule]));
//!     Ok::<(), CreateError>(())
//! })?;
//! # Ok::<(), CreateError>(())
//! ```
//!
//! # One store, for every kind
//!
//! [`MemoryStore`] implements the store contract of `gaslightmq_definition_store_api` once, for any definition.
//! The store trait of a kind comes on top: a trait that asks nothing more is one line,
//! and a trait with questions of its own answers them over the definitions it holds.
//!
//! # No IO
//!
//! The crate depends on the store contract, on the crates of the definitions, and on `gaslightmq_workspace`
//! for the stores and the context that it implements.
//! It has no IO, no system clock and no global randomness.
//! Its methods are async because the contract is, and none of them waits: every future is ready when it is first polled.

mod context;
mod store;
mod stores;
mod uuids;

pub use context::MemoryContext;
pub use store::MemoryStore;
pub use stores::MemoryStores;
pub use uuids::KnownUuids;
