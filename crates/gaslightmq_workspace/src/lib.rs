//! The workspace: the definitions of every kind as one model, and the rules that span them.
//!
//! A workspace holds selectors, presets, rules, pad layouts and environments.
//! Each kind lives in a store of its own, behind the store trait that the crate of its kind holds,
//! and a store can be anything: files, memory, or a service somewhere else.
//! This crate knows no files. It works over the stores, and holds what no single store can check.
//!
//! | Type | Is |
//! |---|---|
//! | [`Workspace`] | The door for every change: create, update, delete and validate, and keep what was made outside it under the UUID it stamped. |
//! | [`WorkspaceContext`] | Everything a workspace is handed, as one type: its stores, and the source of its UUIDs. |
//! | [`Stores`] | The five stores that a workspace works over, one for each kind. |
//! | [`WorkspaceDefinition`] | A definition of a kind that a workspace holds. |
//! | [`DefinitionName`] | The name of one definition of any kind, such as `rule "slow-order-producers"`. |
//! | [`UuidSource`], [`UuidSeed`] | Where the UUIDs of a workspace come from: the time and the random bits that its context hands in. |
//! | [`WorkspaceError`] | One thing that keeps definitions from fitting each other. |
//! | [`CreateError`], [`UpdateError`], [`DeleteError`] | Why a workspace did not make a change. |
//! | [`ValidationError`] | Why the definitions of a workspace are not valid. |
//!
//! The [`prelude`] holds all of them, and those of the `memory` feature when it is on.
//!
//! # One context
//!
//! A workspace has one type parameter, its [`WorkspaceContext`].
//! Whoever puts a process together writes the context, and names the concrete stores there and nowhere else.
//! Code that holds a workspace is generic over the context, and cannot tell which stores it works with.
//!
//! # In memory
//!
//! With the `memory` feature, the crate has a workspace that is kept in memory, and nowhere else.
//!
//! | Type | Is |
//! |---|---|
//! | [`MemoryStores`] | The stores of a workspace: a store in memory for each kind of definition. |
//! | [`MemoryContext`] | The context of a workspace over those stores, with a source of UUIDs. |
//! | [`KnownUuids`] | A source whose UUIDs a test knows before they are made. |
//!
//! A test turns the feature on, and starts its workspace over `MemoryContext::new()`.
//! An engine that runs without the files of a workspace takes its stores from here too.
//!
//! # What a workspace checks
//!
//! Every definition type only holds valid values, and a store keeps every name once within its kind.
//! A workspace checks what is left: how the definitions of every kind fit each other.
//!
//! | Rule | Checked |
//! |---|---|
//! | Every reference names a definition that exists: the selector that a selector includes, the selector and the preset of a rule, and the rows and the columns of a pad layout. | On every create and update, and by [`Workspace::validate`]. |
//! | No selectors include each other in a circle, and every rule can fire with the selector and the preset it names. | On every create and update, and by [`Workspace::validate`]. |
//! | A selector or a preset that another definition uses is not deleted. The error names who uses it. | On every delete. |
//! | Every definition gets a UUID when someone creates it, and keeps it. | On every create and update. |
//! | No two definitions have the same UUID, whatever their kinds are. | On every create and update, and by [`Workspace::validate`]. |
//!
//! A change is refused for what it breaks, and for nothing else.
//! When something was wrong before, as after someone edited a file, a change to another definition still goes through.
//!
//! A delete asks the stores who uses a definition, with the questions that the store trait of each kind holds.
//! It asks only the stores whose definitions can refer to the one that goes.
//!
//! An environment names a world, and no store holds worlds yet, so nothing checks that name.
//!
//! # Errors
//!
//! Each thing that someone asks of a workspace has an error of its own, because its caller does something else
//! with each one.
//!
//! | Asked | Error | What is wrong |
//! |---|---|---|
//! | [`Workspace::create`], [`Workspace::keep`] | [`CreateError`] | The name is taken, the definition does not fit the others, or a store failed. |
//! | [`Workspace::update`] | [`UpdateError`] | No definition has the name, the definition does not fit the others, or a store failed. |
//! | [`Workspace::delete`] | [`DeleteError`] | No definition has the name, other definitions use it, or a store failed. |
//! | [`Workspace::validate`] | [`ValidationError`] | The definitions do not fit each other, or a store failed. |
//!
//! A [`WorkspaceError`] is one thing that does not fit, and an error that says so holds every one of them.
//! The message of an error is complete: it writes them all, in one line.
//! An error names its definitions with a [`DefinitionName`], because one error speaks about several kinds.
//! What a store said is text, in a field `reason`.
//! No error has a [`source`](std::error::Error::source).
//!
//! # Async, and no IO
//!
//! A store of files waits for the file system, so the store contract is async, and so is every method here
//! that asks a store.
//! The crate awaits the stores it is handed, and nothing else: it names no runtime, it starts no task,
//! and it has no IO, no system clock and no global randomness of its own.
//! The rules themselves are plain functions over the definitions that the stores listed.

mod contents;
mod context;
mod error;
mod kind;
#[cfg(feature = "memory")]
mod memory;
mod name;
mod stores;
mod uuid;
mod workspace;

pub use context::WorkspaceContext;
pub use error::{CreateError, DeleteError, UpdateError, ValidationError, WorkspaceError};
pub use kind::WorkspaceDefinition;
#[cfg(feature = "memory")]
pub use memory::{KnownUuids, MemoryContext, MemoryStores};
pub use name::DefinitionName;
pub use stores::Stores;
pub use uuid::{UuidSeed, UuidSource};
pub use workspace::Workspace;

/// Every item of this crate.
///
/// Import it whole wherever code works with a workspace, next to the preludes of the definitions and of the
/// store contract.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let name: RuleName = "slow-order-producers".parse()?;
/// let error = UpdateError::NotFound { name: name.into() };
/// assert_eq!(error.to_string(), "rule \"slow-order-producers\" does not exist");
/// # Ok::<(), RuleNameError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        CreateError, DefinitionName, DeleteError, Stores, UpdateError, UuidSeed, UuidSource, ValidationError,
        Workspace, WorkspaceContext, WorkspaceDefinition, WorkspaceError,
    };
    #[cfg(feature = "memory")]
    #[doc(no_inline)]
    pub use crate::{KnownUuids, MemoryContext, MemoryStores};
}
