//! A workspace that is kept in memory: its stores, its context, and UUIDs that a test knows.
//!
//! The module is behind the `memory` feature.

mod context;
mod stores;
mod uuids;

pub use context::MemoryContext;
pub use stores::MemoryStores;
pub use uuids::KnownUuids;
