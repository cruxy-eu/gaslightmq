//! A workspace that is kept in memory: its stores, and its context.
//!
//! The module is behind the `memory` feature.

mod context;
mod stores;

pub use context::MemoryContext;
pub use stores::MemoryStores;
