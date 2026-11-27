//! What every storage of recordings does, as one check that the tests of any storage run.
//!
//! The [storage contract](gaslightmq_recording::RecordingStorage) says what a storage keeps to,
//! whichever layout stands behind it and wherever its bytes live.
//! This crate holds that behaviour once: [`assert_recording_storage`] takes a way to make an empty storage,
//! and fails when the storage does not do what the contract asks.
//!
//! | Item | Is |
//! |---|---|
//! | [`assert_recording_storage`] | The check: every behaviour of the contract, each on a storage of its own. |
//! | [`BATCH_SIZE`], [`READER_MEMORY`] | The sizes that the storage of the check is made with, so that a small recording is many batches, and more than a reader may hold. |
//! | [`MemoryStorage`] | A storage in memory: the smallest storage that keeps the contract. |
//!
//! ```
//! use futures::executor::block_on;
//! use gaslightmq_recording_conformance::{BATCH_SIZE, MemoryStorage, READER_MEMORY, assert_recording_storage};
//!
//! // The check is a future, and the test of a storage drives it with the executor it has.
//! let empty = || MemoryStorage::new().with_batch_size(BATCH_SIZE).with_reader_memory(READER_MEMORY);
//! block_on(assert_recording_storage(empty));
//! ```
//!
//! # A storage in memory
//!
//! [`MemoryStorage`] keeps the batches of a recording as the bytes that they were written as, in memory.
//! It is here for two reasons.
//! It is what the check is held to: a check that no storage passes proves nothing.
//! And it is what the tests of a recorder or of a reader write to and read from, before a layout exists for them.
//! It is no layout: it keeps no summary, it compresses nothing, and nothing in it outlives its process.
//!
//! # Test support
//!
//! The crate is test support: only the tests of a crate depend on it, as a dev-dependency.
//! `cargo xtask test-support` holds every crate to that.
//!
//! # No IO
//!
//! The crate depends on the storage contract, and on the vocabulary that the recording of the check is written in.
//! It awaits the storage it is handed, and nothing else: it has no executor, no IO, no clock and no randomness.

mod contract;
mod memory;
mod samples;

pub use contract::{BATCH_SIZE, READER_MEMORY, assert_recording_storage};
pub use memory::{MemoryEntries, MemoryReader, MemoryStorage, MemoryWriter};
