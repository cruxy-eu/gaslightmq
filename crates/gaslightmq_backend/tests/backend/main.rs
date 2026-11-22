//! A backend that is written by hand runs on a test executor: the contract needs no runtime.
//!
//! [`cluster`] holds the backend: a cluster of three brokers behind one lock, which waits on wakers of its own.
//! The executor of the `futures` crate drives it, alone or as several tasks on one thread.
//! The tests play the engine: they start calls, drop them, and do with every answer what an engine does.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod cluster;

mod calls;
mod endpoints;
mod engine;
mod incidents;
