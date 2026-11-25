//! A workspace over stores in memory: what it lets through, what it refuses, and the UUIDs it gives.
//!
//! [`support`] holds the definitions of one workspace, and two sets of stores of which one store fails.
//! Every test starts its workspace itself, over a context whose stores hold nothing.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod support;

mod creating;
mod deleting;
mod updating;
mod validating;
