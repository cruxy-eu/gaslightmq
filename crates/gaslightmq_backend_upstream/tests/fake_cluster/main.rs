//! The backend in front of a cluster of hand-written brokers, on sockets of this machine.
//!
//! The brokers answer what a test tells them to, so these tests reach what a real cluster does not do on demand:
//! a broker that hangs up, that never answers, that sends what is no response, or that joins and leaves.
//! `tests/real_cluster` holds the same backend against a real Kafka.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod calls;
mod endpoints;
mod failures;
mod support;
