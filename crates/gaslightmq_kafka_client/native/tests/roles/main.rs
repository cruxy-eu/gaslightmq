//! The producer, the consumer and the admin against peers that speak just enough Kafka.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod connections;
mod groups;
mod isolation;
mod records;
mod support;
