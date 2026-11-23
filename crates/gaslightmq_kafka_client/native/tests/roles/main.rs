//! The producer, the consumer and the admin against peers that speak just enough Kafka.
//!
//! A fake broker runs inside the test, so the test decides what it answers, when, and in how many pieces,
//! and it sees every request and every connection.
//! The tests here need exactly that: a broker that lies, hangs up, stays silent or holds a request,
//! or a look at what the client sent.
//! Whether the client speaks Kafka as a real broker understands it is the question of `tests/kafka`.

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
