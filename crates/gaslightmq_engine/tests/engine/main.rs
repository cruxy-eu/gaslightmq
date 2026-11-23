//! An engine in front of a backend that lives in memory, with clients on real sockets.
//!
//! [`support`] holds what the tests share, as three types:
//! the backend, which does what a test tells it; the engine under test, on a clock that the test steps;
//! and the connection of a client that does what no Kafka client does.
//! A test that sends a request and looks at its answer uses the raw client of `gaslightmq_kafka_client_api`.

mod support;

mod backend;
mod calls;
mod endpoints;
mod events;
mod lies;
mod rules;
mod running;
