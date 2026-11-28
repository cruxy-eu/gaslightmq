//! An engine in front of a backend that lives in memory, with clients on real sockets.
//!
//! [`support`] holds what the tests share, as two types:
//! the backend, which does what a test tells it, and the engine under test, on a clock that the test steps.
//! The client of every test is the raw client of `gaslightmq_kafka_client_api`.

mod support;

mod backend;
mod calls;
mod endpoints;
mod events;
mod journal;
mod lies;
mod queries;
mod recording;
mod rules;
mod running;
