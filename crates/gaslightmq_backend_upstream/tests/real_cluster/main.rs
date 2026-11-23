//! The backend in front of real Kafka brokers.
//!
//! The tests need Docker.
//! Each test starts the cluster it needs, and sets it up and looks into it through the handles of `gaslightmq_test_kafka`.
//! The test is the engine: it hands each request to the backend as a call, and no listener stands in front of it.
//! `tests/fake_cluster` holds what a real cluster does not do on demand.

mod addresses;
mod records;
mod topics;
mod versions;
