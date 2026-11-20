//! Our own Kafka client, written against the facade traits in
//! `gaslightmq-kafka-client-api`.
//!
//! It discovers brokers and partition leaders from Metadata, routes each
//! request to the right broker, negotiates API versions, and retries
//! retriable errors until a delivery timeout. Every failure is reported as a
//! [`ClientError`] instead of a panic, so tests can assert on how a client
//! experiences faults.

mod client;
mod connection;
mod error;

pub use client::{ClientConfig, KafkaClient};
pub use error::ClientError;
