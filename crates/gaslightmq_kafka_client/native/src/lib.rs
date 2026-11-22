//! Our own Kafka client, written against the traits in
//! `gaslightmq_kafka_client_api`.
//!
//! It discovers brokers and partition leaders from Metadata, routes each
//! request to the right broker, negotiates API versions, and retries
//! retriable errors until a delivery timeout.
//!
//! The methods of [`KafkaClient`] itself report every failure as a
//! [`ClientError`], so tests can assert on how a client experiences faults.
//! Its [`Producer`] and [`Admin`] impls carry the failure in their result, as
//! the closest Kafka error. Its [`Consumer`] impl panics on a failure, because
//! the trait has no error to return.
//!
//! [`Producer`]: gaslightmq_kafka_client_api::Producer
//! [`Admin`]: gaslightmq_kafka_client_api::Admin
//! [`Consumer`]: gaslightmq_kafka_client_api::Consumer

mod client;
mod connection;
mod error;

pub use client::{ClientConfig, KafkaClient};
pub use error::ClientError;
