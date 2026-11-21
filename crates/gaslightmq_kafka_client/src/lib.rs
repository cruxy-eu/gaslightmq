//! Our own Kafka client, for tests that need to see how a client
//! experiences what GaslightMQ does to it.
//!
//! [`KafkaClient`] follows the cluster: it discovers brokers and partition
//! leaders from Metadata, routes each request to the right broker,
//! negotiates API versions, and retries retriable errors until a delivery
//! timeout. Every failure is reported as a [`ClientError`] instead of a
//! panic.
//!
//! [`RawKafkaClient`] sends one request to one address at a fixed version,
//! for tests that assert on the protocol itself.

pub mod api_versions;
pub mod create_topics;
pub mod delete_topics;
pub mod fetch;
pub mod find_coordinator;
pub mod list_offsets;
pub mod metadata;
pub mod produce;
pub mod traits;

mod client;
mod connection;
mod error;
mod raw;

pub use api_versions::ApiVersions;
pub use client::{ClientConfig, KafkaClient};
pub use create_topics::CreateTopics;
pub use delete_topics::DeleteTopics;
pub use error::ClientError;
pub use fetch::Fetch;
pub use find_coordinator::FindCoordinator;
pub use list_offsets::ListOffsets;
pub use metadata::Metadata;
pub use produce::Produce;
pub use raw::RawKafkaClient;
pub use traits::{
    Admin, Consumer, HeartbeatResult, JoinGroupResult, PartitionMetadata, ProduceResult, Producer,
    ProtocolConsumer, RecordStream, SyncGroupResult, TopicConfigs, TopicMetadata, Watermarks,
};
