//! What a Kafka client offers to a test, whichever client it is.
//!
//! - [`Producer`], [`Consumer`] and [`Admin`] are what an application does
//!   with a client. A test written against them runs with any client that
//!   implements them.
//! - [`Produce`], [`Fetch`] and the other traits named after an API send one
//!   request of that API and return its response.
//! - [`RawKafkaClient`] implements the API traits by sending each request as
//!   it is given, to one address, at a fixed version.

pub mod api_versions;
pub mod client;
pub mod create_topics;
pub mod delete_topics;
pub mod fetch;
pub mod find_coordinator;
pub mod list_offsets;
pub mod metadata;
pub mod produce;
pub mod traits;

pub use api_versions::ApiVersions;
pub use client::RawKafkaClient;
pub use create_topics::CreateTopics;
pub use delete_topics::DeleteTopics;
pub use fetch::Fetch;
pub use find_coordinator::FindCoordinator;
pub use list_offsets::ListOffsets;
pub use metadata::Metadata;
pub use produce::Produce;
pub use traits::{
    Admin, Consumer, HeartbeatResult, JoinGroupResult, PartitionMetadata, ProduceResult, Producer, ProtocolConsumer,
    RecordStream, SyncGroupResult, TopicConfigs, TopicMetadata, Watermarks,
};
