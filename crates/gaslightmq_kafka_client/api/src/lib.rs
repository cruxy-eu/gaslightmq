//! What a Kafka client offers to a test, whichever client it is.
//!
//! - [`Producer`], [`Consumer`] and [`Admin`] are what an application does
//!   with a client. A test written against them runs with any client that
//!   implements them.
//! - [`Produce`], [`Fetch`] and the other traits named after an API send one
//!   request of that API and return its response.
//! - [`RawKafkaClient`] implements the API traits by sending each request as
//!   it is given, to one address, at a fixed version.
//!
//! # Failures
//!
//! Every call of a client returns a `Result` with a [`ClientError`], and none panics.
//! An implementation turns whatever it meets into a `ClientError`, so a test reads one error, whichever client it runs.
//!
//! A broker that refuses a call is an `Err`, and so is a client that fails, and the two never look alike:
//! [`ClientError::Refused`] holds the error code the broker answered with, and no other variant holds an error code.
//! A client that got no answer says [`ClientError::NoAnswer`], and never the `REQUEST_TIMED_OUT` of a broker.
//!
//! # Where an error code is data
//!
//! | The call | Asks for | An error code of the broker |
//! |---|---|---|
//! | A method of [`Producer`], [`Consumer`], [`Admin`] or [`ProtocolConsumer`] | One partition, one topic or one group. | Is the failure of the call: [`ClientError::Refused`]. |
//! | A method of a trait named after an API | The response to a request, which may name several partitions, topics or groups. | Stays in the response, at the place the broker wrote it. The call is `Ok`. |

pub mod api_versions;
pub mod application;
pub mod client;
pub mod create_topics;
pub mod delete_topics;
pub mod error;
pub mod fetch;
pub mod find_coordinator;
pub mod list_offsets;
pub mod metadata;
pub mod produce;

pub use api_versions::ApiVersions;
pub use application::{
    Admin, Consumer, JoinGroupResult, PartitionMetadata, ProduceResult, Producer, ProtocolConsumer, RecordStream,
    TopicConfigs, TopicMetadata, Watermarks,
};
pub use client::{RawKafkaClient, SentRequest};
pub use create_topics::CreateTopics;
pub use delete_topics::DeleteTopics;
pub use error::ClientError;
pub use fetch::Fetch;
pub use find_coordinator::FindCoordinator;
pub use list_offsets::ListOffsets;
pub use metadata::Metadata;
pub use produce::Produce;
