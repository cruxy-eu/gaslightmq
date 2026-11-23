//! What an application does with a client: it produces, consumes and administers.
//!
//! Every method asks for one thing: one partition, one topic or one group.
//! A broker that refuses it answers with one error code, and that code is the failure of the call:
//! [`ClientError::Refused`]. No result of these traits holds an error code.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use futures::Stream;
use gaslightmq_kernel::MemberId;
use gaslightmq_wire::{Offset, Partition, Record};

use crate::ClientError;

/// The records of a partition, one after the other.
///
/// A fetch that fails is the last item of the stream, as its `Err`.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Record, ClientError>> + Send>>;

/// Where the broker wrote the records of a send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResult {
    pub partition: Partition,
    /// The offset of the first record.
    pub offset: Offset,
}

/// What the coordinator told a consumer that joined a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResult {
    /// The ID of the consumer as a member of the group.
    pub member_id: MemberId,
    /// The member that leads the group.
    pub leader: MemberId,
}

pub trait Producer {
    /// Write `records` to a partition.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the partition,
    /// and another [`ClientError`] when the client fails.
    fn send(
        &self,
        topic: &str,
        partition: Partition,
        records: &[Record],
    ) -> impl Future<Output = Result<ProduceResult, ClientError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: Partition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermarks {
    pub low: Offset,
    pub high: Offset,
}

pub trait Consumer {
    /// Read the records of a partition from `offset` on.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the partition,
    /// and another [`ClientError`] when the client fails.
    fn fetch(
        &self,
        topic: &str,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<Vec<Record>, ClientError>> + Send;

    /// The partitions of `topic`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the topic,
    /// and another [`ClientError`] when the client fails.
    fn metadata_for_topic(&self, topic: &str) -> impl Future<Output = Result<TopicMetadata, ClientError>> + Send;

    /// The partitions of every topic.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for a topic,
    /// and another [`ClientError`] when the client fails.
    fn metadata_for_all_topics(&self) -> impl Future<Output = Result<Vec<TopicMetadata>, ClientError>> + Send;

    /// The earliest and the latest offset of a partition.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the partition,
    /// and another [`ClientError`] when the client fails.
    fn watermarks(
        &self,
        topic: &str,
        partition: Partition,
    ) -> impl Future<Output = Result<Watermarks, ClientError>> + Send;
}

/// Configuration of a topic, as returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigs {
    pub configs: BTreeMap<String, Option<String>>,
}

pub trait Admin {
    /// Describe the configuration of `topic`. `keys` limits the result to
    /// those config names; empty means all.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the topic,
    /// and another [`ClientError`] when the client fails.
    fn describe_topic_configs(
        &self,
        topic: &str,
        keys: &[&str],
    ) -> impl Future<Output = Result<TopicConfigs, ClientError>> + Send;
}

pub trait ProtocolConsumer {
    /// Read the records of a partition from `offset` on.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the broker answers with an error code for the partition,
    /// and another [`ClientError`] when the client fails.
    fn fetch(
        &self,
        topic: &str,
        partition: Partition,
        offset: Offset,
    ) -> impl Future<Output = Result<Vec<Record>, ClientError>> + Send;

    /// The records of a partition from `offset` on, one fetch after the other.
    ///
    /// The stream ends after the first fetch that brings no records.
    /// A fetch that fails is the last item of the stream, as the [`ClientError`] it failed with.
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream;

    /// Join the group as a member.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the coordinator answers with an error code,
    /// [`ClientError::UnreadableAnswer`] when its answer names no member,
    /// and another [`ClientError`] when the client fails.
    fn join_group(&self, group_id: &str) -> impl Future<Output = Result<JoinGroupResult, ClientError>> + Send;

    /// Take the assignment of the generation the consumer joined.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the coordinator answers with an error code,
    /// and another [`ClientError`] when the client fails.
    fn sync_group(&self, group_id: &str) -> impl Future<Output = Result<(), ClientError>> + Send;

    /// Tell the coordinator of the group that the consumer is alive.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] when the coordinator answers with an error code,
    /// and another [`ClientError`] when the client fails.
    fn heartbeat(&self, group_id: &str) -> impl Future<Output = Result<(), ClientError>> + Send;
}
