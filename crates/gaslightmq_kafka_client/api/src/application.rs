use std::future::Future;
use std::pin::Pin;

use futures::Stream;
use gaslightmq_wire::ResponseError;
use gaslightmq_wire::{Offset, Partition, Record};

pub type RecordStream = Pin<Box<dyn Stream<Item = Record> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResult {
    pub error: Option<ResponseError>,
    pub partition: Partition,
    pub offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResult {
    pub error: Option<ResponseError>,
    pub member_id: String,
    pub leader: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResult {
    pub error: Option<ResponseError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResult {
    pub error: Option<ResponseError>,
}

pub trait Producer {
    fn send(&self, topic: &str, partition: Partition, records: &[Record])
    -> impl Future<Output = ProduceResult> + Send;
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
    fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> impl Future<Output = Vec<Record>> + Send;
    fn metadata_for_topic(&self, topic: &str) -> impl Future<Output = TopicMetadata> + Send;
    fn metadata_for_all_topics(&self) -> impl Future<Output = Vec<TopicMetadata>> + Send;
    fn watermarks(&self, topic: &str, partition: Partition) -> impl Future<Output = Watermarks> + Send;
}

/// Configuration of a topic, as returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigs {
    pub error: Option<ResponseError>,
    pub configs: std::collections::BTreeMap<String, Option<String>>,
}

pub trait Admin {
    /// Describe the configuration of `topic`. `keys` limits the result to
    /// those config names; empty means all.
    fn describe_topic_configs(&self, topic: &str, keys: &[&str]) -> impl Future<Output = TopicConfigs> + Send;
}

pub trait ProtocolConsumer {
    fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> impl Future<Output = Vec<Record>> + Send;
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream;
    fn join_group(&self, group_id: &str) -> impl Future<Output = JoinGroupResult> + Send;
    fn sync_group(&self, group_id: &str) -> impl Future<Output = SyncGroupResult> + Send;
    fn heartbeat(&self, group_id: &str) -> impl Future<Output = HeartbeatResult> + Send;
}
