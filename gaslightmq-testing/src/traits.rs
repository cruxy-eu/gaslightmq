use async_trait::async_trait;
use futures::Stream;
use gaslightmq_kernel::{Offset, Partition};
use kafka_protocol::error::ResponseError;
use std::pin::Pin;

use crate::record::Record;

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

#[async_trait]
pub trait Producer {
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> ProduceResult;
}

#[async_trait]
pub trait Consumer {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record>;
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream;
    async fn join_group(&self, group_id: &str) -> JoinGroupResult;
    async fn sync_group(&self, group_id: &str) -> SyncGroupResult;
    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult;
}
