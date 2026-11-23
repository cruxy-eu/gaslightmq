//! The log of one partition.

use std::ops::Range;

use bytes::Bytes;
use gaslightmq_kernel::{BrokerId, TopicName};
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::{FetchRequest, ListOffsetsRequest, ProduceRequest, TransactionalId};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Offset, Partition, Record, RecordBatch, ResponseError};

use crate::cluster::{Broker, KafkaCluster};
use crate::error::KafkaClusterError;
use crate::metadata::{self, wire_name};
use crate::{version, wire_millis};

/// The timestamps with which `ListOffsets` asks for an end of the log.
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;

/// What a Fetch and a `ListOffsets` carry for a client that reads records of open transactions too,
/// and for one that does not.
const READ_UNCOMMITTED: i8 = 0;
const READ_COMMITTED: i8 = 1;

/// The `replica_id` of a request that comes from a client, and not from another broker.
/// A broker answers another broker with the end of its log, whatever the high watermark is.
const NOT_A_REPLICA: gaslightmq_wire::messages::BrokerId = gaslightmq_wire::messages::BrokerId(-1);

/// What a Produce carries for "every in-sync replica has the records".
const ACKS_ALL: i16 = -1;

/// The log of one partition of a topic: write records to it, and read back what it holds.
///
/// Every call asks the cluster who leads the partition, and sends one request to that broker.
/// It retries nothing: an error of the broker comes back as it is.
#[derive(Debug, Clone)]
pub struct PartitionLog<'a> {
    cluster: &'a KafkaCluster,
    topic: TopicName,
    partition: Partition,
}

impl<'a> PartitionLog<'a> {
    pub(crate) fn new(cluster: &'a KafkaCluster, topic: TopicName, partition: Partition) -> Self {
        Self {
            cluster,
            topic,
            partition,
        }
    }

    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// The broker that leads the partition.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with `UnknownTopicOrPartition` when the topic has no such partition,
    /// and with `LeaderNotAvailable` when no broker leads it.
    pub async fn leader(&self) -> Result<BrokerId, KafkaClusterError> {
        self.leading_broker().await.map(Broker::id)
    }

    /// Write `records` as one batch, and return the offset of the first one.
    /// Every in-sync replica has the records when this returns.
    ///
    /// The records are written as they are: one without a timestamp gets the timestamp 0, and not the time of the write.
    /// The cluster keeps records of any age, so such a record stays.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the leader,
    /// and [`KafkaClusterError::InvalidRecords`] when the records make no batch.
    pub async fn produce(&self, records: &[Record]) -> Result<Offset, KafkaClusterError> {
        let batch = RecordBatch::new(records)
            .encode()
            .map_err(|error| KafkaClusterError::InvalidRecords {
                reason: error.to_string(),
            })?;
        self.produce_batch(batch, None).await
    }

    /// Every record the partition holds, from its first offset to its last.
    /// The marker that ends a transaction is a record of the log, and it is among them.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the leader.
    pub async fn records(&self) -> Result<Vec<Record>, KafkaClusterError> {
        let leader = self.leading_broker().await?;
        let held = self.offsets_at(leader, READ_UNCOMMITTED).await?;
        let mut records = Vec::new();
        let mut next = held.start;
        while next < held.end {
            let fetched = self.fetch(leader, next).await?;
            let last = fetched
                .iter()
                .filter_map(|record| record.offset)
                .filter(|offset| *offset >= next)
                .max()
                .ok_or_else(|| KafkaClusterError::Unanswered {
                    request: format!("Fetch for {}", self.about()),
                    reason: format!("the leader gave no record from offset {next} on, and holds more"),
                })?;
            records.extend(fetched.into_iter().filter(|record| {
                record
                    .offset
                    .is_some_and(|offset| held.contains(&offset) && offset >= next)
            }));
            next = Offset::new(last.value().saturating_add(1));
        }
        Ok(records)
    }

    /// The offsets the partition holds: from the first one it still has, up to the high watermark,
    /// which is the offset the next record gets.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the leader.
    pub async fn offsets(&self) -> Result<Range<Offset>, KafkaClusterError> {
        let leader = self.leading_broker().await?;
        self.offsets_at(leader, READ_UNCOMMITTED).await
    }

    /// The offset of the first record of a transaction that is still open, or the high watermark without one.
    /// It is the offset up to which the leader gives records to a consumer that reads committed records only.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the leader.
    pub async fn last_stable_offset(&self) -> Result<Offset, KafkaClusterError> {
        let leader = self.leading_broker().await?;
        self.list_offset(leader, LATEST, READ_COMMITTED).await
    }

    /// How an error names the partition.
    pub(crate) fn about(&self) -> String {
        format!("partition {} of topic `{}`", self.partition, self.topic)
    }

    /// Write an encoded batch, as part of the transaction of `transactional_id` when there is one.
    pub(crate) async fn produce_batch(
        &self,
        batch: Bytes,
        transactional_id: Option<&str>,
    ) -> Result<Offset, KafkaClusterError> {
        let leader = self.leading_broker().await?;
        let request = ProduceRequest::default()
            .with_transactional_id(transactional_id.map(|id| TransactionalId(StrBytes::from_string(id.to_owned()))))
            .with_acks(ACKS_ALL)
            .with_timeout_ms(wire_millis(self.cluster.configuration().request_timeout))
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(wire_name(&self.topic))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(i32::from(self.partition))
                            .with_records(Some(batch)),
                    ]),
            ]);
        let produced = self.cluster.ask(leader, version::PRODUCE, &request).await?;
        let written = produced
            .responses
            .iter()
            .filter(|topic| topic.name.as_str() == self.topic.as_str())
            .flat_map(|topic| &topic.partition_responses)
            .find(|partition| partition.index == i32::from(self.partition))
            .ok_or_else(|| self.not_named(ApiKey::Produce))?;
        self.accepted(ApiKey::Produce, written.error_code)?;
        Ok(Offset::from(written.base_offset))
    }

    async fn offsets_at(&self, leader: &Broker, isolation_level: i8) -> Result<Range<Offset>, KafkaClusterError> {
        Ok(self.list_offset(leader, EARLIEST, isolation_level).await?
            ..self.list_offset(leader, LATEST, isolation_level).await?)
    }

    async fn list_offset(&self, leader: &Broker, end: i64, isolation_level: i8) -> Result<Offset, KafkaClusterError> {
        let request = ListOffsetsRequest::default()
            .with_replica_id(NOT_A_REPLICA)
            .with_isolation_level(isolation_level)
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(wire_name(&self.topic))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(i32::from(self.partition))
                            .with_timestamp(end),
                    ]),
            ]);
        let listed = self.cluster.ask(leader, version::LIST_OFFSETS, &request).await?;
        let found = listed
            .topics
            .iter()
            .filter(|topic| topic.name.as_str() == self.topic.as_str())
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.partition_index == i32::from(self.partition))
            .ok_or_else(|| self.not_named(ApiKey::ListOffsets))?;
        self.accepted(ApiKey::ListOffsets, found.error_code)?;
        Ok(Offset::from(found.offset))
    }

    /// One Fetch from `offset` on, which the leader answers at once.
    /// It sets no limit of its own to the size of the answer, so the limits of the broker apply.
    async fn fetch(&self, leader: &Broker, offset: Offset) -> Result<Vec<Record>, KafkaClusterError> {
        let request = FetchRequest::default()
            .with_replica_id(NOT_A_REPLICA)
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_max_bytes(i32::MAX)
            .with_isolation_level(READ_UNCOMMITTED)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(wire_name(&self.topic))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(i32::from(self.partition))
                            .with_fetch_offset(i64::from(offset))
                            .with_partition_max_bytes(i32::MAX),
                    ]),
            ]);
        let fetched = self.cluster.ask(leader, version::FETCH, &request).await?;
        self.accepted(ApiKey::Fetch, fetched.error_code)?;
        let partition = fetched
            .responses
            .iter()
            .filter(|topic| topic.topic.as_str() == self.topic.as_str())
            .flat_map(|topic| &topic.partitions)
            .find(|partition| partition.partition_index == i32::from(self.partition))
            .ok_or_else(|| self.not_named(ApiKey::Fetch))?;
        self.accepted(ApiKey::Fetch, partition.error_code)?;
        let Some(batches) = &partition.records else {
            return Ok(Vec::new());
        };
        let batches = RecordBatch::decode_all(batches).map_err(|error| KafkaClusterError::Unanswered {
            request: format!("Fetch for {}", self.about()),
            reason: error.to_string(),
        })?;
        Ok(batches.into_iter().flat_map(|batch| batch.records).collect())
    }

    /// The broker that leads the partition, as the first broker of the cluster knows it.
    async fn leading_broker(&self) -> Result<&'a Broker, KafkaClusterError> {
        let cluster = self.cluster;
        let described = metadata::of_topics(cluster, cluster.any_broker(), Some(&[&self.topic])).await?;
        let topic = metadata::topic(&described, &self.topic).ok_or_else(|| self.not_named(ApiKey::Metadata))?;
        self.accepted(ApiKey::Metadata, topic.error_code)?;
        // Metadata lists the partitions a topic has, and has no error for one that it does not list.
        // A broker answers a request about such a partition with this error.
        let partition = topic
            .partitions
            .iter()
            .find(|partition| partition.partition_index == i32::from(self.partition))
            .ok_or_else(|| {
                self.unknown(
                    ResponseError::UnknownTopicOrPartition,
                    "the topic has no such partition",
                )
            })?;
        self.accepted(ApiKey::Metadata, partition.error_code)?;
        BrokerId::from_wire(partition.leader_id.0)
            .ok()
            .flatten()
            .and_then(|leader| cluster.broker(leader))
            .ok_or_else(|| self.unknown(ResponseError::LeaderNotAvailable, "no broker of the cluster leads it"))
    }

    /// The error of the cluster in `error_code`, when there is one.
    fn accepted(&self, api: ApiKey, error_code: i16) -> Result<(), KafkaClusterError> {
        KafkaClusterError::refused(api, &self.about(), error_code)
    }

    fn not_named(&self, api: ApiKey) -> KafkaClusterError {
        KafkaClusterError::not_named(api, &self.about())
    }

    /// What the Metadata of the cluster says about a partition that no request can go to.
    fn unknown(&self, error: ResponseError, detail: &str) -> KafkaClusterError {
        KafkaClusterError::Refused {
            api: ApiKey::Metadata,
            error,
            about: self.about(),
            detail: Some(detail.to_owned()),
        }
    }
}
