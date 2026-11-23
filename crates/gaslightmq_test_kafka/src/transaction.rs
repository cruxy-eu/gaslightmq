//! A transaction on the cluster, as a transactional producer runs it.
//!
//! A test needs one to see what `isolation.level` does:
//! a consumer that reads committed records only stops before the first record of a transaction that is still open.
//!
//! # The batch of a transaction
//!
//! `gaslightmq_wire` encodes a record batch without a producer.
//! A batch of a transaction names its producer and says that it is transactional, in the header of the batch.
//! This module therefore sets those fields in the encoded bytes, and computes the checksum of the batch again.
//! The broker checks both: a batch that is set up wrongly is refused.

use bytes::{Bytes, BytesMut};
use gaslightmq_kernel::BrokerId;
use gaslightmq_wire::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use gaslightmq_wire::messages::{
    AddPartitionsToTxnRequest, EndTxnRequest, FindCoordinatorRequest, InitProducerIdRequest, ProducerId,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Offset, Record, RecordBatch};

use crate::cluster::{Broker, KafkaCluster};
use crate::error::KafkaClusterError;
use crate::metadata::wire_name;
use crate::partition::PartitionLog;
use crate::{version, wire_millis};

/// The key type with which `FindCoordinator` asks for the coordinator of a transaction.
const TRANSACTION_COORDINATOR: i8 = 1;

/// Where the fields of a record batch stand, in the format Kafka has written since 0.11.
/// The checksum covers everything from the attributes to the end of the batch.
const CHECKSUM_AT: usize = 17;
const ATTRIBUTES_AT: usize = 21;
const PRODUCER_ID_AT: usize = 43;
const PRODUCER_EPOCH_AT: usize = 51;
const BASE_SEQUENCE_AT: usize = 53;
const RECORDS_AT: usize = 61;

/// The bit of the attributes that says a batch belongs to a transaction.
const TRANSACTIONAL: u8 = 0b0001_0000;

/// A transaction that is open on the cluster.
///
/// [`KafkaCluster::begin_transaction`] begins one.
/// The records it [writes](Self::produce) are in the log at once, with an offset each.
/// A consumer that reads committed records only gets none of them until the transaction [commits](Self::commit),
/// and none of the records behind them either.
///
/// ```no_run
/// use gaslightmq_test_kafka::KafkaCluster;
/// use gaslightmq_wire::Record;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), gaslightmq_test_kafka::KafkaClusterError> {
/// let cluster = KafkaCluster::plaintext().start().await?;
/// let payments = cluster.topics().create("payments").await?.partition(0);
///
/// let mut transaction = cluster.begin_transaction("payments-tx").await?;
/// let pending = transaction.produce(&payments, &[Record::new("pending")]).await?;
///
/// // The record is in the log, and the stable part of the log ends before it.
/// assert_eq!(payments.offsets().await?.end.value(), pending.value() + 1);
/// assert_eq!(payments.last_stable_offset().await?, pending);
///
/// transaction.commit().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Transaction<'a> {
    cluster: &'a KafkaCluster,
    transactional_id: String,
    coordinator: &'a Broker,
    producer_id: i64,
    producer_epoch: i16,
    /// The partitions the transaction wrote to, each with the sequence number of its next record.
    written: Vec<(PartitionLog<'a>, i32)>,
}

/// How a transaction ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Committed,
    Aborted,
}

impl<'a> Transaction<'a> {
    pub(crate) async fn begin(cluster: &'a KafkaCluster, transactional_id: &str) -> Result<Self, KafkaClusterError> {
        let mut transaction = Self {
            cluster,
            transactional_id: transactional_id.to_owned(),
            coordinator: cluster.any_broker(),
            producer_id: -1,
            producer_epoch: -1,
            written: Vec::new(),
        };
        transaction.coordinator = transaction.find_coordinator().await?;
        (transaction.producer_id, transaction.producer_epoch) = transaction.init_producer_id().await?;
        Ok(transaction)
    }

    /// The `transactional.id` the transaction runs under.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Write `records` to `partition` as one batch of the transaction, and return the offset of the first one.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the coordinator or of the leader of the partition,
    /// and [`KafkaClusterError::InvalidRecords`] when there are no records, or they make no batch.
    pub async fn produce(
        &mut self,
        partition: &PartitionLog<'a>,
        records: &[Record],
    ) -> Result<Offset, KafkaClusterError> {
        let invalid = |reason: String| KafkaClusterError::InvalidRecords { reason };
        let plain = RecordBatch::new(records)
            .encode()
            .map_err(|error| invalid(error.to_string()))?;
        let place = self.place_of(partition).await?;
        let sequence = self.written[place].1;
        let batch = in_transaction(&plain, self.producer_id, self.producer_epoch, sequence)
            .ok_or_else(|| invalid("a transaction writes at least one record at a time".to_owned()))?;
        let offset = partition.produce_batch(batch, Some(&self.transactional_id)).await?;
        let records = i32::try_from(records.len()).unwrap_or(i32::MAX);
        self.written[place].1 = sequence.saturating_add(records);
        Ok(offset)
    }

    /// Commit the transaction, and return once its records are stable in every partition it wrote to.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the coordinator.
    pub async fn commit(self) -> Result<(), KafkaClusterError> {
        self.end(Outcome::Committed).await
    }

    /// Abort the transaction, and return once every partition it wrote to holds the marker that says so.
    /// The records stay in the log, and a consumer that reads committed records only leaves them out.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the coordinator.
    pub async fn abort(self) -> Result<(), KafkaClusterError> {
        self.end(Outcome::Aborted).await
    }

    /// Where `partition` stands in the list of written partitions. A new partition joins the transaction first.
    async fn place_of(&mut self, partition: &PartitionLog<'a>) -> Result<usize, KafkaClusterError> {
        let same = |written: &(PartitionLog<'a>, i32)| {
            written.0.topic() == partition.topic() && written.0.partition() == partition.partition()
        };
        if let Some(place) = self.written.iter().position(same) {
            return Ok(place);
        }
        let request = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(self.wire_id())
            .with_v3_and_below_producer_id(ProducerId(self.producer_id))
            .with_v3_and_below_producer_epoch(self.producer_epoch)
            .with_v3_and_below_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(wire_name(partition.topic()))
                    .with_partitions(vec![i32::from(partition.partition())]),
            ]);
        self.settle(ApiKey::AddPartitionsToTxn, async || {
            let added = self
                .cluster
                .ask(self.coordinator, version::ADD_PARTITIONS_TO_TXN, &request)
                .await?;
            Ok(added
                .results_by_topic_v3_and_below
                .iter()
                .flat_map(|topic| &topic.results_by_partition)
                .map(|result| result.partition_error_code)
                .find(|error_code| *error_code != 0)
                .map_or(Ok(()), Err))
        })
        .await?;
        self.written.push((partition.clone(), 0));
        Ok(self.written.len() - 1)
    }

    async fn end(self, outcome: Outcome) -> Result<(), KafkaClusterError> {
        // A transaction that wrote nothing never began for the coordinator, so there is nothing to end.
        if self.written.is_empty() {
            return Ok(());
        }
        let request = EndTxnRequest::default()
            .with_transactional_id(self.wire_id())
            .with_producer_id(ProducerId(self.producer_id))
            .with_producer_epoch(self.producer_epoch)
            .with_committed(outcome == Outcome::Committed);
        self.settle(ApiKey::EndTxn, async || {
            let ended = self.cluster.ask(self.coordinator, version::END_TXN, &request).await?;
            Ok(code(ended.error_code))
        })
        .await?;
        // The coordinator writes the marker of each partition after it answered.
        let waited = self.cluster.configuration().default_api_timeout;
        self.cluster
            .until(waited.duration(), || self.every_partition_is_stable())
            .await
            .ok_or_else(|| KafkaClusterError::not_settled(&self.about(), waited))
    }

    /// Whether the stable part of every written partition reaches the end of its log.
    async fn every_partition_is_stable(&self) -> Option<()> {
        for (partition, _) in &self.written {
            let end = partition.offsets().await.ok()?.end;
            if partition.last_stable_offset().await.ok()? != end {
                return None;
            }
        }
        Some(())
    }

    /// The broker that coordinates the transaction.
    /// The first question makes the cluster create the topic that holds the state of its transactions,
    /// so the answer may take a moment.
    async fn find_coordinator(&self) -> Result<&'a Broker, KafkaClusterError> {
        let request = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(self.transactional_id.clone()))
            .with_key_type(TRANSACTION_COORDINATOR);
        let coordinator = self
            .settle(ApiKey::FindCoordinator, async || {
                let found = self
                    .cluster
                    .ask(self.cluster.any_broker(), version::FIND_COORDINATOR, &request)
                    .await?;
                Ok(code(found.error_code).map(|()| found.node_id.0))
            })
            .await?;
        BrokerId::from_wire(coordinator)
            .ok()
            .flatten()
            .and_then(|coordinator| self.cluster.broker(coordinator))
            .ok_or_else(|| KafkaClusterError::Unanswered {
                request: format!("{:?} for {}", ApiKey::FindCoordinator, self.about()),
                reason: format!("the cluster named broker {coordinator}, which is none of its brokers"),
            })
    }

    /// The producer ID and epoch the coordinator gives the transactional ID.
    async fn init_producer_id(&self) -> Result<(i64, i16), KafkaClusterError> {
        let request = InitProducerIdRequest::default()
            .with_transactional_id(Some(self.wire_id()))
            .with_transaction_timeout_ms(wire_millis(self.cluster.configuration().transaction_timeout));
        self.settle(ApiKey::InitProducerId, async || {
            let initialized = self
                .cluster
                .ask(self.coordinator, version::INIT_PRODUCER_ID, &request)
                .await?;
            Ok(code(initialized.error_code).map(|()| (initialized.producer_id.0, initialized.producer_epoch)))
        })
        .await
    }

    /// Ask the coordinator until it answers with something else than an error that passes,
    /// such as "the coordinator is loading" or "the transaction before this one is still ending".
    /// `ask` returns the error code of the answer when there is one.
    async fn settle<T>(
        &self,
        api: ApiKey,
        ask: impl AsyncFn() -> Result<Result<T, i16>, KafkaClusterError>,
    ) -> Result<T, KafkaClusterError> {
        let waited = self.cluster.configuration().default_api_timeout;
        self.cluster
            .until(waited.duration(), async || match ask().await {
                Ok(Ok(answer)) => Some(Ok(answer)),
                Ok(Err(error_code)) => match KafkaClusterError::refused(api, &self.about(), error_code) {
                    Err(KafkaClusterError::Refused { error, .. }) if error.is_retriable() => None,
                    Err(
                        refused @ (KafkaClusterError::Refused { .. }
                        // cannot come from an error code
                        | KafkaClusterError::NotStarted { .. }
                        | KafkaClusterError::InvalidTopicName { .. }
                        | KafkaClusterError::InvalidRecords { .. }
                        | KafkaClusterError::Unanswered { .. }
                        | KafkaClusterError::NotSettledInTime { .. }),
                    ) => Some(Err(refused)),
                    Ok(()) => None,
                },
                Err(unanswered) => Some(Err(unanswered)),
            })
            .await
            .unwrap_or_else(|| Err(KafkaClusterError::not_settled(&self.about(), waited)))
    }

    /// How an error names the transaction.
    fn about(&self) -> String {
        format!("transaction `{}`", self.transactional_id)
    }

    fn wire_id(&self) -> gaslightmq_wire::messages::TransactionalId {
        gaslightmq_wire::messages::TransactionalId(StrBytes::from_string(self.transactional_id.clone()))
    }
}

/// An error code as a result: `Ok` for "no error".
fn code(error_code: i16) -> Result<(), i16> {
    if error_code == 0 { Ok(()) } else { Err(error_code) }
}

/// `batch` as a batch of a transaction: with its producer, the sequence number of its first record,
/// the bit that says it is transactional, and the checksum of the changed bytes.
/// `None` when `batch` is too short to be a record batch, as the encoding of no records is.
fn in_transaction(batch: &Bytes, producer_id: i64, producer_epoch: i16, base_sequence: i32) -> Option<Bytes> {
    if batch.len() < RECORDS_AT {
        return None;
    }
    let mut batch = BytesMut::from(&batch[..]);
    batch[ATTRIBUTES_AT + 1] |= TRANSACTIONAL;
    batch[PRODUCER_ID_AT..PRODUCER_EPOCH_AT].copy_from_slice(&producer_id.to_be_bytes());
    batch[PRODUCER_EPOCH_AT..BASE_SEQUENCE_AT].copy_from_slice(&producer_epoch.to_be_bytes());
    batch[BASE_SEQUENCE_AT..BASE_SEQUENCE_AT + 4].copy_from_slice(&base_sequence.to_be_bytes());
    let checksum = crc32c(&batch[ATTRIBUTES_AT..]);
    batch[CHECKSUM_AT..ATTRIBUTES_AT].copy_from_slice(&checksum.to_be_bytes());
    Some(batch.freeze())
}

/// The CRC-32C checksum of `bytes`, which a record batch carries.
fn crc32c(bytes: &[u8]) -> u32 {
    /// The polynomial of CRC-32C, with its bits reversed.
    const POLYNOMIAL: u32 = 0x82F6_3B78;
    !bytes.iter().fold(!0, |checksum, byte| {
        (0..8).fold(checksum ^ u32::from(*byte), |checksum, _| {
            if checksum & 1 == 1 {
                (checksum >> 1) ^ POLYNOMIAL
            } else {
                checksum >> 1
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checksum_is_crc_32c() {
        // The check value of CRC-32C.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn a_batch_of_a_transaction_keeps_its_records() {
        let plain = RecordBatch::new([Record::new("pending").with_key("p-1"), Record::new("settled")])
            .encode()
            .unwrap();

        let transactional = in_transaction(&plain, 4_000, 5, 7).unwrap();

        assert_eq!(transactional.len(), plain.len());
        assert_eq!(transactional[RECORDS_AT..], plain[RECORDS_AT..]);
        assert_eq!(transactional[PRODUCER_ID_AT..PRODUCER_EPOCH_AT], 4_000i64.to_be_bytes());
        assert_eq!(transactional[PRODUCER_EPOCH_AT..BASE_SEQUENCE_AT], 5i16.to_be_bytes());
        assert_eq!(
            transactional[BASE_SEQUENCE_AT..BASE_SEQUENCE_AT + 4],
            7i32.to_be_bytes()
        );
        // The codec checks the checksum when it decodes, so the batch reads back only when the checksum is right.
        let read = RecordBatch::decode_all(&transactional).unwrap();
        assert_eq!(read[0].records.len(), 2);
        assert_eq!(read[0].records[0].key.as_deref(), Some(&b"p-1"[..]));
    }

    #[test]
    fn the_checksum_of_the_codec_is_the_one_computed_here() {
        let plain = RecordBatch::new([Record::new("pending")]).encode().unwrap();

        let written = u32::from_be_bytes(plain[CHECKSUM_AT..ATTRIBUTES_AT].try_into().unwrap());

        assert_eq!(written, crc32c(&plain[ATTRIBUTES_AT..]));
    }

    #[test]
    fn no_records_make_no_batch() {
        let none = RecordBatch::new(Vec::new()).encode().unwrap();

        assert_eq!(in_transaction(&none, 4_000, 5, 0), None);
    }
}
