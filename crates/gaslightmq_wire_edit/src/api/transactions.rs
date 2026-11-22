//! The APIs of producers and transactions.
//! `AddPartitionsToTxn` has partitions as parts. The others only fail as a whole.

use gaslightmq_wire::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use gaslightmq_wire::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
};
use gaslightmq_wire::messages::describe_producers_response::{PartitionResponse, TopicResponse};
use gaslightmq_wire::messages::describe_transactions_response::TransactionState;
use gaslightmq_wire::messages::write_txn_markers_response::{
    WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
};
use gaslightmq_wire::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, AllocateProducerIdsRequest,
    DescribeProducersRequest, DescribeProducersResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
    EndTxnRequest, InitProducerIdRequest, ListTransactionsRequest, WriteTxnMarkersRequest, WriteTxnMarkersResponse,
};

use super::fails_as_a_whole;
use crate::body::{Editable, Keep, TopicKey, Verdict};
use crate::entry::{group, partition, response, response_in_parts, topic};

fails_as_a_whole!(
    AddOffsetsToTxnRequest,
    AllocateProducerIdsRequest,
    EndTxnRequest,
    InitProducerIdRequest,
    ListTransactionsRequest,
);

/// Removes the partitions that `keep` does not keep from the topics of one transaction.
fn retain_partitions(topics: &mut Vec<AddPartitionsToTxnTopic>, keep: &mut Keep<'_>) {
    topics.retain_mut(|topic| keep.partitions(TopicKey::named(&topic.name), &mut topic.partitions, |index| *index));
}

/// The failed partitions of the topics of one transaction.
fn failed_partitions(topics: &[AddPartitionsToTxnTopic], verdict: &Verdict<'_>) -> Vec<AddPartitionsToTxnTopicResult> {
    let topics = topics.iter().filter_map(|topic| {
        let indexes = topic.partitions.iter().copied();
        let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
            AddPartitionsToTxnPartitionResult::default()
                .with_partition_index(index)
                .with_partition_error_code(error_code)
        })?;
        Some(
            AddPartitionsToTxnTopicResult::default()
                .with_name(topic.name.clone())
                .with_results_by_partition(partitions),
        )
    });
    topics.collect()
}

/// Up to version 3 an `AddPartitionsToTxn` is about one transaction, and its topics stand in the request itself.
/// Since version 4 it is about several transactions, each with its topics.
impl Editable for AddPartitionsToTxnRequest {
    crate::api::response_has_error_code!();

    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        retain_partitions(&mut self.v3_and_below_topics, keep);
        self.transactions.retain_mut(|transaction| {
            let before = transaction.topics.len();
            retain_partitions(&mut transaction.topics, keep);
            // A transaction goes when it lost its last topic.
            transaction.topics.len() == before || !transaction.topics.is_empty()
        });
    }

    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> AddPartitionsToTxnResponse {
        let transactions = self.transactions.iter().filter_map(|transaction| {
            let topics = failed_partitions(&transaction.topics, verdict);
            (!topics.is_empty()).then(|| {
                AddPartitionsToTxnResult::default()
                    .with_transactional_id(transaction.transactional_id.clone())
                    .with_topic_results(topics)
            })
        });
        // The response itself has an error code since version 4.
        let of_call = if version >= 4 { verdict.call() } else { 0 };
        AddPartitionsToTxnResponse::default()
            .with_error_code(of_call)
            .with_results_by_transaction(transactions.collect())
            .with_results_by_topic_v3_and_below(failed_partitions(&self.v3_and_below_topics, verdict))
    }

    response_in_parts!();
}

response!(
    AddPartitionsToTxnResponse,
    results_by_transaction,
    results_by_topic_v3_and_below
);
group!(AddPartitionsToTxnResult, transactional_id, topic_results);
topic!(
    AddPartitionsToTxnTopicResult,
    |topic| TopicKey::named(&topic.name),
    results_by_partition
);
partition!(AddPartitionsToTxnPartitionResult, partition_index, partition_error_code);

impl Editable for WriteTxnMarkersRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> WriteTxnMarkersResponse {
        let markers = self.markers.iter().map(|marker| {
            let topics = marker.topics.iter().map(|topic| {
                let partitions = topic.partition_indexes.iter().map(|index| {
                    WritableTxnMarkerPartitionResult::default()
                        .with_partition_index(*index)
                        .with_error_code(verdict.call())
                });
                WritableTxnMarkerTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions.collect())
            });
            WritableTxnMarkerResult::default()
                .with_producer_id(marker.producer_id)
                .with_topics(topics.collect())
        });
        WriteTxnMarkersResponse::default().with_markers(markers.collect())
    }
}

impl Editable for DescribeTransactionsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeTransactionsResponse {
        let transactions = self.transactional_ids.iter().map(|id| {
            TransactionState::default()
                .with_transactional_id(id.clone())
                .with_error_code(verdict.call())
        });
        DescribeTransactionsResponse::default().with_transaction_states(transactions.collect())
    }
}

impl Editable for DescribeProducersRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DescribeProducersResponse {
        let topics = self.topics.iter().map(|topic| {
            let partitions = topic.partition_indexes.iter().map(|index| {
                PartitionResponse::default()
                    .with_partition_index(*index)
                    .with_error_code(verdict.call())
            });
            TopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions.collect())
        });
        DescribeProducersResponse::default().with_topics(topics.collect())
    }
}
