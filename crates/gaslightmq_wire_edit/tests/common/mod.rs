//! What the tests over every API share: the topics of the examples, and one request of every API that has parts.
//! A test file that includes this module uses all of it, so nothing here is dead code in any of them.
//!
//! Every request names the same three parts, so one test covers every API and every version of it.
//! On an API whose parts are partitions they are `orders.eu/0`, `orders.eu/1` and `payments.settled/0`.
//! On an API whose parts are topics they are `orders.eu`, `payments.settled` and `audit.log`.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
};
use gaslightmq_wire::messages::create_partitions_request::CreatePartitionsTopic;
use gaslightmq_wire::messages::create_topics_request::CreatableTopic;
use gaslightmq_wire::messages::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};
use gaslightmq_wire::messages::delete_topics_request::DeleteTopicState;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic};
use gaslightmq_wire::messages::offset_delete_request::{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic};
use gaslightmq_wire::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use gaslightmq_wire::messages::offset_for_leader_epoch_request::{OffsetForLeaderPartition, OffsetForLeaderTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use gaslightmq_wire::messages::{
    AddPartitionsToTxnRequest, CreatePartitionsRequest, CreateTopicsRequest, DeleteRecordsRequest, DeleteTopicsRequest,
    FetchRequest, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetDeleteRequest, OffsetFetchRequest,
    OffsetForLeaderEpochRequest, ProduceRequest, TxnOffsetCommitRequest,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Record, RecordBatch, messages};
use gaslightmq_wire_edit::prelude::*;

pub const ORDERS: &str = "orders.eu";
pub const PAYMENTS: &str = "payments.settled";
pub const AUDIT: &str = "audit.log";

/// The partitions that every request of an API with partitions names: two of one topic, and one of another.
pub const PARTITIONS: [(&str, &[i32]); 2] = [(ORDERS, &[0, 1]), (PAYMENTS, &[0])];

/// The topics that every request of an API with topics names.
pub const TOPICS: [&str; 3] = [ORDERS, PAYMENTS, AUDIT];

/// A text as Kafka messages hold it.
pub fn text(text: &'static str) -> StrBytes {
    StrBytes::from_static_str(text)
}

/// A topic name as Kafka messages hold it.
pub fn wire_name(name: &'static str) -> messages::TopicName {
    text(name).into()
}

/// The ID of an example topic as the kernel holds it: every byte is the position of the topic among [`TOPICS`], from one.
pub fn topic_id(name: &str) -> TopicId {
    TopicId::new(id_bytes(name)).unwrap()
}

fn id_bytes(name: &str) -> [u8; 16] {
    let position = TOPICS.iter().position(|topic| *topic == name).unwrap();
    [u8::try_from(position + 1).unwrap(); 16]
}

/// The ID of an example topic as Kafka messages hold it.
/// The messages have a UUID type of their own, which converts from bytes.
pub fn wire_id<Id: TryFrom<Vec<u8>>>(name: &str) -> Id
where
    Id::Error: std::fmt::Debug,
{
    id_bytes(name).to_vec().try_into().unwrap()
}

/// The name of every example topic by its ID.
pub fn topic_names() -> TopicNames {
    TOPICS
        .iter()
        .map(|name| (topic_id(name), TopicName::new(*name).unwrap()))
        .collect()
}

/// One record whose value is `value`, as a Produce carries it.
pub fn records(value: &'static str) -> bytes::Bytes {
    RecordBatch::new([Record::new(value)]).encode().unwrap()
}

/// A Produce that names topics by ID since version 13.
pub fn produce(version: i16) -> ProduceRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes.iter().map(|index| {
            PartitionProduceData::default()
                .with_index(*index)
                .with_records(Some(records("a record")))
        });
        let topic = TopicProduceData::default().with_partition_data(partitions.collect());
        if version >= 13 {
            topic.with_topic_id(wire_id(name))
        } else {
            topic.with_name(wire_name(name))
        }
    });
    ProduceRequest::default().with_acks(-1).with_topic_data(topics.to_vec())
}

/// A Fetch that names topics by ID since version 13.
pub fn fetch(version: i16) -> FetchRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| FetchPartition::default().with_partition(*index));
        let topic = FetchTopic::default().with_partitions(partitions.collect());
        if version >= 13 {
            topic.with_topic_id(wire_id(name))
        } else {
            topic.with_topic(wire_name(name))
        }
    });
    FetchRequest::default().with_topics(topics.to_vec())
}

pub fn list_offsets(_version: i16) -> ListOffsetsRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| ListOffsetsPartition::default().with_partition_index(*index));
        ListOffsetsTopic::default()
            .with_name(wire_name(name))
            .with_partitions(partitions.collect())
    });
    ListOffsetsRequest::default().with_topics(topics.to_vec())
}

pub fn offset_for_leader_epoch(_version: i16) -> OffsetForLeaderEpochRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| OffsetForLeaderPartition::default().with_partition(*index));
        OffsetForLeaderTopic::default()
            .with_topic(wire_name(name))
            .with_partitions(partitions.collect())
    });
    OffsetForLeaderEpochRequest::default().with_topics(topics.to_vec())
}

pub fn delete_records(_version: i16) -> DeleteRecordsRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| DeleteRecordsPartition::default().with_partition_index(*index));
        DeleteRecordsTopic::default()
            .with_name(wire_name(name))
            .with_partitions(partitions.collect())
    });
    DeleteRecordsRequest::default().with_topics(topics.to_vec())
}

pub fn offset_commit(_version: i16) -> OffsetCommitRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| OffsetCommitRequestPartition::default().with_partition_index(*index));
        OffsetCommitRequestTopic::default()
            .with_name(wire_name(name))
            .with_partitions(partitions.collect())
    });
    OffsetCommitRequest::default()
        .with_group_id(text("analytics-etl").into())
        .with_topics(topics.to_vec())
}

/// An `OffsetFetch` for one group, which is one of several groups since version 8.
pub fn offset_fetch(version: i16) -> OffsetFetchRequest {
    if version >= 8 {
        let topics = PARTITIONS.map(|(name, indexes)| {
            OffsetFetchRequestTopics::default()
                .with_name(wire_name(name))
                .with_partition_indexes(indexes.to_vec())
        });
        let group = OffsetFetchRequestGroup::default()
            .with_group_id(text("analytics-etl").into())
            .with_topics(Some(topics.to_vec()));
        OffsetFetchRequest::default().with_groups(vec![group])
    } else {
        let topics = PARTITIONS.map(|(name, indexes)| {
            OffsetFetchRequestTopic::default()
                .with_name(wire_name(name))
                .with_partition_indexes(indexes.to_vec())
        });
        OffsetFetchRequest::default()
            .with_group_id(text("analytics-etl").into())
            .with_topics(Some(topics.to_vec()))
    }
}

pub fn offset_delete(_version: i16) -> OffsetDeleteRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| OffsetDeleteRequestPartition::default().with_partition_index(*index));
        OffsetDeleteRequestTopic::default()
            .with_name(wire_name(name))
            .with_partitions(partitions.collect())
    });
    OffsetDeleteRequest::default()
        .with_group_id(text("analytics-etl").into())
        .with_topics(topics.to_vec())
}

pub fn txn_offset_commit(_version: i16) -> TxnOffsetCommitRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        let partitions = indexes
            .iter()
            .map(|index| TxnOffsetCommitRequestPartition::default().with_partition_index(*index));
        TxnOffsetCommitRequestTopic::default()
            .with_name(wire_name(name))
            .with_partitions(partitions.collect())
    });
    TxnOffsetCommitRequest::default()
        .with_transactional_id(text("payments-tx-1").into())
        .with_group_id(text("analytics-etl").into())
        .with_topics(topics.to_vec())
}

/// An `AddPartitionsToTxn` for one transaction, which is one of several transactions since version 4.
pub fn add_partitions_to_txn(version: i16) -> AddPartitionsToTxnRequest {
    let topics = PARTITIONS.map(|(name, indexes)| {
        AddPartitionsToTxnTopic::default()
            .with_name(wire_name(name))
            .with_partitions(indexes.to_vec())
    });
    if version >= 4 {
        let transaction = AddPartitionsToTxnTransaction::default()
            .with_transactional_id(text("payments-tx-1").into())
            .with_topics(topics.to_vec());
        AddPartitionsToTxnRequest::default().with_transactions(vec![transaction])
    } else {
        AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(text("payments-tx-1").into())
            .with_v3_and_below_topics(topics.to_vec())
    }
}

pub fn metadata(_version: i16) -> MetadataRequest {
    let topics = TOPICS.map(|name| MetadataRequestTopic::default().with_name(Some(wire_name(name))));
    MetadataRequest::default().with_topics(Some(topics.to_vec()))
}

pub fn create_topics(_version: i16) -> CreateTopicsRequest {
    let topics = TOPICS.map(|name| {
        CreatableTopic::default()
            .with_name(wire_name(name))
            .with_num_partitions(3)
            .with_replication_factor(1)
    });
    CreateTopicsRequest::default().with_topics(topics.to_vec())
}

/// A `DeleteTopics` that names topics in the list of version 6 since that version.
pub fn delete_topics(version: i16) -> DeleteTopicsRequest {
    if version >= 6 {
        let topics = TOPICS.map(|name| DeleteTopicState::default().with_name(Some(wire_name(name))));
        DeleteTopicsRequest::default().with_topics(topics.to_vec())
    } else {
        DeleteTopicsRequest::default().with_topic_names(TOPICS.map(wire_name).to_vec())
    }
}

pub fn create_partitions(_version: i16) -> CreatePartitionsRequest {
    let topics = TOPICS.map(|name| {
        CreatePartitionsTopic::default()
            .with_name(wire_name(name))
            .with_count(6)
    });
    CreatePartitionsRequest::default().with_topics(topics.to_vec())
}
