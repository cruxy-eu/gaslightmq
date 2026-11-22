//! The APIs that keep the offsets of a group: `OffsetCommit`, `OffsetFetch`, `OffsetDelete` and `TxnOffsetCommit`.
//! Their parts are partitions.

use gaslightmq_wire::messages::offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic};
use gaslightmq_wire::messages::offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic};
use gaslightmq_wire::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions, OffsetFetchResponseTopic,
    OffsetFetchResponseTopics,
};
use gaslightmq_wire::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use gaslightmq_wire::messages::{
    OffsetCommitRequest, OffsetCommitResponse, OffsetDeleteRequest, OffsetDeleteResponse, OffsetFetchRequest,
    OffsetFetchResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};

use crate::body::{Editable, Keep, TopicKey, Verdict};
use crate::entry::{group, partition, response, response_in_parts, topic};

/// No offset: what a broker writes where a response has no offset to give.
const NO_OFFSET: i64 = -1;

impl Editable for OffsetCommitRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
            keep.partitions(asked, &mut topic.partitions, |partition| partition.partition_index)
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> OffsetCommitResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
            let indexes = topic.partitions.iter().map(|partition| partition.partition_index);
            let partitions = verdict.partitions(asked, indexes, |index, error_code| {
                OffsetCommitResponsePartition::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
            })?;
            Some(
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions),
            )
        });
        OffsetCommitResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(OffsetCommitResponse, topics);
topic!(
    OffsetCommitResponseTopic,
    |topic| TopicKey::new(&topic.name, topic.topic_id.into_bytes()),
    partitions
);
partition!(OffsetCommitResponsePartition, partition_index, error_code);

/// Up to version 7 an `OffsetFetch` asks for one group, and its topics stand in the request itself.
/// Since version 8 it asks for several groups, each with its topics.
/// A group that asks for every topic names none, so it has no parts.
impl Editable for OffsetFetchRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        if let Some(topics) = &mut self.topics {
            topics.retain_mut(|topic| {
                keep.partitions(TopicKey::named(&topic.name), &mut topic.partition_indexes, |index| {
                    *index
                })
            });
        }
        self.groups.retain_mut(|group| {
            let Some(topics) = &mut group.topics else {
                // The group asks for every topic. That is no part, and it stays.
                keep.something_else();
                return true;
            };
            let before = topics.len();
            topics.retain_mut(|topic| {
                let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
                keep.partitions(asked, &mut topic.partition_indexes, |index| *index)
            });
            // A group goes when it lost its last topic.
            topics.len() == before || !topics.is_empty()
        });
    }

    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> OffsetFetchResponse {
        if version >= 8 {
            let groups = self.groups.iter().filter_map(|group| {
                let topics: Vec<OffsetFetchResponseTopics> = group
                    .topics
                    .iter()
                    .flatten()
                    .filter_map(|topic| {
                        let asked = TopicKey::new(&topic.name, topic.topic_id.into_bytes());
                        let indexes = topic.partition_indexes.iter().copied();
                        let partitions = verdict.partitions(asked, indexes, |index, error_code| {
                            OffsetFetchResponsePartitions::default()
                                .with_partition_index(index)
                                .with_error_code(error_code)
                                .with_committed_offset(NO_OFFSET)
                        })?;
                        Some(
                            OffsetFetchResponseTopics::default()
                                .with_name(topic.name.clone())
                                .with_topic_id(topic.topic_id)
                                .with_partitions(partitions),
                        )
                    })
                    .collect();
                // A group is in the response when the whole call fails, or when one of its partitions does.
                (verdict.call() != 0 || !topics.is_empty()).then(|| {
                    OffsetFetchResponseGroup::default()
                        .with_group_id(group.group_id.clone())
                        .with_error_code(verdict.call())
                        .with_topics(topics)
                })
            });
            return OffsetFetchResponse::default().with_groups(groups.collect());
        }

        let topics = self.topics.iter().flatten().filter_map(|topic| {
            let indexes = topic.partition_indexes.iter().copied();
            let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
                OffsetFetchResponsePartition::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
                    .with_committed_offset(NO_OFFSET)
            })?;
            Some(
                OffsetFetchResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            )
        });
        // The response itself has an error code since version 2.
        let of_call = if version >= 2 { verdict.call() } else { 0 };
        OffsetFetchResponse::default()
            .with_error_code(of_call)
            .with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(OffsetFetchResponse, topics, groups);
topic!(
    OffsetFetchResponseTopic,
    |topic| TopicKey::named(&topic.name),
    partitions
);
partition!(OffsetFetchResponsePartition, partition_index, error_code);
group!(OffsetFetchResponseGroup, group_id, topics);
topic!(
    OffsetFetchResponseTopics,
    |topic| TopicKey::new(&topic.name, topic.topic_id.into_bytes()),
    partitions
);
partition!(OffsetFetchResponsePartitions, partition_index, error_code);

impl Editable for OffsetDeleteRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            keep.partitions(TopicKey::named(&topic.name), &mut topic.partitions, |partition| {
                partition.partition_index
            })
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> OffsetDeleteResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let indexes = topic.partitions.iter().map(|partition| partition.partition_index);
            let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
                OffsetDeleteResponsePartition::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
            })?;
            Some(
                OffsetDeleteResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            )
        });
        OffsetDeleteResponse::default()
            .with_error_code(verdict.call())
            .with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(OffsetDeleteResponse, topics);
topic!(
    OffsetDeleteResponseTopic,
    |topic| TopicKey::named(&topic.name),
    partitions
);
partition!(OffsetDeleteResponsePartition, partition_index, error_code);

impl Editable for TxnOffsetCommitRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain_mut(|topic| {
            keep.partitions(TopicKey::named(&topic.name), &mut topic.partitions, |partition| {
                partition.partition_index
            })
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> TxnOffsetCommitResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let indexes = topic.partitions.iter().map(|partition| partition.partition_index);
            let partitions = verdict.partitions(TopicKey::named(&topic.name), indexes, |index, error_code| {
                TxnOffsetCommitResponsePartition::default()
                    .with_partition_index(index)
                    .with_error_code(error_code)
            })?;
            Some(
                TxnOffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            )
        });
        TxnOffsetCommitResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(TxnOffsetCommitResponse, topics);
topic!(
    TxnOffsetCommitResponseTopic,
    |topic| TopicKey::named(&topic.name),
    partitions
);
partition!(TxnOffsetCommitResponsePartition, partition_index, error_code);
