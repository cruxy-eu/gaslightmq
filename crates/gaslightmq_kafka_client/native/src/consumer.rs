//! The consumer.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use futures::StreamExt;
use gaslightmq_kafka_client_api::{
    Consumer, HeartbeatResult, JoinGroupResult, ProtocolConsumer, RecordStream, SyncGroupResult, TopicMetadata,
    Watermarks,
};
use gaslightmq_wire::messages::GroupId;
use gaslightmq_wire::{Offset, Partition, Record};

use crate::configuration::{ConsumerConfiguration, RoleNames};
use crate::error::ClientError;
use crate::io::access::ClusterAccess;
use crate::logic::call::Operation;
use crate::logic::cluster::{TopicPartition, group_id, topic_name};
use crate::logic::fetch::FetchRecords;
use crate::logic::group::Membership;
use crate::logic::offsets::{ListOffset, LogEnd};
use crate::logic::topics::DescribeTopics;

/// The names of the consumers that are configured without a client ID.
static NAMES: RoleNames = RoleNames::new("consumer");

/// A consumer: it reads records from the leader of a partition, and it is a member of consumer groups.
///
/// It has its own connections, its own view of the cluster, its own memberships and its own `client.id`.
/// Two consumers share nothing, and neither does a consumer with a producer or an admin.
///
/// # Groups
///
/// A consumer is one member of each group it joins: it has its own member ID and generation there.
/// Two consumers that join the same group are two members of it, as two applications would be.
/// Everything the consumer says about a group travels on connections of its own:
/// asking who coordinates the group, and talking to the coordinator.
/// A Fetch that waits for records therefore never holds up a Heartbeat.
///
/// A consumer joins a group without a subscription, so the group assigns it no partition.
/// It sends a heartbeat when [`heartbeat`](Self::heartbeat) is called, and not on its own.
///
/// # Failures
///
/// The methods of the consumer itself return a failure as a [`ClientError`].
/// The methods of [`Consumer`] return a value and no error, so they panic on a failure.
/// The group methods of [`ProtocolConsumer`] carry the closest Kafka error in their result.
pub struct KafkaConsumer {
    inner: Arc<Inner>,
}

/// What a consumer owns. The streams it hands out keep it alive.
struct Inner {
    configuration: ConsumerConfiguration,
    cluster: ClusterAccess,
    memberships: Mutex<HashMap<GroupId, Membership>>,
}

/// What the coordinator told a consumer that joined a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedGroup {
    /// The ID of the consumer as a member of the group.
    pub member_id: String,
    /// The generation of the group that the consumer is in.
    pub generation: i32,
    /// The member that leads the group in this generation.
    pub leader: String,
}

impl KafkaConsumer {
    /// A consumer with `configuration`. It connects when it first asks something.
    ///
    /// A consumer that is configured without a client ID names itself `consumer-1`,
    /// and the next one `consumer-2`, as Kafka's Java consumer does.
    pub fn new(mut configuration: ConsumerConfiguration) -> Self {
        configuration.client.client_id = NAMES.client_id(&configuration.client.client_id);
        Self {
            inner: Arc::new(Inner {
                cluster: ClusterAccess::new(configuration.client.clone()),
                configuration,
                memberships: Mutex::default(),
            }),
        }
    }

    /// What the consumer runs with. Its client ID is the one the consumer sends.
    pub fn configuration(&self) -> &ConsumerConfiguration {
        &self.inner.configuration
    }

    /// Read the records of a partition from `offset` on.
    ///
    /// The broker answers when it has [`fetch_min_bytes`](ConsumerConfiguration::fetch_min_bytes),
    /// or after [`fetch_max_wait`](ConsumerConfiguration::fetch_max_wait), with no records when none arrived.
    ///
    /// # Errors
    ///
    /// The error the broker or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Result<Vec<Record>, ClientError> {
        self.inner.fetch(topic, partition, offset).await
    }

    /// The records of a partition from `offset` on, one Fetch after the other.
    /// Each Fetch starts after the last record of the one before it.
    /// The stream ends at the first Fetch that brings no records or fails.
    pub fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        let consumer = Arc::clone(&self.inner);
        let topic = topic.to_owned();
        Box::pin(
            futures::stream::unfold(u64::from(offset), move |next_offset| {
                let consumer = Arc::clone(&consumer);
                let topic = topic.clone();
                async move {
                    let records = consumer
                        .fetch(&topic, partition, Offset::from(next_offset))
                        .await
                        .ok()?;
                    let last = records.last()?.offset?;
                    Some((records, u64::from(last).saturating_add(1)))
                }
            })
            .flat_map(futures::stream::iter),
        )
    }

    /// The partitions of `topic`.
    ///
    /// # Errors
    ///
    /// [`ClientError::Protocol`] when the response does not describe the topic.
    /// Otherwise the error the broker or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn metadata_for_topic(&self, topic: &str) -> Result<TopicMetadata, ClientError> {
        self.inner
            .describe_topics(Some(topic))
            .await?
            .into_iter()
            .find(|described| described.name == topic)
            .ok_or_else(|| ClientError::Protocol(format!("no metadata for topic {topic}")))
    }

    /// The partitions of every topic.
    ///
    /// # Errors
    ///
    /// The error the broker or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn metadata_for_all_topics(&self) -> Result<Vec<TopicMetadata>, ClientError> {
        self.inner.describe_topics(None).await
    }

    /// The earliest and the latest offset of a partition.
    ///
    /// # Errors
    ///
    /// The error the broker or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn watermarks(&self, topic: &str, partition: Partition) -> Result<Watermarks, ClientError> {
        let low = self.inner.list_offset(topic, partition, LogEnd::Earliest).await?;
        let high = self.inner.list_offset(topic, partition, LogEnd::Latest).await?;
        Ok(Watermarks {
            low: Offset::from(low),
            high: Offset::from(high),
        })
    }

    /// Join `group` as a member, or join it again under the member ID the consumer has there.
    ///
    /// The coordinator holds a join until every member of the group joined, for at most
    /// [`max_poll_interval`](ConsumerConfiguration::max_poll_interval).
    /// A join may therefore take that long and the request timeout on top,
    /// also when the default API timeout is shorter.
    ///
    /// # Errors
    ///
    /// The error the coordinator or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the time of the join was over.
    pub async fn join_group(&self, group: &str) -> Result<JoinedGroup, ClientError> {
        let inner = &self.inner;
        let group = group_id(group);
        let join = inner.membership(&group).join(&inner.configuration, group.clone());
        let (join, joined) = inner
            .cluster
            .carry_out(join, inner.configuration.default_api_timeout, None)
            .await;
        let mut memberships = inner.memberships();
        let membership = memberships.entry(group).or_default();
        match joined {
            Ok(joined) => {
                membership.joined(joined);
                Ok(JoinedGroup {
                    member_id: membership.member_id().to_owned(),
                    generation: membership.generation(),
                    leader: membership.leader().to_owned(),
                })
            }
            Err(error) => {
                membership.join_failed(&join, &error);
                Err(error)
            }
        }
    }

    /// Take the assignment of the generation the consumer joined, and return it as it arrived.
    /// When the consumer leads `group`, it also hands every member its assignment: no partition at all.
    ///
    /// # Errors
    ///
    /// The error the coordinator or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn sync_group(&self, group: &str) -> Result<Bytes, ClientError> {
        let inner = &self.inner;
        let group = group_id(group);
        let sync = inner.membership(&group).sync(group.clone());
        let synced = inner.call(sync).await;
        inner.remember(group, synced, |membership, assignment| {
            membership.synced(assignment);
            membership.assignment().clone()
        })
    }

    /// Tell the coordinator of `group` that the consumer is alive.
    ///
    /// # Errors
    ///
    /// The error the coordinator or the connection gave,
    /// or [`ClientError::CallTimedOut`] when retriable errors lasted until the default API timeout.
    pub async fn heartbeat(&self, group: &str) -> Result<(), ClientError> {
        let inner = &self.inner;
        let group = group_id(group);
        let heartbeat = inner.membership(&group).heartbeat(group.clone());
        let alive = inner.call(heartbeat).await;
        inner.remember(group, alive, |_, ()| ())
    }
}

impl Inner {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Result<Vec<Record>, ClientError> {
        let fetch = FetchRecords {
            configuration: &self.configuration,
            partition: TopicPartition::new(topic, partition),
            offset: i64::from(offset),
        };
        self.call(fetch).await
    }

    async fn describe_topics(&self, topic: Option<&str>) -> Result<Vec<TopicMetadata>, ClientError> {
        let describe = DescribeTopics {
            topic: topic.map(topic_name),
            allow_auto_create_topics: self.configuration.allow_auto_create_topics,
        };
        self.call(describe).await
    }

    async fn list_offset(&self, topic: &str, partition: Partition, end: LogEnd) -> Result<i64, ClientError> {
        let list = ListOffset {
            configuration: &self.configuration,
            partition: TopicPartition::new(topic, partition),
            end,
        };
        self.call(list).await
    }

    /// Carry out `operation` within the default API timeout.
    /// Kafka's consumer has no number of retries: only the time ends them.
    async fn call<O: Operation>(&self, operation: O) -> Result<O::Output, ClientError> {
        self.cluster
            .call(operation, self.configuration.default_api_timeout, None)
            .await
    }

    /// What the consumer knows about its membership of `group`, as it is now.
    fn membership(&self, group: &GroupId) -> Membership {
        self.memberships().get(group).cloned().unwrap_or_default()
    }

    /// Let the membership of `group` take over what the coordinator answered.
    fn remember<A, T>(
        &self,
        group: GroupId,
        answer: Result<A, ClientError>,
        accept: impl FnOnce(&mut Membership, A) -> T,
    ) -> Result<T, ClientError> {
        let mut memberships = self.memberships();
        let membership = memberships.entry(group).or_default();
        match answer {
            Ok(answer) => Ok(accept(membership, answer)),
            Err(error) => {
                membership.refused(&error);
                Err(error)
            }
        }
    }

    fn memberships(&self) -> MutexGuard<'_, HashMap<GroupId, Membership>> {
        self.memberships.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A consumer with the default configuration, which starts from `bootstrap`.
impl From<SocketAddr> for KafkaConsumer {
    fn from(bootstrap: SocketAddr) -> Self {
        Self::new(ConsumerConfiguration::new(bootstrap))
    }
}

/// The methods of [`Consumer`] return a value and no error.
/// A test that wants the error calls the method of the same name on [`KafkaConsumer`] itself.
#[expect(
    clippy::panic,
    reason = "the methods of `Consumer` return a value and no error, so a failure panics, as each method documents"
)]
impl Consumer for KafkaConsumer {
    /// # Panics
    ///
    /// When [`KafkaConsumer::fetch`] fails.
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        KafkaConsumer::fetch(self, topic, partition, offset)
            .await
            .unwrap_or_else(|error| panic!("fetch {topic}-{partition} failed: {error}"))
    }

    /// # Panics
    ///
    /// When [`KafkaConsumer::metadata_for_topic`] fails.
    async fn metadata_for_topic(&self, topic: &str) -> TopicMetadata {
        KafkaConsumer::metadata_for_topic(self, topic)
            .await
            .unwrap_or_else(|error| panic!("metadata for {topic} failed: {error}"))
    }

    /// # Panics
    ///
    /// When [`KafkaConsumer::metadata_for_all_topics`] fails.
    async fn metadata_for_all_topics(&self) -> Vec<TopicMetadata> {
        KafkaConsumer::metadata_for_all_topics(self)
            .await
            .unwrap_or_else(|error| panic!("metadata failed: {error}"))
    }

    /// # Panics
    ///
    /// When [`KafkaConsumer::watermarks`] fails.
    async fn watermarks(&self, topic: &str, partition: Partition) -> Watermarks {
        KafkaConsumer::watermarks(self, topic, partition)
            .await
            .unwrap_or_else(|error| panic!("watermarks of {topic}-{partition} failed: {error}"))
    }
}

impl ProtocolConsumer for KafkaConsumer {
    /// # Panics
    ///
    /// When [`KafkaConsumer::fetch`] fails.
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        Consumer::fetch(self, topic, partition, offset).await
    }

    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        KafkaConsumer::stream(self, topic, partition, offset)
    }

    async fn join_group(&self, group_id: &str) -> JoinGroupResult {
        match KafkaConsumer::join_group(self, group_id).await {
            Ok(joined) => JoinGroupResult {
                error: None,
                member_id: joined.member_id,
                leader: joined.leader,
            },
            Err(error) => JoinGroupResult {
                error: Some(error.to_response_error()),
                member_id: String::new(),
                leader: String::new(),
            },
        }
    }

    async fn sync_group(&self, group_id: &str) -> SyncGroupResult {
        SyncGroupResult {
            error: failure_of(KafkaConsumer::sync_group(self, group_id).await.map(drop)),
        }
    }

    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult {
        HeartbeatResult {
            error: failure_of(KafkaConsumer::heartbeat(self, group_id).await),
        }
    }
}

/// The closest Kafka error of a call that failed.
fn failure_of(result: Result<(), ClientError>) -> Option<gaslightmq_wire::ResponseError> {
    result.err().map(|error| error.to_response_error())
}
