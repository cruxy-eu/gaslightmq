//! The topics of a cluster.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use gaslightmq_kernel::{BrokerId, TopicName};
use gaslightmq_wire::messages::create_topics_request::{
    CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
};
use gaslightmq_wire::messages::describe_configs_request::DescribeConfigsResource;
use gaslightmq_wire::messages::{CreateTopicsRequest, DeleteTopicsRequest, DescribeConfigsRequest};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Partition, ResponseError};

use crate::cluster::KafkaCluster;
use crate::error::KafkaClusterError;
use crate::metadata::{self, wire_name};
use crate::partition::PartitionLog;
use crate::{version, wire_millis};

/// The resource type of a topic in `DescribeConfigs`.
const TOPIC_RESOURCE: i8 = 2;

/// What `CreateTopics` carries for "as the broker is configured": its `num.partitions` and `default.replication.factor`.
const BROKER_DEFAULT: i16 = -1;

/// The names this process handed out, so each one is new.
static NAMED: AtomicU64 = AtomicU64::new(0);

/// A name that starts with `prefix` and that no other call on the machine got while this process runs:
/// `orders-4711-1`, then `orders-4711-2`, with the ID of the process in the middle.
///
/// Tests that share a cluster use it for whatever they must not share: a topic, or a consumer group.
/// Tests of several processes that share a network of containers use it for the network and the aliases on it.
///
/// ```
/// use gaslightmq_test_kafka::unique_name;
///
/// assert_ne!(unique_name("analytics"), unique_name("analytics"));
/// assert!(unique_name("analytics").starts_with("analytics-"));
/// ```
pub fn unique_name(prefix: &str) -> String {
    let process = std::process::id();
    format!("{prefix}-{process}-{}", NAMED.fetch_add(1, Ordering::Relaxed) + 1)
}

/// A topic to create, named as Kafka's own admin client names it.
///
/// A topic without a number of partitions or a replication factor gets those of the broker:
/// its `num.partitions` and its `default.replication.factor`.
///
/// ```
/// use gaslightmq_test_kafka::NewTopic;
///
/// let orders = NewTopic::named("orders").with_partitions(3).with_config("retention.ms", "60000");
/// assert_eq!(orders.name(), "orders");
///
/// // A name of its own, for a test that shares its cluster.
/// let payments = NewTopic::unique("payments");
/// assert!(payments.name().starts_with("payments-"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    partitions: Option<u32>,
    replication_factor: Option<u16>,
    replica_assignment: Vec<Vec<BrokerId>>,
    configs: BTreeMap<String, String>,
}

impl NewTopic {
    /// A topic of that name. Creating it fails when the text is no topic name.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: None,
            replication_factor: None,
            replica_assignment: Vec::new(),
            configs: BTreeMap::new(),
        }
    }

    /// A topic with a [name of its own](unique_name), which starts with `prefix`.
    pub fn unique(prefix: &str) -> Self {
        Self::named(unique_name(prefix))
    }

    /// The name the topic will have.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Give the topic that many partitions.
    #[must_use]
    pub fn with_partitions(mut self, partitions: u32) -> Self {
        self.partitions = Some(partitions);
        self
    }

    /// Keep every partition on that many brokers.
    #[must_use]
    pub fn with_replication_factor(mut self, replication_factor: u16) -> Self {
        self.replication_factor = Some(replication_factor);
        self
    }

    /// Say which brokers hold each partition: one list of brokers per partition, in the order of the partitions.
    /// The first broker of a list leads its partition.
    /// The assignment decides the number of partitions and the replication factor, so it replaces both.
    #[must_use]
    pub fn with_replica_assignment<P, B>(mut self, replica_assignment: P) -> Self
    where
        P: IntoIterator<Item = B>,
        B: IntoIterator<Item = BrokerId>,
    {
        self.replica_assignment = replica_assignment
            .into_iter()
            .map(|brokers| brokers.into_iter().collect())
            .collect();
        self
    }

    /// Set a config of the topic, under the name Kafka gives it, such as `retention.ms`.
    #[must_use]
    pub fn with_config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(name.into(), value.into());
        self
    }

    /// The topic as `CreateTopics` carries it, under its checked `name`.
    fn creatable(&self, name: &TopicName) -> CreatableTopic {
        let assigned = !self.replica_assignment.is_empty();
        let partitions = match self.partitions {
            Some(partitions) if !assigned => i32::try_from(partitions).unwrap_or(i32::MAX),
            _ => i32::from(BROKER_DEFAULT),
        };
        let replication_factor = match self.replication_factor {
            Some(replication_factor) if !assigned => i16::try_from(replication_factor).unwrap_or(i16::MAX),
            _ => BROKER_DEFAULT,
        };
        let assignments = (0..)
            .zip(&self.replica_assignment)
            .map(|(partition, brokers)| {
                CreatableReplicaAssignment::default()
                    .with_partition_index(partition)
                    .with_broker_ids(
                        brokers
                            .iter()
                            .map(|broker| gaslightmq_wire::messages::BrokerId(i32::from(*broker)))
                            .collect(),
                    )
            })
            .collect();
        let configs = self
            .configs
            .iter()
            .map(|(name, value)| {
                CreatableTopicConfig::default()
                    .with_name(StrBytes::from_string(name.clone()))
                    .with_value(Some(StrBytes::from_string(value.clone())))
            })
            .collect();
        CreatableTopic::default()
            .with_name(wire_name(name))
            .with_num_partitions(partitions)
            .with_replication_factor(replication_factor)
            .with_assignments(assignments)
            .with_configs(configs)
    }
}

impl From<&str> for NewTopic {
    fn from(name: &str) -> Self {
        Self::named(name)
    }
}

impl From<String> for NewTopic {
    fn from(name: String) -> Self {
        Self::named(name)
    }
}

impl From<&TopicName> for NewTopic {
    fn from(name: &TopicName) -> Self {
        Self::named(name.as_str())
    }
}

/// The topics of one [`KafkaCluster`].
#[derive(Debug, Clone, Copy)]
pub struct Topics<'a> {
    cluster: &'a KafkaCluster,
}

impl<'a> Topics<'a> {
    pub(crate) fn new(cluster: &'a KafkaCluster) -> Self {
        Self { cluster }
    }

    /// Create a topic, and return once every broker knows it and every partition has a leader.
    /// A client can then write to the topic and read from it at once, whichever broker it asks.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::InvalidTopicName`] when the name is no topic name,
    /// [`KafkaClusterError::Refused`] with the error of the cluster, such as `TopicAlreadyExists`,
    /// and [`KafkaClusterError::NotSettledInTime`] when the brokers took longer than the default API timeout.
    pub async fn create(&self, topic: impl Into<NewTopic>) -> Result<Topic<'a>, KafkaClusterError> {
        let topic = topic.into();
        let name = TopicName::new(topic.name()).map_err(|error| KafkaClusterError::InvalidTopicName { error })?;
        let (cluster, about) = (self.cluster, about(&name));
        let request = CreateTopicsRequest::default()
            .with_timeout_ms(wire_millis(cluster.configuration().request_timeout))
            .with_topics(vec![topic.creatable(&name)]);
        let created = cluster
            .ask(cluster.any_broker(), version::CREATE_TOPICS, &request)
            .await?;
        let result = created
            .topics
            .iter()
            .find(|result| result.name.as_str() == name.as_str())
            .ok_or_else(|| KafkaClusterError::not_named(ApiKey::CreateTopics, &about))?;
        KafkaClusterError::refused(ApiKey::CreateTopics, &about, result.error_code)
            .map_err(|refused| refused.with_detail(result.error_message.as_ref()))?;
        let waited = cluster.configuration().default_api_timeout;
        let partitions = cluster
            .until(waited.duration(), || self.partitions_every_broker_leads(&name))
            .await
            .ok_or_else(|| KafkaClusterError::not_settled(&about, waited))?;
        Ok(Topic {
            cluster,
            name,
            partitions,
        })
    }

    /// The topic of that name.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with `UnknownTopicOrPartition` when the cluster has no such topic.
    pub async fn get(&self, name: &TopicName) -> Result<Topic<'a>, KafkaClusterError> {
        let (cluster, about) = (self.cluster, about(name));
        let described = metadata::of_topics(cluster, cluster.any_broker(), Some(&[name])).await?;
        let topic =
            metadata::topic(&described, name).ok_or_else(|| KafkaClusterError::not_named(ApiKey::Metadata, &about))?;
        KafkaClusterError::refused(ApiKey::Metadata, &about, topic.error_code)?;
        Ok(Topic {
            cluster,
            name: name.clone(),
            partitions: u32::try_from(topic.partitions.len()).unwrap_or(u32::MAX),
        })
    }

    /// The names of every topic that tests or clients created, in the order of the names.
    /// Kafka's own topics, such as `__consumer_offsets`, are left out.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Unanswered`] when the broker did not answer.
    pub async fn list(&self) -> Result<Vec<TopicName>, KafkaClusterError> {
        let cluster = self.cluster;
        let described = metadata::of_topics(cluster, cluster.any_broker(), None).await?;
        let mut names: Vec<TopicName> = described
            .topics
            .iter()
            .filter(|topic| !topic.is_internal)
            .filter_map(|topic| TopicName::new(topic.name.as_ref()?.as_str()).ok())
            .collect();
        names.sort();
        Ok(names)
    }

    /// Delete a topic, and return once no broker knows it any more.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with `UnknownTopicOrPartition` when the cluster has no such topic,
    /// and [`KafkaClusterError::NotSettledInTime`] when the brokers took longer than the default API timeout.
    pub async fn delete(&self, name: &TopicName) -> Result<(), KafkaClusterError> {
        let (cluster, about) = (self.cluster, about(name));
        let request = DeleteTopicsRequest::default()
            .with_timeout_ms(wire_millis(cluster.configuration().request_timeout))
            .with_topic_names(vec![wire_name(name)]);
        let deleted = cluster
            .ask(cluster.any_broker(), version::DELETE_TOPICS, &request)
            .await?;
        let result = deleted
            .responses
            .iter()
            .find(|result| {
                result
                    .name
                    .as_ref()
                    .is_some_and(|deleted| deleted.as_str() == name.as_str())
            })
            .ok_or_else(|| KafkaClusterError::not_named(ApiKey::DeleteTopics, &about))?;
        KafkaClusterError::refused(ApiKey::DeleteTopics, &about, result.error_code)?;
        let waited = cluster.configuration().default_api_timeout;
        cluster
            .until(waited.duration(), || self.no_broker_knows(name))
            .await
            .ok_or_else(|| KafkaClusterError::not_settled(&about, waited))
    }

    /// The number of partitions of `topic`, once every broker knows the topic and a leader for each partition,
    /// and each of those leaders serves its partition.
    ///
    /// A broker names the leader of a new partition a moment before that leader is ready to lead,
    /// and until then the leader refuses what is written to the partition.
    async fn partitions_every_broker_leads(&self, topic: &TopicName) -> Option<u32> {
        let mut partitions = None;
        for broker in self.cluster.brokers() {
            let described = metadata::of_topics(self.cluster, broker, Some(&[topic])).await.ok()?;
            let known = metadata::topic(&described, topic)?;
            let led = known.error_code == 0
                && !known.partitions.is_empty()
                && known
                    .partitions
                    .iter()
                    .all(|partition| partition.error_code == 0 && partition.leader_id.0 >= 0);
            if !led {
                return None;
            }
            partitions = u32::try_from(known.partitions.len()).ok();
        }
        for partition in 0..partitions? {
            PartitionLog::new(self.cluster, topic.clone(), Partition::new(partition))
                .offsets()
                .await
                .ok()?;
        }
        partitions
    }

    /// Whether every broker answers that it does not know `topic`.
    async fn no_broker_knows(&self, topic: &TopicName) -> Option<()> {
        for broker in self.cluster.brokers() {
            let described = metadata::of_topics(self.cluster, broker, Some(&[topic])).await.ok()?;
            let known = metadata::topic(&described, topic)?;
            if known.error_code != ResponseError::UnknownTopicOrPartition.code() {
                return None;
            }
        }
        Some(())
    }
}

/// How an error names `topic`.
fn about(topic: &TopicName) -> String {
    format!("topic `{topic}`")
}

/// One topic of a [`KafkaCluster`].
#[derive(Debug, Clone)]
pub struct Topic<'a> {
    cluster: &'a KafkaCluster,
    name: TopicName,
    partitions: u32,
}

impl<'a> Topic<'a> {
    pub fn name(&self) -> &TopicName {
        &self.name
    }

    /// How many partitions the topic had when this handle was made.
    pub fn partitions(&self) -> u32 {
        self.partitions
    }

    /// The log of one partition: write records to it, and read back what it holds.
    pub fn partition(&self, partition: impl Into<Partition>) -> PartitionLog<'a> {
        PartitionLog::new(self.cluster, self.name.clone(), partition.into())
    }

    /// The configs of the topic as the cluster applies them, each under the name Kafka gives it,
    /// with the defaults of the broker for what nobody set. A config without a value holds `None`.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the cluster.
    pub async fn configs(&self) -> Result<BTreeMap<String, Option<String>>, KafkaClusterError> {
        let (cluster, name, about) = (self.cluster, &self.name, about(&self.name));
        let request = DescribeConfigsRequest::default().with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(TOPIC_RESOURCE)
                .with_resource_name(StrBytes::from_string(name.as_str().to_owned())),
        ]);
        let described = cluster
            .ask(cluster.any_broker(), version::DESCRIBE_CONFIGS, &request)
            .await?;
        let result = described
            .results
            .iter()
            .find(|result| result.resource_name.as_str() == name.as_str())
            .ok_or_else(|| KafkaClusterError::not_named(ApiKey::DescribeConfigs, &about))?;
        KafkaClusterError::refused(ApiKey::DescribeConfigs, &about, result.error_code)
            .map_err(|refused| refused.with_detail(result.error_message.as_ref()))?;
        Ok(result
            .configs
            .iter()
            .map(|config| (config.name.to_string(), config.value.as_ref().map(ToString::to_string)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> TopicName {
        TopicName::new("orders").unwrap()
    }

    #[test]
    fn a_topic_without_numbers_gets_those_of_the_broker() {
        let topic = NewTopic::named("orders").creatable(&name());

        assert_eq!(topic.num_partitions, -1);
        assert_eq!(topic.replication_factor, -1);
        assert_eq!(topic.assignments.len(), 0);
    }

    #[test]
    fn a_topic_carries_its_numbers_and_its_configs() {
        let topic = NewTopic::named("orders")
            .with_partitions(3)
            .with_replication_factor(2)
            .with_config("retention.ms", "60000")
            .creatable(&name());

        assert_eq!(topic.name.as_str(), "orders");
        assert_eq!(topic.num_partitions, 3);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.configs[0].name.as_str(), "retention.ms");
        assert_eq!(topic.configs[0].value.as_deref(), Some("60000"));
    }

    #[test]
    fn an_assignment_replaces_the_numbers() {
        let topic = NewTopic::named("orders")
            .with_partitions(7)
            .with_replica_assignment([
                [BrokerId::from(2), BrokerId::from(1)],
                [BrokerId::from(3), BrokerId::from(2)],
            ])
            .creatable(&name());

        assert_eq!(topic.num_partitions, -1);
        assert_eq!(topic.replication_factor, -1);
        let assigned: Vec<(i32, Vec<i32>)> = topic
            .assignments
            .iter()
            .map(|assignment| {
                (
                    assignment.partition_index,
                    assignment.broker_ids.iter().map(|broker| broker.0).collect(),
                )
            })
            .collect();
        assert_eq!(assigned, [(0, vec![2, 1]), (1, vec![3, 2])]);
    }
}
