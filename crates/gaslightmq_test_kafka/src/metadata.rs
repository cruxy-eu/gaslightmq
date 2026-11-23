//! What a broker knows about the cluster and its topics.

use gaslightmq_kernel::TopicName;
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::metadata_response::MetadataResponseTopic;
use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse};
use gaslightmq_wire::protocol::StrBytes;

use crate::cluster::{Broker, KafkaCluster};
use crate::error::KafkaClusterError;
use crate::version;

/// `name` as the messages of the codec carry it.
pub(crate) fn wire_name(name: &TopicName) -> gaslightmq_wire::messages::TopicName {
    gaslightmq_wire::messages::TopicName(StrBytes::from_string(name.as_str().to_owned()))
}

/// Ask `broker` for its Metadata of `topics`: of every topic for `None`, and of no topic for an empty list.
/// The broker creates no topic for the question.
pub(crate) async fn of_topics(
    cluster: &KafkaCluster,
    broker: &Broker,
    topics: Option<&[&TopicName]>,
) -> Result<MetadataResponse, KafkaClusterError> {
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|name| MetadataRequestTopic::default().with_name(Some(wire_name(name))))
            .collect()
    });
    let request = MetadataRequest::default()
        .with_topics(topics)
        .with_allow_auto_topic_creation(false);
    cluster.ask(broker, version::METADATA, &request).await
}

/// What `metadata` says about `topic`, when it names the topic.
pub(crate) fn topic<'a>(metadata: &'a MetadataResponse, topic: &TopicName) -> Option<&'a MetadataResponseTopic> {
    metadata.topics.iter().find(|described| {
        described
            .name
            .as_ref()
            .is_some_and(|name| name.as_str() == topic.as_str())
    })
}
