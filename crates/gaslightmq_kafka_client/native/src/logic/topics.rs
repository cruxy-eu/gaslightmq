//! Asking which topics the cluster has, and who leads their partitions.

use std::time::Duration;

use gaslightmq_clock::Instant;
use gaslightmq_kafka_client_api::{ClientError, PartitionMetadata, TopicMetadata};
use gaslightmq_wire::Partition;
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse, TopicName};

use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::{ClusterView, Route};
use crate::logic::versions::Versions;

/// The versions of Metadata this client speaks.
pub(crate) const VERSIONS: Versions = Versions::new(1, 12);

/// A Metadata request for `topic`, or for every topic when there is none.
/// `allow_auto_create_topics` says whether the broker may create a topic that it does not have.
/// A broker that only speaks Metadata before version 4 cannot be told not to, so that request does not encode.
pub(crate) fn request(topic: Option<&TopicName>, allow_auto_create_topics: bool) -> MetadataRequest {
    MetadataRequest::default()
        .with_allow_auto_topic_creation(allow_auto_create_topics)
        .with_topics(topic.map(|name| vec![MetadataRequestTopic::default().with_name(Some(name.clone()))]))
}

/// The error a Metadata response reports for `topic`.
pub(crate) fn error_of(metadata: &MetadataResponse, topic: &TopicName) -> Option<ClientError> {
    metadata
        .topics
        .iter()
        .find(|described| described.name.as_ref() == Some(topic))
        .and_then(|described| ClientError::refusal(described.error_code))
}

/// Ask for the partitions of one topic, or of every topic.
/// The cluster view takes over the leaders that come back.
pub(crate) struct DescribeTopics {
    pub topic: Option<TopicName>,
    pub allow_auto_create_topics: bool,
}

impl Operation for DescribeTopics {
    type Request = MetadataRequest;
    type Output = Vec<TopicMetadata>;

    const VERSIONS: Versions = VERSIONS;

    fn route(&self) -> Route {
        Route::AnyBroker
    }

    fn request(&self, _within: Duration) -> MetadataRequest {
        request(self.topic.as_ref(), self.allow_auto_create_topics)
    }

    fn allow_auto_create_topics(&self) -> bool {
        self.allow_auto_create_topics
    }

    fn learn(&self, response: &MetadataResponse, cluster: &mut ClusterView, now: Instant) {
        cluster.learn(response, now);
    }

    fn read(&mut self, response: MetadataResponse) -> Result<Reading<Vec<TopicMetadata>>, ClientError> {
        response
            .topics
            .iter()
            .map(|topic| {
                if let Some(error) = ClientError::refusal(topic.error_code) {
                    return Err(error);
                }
                let mut partitions: Vec<PartitionMetadata> = topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionMetadata {
                        id: Partition::from(partition.partition_index),
                    })
                    .collect();
                partitions.sort_by_key(|partition| i32::from(partition.id));
                Ok(TopicMetadata {
                    name: topic.name.as_ref().map_or_default(|name| name.as_str().to_owned()),
                    partitions,
                })
            })
            .collect::<Result<_, _>>()
            .map(Reading::Done)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::ResponseError;

    use super::*;
    use crate::logic::cluster::tests::{metadata, partition, to_broker, topic, view};
    use crate::logic::cluster::topic_name;

    fn describe_all() -> DescribeTopics {
        DescribeTopics {
            topic: None,
            allow_auto_create_topics: true,
        }
    }

    #[test]
    fn a_request_names_one_topic_or_none() {
        let one = DescribeTopics {
            topic: Some(topic_name("orders")),
            allow_auto_create_topics: false,
        };
        let all = describe_all();

        let asked = one.request(Duration::ZERO).topics.unwrap();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].name, Some(topic_name("orders")));
        assert!(!one.request(Duration::ZERO).allow_auto_topic_creation);
        assert_eq!(all.request(Duration::ZERO).topics, None);
        assert!(all.request(Duration::ZERO).allow_auto_topic_creation);
        assert_eq!(all.route(), Route::AnyBroker);
    }

    #[test]
    fn the_partitions_of_each_topic_come_back_in_order() {
        let mut orders = topic("orders", &[1, 1, 1]);
        orders.partitions.reverse();

        let read = describe_all()
            .read(metadata(&[1], vec![orders, topic("payments", &[1])]))
            .unwrap();

        let Reading::Done(topics) = read else {
            panic!("the call is over")
        };
        let ids = |topic: &TopicMetadata| topic.partitions.iter().map(|p| i32::from(p.id)).collect::<Vec<_>>();
        assert_eq!(topics[0].name, "orders");
        assert_eq!(ids(&topics[0]), [0, 1, 2]);
        assert_eq!(topics[1].name, "payments");
        assert_eq!(ids(&topics[1]), [0]);
    }

    #[test]
    fn the_error_of_a_topic_is_the_failure_of_the_call() {
        let unknown = topic("orders", &[]).with_error_code(ResponseError::UnknownTopicOrPartition.code());
        let response = metadata(&[1], vec![unknown]);

        assert_eq!(
            error_of(&response, &topic_name("orders")),
            Some(ClientError::Refused {
                error: ResponseError::UnknownTopicOrPartition
            })
        );
        assert_eq!(error_of(&response, &topic_name("payments")), None);
        assert_eq!(
            describe_all().read(response).unwrap_err(),
            ClientError::Refused {
                error: ResponseError::UnknownTopicOrPartition
            }
        );
    }

    #[test]
    fn the_view_takes_over_the_leaders_of_the_response() {
        let mut cluster = view();

        describe_all().learn(
            &metadata(&[1, 2], vec![topic("orders", &[2])]),
            &mut cluster,
            Instant::ZERO,
        );

        assert_eq!(
            cluster.leader(&partition("orders", 0), Instant::ZERO),
            Some(to_broker(2))
        );
    }
}
