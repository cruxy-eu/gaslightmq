//! What a cluster is configured with.
//!
//! A property is a public field.
//! A property that Kafka's own clients or brokers have keeps the name, the meaning and the default of that property,
//! and the documentation of the field names it.
//! A property that Kafka does not have says that it is ours.

use std::collections::BTreeMap;

use gaslightmq_clock::TimeSpan;

use crate::image::KafkaImage;

/// What one [`KafkaCluster`](crate::KafkaCluster) is configured with.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_test_kafka::KafkaCluster;
///
/// let mut configuration = KafkaCluster::plaintext()
///     .with_brokers(3)
///     .with_broker_property("auto.create.topics.enable", "false");
/// configuration.start_timeout = TimeSpan::from_secs(20);
///
/// assert_eq!(configuration.brokers, 3);
/// assert_eq!(configuration.image, None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct KafkaClusterConfiguration {
    /// Ours. Default: none.
    ///
    /// The image the brokers run.
    /// Without one, the cluster runs the image that the environment variable
    /// [`GASLIGHTMQ_TEST_KAFKA_IMAGE`](KafkaImage::VARIABLE) names,
    /// and without that variable the [default image](KafkaImage::default).
    pub image: Option<KafkaImage>,
    /// Ours. Default: 1.
    ///
    /// How many brokers the cluster has. Their IDs count from 1.
    /// A cluster has at least one broker, so it does not start with zero.
    pub brokers: u8,
    /// Ours. Default: empty.
    ///
    /// Properties of every broker, under the names Kafka gives them, such as `auto.create.topics.enable`.
    /// A property set here replaces the one the cluster would set itself.
    /// The cluster sets the properties that make its brokers one cluster: the node ID, the roles, the listeners,
    /// the quorum and the log directory. Setting one of those here breaks the cluster.
    /// It also sets `group.initial.rebalance.delay.ms` to 0, so the first member of a group joins at once,
    /// and `log.retention.ms` to -1, so a record stays whatever its timestamp says about its age.
    /// It keeps the replicas of Kafka's own topics within the number of brokers.
    pub broker_properties: BTreeMap<String, String>,
    /// Ours. Default: 1 minute.
    ///
    /// How long the brokers get to start and to know each other, once the machine holds the image.
    /// Pulling the image takes as long as it takes.
    pub start_timeout: TimeSpan,
    /// Kafka: `request.timeout.ms`. Default: 30 seconds.
    ///
    /// How long one request of the cluster's own handles waits for its response.
    pub request_timeout: TimeSpan,
    /// Kafka: `default.api.timeout.ms`. Default: 1 minute.
    ///
    /// How long creating or deleting a topic may take, until every broker knows about it.
    pub default_api_timeout: TimeSpan,
    /// Kafka: `retry.backoff.ms`. Default: 100 milliseconds.
    ///
    /// How long a handle waits before it asks the brokers again whether they know what it waits for.
    pub retry_backoff: TimeSpan,
    /// Kafka: `transaction.timeout.ms`. Default: 1 minute.
    ///
    /// How long the broker keeps a [transaction](crate::KafkaCluster::begin_transaction) open before it aborts it.
    pub transaction_timeout: TimeSpan,
    /// Ours. Default: 40.
    ///
    /// How many of the last lines the brokers logged an error shows, when the cluster does not start.
    pub log_lines_in_errors: usize,
}

impl KafkaClusterConfiguration {
    /// A cluster of one broker whose listener speaks plaintext, with every default.
    pub fn plaintext() -> Self {
        Self {
            image: None,
            brokers: 1,
            broker_properties: BTreeMap::new(),
            start_timeout: TimeSpan::from_secs(60),
            request_timeout: TimeSpan::from_secs(30),
            default_api_timeout: TimeSpan::from_secs(60),
            retry_backoff: TimeSpan::from_millis(100),
            transaction_timeout: TimeSpan::from_secs(60),
            log_lines_in_errors: 40,
        }
    }

    /// Run `image`, whatever the environment says.
    #[must_use]
    pub fn with_image(mut self, image: KafkaImage) -> Self {
        self.image = Some(image);
        self
    }

    /// Have `brokers` brokers, with the IDs 1 to `brokers`.
    #[must_use]
    pub fn with_brokers(mut self, brokers: u8) -> Self {
        self.brokers = brokers;
        self
    }

    /// Set a property of every broker, under the name Kafka gives it.
    #[must_use]
    pub fn with_broker_property(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.broker_properties.insert(name.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_one_broker_and_kafkas_timeouts() {
        let configuration = KafkaClusterConfiguration::plaintext();

        assert_eq!(configuration.image, None);
        assert_eq!(configuration.brokers, 1);
        assert!(configuration.broker_properties.is_empty());
        assert_eq!(configuration.start_timeout, TimeSpan::from_secs(60));
        assert_eq!(configuration.request_timeout, TimeSpan::from_millis(30_000));
        assert_eq!(configuration.default_api_timeout, TimeSpan::from_millis(60_000));
        assert_eq!(configuration.retry_backoff, TimeSpan::from_millis(100));
        assert_eq!(configuration.transaction_timeout, TimeSpan::from_millis(60_000));
        assert_eq!(configuration.log_lines_in_errors, 40);
    }

    #[test]
    fn a_property_set_twice_keeps_the_last_value() {
        let configuration = KafkaClusterConfiguration::plaintext()
            .with_broker_property("num.partitions", "2")
            .with_broker_property("num.partitions", "3");

        assert_eq!(
            configuration.broker_properties,
            BTreeMap::from([("num.partitions".to_owned(), "3".to_owned())])
        );
    }
}
