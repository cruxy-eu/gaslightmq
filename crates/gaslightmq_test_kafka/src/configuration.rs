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
    /// Ours. Default: none.
    ///
    /// The network of containers that the cluster joins, so that containers next to the test reach its brokers:
    /// a [network listener](Self::network_listener) serves them.
    /// The cluster creates the network when it is missing, and removes it again when it ends, if it created it.
    pub network: Option<String>,
    /// Ours. Default: none.
    ///
    /// The listener of every broker for the clients on the [network](Self::network), as the listener of Kafka's
    /// `listeners` and `advertised.listeners` that such a client connects to. It needs a network.
    pub network_listener: Option<NetworkListener>,
}

/// The listener of the brokers of a cluster for the clients on its network: `kafka:19092`.
///
/// The cluster is known on the network under `alias`. Broker 1 listens on `port`, and each broker after it on the
/// port after the one before it, and each one advertises `alias` with its port.
///
/// ```
/// use gaslightmq_test_kafka::NetworkListener;
///
/// let listener = NetworkListener::new("kafka", 19092);
/// assert_eq!(listener.to_string(), "kafka:19092");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkListener {
    /// The name that the cluster is known under on the network.
    pub alias: String,
    /// The port of broker 1. The broker with ID `n` listens on the `n - 1`th port after it.
    pub port: u16,
}

impl NetworkListener {
    /// A listener under `alias`, from `port` on.
    pub fn new(alias: impl Into<String>, port: u16) -> Self {
        Self {
            alias: alias.into(),
            port,
        }
    }
}

/// A network of containers, by its name: the cluster and the containers next to the test that reach it join it.
///
/// ```
/// use gaslightmq_test_kafka::ContainerNetwork;
///
/// let network = ContainerNetwork::named("orders-test");
/// assert_eq!(network.name(), "orders-test");
///
/// // A network of its own, for a test that runs beside others.
/// assert_ne!(ContainerNetwork::unique("orders-test"), ContainerNetwork::unique("orders-test"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerNetwork {
    name: String,
}

impl ContainerNetwork {
    /// The network named `name`.
    pub fn named(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// The name of the network, which the container runtime knows it by.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for NetworkListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.alias, self.port)
    }
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
            network: None,
            network_listener: None,
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

    /// Join the network of containers `network`, and serve its clients on `listener`.
    #[must_use]
    pub fn with_network(mut self, network: &ContainerNetwork, listener: NetworkListener) -> Self {
        self.network = Some(network.name().to_owned());
        self.network_listener = Some(listener);
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
        assert_eq!(configuration.network, None);
        assert_eq!(configuration.network_listener, None);
    }

    #[test]
    fn a_cluster_on_a_network_serves_its_clients_on_a_listener_of_its_own() {
        let configuration = KafkaClusterConfiguration::plaintext().with_network(
            &ContainerNetwork::named("orders-test"),
            NetworkListener::new("kafka", 19092),
        );

        assert_eq!(configuration.network.as_deref(), Some("orders-test"));
        assert_eq!(
            configuration.network_listener.map(|listener| listener.to_string()),
            Some("kafka:19092".to_owned())
        );
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
