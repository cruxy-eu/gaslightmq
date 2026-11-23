//! A cluster of real Kafka brokers.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_kernel::BrokerId;
use gaslightmq_wire::protocol::Request as KafkaRequest;
use tokio::time::Instant;

use crate::configuration::KafkaClusterConfiguration;
use crate::docker::{self, Container, Stalled};
use crate::error::KafkaClusterError;
use crate::image::KafkaImage;
use crate::topic::Topics;
use crate::transaction::Transaction;
use crate::{metadata, properties, request};

/// A cluster of real Kafka brokers, which runs for as long as this value lives.
///
/// [`KafkaCluster::plaintext`] is the configuration of a cluster of one broker,
/// and [`start`](KafkaClusterConfiguration::start) runs it.
/// The cluster is ready when `start` returns: every broker serves clients and knows every other broker.
///
/// ```no_run
/// use gaslightmq_test_kafka::KafkaCluster;
/// use gaslightmq_wire::Record;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), gaslightmq_test_kafka::KafkaClusterError> {
/// let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
///
/// let orders = cluster.topics().create("orders").await?;
/// orders.partition(0).produce(&[Record::new("placed").with_key("o-1")]).await?;
///
/// let records = orders.partition(0).records().await?;
/// assert_eq!(records[0].value.as_deref(), Some(&b"placed"[..]));
/// # Ok(())
/// # }
/// ```
///
/// # What a test connects to
///
/// Every broker has an address on the host, which it also advertises to its clients:
/// [`bootstrap_servers`](Self::bootstrap_servers) names them all, and [`brokers`](Self::brokers) lists them by ID.
///
/// # What sets the cluster up
///
/// [`topics`](Self::topics) and the handles it gives create topics, write records and read them back.
/// They send one request at one fixed version to the broker that has to answer it,
/// and share nothing with the client that a test is about, except the codec.
///
/// # When the cluster ends
///
/// The cluster ends when this value is dropped, and when the process of the test ends for any other reason.
/// A cluster in a `static`, which tests share and nothing ever drops, therefore leaves nothing behind.
pub struct KafkaCluster {
    image: KafkaImage,
    brokers: Vec<Broker>,
    configuration: KafkaClusterConfiguration,
    /// The cluster runs while this lives.
    container: Container,
}

/// One broker of a [`KafkaCluster`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Broker {
    id: BrokerId,
    address: SocketAddr,
}

impl Broker {
    /// The node ID of the broker. The brokers of a cluster count from 1.
    pub fn id(&self) -> BrokerId {
        self.id
    }

    /// The address on the host that the broker serves clients on, and advertises to them.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

impl KafkaClusterConfiguration {
    /// Start the cluster this configuration describes. See [`KafkaCluster::start`].
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::NotStarted`], which says why no cluster runs.
    pub async fn start(self) -> Result<KafkaCluster, KafkaClusterError> {
        KafkaCluster::start(self).await
    }
}

impl KafkaCluster {
    /// The configuration of a cluster of one broker whose listener speaks plaintext.
    pub fn plaintext() -> KafkaClusterConfiguration {
        KafkaClusterConfiguration::plaintext()
    }

    /// Start a cluster, and return once every broker serves clients and knows every other broker.
    ///
    /// The image is pulled first when the machine does not hold it.
    /// Starting holds the thread of the caller while it sets the brokers up.
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::NotStarted`], which says why no cluster runs:
    /// the configuration describes none, the machine cannot run one, or the brokers did not start.
    pub async fn start(configuration: KafkaClusterConfiguration) -> Result<Self, KafkaClusterError> {
        if configuration.brokers == 0 {
            return Err(KafkaClusterError::not_started(
                "a cluster has at least one broker, and the configuration asks for none",
            ));
        }
        if let Some(name) = properties::first_unwritable(&configuration.broker_properties) {
            return Err(KafkaClusterError::not_started(format!(
                "broker property `{name}` is no single line of a properties file"
            )));
        }
        let image = match &configuration.image {
            Some(image) => image.clone(),
            None => image_of_the_environment()?,
        };
        docker::require_engine()?;
        docker::require_image(&image)?;

        let ids: Vec<u8> = (1..=configuration.brokers).collect();
        let ports: Vec<u16> = ids.iter().map(|broker| properties::client_port(*broker)).collect();
        let deadline = Instant::now() + configuration.start_timeout.duration();
        let mut container = Container::run(&image, &ports, configuration.log_lines_in_errors)?;
        let not_ready = format!(
            "the brokers were not ready within {} ms",
            configuration.start_timeout.as_millis()
        );
        let stalled = |container: &Container, stalled| KafkaClusterError::NotStarted {
            reason: match stalled {
                Stalled::Ended => "the brokers stopped before all of them had started".to_owned(),
                Stalled::TimedOut => not_ready.clone(),
            },
            log: container.log(),
        };
        container
            .reaches(|progress| progress.awaiting_configuration, remaining(deadline))
            .await
            .map_err(|error| stalled(&container, error))?;

        let published = container.published_ports()?;
        let mut brokers = Vec::new();
        let mut broker_properties = BTreeMap::new();
        for broker in ids {
            let address = *published.get(&properties::client_port(broker)).ok_or_else(|| {
                KafkaClusterError::not_started(format!(
                    "the container runtime did not say where clients reach broker {broker}"
                ))
            })?;
            let of_broker =
                properties::of_broker(broker, configuration.brokers, address, &configuration.broker_properties);
            broker_properties.insert(broker, properties::lines(&of_broker));
            brokers.push(Broker {
                id: BrokerId::from(u16::from(broker)),
                address,
            });
        }
        container.configure(&broker_properties)?;
        container
            .reaches(
                |progress| progress.brokers_started >= brokers.len(),
                remaining(deadline),
            )
            .await
            .map_err(|error| stalled(&container, error))?;

        let cluster = Self {
            image,
            brokers,
            configuration,
            container,
        };
        if cluster
            .until(remaining(deadline), || cluster.every_broker_knows_every_broker())
            .await
            .is_none()
        {
            return Err(KafkaClusterError::NotStarted {
                reason: not_ready,
                log: cluster.container.log(),
            });
        }
        Ok(cluster)
    }

    /// The image the brokers run.
    pub fn image(&self) -> &KafkaImage {
        &self.image
    }

    /// What the cluster runs with.
    pub fn configuration(&self) -> &KafkaClusterConfiguration {
        &self.configuration
    }

    /// Every broker of the cluster, in the order of their IDs.
    pub fn brokers(&self) -> &[Broker] {
        &self.brokers
    }

    /// The broker with that ID, when the cluster has one.
    pub fn broker(&self, id: BrokerId) -> Option<&Broker> {
        self.brokers.iter().find(|broker| broker.id == id)
    }

    /// The addresses of every broker, in the text form of Kafka's `bootstrap.servers`: `127.0.0.1:50123,127.0.0.1:50124`.
    pub fn bootstrap_servers(&self) -> String {
        let addresses: Vec<String> = self.brokers.iter().map(|broker| broker.address.to_string()).collect();
        addresses.join(",")
    }

    /// The topics of the cluster: create one, look one up, list them, delete one.
    pub fn topics(&self) -> Topics<'_> {
        Topics::new(self)
    }

    /// Begin a transaction under `transactional_id`, as a transactional producer does.
    ///
    /// The transaction stays open until someone [commits](Transaction::commit) or [aborts](Transaction::abort) it,
    /// or until the broker aborts it after the
    /// [transaction timeout](KafkaClusterConfiguration::transaction_timeout).
    ///
    /// # Errors
    ///
    /// [`KafkaClusterError::Refused`] with the error of the coordinator.
    pub async fn begin_transaction(&self, transactional_id: &str) -> Result<Transaction<'_>, KafkaClusterError> {
        Transaction::begin(self, transactional_id).await
    }

    /// The broker that requests go to which every broker answers the same way.
    pub(crate) fn any_broker(&self) -> &Broker {
        &self.brokers[0]
    }

    /// Send `body` to `broker` at `version`, and wait for the response for the request timeout.
    pub(crate) async fn ask<R: KafkaRequest>(
        &self,
        broker: &Broker,
        version: i16,
        body: &R,
    ) -> Result<R::Response, KafkaClusterError> {
        request::send(
            broker.address,
            version,
            body,
            self.configuration.request_timeout.duration(),
        )
        .await
    }

    /// Ask `question` until it has an answer, and wait the retry backoff between two tries.
    /// `None` when `within` is over first.
    pub(crate) async fn until<T, F, Fut>(&self, within: Duration, mut question: F) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        let deadline = Instant::now() + within;
        let backoff = self.configuration.retry_backoff.duration();
        loop {
            if let Some(answer) = question().await {
                return Some(answer);
            }
            if Instant::now() + backoff > deadline {
                return None;
            }
            tokio::time::sleep(backoff).await;
        }
    }

    /// Whether the Metadata of every broker names every broker of the cluster.
    async fn every_broker_knows_every_broker(&self) -> Option<()> {
        for broker in &self.brokers {
            let known = metadata::of_topics(self, broker, Some(&[])).await.ok()?;
            if known.brokers.len() != self.brokers.len() {
                return None;
            }
        }
        Some(())
    }
}

impl fmt::Debug for KafkaCluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaCluster")
            .field("image", &self.image)
            .field("brokers", &self.brokers)
            .finish_non_exhaustive()
    }
}

/// The time that is left until `deadline`.
fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// The image that the environment names for every cluster of the test run, or the default image.
fn image_of_the_environment() -> Result<KafkaImage, KafkaClusterError> {
    match std::env::var_os(KafkaImage::VARIABLE) {
        Some(text) if !text.is_empty() => text.to_string_lossy().parse().map_err(|error| {
            KafkaClusterError::not_started(format!("environment variable {}: {error}", KafkaImage::VARIABLE))
        }),
        _ => Ok(KafkaImage::default()),
    }
}
