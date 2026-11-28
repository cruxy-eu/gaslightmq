//! A real Kafka cluster for a test.
//!
//! | Type | Is |
//! |---|---|
//! | [`KafkaCluster`] | A cluster of real brokers, which runs for as long as the value lives. |
//! | [`KafkaClusterConfiguration`] | What a cluster is configured with: its image, its number of brokers and its timeouts. |
//! | [`ContainerNetwork`], [`NetworkListener`] | A network of containers that the cluster joins, and the listener of its brokers for the clients there. |
//! | [`KafkaImage`] | The image the brokers run: Apache Kafka as a native binary, at the version the workspace tests against. |
//! | [`Broker`] | One broker of a cluster: its ID, and the address a client reaches it at. |
//! | [`Topics`] | The topics of a cluster: create one, look one up, list them, delete one. |
//! | [`NewTopic`] | A topic to create: its name, its partitions, its replicas and its configs. |
//! | [`Topic`] | One topic of a cluster, with its partitions and its configs. |
//! | [`PartitionLog`] | The log of one partition: write records to it, and read back what it holds. |
//! | [`Transaction`] | A transaction that is open on the cluster. |
//! | [`KafkaClusterError`] | Why a cluster or one of its handles could not do what it was asked to do. |
//!
//! A test of a Kafka client starts a cluster, sets it up through the cluster's own handles,
//! runs the client against the addresses of the brokers, and looks into the cluster through the handles again.
//!
//! ```no_run
//! use gaslightmq_test_kafka::{KafkaCluster, NewTopic};
//! use gaslightmq_wire::Record;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), gaslightmq_test_kafka::KafkaClusterError> {
//! let cluster = KafkaCluster::plaintext().with_brokers(3).start().await?;
//! let orders = cluster.topics().create(NewTopic::named("orders").with_partitions(3)).await?;
//!
//! // The client under test connects to these addresses.
//! let bootstrap_servers = cluster.bootstrap_servers();
//! # let _ = bootstrap_servers;
//!
//! // What it wrote is in the log of the partition, whoever asks.
//! orders.partition(2).produce(&[Record::new("placed").with_key("o-1")]).await?;
//! let records = orders.partition(2).records().await?;
//! assert_eq!(records.len(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! # What the machine needs
//!
//! A cluster runs in a container, so a test that starts one needs Docker:
//! the `docker` command, and a Docker engine that publishes ports on `127.0.0.1` of the machine the test runs on.
//! Without them [`KafkaClusterConfiguration::start`] returns an error that says so.
//!
//! # The version of Kafka
//!
//! The brokers run [`KafkaImage::default`]: `apache/kafka-native` at the version this workspace tests against.
//! The environment variable [`GASLIGHTMQ_TEST_KAFKA_IMAGE`](KafkaImage::VARIABLE) names another image for a whole test run,
//! and [`KafkaClusterConfiguration::with_image`] names one for a single cluster.
//!
//! # Several brokers
//!
//! [`KafkaClusterConfiguration::with_brokers`] asks for a cluster of several brokers.
//! Each one is a real broker with an address of its own, and partitions have their leaders on different brokers.
//! Three brokers start about as fast as one.
//!
//! # What sets a cluster up is not what a test is about
//!
//! The handles of a cluster send one request at one fixed version to the broker that has to answer it.
//! They negotiate no version, retry nothing and keep no connection.
//! They share the codec of `gaslightmq_wire` with the clients of this workspace, and no decision of a client.
//! A bug in how a client finds a leader, picks a version or tries again therefore cannot hide itself
//! in the code that checks what the client did.
//!
//! # Tests that share a cluster
//!
//! A cluster holds nothing that belongs to an async runtime, so one cluster in a `static` serves every test of a binary,
//! each on a runtime of its own.
//! Such tests keep apart by what they name: [`NewTopic::unique`] and [`unique_name`] hand out names that no one else got.
//!
//! # Records
//!
//! A record is the [`Record`](gaslightmq_wire::Record) of `gaslightmq_wire`, with its key, value and headers.
//! A partition is a [`Partition`](gaslightmq_wire::Partition) and an offset an [`Offset`](gaslightmq_wire::Offset)
//! of the same crate, as in the traits of `gaslightmq_kafka_client_api`.
//! A topic name and a broker ID are the value objects of `gaslightmq_kernel`.

mod cluster;
mod configuration;
mod docker;
mod error;
mod image;
mod metadata;
mod network;
mod partition;
mod properties;
mod request;
mod topic;
mod transaction;

use gaslightmq_clock::TimeSpan;

pub use cluster::{Broker, KafkaCluster};
pub use configuration::{ContainerNetwork, KafkaClusterConfiguration, NetworkListener};
pub use error::KafkaClusterError;
pub use image::{KafkaImage, KafkaImageError};
pub use partition::PartitionLog;
pub use topic::{NewTopic, Topic, Topics, unique_name};
pub use transaction::Transaction;

/// The version of each API that the handles of a cluster speak.
///
/// Every version is fixed, and Kafka has spoken each of them since 3.0, so a cluster of an older image answers them too.
/// Each one still names a topic by its name.
mod version {
    pub(crate) const PRODUCE: i16 = 8;
    pub(crate) const FETCH: i16 = 11;
    pub(crate) const LIST_OFFSETS: i16 = 5;
    pub(crate) const METADATA: i16 = 9;
    pub(crate) const FIND_COORDINATOR: i16 = 3;
    pub(crate) const CREATE_TOPICS: i16 = 5;
    pub(crate) const DELETE_TOPICS: i16 = 4;
    pub(crate) const INIT_PRODUCER_ID: i16 = 2;
    pub(crate) const ADD_PARTITIONS_TO_TXN: i16 = 3;
    pub(crate) const END_TXN: i16 = 2;
    pub(crate) const DESCRIBE_CONFIGS: i16 = 2;
}

/// A time span as a request carries it: whole milliseconds in a signed 32-bit number.
/// A longer time span travels as the largest such number.
fn wire_millis(span: TimeSpan) -> i32 {
    i32::try_from(span.as_millis()).unwrap_or(i32::MAX)
}
