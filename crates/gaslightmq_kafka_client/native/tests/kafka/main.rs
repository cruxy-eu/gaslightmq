//! The producer, the consumer and the admin against real Kafka brokers.
//!
//! The tests need Docker.
//! They share one cluster of three brokers, and keep apart by the names of their topics and groups.
//! Whatever sets the cluster up or looks into it goes through the handles of `gaslightmq_test_kafka`,
//! and never through the client that the test is about.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

mod configuration;
mod groups;
mod records;
mod topics;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_native::{
    AdminConfiguration, BootstrapServers, ConsumerConfiguration, KafkaAdmin, KafkaConsumer, KafkaProducer,
    ProducerConfiguration,
};
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::{Offset, Partition, Record};
use tokio::sync::OnceCell;

/// The shortest session a member of a group may ask the brokers of [`kafka`] for.
/// Kafka's own default is six seconds, which a test of a session that runs out would have to wait for.
const SHORTEST_SESSION_MS: u64 = 1_000;

/// The time of a call that cannot succeed, and the wait between its tries.
///
/// Such a call tries twice: a third try would start after its time, so it ends with the error of the second one.
/// Its time is then far from over, so a request never runs out of time instead, also on a slow machine.
const HOPELESS_CALL: TimeSpan = TimeSpan::from_millis(1_500);
const HOPELESS_BACKOFF: TimeSpan = TimeSpan::from_millis(600);

/// The cluster of three brokers that the tests of this binary share.
/// The first test that asks for it starts it, and it ends with the process.
async fn kafka() -> &'static KafkaCluster {
    static CLUSTER: OnceCell<KafkaCluster> = OnceCell::const_new();
    CLUSTER
        .get_or_init(|| async {
            KafkaCluster::plaintext()
                .with_brokers(3)
                .with_broker_property("group.min.session.timeout.ms", SHORTEST_SESSION_MS.to_string())
                .start()
                .await
                .unwrap_or_else(|error| panic!("{error}"))
        })
        .await
}

/// The addresses of every broker of `cluster`, as a client is configured with them.
fn bootstrap_servers(cluster: &KafkaCluster) -> BootstrapServers {
    cluster.bootstrap_servers().parse().unwrap()
}

/// What a producer with Kafka's defaults is configured with, to start from the brokers of `cluster`.
fn producing(cluster: &KafkaCluster) -> ProducerConfiguration {
    ProducerConfiguration::new(bootstrap_servers(cluster))
}

/// What a consumer with Kafka's defaults is configured with, to start from the brokers of `cluster`.
fn consuming(cluster: &KafkaCluster) -> ConsumerConfiguration {
    ConsumerConfiguration::new(bootstrap_servers(cluster))
}

fn producer(cluster: &KafkaCluster) -> KafkaProducer {
    KafkaProducer::new(producing(cluster))
}

fn consumer(cluster: &KafkaCluster) -> KafkaConsumer {
    KafkaConsumer::new(consuming(cluster))
}

fn admin(cluster: &KafkaCluster) -> KafkaAdmin {
    KafkaAdmin::new(AdminConfiguration::new(bootstrap_servers(cluster)))
}

fn partition(number: u32) -> Partition {
    Partition::new(number)
}

fn offset(offset: u64) -> Offset {
    Offset::new(offset)
}

/// The values of `records`, as text.
fn values(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|record| String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()).into_owned())
        .collect()
}
