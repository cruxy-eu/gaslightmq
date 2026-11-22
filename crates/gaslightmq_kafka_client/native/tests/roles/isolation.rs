//! What one role does never reaches another role, and a Fetch never holds up a Heartbeat.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use gaslightmq_kafka_client_native::{
    ConsumerConfiguration, KafkaAdmin, KafkaConsumer, KafkaProducer, ProducerConfiguration,
};
use gaslightmq_wire::{ApiKey, Offset, Partition, Record, Request};
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::support::{Cluster, FakeBroker, TOPIC};

/// A cluster of one broker that holds every Fetch until the test lets it go.
async fn cluster_that_holds_a_fetch() -> (FakeBroker, Arc<Notify>) {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let let_go = Arc::new(Notify::new());
    let held = Arc::clone(&let_go);
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let held = Arc::clone(&held);
        async move {
            if request.api().known() == Some(ApiKey::Fetch) {
                held.notified().await;
            }
            cluster.answer(&request)
        }
    });
    (broker, let_go)
}

fn start() -> (Partition, Offset) {
    (Partition::from(0), Offset::from(0i64))
}

#[tokio::test]
async fn a_heartbeat_does_not_wait_for_a_fetch_of_the_same_consumer() {
    let (broker, let_go) = cluster_that_holds_a_fetch().await;
    let consumer = KafkaConsumer::from(broker.address());
    consumer.join_group("analytics").await.unwrap();
    consumer.sync_group("analytics").await.unwrap();
    let (partition, offset) = start();

    let fetch = consumer.fetch(TOPIC, partition, offset);
    tokio::pin!(fetch);
    tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        () = broker.receives(1, ApiKey::Fetch) => {}
    }

    // The Fetch waits at the broker, and the Heartbeat gets its answer all the same.
    let heartbeat = tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        heartbeat = timeout(Duration::from_secs(10), consumer.heartbeat("analytics")) => heartbeat,
    };
    assert_eq!(heartbeat, Ok(Ok(())));

    // Both went to the same broker, each on a connection of its own.
    let fetched_on = broker.arrivals_of(ApiKey::Fetch)[0].connection;
    let heartbeat_on = broker.arrivals_of(ApiKey::Heartbeat)[0].connection;
    assert_ne!(fetched_on, heartbeat_on);

    let_go.notify_one();
    assert_eq!(fetch.await, Ok(vec![]));
}

/// The other side of the test above: the broker does hold what comes after a Fetch on the same connection.
/// Without a connection of its own, the Heartbeat would wait as this `ListOffsets` does.
#[tokio::test]
async fn a_request_on_the_connection_of_a_fetch_waits_for_the_fetch() {
    let (broker, let_go) = cluster_that_holds_a_fetch().await;
    let consumer = KafkaConsumer::from(broker.address());
    let (partition, offset) = start();

    let fetch = consumer.fetch(TOPIC, partition, offset);
    let watermarks = consumer.watermarks(TOPIC, partition);
    tokio::pin!(fetch, watermarks);
    tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        () = broker.receives(1, ApiKey::Fetch) => {}
    }

    tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        _ = &mut watermarks => panic!("the watermarks wait behind the Fetch"),
        () = tokio::time::sleep(Duration::from_millis(200)) => {}
    }

    let_go.notify_one();
    assert_eq!(fetch.await, Ok(vec![]));
    assert!(watermarks.await.is_ok());
}

#[tokio::test]
async fn metadata_that_the_producer_receives_does_not_move_the_leaders_of_the_consumer() {
    let (first, second) = (FakeBroker::bind().await, FakeBroker::bind().await);
    let cluster = Cluster::of(&[(1, &first), (2, &second)]);
    // The producer hears that broker 2 leads the partition. Everyone else hears that broker 1 does.
    let answer = move |request: Request| {
        let cluster = cluster.clone();
        async move {
            if request.api().known() == Some(ApiKey::Metadata) {
                let leader = if request.client_id() == Some("order-ingest") {
                    2
                } else {
                    1
                };
                return cluster.metadata(&request, leader);
            }
            cluster.answer(&request)
        }
    };
    first.serve(answer.clone());
    second.serve(answer);
    let mut consuming = ConsumerConfiguration::new(first.address());
    consuming.client.client_id = "analytics-etl".parse().unwrap();
    let consumer = KafkaConsumer::new(consuming);
    let mut producing = ProducerConfiguration::new(first.address());
    producing.client.client_id = "order-ingest".parse().unwrap();
    let producer = KafkaProducer::new(producing);
    let (partition, offset) = start();

    consumer.fetch(TOPIC, partition, offset).await.unwrap();
    producer.send(TOPIC, partition, &[Record::new("paid")]).await.unwrap();
    consumer.fetch(TOPIC, partition, offset).await.unwrap();

    // The producer followed its Metadata to broker 2.
    assert_eq!(second.arrivals_of(ApiKey::Produce).len(), 1);
    assert!(first.arrivals_of(ApiKey::Produce).is_empty());
    // The consumer stayed with broker 1, and had no reason to ask again.
    assert_eq!(first.arrivals_of(ApiKey::Fetch).len(), 2);
    assert!(second.arrivals_of(ApiKey::Fetch).is_empty());
    let asked_by_the_consumer = first
        .arrivals_of(ApiKey::Metadata)
        .into_iter()
        .chain(second.arrivals_of(ApiKey::Metadata))
        .filter(|arrival| arrival.client_id() == "analytics-etl")
        .count();
    assert_eq!(asked_by_the_consumer, 1);
}

#[tokio::test]
async fn every_role_has_connections_and_a_client_id_of_its_own() {
    let (_, broker) = Cluster::of_one().await;
    let producer = KafkaProducer::from(broker.address());
    let consumer = KafkaConsumer::from(broker.address());
    let admin = KafkaAdmin::from(broker.address());
    let (partition, offset) = start();

    producer.send(TOPIC, partition, &[Record::new("paid")]).await.unwrap();
    consumer.fetch(TOPIC, partition, offset).await.unwrap();
    admin.describe_topic_configs(TOPIC, &[]).await.unwrap();

    let mut clients_of_connection: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
    for arrival in broker.arrivals() {
        clients_of_connection
            .entry(arrival.connection)
            .or_default()
            .insert(arrival.client_id().to_owned());
    }
    assert!(
        clients_of_connection.values().all(|clients| clients.len() == 1),
        "a connection carries the requests of one role: {clients_of_connection:?}"
    );
    let clients: BTreeSet<String> = clients_of_connection.into_values().flatten().collect();
    let configured = [
        &producer.configuration().client.client_id,
        &consumer.configuration().client.client_id,
        &admin.configuration().client.client_id,
    ];
    assert_eq!(clients, configured.iter().map(ToString::to_string).collect());
    assert_eq!(clients.len(), 3);
}

#[tokio::test]
async fn a_role_without_a_client_id_names_itself_as_kafkas_clients_do() {
    let (_, broker) = Cluster::of_one().await;
    let (one, other) = (
        KafkaProducer::from(broker.address()),
        KafkaProducer::from(broker.address()),
    );
    let consumer = KafkaConsumer::from(broker.address());
    let admin = KafkaAdmin::from(broker.address());

    let number_of = |client_id: &str, role: &str| -> u32 { client_id.strip_prefix(role).unwrap().parse().unwrap() };
    let one_name = one.configuration().client.client_id.to_string();
    let other_name = other.configuration().client.client_id.to_string();
    assert!(number_of(&one_name, "producer-") < number_of(&other_name, "producer-"));
    assert!(number_of(consumer.configuration().client.client_id.as_str(), "consumer-") >= 1);
    assert!(number_of(admin.configuration().client.client_id.as_str(), "adminclient-") >= 1);

    // The name is what the broker hears.
    one.send(TOPIC, Partition::from(0), &[Record::new("paid")])
        .await
        .unwrap();
    assert_eq!(broker.arrivals_of(ApiKey::Produce)[0].client_id(), one_name);
}

#[tokio::test]
async fn two_producers_share_no_connection() {
    let (_, broker) = Cluster::of_one().await;
    let (one, other) = (
        KafkaProducer::from(broker.address()),
        KafkaProducer::from(broker.address()),
    );
    let (partition, _) = start();

    one.send(TOPIC, partition, &[Record::new("a")]).await.unwrap();
    other.send(TOPIC, partition, &[Record::new("b")]).await.unwrap();

    let produced_on: Vec<usize> = broker
        .arrivals_of(ApiKey::Produce)
        .iter()
        .map(|arrival| arrival.connection)
        .collect();
    assert_eq!(produced_on.len(), 2);
    assert_ne!(produced_on[0], produced_on[1]);
    // Each producer asked for the leader itself: neither learned it from the other.
    assert_eq!(broker.arrivals_of(ApiKey::Metadata).len(), 2);
}
