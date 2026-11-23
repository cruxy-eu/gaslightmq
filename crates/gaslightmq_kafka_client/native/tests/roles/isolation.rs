//! What one role does never reaches another role, and a Fetch never holds up a Heartbeat.

use std::collections::{BTreeMap, BTreeSet};
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_native::{
    ClientError, ConsumerConfiguration, KafkaAdmin, KafkaConsumer, KafkaProducer, ProducerConfiguration,
};
use gaslightmq_wire::messages::{HeartbeatResponse, JoinGroupRequest};
use gaslightmq_wire::{ApiKey, Offset, Partition, Record, Request, ResponseError};
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

#[tokio::test]
async fn a_group_call_that_first_has_to_find_its_coordinator_does_not_wait_for_a_fetch() {
    let (broker, let_go) = cluster_that_holds_a_fetch().await;
    let consumer = KafkaConsumer::from(broker.address());
    let (partition, offset) = start();

    // The consumer knows the broker from its Fetch, and nothing about any group.
    let fetch = consumer.fetch(TOPIC, partition, offset);
    tokio::pin!(fetch);
    tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        () = broker.receives(1, ApiKey::Fetch) => {}
    }

    let group_calls = async {
        let heartbeat = consumer.heartbeat("analytics").await;
        let joined = consumer.join_group("billing").await;
        (heartbeat, joined.map(|joined| joined.generation))
    };
    let answers = tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        answers = timeout(Duration::from_secs(10), group_calls) => answers,
    };
    assert_eq!(answers, Ok((Ok(()), Ok(1))));

    // Nothing about a group travelled on the connection of the Fetch.
    let fetched_on = broker.arrivals_of(ApiKey::Fetch)[0].connection;
    let about_groups = [ApiKey::FindCoordinator, ApiKey::Heartbeat, ApiKey::JoinGroup];
    for api in about_groups {
        let arrivals = broker.arrivals_of(api);
        assert!(!arrivals.is_empty(), "{api:?}");
        assert!(
            arrivals.iter().all(|arrival| arrival.connection != fetched_on),
            "{api:?} on the connection of the Fetch"
        );
    }

    let_go.notify_one();
    assert_eq!(fetch.await, Ok(vec![]));
}

#[tokio::test]
async fn a_heartbeat_whose_coordinator_moved_finds_it_again_without_waiting_for_a_fetch() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let let_go = Arc::new(Notify::new());
    let held = Arc::clone(&let_go);
    let moved_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let held = Arc::clone(&held);
        let moved_once = Arc::clone(&moved_once);
        async move {
            match request.api().known() {
                Some(ApiKey::Fetch) => held.notified().await,
                Some(ApiKey::Heartbeat) if moved_once.swap(false, Ordering::Relaxed) => {
                    let moved = HeartbeatResponse::default().with_error_code(ResponseError::NotCoordinator.code());
                    return request.response(&moved).unwrap();
                }
                _ => {}
            }
            cluster.answer(&request)
        }
    });
    let consumer = KafkaConsumer::from(broker.address());
    consumer.join_group("analytics").await.unwrap();
    let (partition, offset) = start();

    let fetch = consumer.fetch(TOPIC, partition, offset);
    tokio::pin!(fetch);
    tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        () = broker.receives(1, ApiKey::Fetch) => {}
    }

    let heartbeat = tokio::select! {
        _ = &mut fetch => panic!("the broker holds the Fetch"),
        heartbeat = timeout(Duration::from_secs(10), consumer.heartbeat("analytics")) => heartbeat,
    };

    assert_eq!(heartbeat, Ok(Ok(())));
    assert_eq!(broker.arrivals_of(ApiKey::Heartbeat).len(), 2);
    assert_eq!(broker.arrivals_of(ApiKey::FindCoordinator).len(), 2);
    let_go.notify_one();
    assert_eq!(fetch.await, Ok(vec![]));
}

#[tokio::test]
async fn a_request_that_waits_for_its_turn_on_a_connection_ends_with_its_call() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    broker.serve(move |request| {
        let cluster = cluster.clone();
        async move {
            if request.api().known() == Some(ApiKey::JoinGroup) {
                let joining: JoinGroupRequest = request.body().unwrap();
                if !joining.member_id.is_empty() {
                    // The coordinator waits for the other members of the group, and they never come.
                    pending::<()>().await;
                }
            }
            cluster.answer(&request)
        }
    });
    let mut configuration = ConsumerConfiguration::new(broker.address());
    configuration.default_api_timeout = TimeSpan::from_millis(300);
    let consumer = KafkaConsumer::new(configuration);

    // A join has the time of a rebalance, which is much longer than the time of a heartbeat.
    let join = consumer.join_group("analytics");
    tokio::pin!(join);
    tokio::select! {
        _ = &mut join => panic!("the coordinator holds the join"),
        () = broker.receives(2, ApiKey::JoinGroup) => {}
    }

    // The heartbeat of another group waits behind the join on the connection to the coordinator.
    let heartbeat = tokio::select! {
        _ = &mut join => panic!("the coordinator holds the join"),
        heartbeat = timeout(Duration::from_secs(10), consumer.heartbeat("billing")) => heartbeat,
    };

    assert_eq!(
        heartbeat,
        Ok(Err(ClientError::OutOfTime {
            error: Box::new(ClientError::NoAnswer)
        }))
    );
    assert!(broker.arrivals_of(ApiKey::Heartbeat).is_empty());
}

/// The other side of the tests above: the broker does hold what comes after a Fetch on the same connection.
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
