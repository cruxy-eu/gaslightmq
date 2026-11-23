//! How a role opens, uses and loses its connections.

use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_native::{
    AdminConfiguration, ClientError, ConsumerConfiguration, KafkaAdmin, KafkaConsumer, KafkaProducer,
    ProducerConfiguration,
};
use gaslightmq_wire::messages::ProduceResponse;
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Offset, Partition, Record, ResponseError};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::support::{Cluster, FakeBroker, TOPIC};

#[tokio::test]
async fn a_response_that_arrives_in_pieces_is_put_back_together() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    broker.serve_in_pieces(move |request| {
        let cluster = cluster.clone();
        async move { cluster.answer(&request) }
    });
    let consumer = KafkaConsumer::from(broker.address());

    // The greeting and then Metadata, twice on the same connection.
    assert_eq!(consumer.metadata_for_all_topics().await.unwrap().len(), 1);
    assert_eq!(consumer.metadata_for_all_topics().await.unwrap().len(), 1);
}

#[tokio::test]
async fn every_connection_starts_with_a_greeting() {
    let (_, broker) = Cluster::of_one().await;
    let consumer = KafkaConsumer::from(broker.address());

    consumer
        .fetch(TOPIC, Partition::from(0), Offset::from(0i64))
        .await
        .unwrap();
    consumer.heartbeat("analytics").await.unwrap();

    let arrivals = broker.arrivals();
    let connections = arrivals.iter().map(|arrival| arrival.connection).max().unwrap() + 1;
    assert!(connections >= 3, "bootstrap, leader and coordinator: {connections}");
    for connection in 0..connections {
        let first = arrivals
            .iter()
            .find(|arrival| arrival.connection == connection)
            .unwrap();
        assert_eq!(first.api(), ApiKey::ApiVersions);
        let greetings = arrivals
            .iter()
            .filter(|arrival| arrival.connection == connection && arrival.api() == ApiKey::ApiVersions)
            .count();
        assert_eq!(greetings, 1);
    }
}

#[tokio::test]
async fn a_peer_that_hangs_up_fails_the_call_when_its_time_runs_out() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let mut configuration = ConsumerConfiguration::new(address);
    configuration.default_api_timeout = TimeSpan::from_millis(300);
    let consumer = KafkaConsumer::new(configuration);

    let error = consumer.metadata_for_all_topics().await.unwrap_err();

    assert!(
        matches!(error, ClientError::OutOfTime { error: ref last } if matches!(**last, ClientError::ConnectionLost { .. })),
        "{error}"
    );
}

#[tokio::test]
async fn a_greeting_without_an_answer_times_out_as_a_request_does() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut silent = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            silent.push(stream);
        }
    });
    let mut configuration = AdminConfiguration::new(address);
    configuration.client.request_timeout = TimeSpan::from_millis(100);
    configuration.retries = 0;
    let admin = KafkaAdmin::new(configuration);

    let configs = admin.describe_topic_configs(TOPIC, &[]).await;

    assert_eq!(configs, Err(ClientError::NoAnswer));
}

#[tokio::test]
async fn a_request_without_an_answer_times_out_and_the_next_one_opens_a_new_connection() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let silent_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let silent_once = Arc::clone(&silent_once);
        async move {
            if silent_once.swap(false, Ordering::Relaxed) {
                pending::<()>().await;
            }
            cluster.answer(&request)
        }
    });
    let mut configuration = AdminConfiguration::new(broker.address());
    configuration.client.request_timeout = TimeSpan::from_millis(100);
    configuration.retries = 0;
    let admin = KafkaAdmin::new(configuration);

    assert_eq!(
        admin.describe_topic_configs(TOPIC, &[]).await,
        Err(ClientError::NoAnswer)
    );
    assert!(admin.describe_topic_configs(TOPIC, &[]).await.is_ok());

    let asked_on: Vec<usize> = broker
        .arrivals_of(ApiKey::DescribeConfigs)
        .iter()
        .map(|arrival| arrival.connection)
        .collect();
    assert_eq!(asked_on, [0, 1]);
}

#[tokio::test]
async fn a_caller_that_gives_up_leaves_no_response_behind_for_the_next_request() {
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
    let consumer = KafkaConsumer::from(broker.address());
    let partition = Partition::from(0);

    // The caller stops waiting while the Fetch is at the broker.
    tokio::select! {
        _ = consumer.fetch(TOPIC, partition, Offset::from(0i64)) => panic!("the broker holds the Fetch"),
        () = broker.receives(1, ApiKey::Fetch) => {}
    }
    let_go.notify_one();

    // The next request to the same broker gets its own response, on a new connection.
    let watermarks = consumer.watermarks(TOPIC, partition).await.unwrap();

    assert_eq!(watermarks.high, Offset::from(0i64));
    let fetched_on = broker.arrivals_of(ApiKey::Fetch)[0].connection;
    let listed_on = broker.arrivals_of(ApiKey::ListOffsets)[0].connection;
    assert_ne!(fetched_on, listed_on);
}

/// A cluster of one broker that refuses the first Produce with `NOT_LEADER_OR_FOLLOWER`.
async fn cluster_that_refuses_the_first_produce() -> FakeBroker {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let refuse_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let refuse_once = Arc::clone(&refuse_once);
        async move {
            if request.api().known() == Some(ApiKey::Produce) && refuse_once.swap(false, Ordering::Relaxed) {
                let refused = PartitionProduceResponse::default()
                    .with_index(0)
                    .with_error_code(ResponseError::NotLeaderOrFollower.code());
                return request
                    .response(&ProduceResponse::default().with_responses(vec![
                        TopicProduceResponse::default()
                            .with_name(StrBytes::from_static_str(TOPIC).into())
                            .with_partition_responses(vec![refused]),
                    ]))
                    .unwrap();
            }
            cluster.answer(&request)
        }
    });
    broker
}

#[tokio::test]
async fn a_retriable_error_makes_the_producer_ask_for_the_leader_and_send_again() {
    let broker = cluster_that_refuses_the_first_produce().await;
    let mut configuration = ProducerConfiguration::new(broker.address());
    configuration.client.retry_backoff = TimeSpan::from_millis(10);
    let producer = KafkaProducer::new(configuration);

    let offset = producer.send(TOPIC, Partition::from(0), &[Record::new("paid")]).await;

    assert_eq!(offset, Ok(Offset::from(0i64)));
    assert_eq!(broker.arrivals_of(ApiKey::Produce).len(), 2);
    assert_eq!(broker.arrivals_of(ApiKey::Metadata).len(), 2);
}

#[tokio::test]
async fn a_producer_without_retries_returns_the_first_error() {
    let broker = cluster_that_refuses_the_first_produce().await;
    let mut configuration = ProducerConfiguration::new(broker.address());
    configuration.retries = 0;
    let producer = KafkaProducer::new(configuration);

    let offset = producer.send(TOPIC, Partition::from(0), &[Record::new("paid")]).await;

    assert_eq!(
        offset,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );
    assert_eq!(broker.arrivals_of(ApiKey::Produce).len(), 1);
}

#[tokio::test]
async fn a_producer_without_retries_still_waits_for_a_leader() {
    let broker = FakeBroker::bind().await;
    let cluster = Cluster::of(&[(1, &broker)]);
    let electing_once = Arc::new(AtomicBool::new(true));
    broker.serve(move |request| {
        let cluster = cluster.clone();
        let electing_once = Arc::clone(&electing_once);
        async move {
            if request.api().known() == Some(ApiKey::Metadata) && electing_once.swap(false, Ordering::Relaxed) {
                return cluster.metadata(&request, -1);
            }
            cluster.answer(&request)
        }
    });
    let mut configuration = ProducerConfiguration::new(broker.address());
    configuration.retries = 0;
    let producer = KafkaProducer::new(configuration);

    let offset = producer.send(TOPIC, Partition::from(0), &[Record::new("paid")]).await;

    assert_eq!(offset, Ok(Offset::from(0i64)));
    assert_eq!(broker.arrivals_of(ApiKey::Metadata).len(), 2);
    assert_eq!(broker.arrivals_of(ApiKey::Produce).len(), 1);
}

#[tokio::test]
async fn a_call_waits_the_backoff_between_two_tries() {
    let broker = cluster_that_refuses_the_first_produce().await;
    let mut configuration = ProducerConfiguration::new(broker.address());
    configuration.client.retry_backoff = TimeSpan::from_millis(150);
    let producer = KafkaProducer::new(configuration);
    let started = tokio::time::Instant::now();

    producer
        .send(TOPIC, Partition::from(0), &[Record::new("paid")])
        .await
        .unwrap();

    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[tokio::test]
async fn the_wait_between_two_tries_grows_up_to_the_longest_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connected = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&connected);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            log.lock().unwrap().push(tokio::time::Instant::now());
            drop(stream);
        }
    });
    let mut configuration = ConsumerConfiguration::new(address);
    configuration.client.retry_backoff = TimeSpan::from_millis(50);
    configuration.client.retry_backoff_max = TimeSpan::from_millis(100);
    configuration.default_api_timeout = TimeSpan::from_millis(500);
    let consumer = KafkaConsumer::new(configuration);

    consumer.metadata_for_all_topics().await.unwrap_err();

    // The waits are at least 50, 100 and 100 milliseconds.
    // That the third is not 200 is checked where no real time passes, in the tests of the logic.
    let connected = connected.lock().unwrap();
    let waits: Vec<Duration> = connected.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert!(waits.len() >= 3, "{waits:?}");
    assert!(waits[0] >= Duration::from_millis(50), "{waits:?}");
    assert!(waits[1] >= Duration::from_millis(100), "{waits:?}");
    assert!(waits[2] >= Duration::from_millis(100), "{waits:?}");
}
