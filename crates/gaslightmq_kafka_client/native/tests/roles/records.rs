//! What a request carries, and what a trait returns for a failure.
//!
//! Writing and reading against real brokers is in `tests/kafka`.

use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::{Admin, Producer};
use gaslightmq_kafka_client_native::{
    Acks, AdminConfiguration, ClientError, ConsumerConfiguration, IsolationLevel, KafkaAdmin, KafkaConsumer,
    KafkaProducer, ProducerConfiguration,
};
use gaslightmq_wire::messages::{FetchRequest, MetadataRequest, ProduceRequest};
use gaslightmq_wire::{ApiKey, Offset, Partition, Record};
use tokio::net::TcpListener;

use crate::support::{Cluster, TOPIC};

fn partition() -> Partition {
    Partition::from(0)
}

#[tokio::test]
async fn a_request_carries_what_its_role_is_configured_with() {
    let (_, broker) = Cluster::of_one().await;
    let mut producing = ProducerConfiguration::new(broker.address());
    producing.acks = Acks::Leader;
    producing.client.request_timeout = TimeSpan::from_secs(7);
    producing.client.client_id = "order-ingest".parse().unwrap();
    let mut consuming = ConsumerConfiguration::new(broker.address());
    consuming.fetch_max_wait = TimeSpan::from_millis(20);
    consuming.fetch_min_bytes = 8;
    consuming.fetch_max_bytes = 4_096;
    consuming.max_partition_fetch_bytes = 1_024;
    consuming.isolation_level = IsolationLevel::ReadCommitted;
    consuming.allow_auto_create_topics = false;

    KafkaProducer::new(producing)
        .send(TOPIC, partition(), &[Record::new("placed")])
        .await
        .unwrap();
    KafkaConsumer::new(consuming)
        .fetch(TOPIC, partition(), Offset::from(0i64))
        .await
        .unwrap();

    let produced = &broker.arrivals_of(ApiKey::Produce)[0];
    let produce: ProduceRequest = produced.request.body().unwrap();
    assert_eq!(produced.client_id(), "order-ingest");
    assert_eq!(produce.acks, 1);
    assert_eq!(produce.timeout_ms, 7_000);
    let fetch: FetchRequest = broker.arrivals_of(ApiKey::Fetch)[0].request.body().unwrap();
    assert_eq!(fetch.max_wait_ms, 20);
    assert_eq!(fetch.min_bytes, 8);
    assert_eq!(fetch.max_bytes, 4_096);
    assert_eq!(fetch.isolation_level, 1);
    assert_eq!(fetch.topics[0].partitions[0].partition_max_bytes, 1_024);
    // Each role asked for the leader itself: the producer as Kafka's producer does, the consumer as configured.
    let asked: Vec<(String, bool)> = broker
        .arrivals_of(ApiKey::Metadata)
        .iter()
        .map(|arrival| {
            let metadata: MetadataRequest = arrival.request.body().unwrap();
            (arrival.client_id().to_owned(), metadata.allow_auto_topic_creation)
        })
        .collect();
    assert_eq!(asked[0], ("order-ingest".to_owned(), true));
    assert!(!asked[1].1);
}

#[tokio::test]
async fn a_trait_returns_a_failure_of_the_client_and_no_error_code_of_a_broker() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let mut producing = ProducerConfiguration::new(address);
    producing.delivery_timeout = TimeSpan::from_millis(200);
    let mut administering = AdminConfiguration::new(address);
    administering.default_api_timeout = TimeSpan::from_millis(200);

    let written = Producer::send(
        &KafkaProducer::new(producing),
        TOPIC,
        partition(),
        &[Record::new("placed")],
    )
    .await;
    let configs = Admin::describe_topic_configs(&KafkaAdmin::new(administering), TOPIC, &[]).await;

    // Each call kept losing its connection until its time was over.
    assert!(
        matches!(written, Err(ClientError::OutOfTime { error: ref last }) if matches!(**last, ClientError::ConnectionLost { .. })),
        "{written:?}"
    );
    assert!(
        matches!(configs, Err(ClientError::OutOfTime { error: ref last }) if matches!(**last, ClientError::ConnectionLost { .. })),
        "{configs:?}"
    );
}
