//! Writing and reading records, and asking about topics.

use std::collections::BTreeMap;

use futures::StreamExt;
use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::{Admin, Consumer, Producer, ProtocolConsumer, Watermarks};
use gaslightmq_kafka_client_native::{
    Acks, AdminConfiguration, ClientError, ConsumerConfiguration, IsolationLevel, KafkaAdmin, KafkaConsumer,
    KafkaProducer, ProducerConfiguration,
};
use gaslightmq_wire::messages::{FetchRequest, MetadataRequest, ProduceRequest};
use gaslightmq_wire::{ApiKey, Offset, Partition, Record, ResponseError};
use tokio::net::TcpListener;

use crate::support::{Cluster, TOPIC};

fn partition() -> Partition {
    Partition::from(0)
}

fn values(records: &[Record]) -> Vec<&[u8]> {
    records.iter().map(|record| record.value.as_deref().unwrap()).collect()
}

#[tokio::test]
async fn a_consumer_reads_what_a_producer_wrote() {
    let (cluster, broker) = Cluster::of_one().await;
    let producer = KafkaProducer::from(broker.address());
    let consumer = KafkaConsumer::from(broker.address());

    let first = producer
        .send(TOPIC, partition(), &[Record::new("placed"), Record::new("paid")])
        .await;
    let second = producer.send(TOPIC, partition(), &[Record::new("shipped")]).await;

    assert_eq!(first, Ok(Offset::from(0i64)));
    assert_eq!(second, Ok(Offset::from(2i64)));
    assert_eq!(cluster.log().len(), 3);
    let read = consumer.fetch(TOPIC, partition(), Offset::from(1i64)).await.unwrap();
    assert_eq!(values(&read), [b"paid".as_slice(), b"shipped"]);
    assert_eq!(
        consumer.watermarks(TOPIC, partition()).await,
        Ok(Watermarks {
            low: Offset::from(0i64),
            high: Offset::from(3i64)
        })
    );
}

#[tokio::test]
async fn a_stream_reads_until_a_fetch_brings_nothing() {
    let (_, broker) = Cluster::of_one().await;
    let producer = KafkaProducer::from(broker.address());
    let consumer = KafkaConsumer::from(broker.address());
    for value in ["placed", "paid", "shipped"] {
        producer.send(TOPIC, partition(), &[Record::new(value)]).await.unwrap();
    }

    let read: Vec<Record> = consumer.stream(TOPIC, partition(), Offset::from(1i64)).collect().await;

    assert_eq!(values(&read), [b"paid".as_slice(), b"shipped"]);
    // The Fetch that got both records, and the one at the end of the log that got none.
    assert_eq!(broker.arrivals_of(ApiKey::Fetch).len(), 2);
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
async fn a_consumer_asks_for_the_partitions_of_a_topic() {
    let (_, broker) = Cluster::of_one().await;
    let mut configuration = ConsumerConfiguration::new(broker.address());
    configuration.default_api_timeout = TimeSpan::from_millis(300);
    let consumer = KafkaConsumer::new(configuration);

    let orders = consumer.metadata_for_topic(TOPIC).await.unwrap();
    let unknown = consumer.metadata_for_topic("payments").await.unwrap_err();

    assert_eq!(orders.name, TOPIC);
    assert_eq!(orders.partitions.len(), 1);
    assert_eq!(orders.partitions[0].id, partition());
    assert_eq!(
        unknown,
        ClientError::CallTimedOut {
            error: Box::new(ClientError::Broker {
                error: ResponseError::UnknownTopicOrPartition
            })
        }
    );
}

#[tokio::test]
async fn an_admin_asks_how_a_topic_is_configured() {
    let (_, broker) = Cluster::of_one().await;
    let admin = KafkaAdmin::from(broker.address());

    let configs = admin.describe_topic_configs(TOPIC, &["retention.ms"]).await;

    assert_eq!(
        configs,
        Ok(BTreeMap::from([(
            "retention.ms".to_owned(),
            Some("604800000".to_owned())
        )]))
    );
}

/// A test written against the traits runs with any client that implements them.
async fn write_and_read_back(producer: &impl Producer, consumer: &impl Consumer, admin: &impl Admin) {
    let written = producer.send(TOPIC, partition(), &[Record::new("placed")]).await;
    assert_eq!(written.error, None);
    assert_eq!(written.offset, Offset::from(0i64));

    let read = consumer.fetch(TOPIC, partition(), written.offset).await;
    assert_eq!(values(&read), [b"placed".as_slice()]);
    assert_eq!(consumer.metadata_for_topic(TOPIC).await.partitions.len(), 1);
    assert_eq!(consumer.metadata_for_all_topics().await.len(), 1);
    assert_eq!(consumer.watermarks(TOPIC, partition()).await.high, Offset::from(1i64));

    let configs = admin.describe_topic_configs(TOPIC, &[]).await;
    assert_eq!(configs.error, None);
    assert!(configs.configs.contains_key("retention.ms"));
}

#[tokio::test]
async fn each_role_implements_its_trait() {
    let (_, broker) = Cluster::of_one().await;
    let producer = KafkaProducer::from(broker.address());
    let consumer = KafkaConsumer::from(broker.address());
    let admin = KafkaAdmin::from(broker.address());

    write_and_read_back(&producer, &consumer, &admin).await;

    let read = ProtocolConsumer::fetch(&consumer, TOPIC, partition(), Offset::from(0i64)).await;
    assert_eq!(values(&read), [b"placed".as_slice()]);
    let streamed: Vec<Record> = ProtocolConsumer::stream(&consumer, TOPIC, partition(), Offset::from(0i64))
        .collect()
        .await;
    assert_eq!(values(&streamed), [b"placed".as_slice()]);
}

#[tokio::test]
async fn a_trait_without_an_error_carries_a_failure_as_the_closest_kafka_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });
    let mut producing = ProducerConfiguration::new(address);
    producing.retries = 0;
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

    assert_eq!(written.error, Some(ResponseError::NetworkException));
    assert_eq!(configs.error, Some(ResponseError::RequestTimedOut));
}
