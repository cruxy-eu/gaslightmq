//! Asking about topics: their partitions and their configs, and what happens for a topic the cluster does not have.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use gaslightmq_clock::TimeSpan;
use gaslightmq_kafka_client_api::{Admin, Consumer, Producer, ProtocolConsumer, TopicMetadata};
use gaslightmq_kafka_client_native::{ClientError, KafkaConsumer, KafkaProducer};
use gaslightmq_kernel::TopicName;
use gaslightmq_test_kafka::{KafkaCluster, KafkaClusterError, NewTopic, unique_name};
use gaslightmq_wire::{Record, ResponseError};
use tokio::time::timeout;

use crate::{
    HOPELESS_BACKOFF, HOPELESS_CALL, admin, consumer, consuming, kafka, offset, partition, producer, producing, values,
};

/// The error of a call that kept hearing "unknown topic or partition" until its time was over.
fn unknown_until_the_time_was_over() -> ClientError {
    ClientError::CallTimedOut {
        error: Box::new(ClientError::Broker {
            error: ResponseError::UnknownTopicOrPartition,
        }),
    }
}

fn partitions_of(topic: &TopicMetadata) -> Vec<u32> {
    topic.partitions.iter().map(|partition| partition.id.value()).collect()
}

#[tokio::test]
async fn a_consumer_asks_for_the_partitions_of_a_topic() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(3))
        .await
        .unwrap();
    let consumer = consumer(cluster);

    let described = consumer.metadata_for_topic(orders.name().as_str()).await.unwrap();

    assert_eq!(described.name, orders.name().as_str());
    assert_eq!(partitions_of(&described), [0, 1, 2]);
}

#[tokio::test]
async fn a_consumer_asks_for_every_topic_of_the_cluster() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(2))
        .await
        .unwrap();
    let payments = cluster.topics().create(NewTopic::unique("payments")).await.unwrap();
    let consumer = consumer(cluster);

    let described = consumer.metadata_for_all_topics().await.unwrap();

    // Other tests have topics on the same cluster, so there are more than these two.
    let partitions: BTreeMap<&str, Vec<u32>> = described
        .iter()
        .map(|topic| (topic.name.as_str(), partitions_of(topic)))
        .collect();
    assert_eq!(partitions.get(orders.name().as_str()), Some(&vec![0, 1]));
    assert_eq!(partitions.get(payments.name().as_str()), Some(&vec![0]));
}

#[tokio::test]
async fn an_admin_reads_the_configs_of_a_topic() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("retention.ms", "60000"))
        .await
        .unwrap();
    let admin = admin(cluster);

    let asked_for = admin
        .describe_topic_configs(orders.name().as_str(), &["retention.ms", "cleanup.policy"])
        .await;
    let all = admin.describe_topic_configs(orders.name().as_str(), &[]).await;

    // The config the topic was created with, and one it has from the broker.
    assert_eq!(
        asked_for,
        Ok(BTreeMap::from([
            ("cleanup.policy".to_owned(), Some("delete".to_owned())),
            ("retention.ms".to_owned(), Some("60000".to_owned())),
        ]))
    );
    // Without names it gets every config, as the cluster itself lists them.
    assert_eq!(all, Ok(orders.configs().await.unwrap()));
}

#[tokio::test]
async fn an_admin_hears_at_once_that_the_cluster_has_no_such_topic() {
    let cluster = kafka().await;
    let admin = admin(cluster);

    // The admin has a minute for the call, and the answer of the cluster ends it long before.
    let configs = timeout(
        Duration::from_secs(20),
        admin.describe_topic_configs(&unique_name("never-created"), &[]),
    )
    .await;

    assert_eq!(
        configs,
        Ok(Err(ClientError::Broker {
            error: ResponseError::UnknownTopicOrPartition
        }))
    );
}

#[tokio::test]
async fn a_consumer_that_may_not_create_topics_never_finds_a_topic_the_cluster_does_not_have() {
    let cluster = kafka().await;
    let name = unique_name("never-created");
    let mut configuration = consuming(cluster);
    configuration.allow_auto_create_topics = false;
    configuration.default_api_timeout = HOPELESS_CALL;
    configuration.client.retry_backoff = HOPELESS_BACKOFF;
    let consumer = KafkaConsumer::new(configuration);

    let described = consumer.metadata_for_topic(&name).await;
    let read = consumer.fetch(&name, partition(0), offset(0)).await;
    let watermarks = consumer.watermarks(&name, partition(0)).await;

    assert_eq!(described, Err(unknown_until_the_time_was_over()));
    assert_eq!(read, Err(unknown_until_the_time_was_over()));
    assert_eq!(watermarks, Err(unknown_until_the_time_was_over()));
    assert_does_not_exist(cluster, &name).await;
}

#[tokio::test]
async fn a_consumer_that_may_create_topics_gets_the_topic_it_asks_about() {
    let cluster = kafka().await;
    let name = unique_name("created-by-the-question");
    let consumer = consumer(cluster);

    let described = consumer.metadata_for_topic(&name).await.unwrap();

    assert_eq!(described.name, name);
    assert_eq!(partitions_of(&described), [0]);
    let created = cluster.topics().get(&TopicName::new(&name).unwrap()).await.unwrap();
    assert_eq!(created.partitions(), 1);
}

#[tokio::test]
async fn a_producer_gets_the_topic_it_writes_to() {
    let cluster = kafka().await;
    let name = unique_name("created-by-the-first-record");
    let producer = producer(cluster);

    let written = producer.send(&name, partition(0), &[Record::new("placed")]).await;

    assert_eq!(written, Ok(offset(0)));
    let created = cluster.topics().get(&TopicName::new(&name).unwrap()).await.unwrap();
    assert_eq!(values(&created.partition(0).records().await.unwrap()), ["placed"]);
}

#[tokio::test]
async fn a_producer_gets_no_topic_from_a_cluster_that_creates_none() {
    let cluster = KafkaCluster::plaintext()
        .with_broker_property("auto.create.topics.enable", "false")
        .start()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let name = unique_name("never-created");
    let mut configuration = producing(&cluster);
    configuration.delivery_timeout = HOPELESS_CALL;
    configuration.client.retry_backoff = HOPELESS_BACKOFF;
    let producer = KafkaProducer::new(configuration);

    let written = producer.send(&name, partition(0), &[Record::new("placed")]).await;

    assert_eq!(written, Err(unknown_until_the_time_was_over()));
    assert_does_not_exist(&cluster, &name).await;
}

#[tokio::test]
async fn a_partition_that_the_topic_does_not_have_has_no_leader() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let mut configuration = producing(cluster);
    configuration.delivery_timeout = HOPELESS_CALL;
    configuration.client.retry_backoff = HOPELESS_BACKOFF;
    let producer = KafkaProducer::new(configuration);

    let written = producer
        .send(orders.name().as_str(), partition(7), &[Record::new("placed")])
        .await;

    assert_eq!(
        written,
        Err(ClientError::CallTimedOut {
            error: Box::new(ClientError::NoLeader {
                topic: orders.name().to_string(),
                partition: 7
            })
        })
    );
}

#[tokio::test]
async fn a_name_that_is_no_topic_name_is_refused_at_once() {
    let cluster = kafka().await;
    let producer = producer(cluster);

    let written = producer.send("orders/eu", partition(0), &[Record::new("placed")]).await;

    assert_eq!(
        written,
        Err(ClientError::Broker {
            error: ResponseError::InvalidTopicException
        })
    );
}

async fn assert_does_not_exist(cluster: &KafkaCluster, name: &str) {
    let missing = cluster.topics().get(&TopicName::new(name).unwrap()).await.unwrap_err();
    assert!(
        matches!(
            missing,
            KafkaClusterError::Refused {
                error: ResponseError::UnknownTopicOrPartition,
                ..
            }
        ),
        "{missing}"
    );
}

/// A test written against the traits runs with any client that implements them.
async fn write_and_read_back(topic: &str, producer: &impl Producer, consumer: &impl Consumer, admin: &impl Admin) {
    let written = producer.send(topic, partition(0), &[Record::new("placed")]).await;
    assert_eq!(written.error, None);
    assert_eq!(written.offset, offset(0));

    let read = consumer.fetch(topic, partition(0), written.offset).await;
    assert_eq!(values(&read), ["placed"]);
    assert_eq!(consumer.metadata_for_topic(topic).await.partitions.len(), 1);
    assert!(
        consumer
            .metadata_for_all_topics()
            .await
            .iter()
            .any(|described| described.name == topic)
    );
    assert_eq!(consumer.watermarks(topic, partition(0)).await.high, offset(1));

    let configs = admin.describe_topic_configs(topic, &[]).await;
    assert_eq!(configs.error, None);
    assert!(configs.configs.contains_key("retention.ms"));
}

#[tokio::test]
async fn each_role_implements_its_trait() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();
    let topic = orders.name().as_str();
    let mut configuration = consuming(cluster);
    configuration.fetch_max_wait = TimeSpan::from_millis(100);
    let (producer, consumer, admin) = (producer(cluster), KafkaConsumer::new(configuration), admin(cluster));

    write_and_read_back(topic, &producer, &consumer, &admin).await;

    let read = ProtocolConsumer::fetch(&consumer, topic, partition(0), offset(0)).await;
    assert_eq!(values(&read), ["placed"]);
    let streamed: Vec<Record> = ProtocolConsumer::stream(&consumer, topic, partition(0), offset(0))
        .collect()
        .await;
    assert_eq!(values(&streamed), ["placed"]);
}

#[tokio::test]
async fn a_trait_without_an_error_carries_what_the_cluster_answered() {
    let cluster = kafka().await;
    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("max.message.bytes", "100"))
        .await
        .unwrap();

    let written = Producer::send(
        &producer(cluster),
        orders.name().as_str(),
        partition(0),
        &[Record::new(vec![0u8; 1_000])],
    )
    .await;
    let configs = Admin::describe_topic_configs(&admin(cluster), &unique_name("never-created"), &[]).await;

    assert_eq!(written.error, Some(ResponseError::MessageTooLarge));
    assert_eq!(configs.error, Some(ResponseError::UnknownTopicOrPartition));
}
