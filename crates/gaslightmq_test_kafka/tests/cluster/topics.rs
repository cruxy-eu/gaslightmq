//! Creating topics, looking them up and deleting them.

use std::collections::BTreeSet;

use gaslightmq_kernel::{BrokerId, TopicName};
use gaslightmq_test_kafka::{KafkaClusterError, NewTopic};
use gaslightmq_wire::{ApiKey, ResponseError};

use crate::kafka;

/// Whether the cluster answered that it has no such topic.
fn is_unknown(error: &KafkaClusterError) -> bool {
    matches!(
        error,
        KafkaClusterError::Refused {
            error: ResponseError::UnknownTopicOrPartition,
            ..
        }
    )
}

#[tokio::test]
async fn a_topic_is_created_listed_and_deleted() {
    let cluster = kafka().await;
    let topics = cluster.topics();

    let orders = topics.create(NewTopic::unique("orders")).await.unwrap();
    assert!(orders.name().as_str().starts_with("orders-"));
    assert_eq!(orders.partitions(), 1);
    assert!(topics.list().await.unwrap().contains(orders.name()));

    topics.delete(orders.name()).await.unwrap();

    assert!(!topics.list().await.unwrap().contains(orders.name()));
    let gone = topics.get(orders.name()).await.unwrap_err();
    assert!(is_unknown(&gone), "{gone}");
}

#[tokio::test]
async fn a_topic_has_the_partitions_it_was_created_with() {
    let cluster = kafka().await;

    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(3))
        .await
        .unwrap();

    assert_eq!(orders.partitions(), 3);
    assert_eq!(cluster.topics().get(orders.name()).await.unwrap().partitions(), 3);
}

#[tokio::test]
async fn a_topic_keeps_the_configs_it_was_created_with_next_to_the_defaults_of_the_broker() {
    let cluster = kafka().await;

    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_config("retention.ms", "60000"))
        .await
        .unwrap();
    let configs = orders.configs().await.unwrap();

    assert_eq!(configs.get("retention.ms"), Some(&Some("60000".to_owned())));
    assert_eq!(configs.get("cleanup.policy"), Some(&Some("delete".to_owned())));
    assert_eq!(configs.get("no.such.config"), None);
}

#[tokio::test]
async fn the_first_broker_of_an_assignment_leads_its_partition() {
    let cluster = kafka().await;
    let broker = |id: u16| BrokerId::from(id);

    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_replica_assignment([
            vec![broker(3), broker(1)],
            vec![broker(1), broker(2)],
            vec![broker(2), broker(3)],
        ]))
        .await
        .unwrap();

    assert_eq!(orders.partitions(), 3);
    assert_eq!(orders.partition(0).leader().await.unwrap(), broker(3));
    assert_eq!(orders.partition(1).leader().await.unwrap(), broker(1));
    assert_eq!(orders.partition(2).leader().await.unwrap(), broker(2));
}

#[tokio::test]
async fn the_partitions_of_a_topic_spread_over_the_brokers() {
    let cluster = kafka().await;

    let orders = cluster
        .topics()
        .create(NewTopic::unique("orders").with_partitions(3))
        .await
        .unwrap();

    let mut leaders = BTreeSet::new();
    for partition in 0..3 {
        leaders.insert(orders.partition(partition).leader().await.unwrap());
    }
    assert_eq!(leaders.len(), 3, "{leaders:?}");
}

#[tokio::test]
async fn a_topic_cannot_be_created_twice() {
    let cluster = kafka().await;
    let orders = cluster.topics().create(NewTopic::unique("orders")).await.unwrap();

    let error = cluster.topics().create(orders.name()).await.unwrap_err();

    assert!(
        matches!(
            error,
            KafkaClusterError::Refused {
                api: ApiKey::CreateTopics,
                error: ResponseError::TopicAlreadyExists,
                ..
            }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_topic_with_more_replicas_than_brokers_is_refused_with_what_the_cluster_said() {
    let cluster = kafka().await;

    let error = cluster
        .topics()
        .create(NewTopic::unique("orders").with_replication_factor(4))
        .await
        .unwrap_err();

    assert!(
        matches!(
            error,
            KafkaClusterError::Refused {
                error: ResponseError::InvalidReplicationFactor,
                detail: Some(_),
                ..
            }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_text_that_is_no_topic_name_creates_no_topic() {
    let cluster = kafka().await;

    let error = cluster.topics().create("orders/eu").await.unwrap_err();

    assert!(matches!(error, KafkaClusterError::InvalidTopicName { .. }), "{error}");
    assert_eq!(
        error.to_string(),
        "new topic: topic name \"orders/eu\" has the invalid character '/' at position 6"
    );
}

#[tokio::test]
async fn a_topic_that_was_never_created_is_not_found_and_not_created_by_the_question() {
    let cluster = kafka().await;
    let name = TopicName::new(NewTopic::unique("never-created").name()).unwrap();

    let first = cluster.topics().get(&name).await.unwrap_err();
    let deleted = cluster.topics().delete(&name).await.unwrap_err();
    let second = cluster.topics().get(&name).await.unwrap_err();

    assert!(is_unknown(&first), "{first}");
    assert!(is_unknown(&deleted), "{deleted}");
    assert!(is_unknown(&second), "{second}");
    assert_eq!(
        first.to_string(),
        format!("Metadata for topic `{name}`: the cluster answered UnknownTopicOrPartition")
    );
}
