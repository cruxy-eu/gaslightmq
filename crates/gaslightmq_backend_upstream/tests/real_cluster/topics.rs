//! A request that changes the cluster reaches it.

use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_test_kafka::{KafkaCluster, KafkaClusterError, unique_name};
use gaslightmq_wire::messages::create_topics_request::CreatableTopic;
use gaslightmq_wire::messages::{CreateTopicsRequest, CreateTopicsResponse, TopicName};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Request, ResponseError};

#[tokio::test]
async fn a_topic_created_through_the_backend_is_a_topic_of_the_cluster() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    let orders = unique_name("orders");
    let topic = CreatableTopic::default()
        .with_name(TopicName(StrBytes::from_string(orders.clone())))
        .with_num_partitions(3)
        .with_replication_factor(1);
    let create = CreateTopicsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30_000);
    let request = Request::new(7, 1, Some("order-admin"), &create)?;
    let answer = backend
        .call(BackendRequest::new(
            CallId::FIRST,
            ConnectionNumber::new(1),
            Endpoint::Bootstrap,
            request,
        ))
        .await;

    let BackendAnswer::Truth(truth) = answer else {
        anyhow::bail!("the cluster answers a CreateTopics request, and the backend made of it: {answer:?}");
    };
    let created: CreateTopicsResponse = truth.response().body()?;
    assert_eq!(
        (created.topics[0].error_code, created.topics[0].num_partitions),
        (0, 3),
        "{:?}",
        created.topics[0].error_message
    );
    assert!(
        truth.upstream().is_none(),
        "the answer tells no address, so the overlay leaves it as it is"
    );
    // The cluster has the topic, so it does not create it a second time.
    let again = cluster.topics().create(orders).await.unwrap_err();
    assert!(
        matches!(
            again,
            KafkaClusterError::Refused {
                error: ResponseError::TopicAlreadyExists,
                ..
            }
        ),
        "{again}"
    );
    Ok(())
}
