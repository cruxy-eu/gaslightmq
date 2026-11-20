//! A cluster-aware client through the proxy, three fake brokers behind it.
//! Tests only talk to the client through the facade traits.

use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq::proxy_api::{ApiRow, ErrorPad, NodeState, catalog::api};
use gaslightmq_kafka_client_api::{Admin, Consumer, Producer};
use gaslightmq_kafka_client_native::{ClientConfig, KafkaClient};
use gaslightmq_kernel::{Partition, Record};
use gaslightmq_testing::proxy::FakeCluster;
use kafka_protocol::error::ResponseError;

fn client(bootstrap: SocketAddr, delivery_timeout: Duration) -> impl Producer + Consumer + Admin {
    KafkaClient::new(
        ClientConfig::new(bootstrap.to_string())
            .delivery_timeout(delivery_timeout)
            .request_timeout(Duration::from_millis(500)),
    )
}

fn error_row(api_key: i16, code: i16) -> ApiRow {
    let mut row = ApiRow::new(api_key);
    row.error = ErrorPad { enabled: true, probability: 100, code };
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn records_reach_the_leader_of_each_partition() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let proxy = cluster.proxy().await;
    let client = client(proxy.bootstrap_addr(), Duration::from_secs(5));

    for p in 0..3 {
        let result = client.send("orders", Partition::from(p), &[Record::new(format!("p{p}"))]).await;
        assert_eq!(result.error, None);
    }

    for p in 0..3 {
        assert_eq!(cluster.values(p as usize, "orders", p).await, [format!("p{p}")]);
    }
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retriable_error_recovers_once_cleared() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let client = client(proxy.bootstrap_addr(), Duration::from_secs(10));
    proxy.handle().set_row(error_row(api::PRODUCE, ResponseError::NotLeaderOrFollower.code()));

    let handle = proxy.handle();
    let clear = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.clear_rules();
    });
    let result = client.send("orders", Partition::from(0), &[Record::new("retried")]).await;
    clear.await.unwrap();

    assert_eq!(result.error, None);
    assert_eq!(cluster.values(0, "orders", 0).await, ["retried"]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_retriable_error_fails_delivery() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(error_row(api::PRODUCE, ResponseError::MessageTooLarge.code()));
    let client = client(proxy.bootstrap_addr(), Duration::from_secs(5));

    let result = client.send("orders", Partition::from(0), &[Record::new("too big, apparently")]).await;

    assert_eq!(result.error, Some(ResponseError::MessageTooLarge));
    assert!(cluster.values(0, "orders", 0).await.is_empty());
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn down_node_only_affects_its_partitions() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let proxy = cluster.proxy().await;
    let client = client(proxy.bootstrap_addr(), Duration::from_millis(1500));
    assert_eq!(client.send("orders", Partition::from(1), &[Record::new("warm-up")]).await.error, None);

    proxy.handle().set_node_state(1, NodeState::Down).await.unwrap();

    let (still_up, lost) = ([Record::new("still-up")], [Record::new("lost")]);
    let (up, down) = tokio::join!(
        client.send("orders", Partition::from(0), &still_up),
        client.send("orders", Partition::from(1), &lost),
    );
    assert_eq!(up.error, None);
    assert_eq!(down.error, Some(ResponseError::RequestTimedOut));

    proxy.handle().set_node_state(1, NodeState::Live).await.unwrap();
    assert_eq!(client.send("orders", Partition::from(1), &[Record::new("after")]).await.error, None);
    assert_eq!(cluster.values(1, "orders", 1).await, ["warm-up", "after"]);
    proxy.shutdown().await;
}

/// The calls a Restore makes before writing: topic config, then partitions.
#[tokio::test(flavor = "multi_thread")]
async fn restore_preflight_sees_injected_errors() {
    let cluster = FakeCluster::start(1, &[("orders", 2)]).await;
    let proxy = cluster.proxy().await;
    let client = client(proxy.bootstrap_addr(), Duration::from_secs(2));
    proxy.handle().set_row(error_row(api::DESCRIBE_CONFIGS, ResponseError::TopicAuthorizationFailed.code()));

    let configs = client.describe_topic_configs("orders", &["max.message.bytes"]).await;
    assert_eq!(configs.error, Some(ResponseError::TopicAuthorizationFailed));

    let metadata = client.metadata_for_topic("orders").await;
    assert_eq!(metadata.partitions.len(), 2);
    proxy.shutdown().await;
}
