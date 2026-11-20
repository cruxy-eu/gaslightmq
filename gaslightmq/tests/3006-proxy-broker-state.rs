use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_testing::proxy::*;
use gaslightmq::proxy_api::NodeState;
use kafka_protocol::messages::{ApiKey, MetadataResponse};

fn node_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[tokio::test(flavor = "multi_thread")]
async fn down_refuses_and_drops_connections_until_live() {
    let cluster = FakeCluster::start(2, &[("orders", 2)]).await;
    let proxy = cluster.proxy().await;
    let handle = proxy.handle();
    let node1 = handle.nodes()[1].clone();
    let mut existing = TestClient::connect(node_addr(node1.proxy_port)).await.unwrap();
    metadata(&mut existing, None).await.unwrap();

    handle.set_node_state(1, NodeState::Down).await.unwrap();

    assert!(existing.closed_within(Duration::from_secs(2)).await, "existing connection dropped");
    assert!(TestClient::connect(node_addr(node1.proxy_port)).await.is_err(), "new connections refused");
    assert_eq!(handle.nodes()[1].state, NodeState::Down);
    // Other nodes are unaffected.
    let mut other = TestClient::connect(node_addr(handle.nodes()[0].proxy_port)).await.unwrap();
    metadata(&mut other, None).await.unwrap();

    handle.set_node_state(1, NodeState::Live).await.unwrap();

    let mut back = TestClient::connect(node_addr(node1.proxy_port)).await.unwrap();
    let resp = produce(&mut back, &[("orders", 1, &["back"])]).await.unwrap();
    assert_eq!(produce_codes(&resp), [("orders".into(), 1, 0)]);
    assert_eq!(handle.nodes()[1].error, None);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unresponsive_holds_traffic_until_live() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let handle = proxy.handle();
    let port = handle.nodes()[0].proxy_port;
    let mut client = TestClient::connect(node_addr(port)).await.unwrap();
    metadata(&mut client, None).await.unwrap();

    handle.set_node_state(0, NodeState::Unresponsive).await.unwrap();

    let cid = client.send(ApiKey::Metadata, METADATA_V, &metadata_request(None)).await.unwrap();
    let got = client.recv_within::<MetadataResponse>(ApiKey::Metadata, METADATA_V, Duration::from_millis(500)).await.unwrap();
    assert!(got.is_none(), "unresponsive broker must not answer");
    // New connections are accepted but stay silent too.
    let mut late = TestClient::connect(node_addr(port)).await.unwrap();
    late.send(ApiKey::Metadata, METADATA_V, &metadata_request(None)).await.unwrap();

    handle.set_node_state(0, NodeState::Live).await.unwrap();

    let (got, _) = client.recv_within::<MetadataResponse>(ApiKey::Metadata, METADATA_V, TIMEOUT).await.unwrap().unwrap();
    assert_eq!(got, cid);
    assert!(late.recv_within::<MetadataResponse>(ApiKey::Metadata, METADATA_V, TIMEOUT).await.unwrap().is_some());
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_follows_the_broker_it_points_at() {
    let cluster = FakeCluster::start(2, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let handle = proxy.handle();

    handle.set_node_state(0, NodeState::Down).await.unwrap();

    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();
    assert!(client.closed_within(Duration::from_secs(2)).await);

    handle.set_node_state(0, NodeState::Live).await.unwrap();
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();
    metadata(&mut client, None).await.unwrap();
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_node_is_an_error() {
    let cluster = FakeCluster::start(1, &[]).await;
    let proxy = cluster.proxy().await;
    assert!(proxy.handle().set_node_state(7, NodeState::Down).await.is_err());
    proxy.shutdown().await;
}
