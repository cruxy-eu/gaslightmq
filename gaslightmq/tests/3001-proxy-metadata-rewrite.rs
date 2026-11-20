use std::net::SocketAddr;

use gaslightmq_testing::proxy::*;
use kafka_protocol::messages::{ApiKey, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

#[tokio::test(flavor = "multi_thread")]
async fn nodes_are_discovered_at_startup() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let proxy = cluster.proxy().await;

    let nodes = proxy.handle().nodes();
    let ids: Vec<i32> = nodes.iter().map(|n| n.node_id).collect();
    assert_eq!(ids, [0, 1, 2]);
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(n.upstream, cluster.addr(i).to_string());
        assert_ne!(n.proxy_port, cluster.addr(i).port());
    }
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_advertises_only_proxy_listeners() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp = metadata(&mut client, None).await.unwrap();

    let proxy_ports: Vec<i32> = proxy.handle().nodes().iter().map(|n| n.proxy_port as i32).collect();
    let advertised: Vec<(i32, String, i32)> =
        resp.brokers.iter().map(|b| (b.node_id.0, b.host.to_string(), b.port)).collect();
    assert_eq!(
        advertised,
        (0..3).map(|i| (i, "127.0.0.1".to_string(), proxy_ports[i as usize])).collect::<Vec<_>>()
    );
    let leaders: Vec<i32> = resp.topics[0].partitions.iter().map(|p| p.leader_id.0).collect();
    assert_eq!(leaders, [0, 1, 2]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn each_node_listener_reaches_its_own_broker() {
    let cluster = FakeCluster::start(3, &[("orders", 3)]).await;
    let proxy = cluster.proxy().await;

    for node in proxy.handle().nodes() {
        let addr = SocketAddr::from(([127, 0, 0, 1], node.proxy_port));
        let mut client = TestClient::connect(addr).await.unwrap();
        let p = node.node_id;
        let value = format!("via-{p}");
        let resp = produce(&mut client, &[("orders", p, &[value.as_str()])]).await.unwrap();
        assert_eq!(produce_codes(&resp), [("orders".into(), p, 0)]);
        assert_eq!(cluster.values(p as usize, "orders", p).await, [value]);
    }
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn find_coordinator_points_at_proxy() {
    let cluster = FakeCluster::start(2, &[]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let req = FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("group"));
    let resp: FindCoordinatorResponse = client.call(ApiKey::FindCoordinator, 2, &req).await.unwrap();

    let node0 = &proxy.handle().nodes()[0];
    assert_eq!((resp.node_id.0, resp.host.as_str(), resp.port), (0, "127.0.0.1", node0.proxy_port as i32));
    proxy.shutdown().await;
}
