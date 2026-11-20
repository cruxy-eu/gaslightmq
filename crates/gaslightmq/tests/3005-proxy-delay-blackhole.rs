use std::time::{Duration, Instant};

use gaslightmq_testing::proxy::*;
use gaslightmq::proxy_api::{ApiRow, DelayPad, Pad, catalog::api};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, MetadataResponse};

#[tokio::test(flavor = "multi_thread")]
async fn delay_holds_responses_back_in_order() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut row = ApiRow::new(api::METADATA);
    row.delay = DelayPad { enabled: true, probability: 100, ms: 300 };
    proxy.handle().set_row(row);
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let start = Instant::now();
    let slow = client.send(ApiKey::Metadata, METADATA_V, &metadata_request(None)).await.unwrap();
    let fast = client.send(ApiKey::ApiVersions, 0, &ApiVersionsRequest::default()).await.unwrap();

    let (cid, _): (i32, MetadataResponse) = client.recv(ApiKey::Metadata, METADATA_V).await.unwrap();
    assert_eq!(cid, slow);
    assert!(start.elapsed() >= Duration::from_millis(300));
    let (cid, _): (i32, ApiVersionsResponse) = client.recv(ApiKey::ApiVersions, 0).await.unwrap();
    assert_eq!(cid, fast);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blackhole_stalls_the_connection_but_not_new_ones() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut row = ApiRow::new(api::PRODUCE);
    row.blackhole = Pad { enabled: true, probability: 100 };
    proxy.handle().set_row(row);
    let mut stuck = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    stuck.send(ApiKey::Produce, PRODUCE_V, &produce_request(-1, &[("orders", 0, &["void"])])).await.unwrap();
    stuck.send(ApiKey::ApiVersions, 0, &ApiVersionsRequest::default()).await.unwrap();
    let got = stuck.recv_within::<ApiVersionsResponse>(ApiKey::ApiVersions, 0, Duration::from_millis(500)).await.unwrap();
    assert!(got.is_none(), "a blackholed connection must stay silent");

    proxy.handle().clear_rules();
    // The stuck connection stays stuck, like a hung broker; a fresh one works.
    let mut fresh = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();
    let resp = produce(&mut fresh, &[("orders", 0, &["kept"])]).await.unwrap();
    assert_eq!(produce_codes(&resp), [("orders".into(), 0, 0)]);
    assert_eq!(cluster.values(0, "orders", 0).await, ["kept"]);
    proxy.shutdown().await;
}
