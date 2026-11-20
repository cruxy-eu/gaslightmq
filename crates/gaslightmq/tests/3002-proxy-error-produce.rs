use std::time::Duration;

use gaslightmq_testing::proxy::*;
use gaslightmq::proxy_api::{ApiRow, ErrorPad, catalog::api};
use kafka_protocol::messages::{ApiKey, MetadataResponse};

fn produce_error(code: i16, topic: Option<&str>) -> ApiRow {
    let mut row = ApiRow::new(api::PRODUCE);
    row.error = ErrorPad { enabled: true, probability: 100, code };
    row.topic = topic.map(str::to_string);
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn every_partition_fails_and_nothing_is_written() {
    let cluster = FakeCluster::start(1, &[("orders", 2)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(produce_error(6, None));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp = produce(&mut client, &[("orders", 0, &["a"]), ("orders", 1, &["b"])]).await.unwrap();

    assert_eq!(produce_codes(&resp), [("orders".into(), 0, 6), ("orders".into(), 1, 6)]);
    assert!(resp.responses[0].partition_responses.iter().all(|p| p.base_offset == -1));
    assert!(cluster.values(0, "orders", 0).await.is_empty());
    assert!(cluster.values(0, "orders", 1).await.is_empty());
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clearing_the_row_restores_traffic() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(produce_error(19, None));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let failed = produce(&mut client, &[("orders", 0, &["lost"])]).await.unwrap();
    assert_eq!(produce_codes(&failed), [("orders".into(), 0, 19)]);

    proxy.handle().clear_rules();
    let ok = produce(&mut client, &[("orders", 0, &["kept"])]).await.unwrap();
    assert_eq!(produce_codes(&ok), [("orders".into(), 0, 0)]);
    assert_eq!(cluster.values(0, "orders", 0).await, ["kept"]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acks_zero_with_error_is_dropped_silently() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(produce_error(6, None));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    client.send(ApiKey::Produce, PRODUCE_V, &produce_request(0, &[("orders", 0, &["x"])])).await.unwrap();
    let next = client.send(ApiKey::Metadata, METADATA_V, &metadata_request(None)).await.unwrap();
    let (cid, _): (i32, MetadataResponse) = client
        .recv_within(ApiKey::Metadata, METADATA_V, Duration::from_secs(5))
        .await
        .unwrap()
        .expect("metadata response");

    assert_eq!(cid, next);
    assert!(cluster.values(0, "orders", 0).await.is_empty());
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_scope_fails_whole_request_touching_the_topic() {
    let cluster = FakeCluster::start(1, &[("orders", 1), ("audit", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(produce_error(29, Some("audit")));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    // The proxy never rewrites a request, so a batch touching the scoped
    // topic fails as a whole and nothing is written.
    let resp = produce(&mut client, &[("orders", 0, &["o"]), ("audit", 0, &["a"])]).await.unwrap();
    assert_eq!(produce_codes(&resp), [("audit".into(), 0, 29), ("orders".into(), 0, 29)]);
    assert!(cluster.values(0, "orders", 0).await.is_empty());
    assert!(cluster.values(0, "audit", 0).await.is_empty());

    // Requests that don't touch the topic are left alone entirely.
    let resp = produce(&mut client, &[("orders", 0, &["o2"])]).await.unwrap();
    assert_eq!(produce_codes(&resp), [("orders".into(), 0, 0)]);
    assert_eq!(cluster.values(0, "orders", 0).await, ["o2"]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn probability_is_honoured_with_a_seed() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy_with(|c| c.seed(42)).await;
    let mut row = produce_error(6, None);
    row.error.probability = 50;
    proxy.handle().set_row(row);
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let mut failed = 0;
    for _ in 0..200 {
        let resp = produce(&mut client, &[("orders", 0, &["v"])]).await.unwrap();
        if produce_codes(&resp)[0].2 != 0 {
            failed += 1;
        }
    }
    assert!((60..140).contains(&failed), "{failed} of 200 failed");
    assert_eq!(cluster.values(0, "orders", 0).await.len(), 200 - failed);
    proxy.shutdown().await;
}
