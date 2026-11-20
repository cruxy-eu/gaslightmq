use gaslightmq_testing::proxy::*;
use gaslightmq::proxy_api::catalog::api;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse};

#[tokio::test(flavor = "multi_thread")]
async fn api_versions_pass_through_with_caps() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp: ApiVersionsResponse = client.call(ApiKey::ApiVersions, 3, &ApiVersionsRequest::default()).await.unwrap();

    assert_eq!(resp.error_code, 0);
    let produce = resp.api_keys.iter().find(|k| k.api_key == api::PRODUCE).unwrap();
    assert!(produce.max_version <= 9);
    assert!(resp.api_keys.iter().any(|k| k.api_key == api::METADATA));
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_and_fetch_round_trip() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp = produce(&mut client, &[("orders", 0, &["a", "b"])]).await.unwrap();
    assert_eq!(produce_codes(&resp), [("orders".into(), 0, 0)]);

    let values = fetch_values(&mut client, "orders", 0).await.unwrap();
    assert_eq!(values, ["a", "b"]);
    assert_eq!(cluster.values(0, "orders", 0).await, ["a", "b"]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acks_zero_produce_gets_no_response_and_keeps_order() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    client.send(ApiKey::Produce, PRODUCE_V, &produce_request(0, &[("orders", 0, &["fire-and-forget"])])).await.unwrap();
    // The next response on the connection belongs to the next request.
    let resp = metadata(&mut client, Some(&["orders"])).await.unwrap();
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(cluster.values(0, "orders", 0).await, ["fire-and-forget"]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_requests_are_answered_in_order() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let mut sent = Vec::new();
    for _ in 0..10 {
        sent.push(client.send(ApiKey::Metadata, METADATA_V, &metadata_request(None)).await.unwrap());
    }
    for cid in sent {
        let (got, _): (i32, kafka_protocol::messages::MetadataResponse) =
            client.recv(ApiKey::Metadata, METADATA_V).await.unwrap();
        assert_eq!(got, cid);
    }
    proxy.shutdown().await;
}
