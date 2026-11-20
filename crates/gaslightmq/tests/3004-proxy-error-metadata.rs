use gaslightmq_testing::proxy::*;
use gaslightmq::proxy_api::{ApiRow, ErrorPad, catalog::api};

fn metadata_error(code: i16, topic: Option<&str>) -> ApiRow {
    let mut row = ApiRow::new(api::METADATA);
    row.error = ErrorPad { enabled: true, probability: 100, code };
    row.topic = topic.map(str::to_string);
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn listed_topics_are_answered_by_the_proxy() {
    let cluster = FakeCluster::start(2, &[("orders", 2)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();
    // Learn the cluster id through a normal request first.
    metadata(&mut client, None).await.unwrap();
    proxy.handle().set_row(metadata_error(5, None));

    let resp = metadata(&mut client, Some(&["orders"])).await.unwrap();

    assert_eq!(resp.topics.len(), 1);
    assert_eq!((resp.topics[0].error_code, resp.topics[0].partitions.len()), (5, 0));
    let proxy_ports: Vec<i32> = proxy.handle().nodes().iter().map(|n| n.proxy_port as i32).collect();
    assert_eq!(resp.brokers.iter().map(|b| b.port).collect::<Vec<_>>(), proxy_ports);
    assert_eq!(resp.cluster_id.as_deref(), Some("gaslightmq"));
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn all_topics_request_fails_only_scoped_topic() {
    let cluster = FakeCluster::start(1, &[("orders", 3), ("audit", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(metadata_error(3, Some("orders")));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp = metadata(&mut client, None).await.unwrap();

    let mut topics: Vec<(String, i16, usize)> = resp
        .topics
        .iter()
        .map(|t| (t.name.as_ref().unwrap().to_string(), t.error_code, t.partitions.len()))
        .collect();
    topics.sort();
    assert_eq!(topics, [("audit".into(), 0, 1), ("orders".into(), 3, 0)]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn listed_topics_partly_in_scope_are_forwarded_and_marked() {
    let cluster = FakeCluster::start(1, &[("orders", 3), ("audit", 1)]).await;
    let proxy = cluster.proxy().await;
    proxy.handle().set_row(metadata_error(3, Some("orders")));
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp = metadata(&mut client, Some(&["orders", "audit"])).await.unwrap();

    let mut topics: Vec<(String, i16, usize)> = resp
        .topics
        .iter()
        .map(|t| (t.name.as_ref().unwrap().to_string(), t.error_code, t.partitions.len()))
        .collect();
    topics.sort();
    assert_eq!(topics, [("audit".into(), 0, 1), ("orders".into(), 3, 0)]);
    proxy.shutdown().await;
}
