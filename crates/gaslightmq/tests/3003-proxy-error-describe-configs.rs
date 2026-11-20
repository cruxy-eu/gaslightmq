use gaslightmq::proxy_api::{ApiRow, ErrorPad, catalog::api};
use gaslightmq_testing::proxy::*;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::{ApiKey, DescribeConfigsRequest, DescribeConfigsResponse};
use kafka_protocol::protocol::StrBytes;

fn request() -> DescribeConfigsRequest {
    DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_static_str("orders"))
            .with_configuration_keys(Some(vec![StrBytes::from_static_str("max.message.bytes")])),
    ])
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_configs_passes_through() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    let resp: DescribeConfigsResponse = client.call(ApiKey::DescribeConfigs, 4, &request()).await.unwrap();

    let result = &resp.results[0];
    assert_eq!(result.error_code, 0);
    let configs: Vec<(&str, Option<&str>)> =
        result.configs.iter().map(|c| (c.name.as_str(), c.value.as_deref())).collect();
    assert_eq!(configs, [("max.message.bytes", Some("1048588"))]);
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_configs_error_is_synthesized() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let proxy = cluster.proxy().await;
    let mut row = ApiRow::new(api::DESCRIBE_CONFIGS);
    row.error = ErrorPad { enabled: true, probability: 100, code: 29 };
    proxy.handle().set_row(row);
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();

    for version in [1, 4] {
        let resp: DescribeConfigsResponse = client.call(ApiKey::DescribeConfigs, version, &request()).await.unwrap();
        assert_eq!(resp.results.len(), 1);
        let result = &resp.results[0];
        assert_eq!((result.error_code, result.resource_type, result.resource_name.as_str()), (29, 2, "orders"));
        // Only the proxy writes this message, so the broker never saw the request.
        assert_eq!(result.error_message.as_deref(), Some("injected by gaslightmq-proxy"));
        assert!(result.configs.is_empty());
    }
    proxy.shutdown().await;
}
