use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{ApiVersions, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest};

#[tokio::test]
async fn api_versions_returns_all_supported_apis() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", BlackHole::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let request = ApiVersionsRequest::default();
    let response = client.api_versions(request).await;

    assert_eq!(response.error_code, 0);

    let api_keys: Vec<i16> = response.api_keys.iter().map(|k| k.api_key).collect();
    assert!(api_keys.contains(&(ApiKey::Produce as i16)));
    assert!(api_keys.contains(&(ApiKey::Fetch as i16)));
    assert!(api_keys.contains(&(ApiKey::Metadata as i16)));
    assert!(api_keys.contains(&(ApiKey::ListOffsets as i16)));
    assert!(api_keys.contains(&(ApiKey::FindCoordinator as i16)));
    assert!(api_keys.contains(&(ApiKey::JoinGroup as i16)));
    assert!(api_keys.contains(&(ApiKey::SyncGroup as i16)));
    assert!(api_keys.contains(&(ApiKey::Heartbeat as i16)));
    assert!(api_keys.contains(&(ApiKey::ApiVersions as i16)));

    harness.shutdown().await;
}

#[tokio::test]
async fn api_versions_correct_version_ranges() {
    let harness = Harness::with(BrokerBuilder::new().topic("test", BlackHole::new(1)));
    let client: RawKafkaClient = harness.client().await;

    let request = ApiVersionsRequest::default();
    let response = client.api_versions(request).await;

    let produce = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Produce as i16)
        .unwrap();
    assert_eq!(produce.min_version, 3);
    assert_eq!(produce.max_version, 9);

    let fetch = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Fetch as i16)
        .unwrap();
    assert_eq!(fetch.min_version, 4);
    assert_eq!(fetch.max_version, 12);

    let api_versions = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::ApiVersions as i16)
        .unwrap();
    assert_eq!(api_versions.min_version, 0);
    assert_eq!(api_versions.max_version, 3);

    harness.shutdown().await;
}
