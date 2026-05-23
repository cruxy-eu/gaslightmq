use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{FindCoordinator, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::protocol::StrBytes;

#[tokio::test]
async fn find_coordinator_returns_self() {
    let harness = Harness::start(
        BrokerBuilder::new().topic("test", BlackHole::new(1)),
        RawKafkaClient::new,
    )
    .await;

    let request =
        FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("test-group"));
    let response = harness.client.find_coordinator(request).await;

    assert_eq!(response.error_code, 0);
    assert_eq!(response.node_id.0, 0);
    assert_eq!(response.port, harness.broker.addr().port() as i32);

    harness.shutdown().await;
}
