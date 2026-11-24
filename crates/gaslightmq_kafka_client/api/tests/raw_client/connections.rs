//! A connection that opens before its first request, and what a client does on one that no Kafka client does.

use std::pin::pin;

use futures::poll;
use gaslightmq_kafka_client_api::{ClientError, RawKafkaClient};
use gaslightmq_wire::ApiKey;
use gaslightmq_wire::messages::{BrokerId, MetadataResponse};

use crate::support::{PlayedBroker, soon};

/// What a browser sends to the port of a broker.
const NO_REQUEST: &[u8] = b"GET / HTTP/1.1\r\n\r\n";

#[tokio::test]
async fn a_client_that_connects_has_its_connection_before_it_sends_a_request() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;

    let client = RawKafkaClient::connect(broker.address()?).await?;

    // The broker has the connection, and the first request of the client arrives on it.
    let mut connection = broker.connection().await?;
    client.send_metadata().await?;
    let asked = connection.request().await?;
    assert_eq!(asked.api().known(), Some(ApiKey::Metadata));
    Ok(())
}

#[tokio::test]
async fn a_client_does_not_connect_to_an_address_that_nobody_listens_on() -> anyhow::Result<()> {
    let nobody = PlayedBroker::listening().await?.gone()?;

    let failure = soon(RawKafkaClient::connect(nobody)).await?.err();

    assert!(matches!(failure, Some(ClientError::Unreachable { .. })), "{failure:?}");
    Ok(())
}

#[tokio::test]
async fn a_client_waits_for_the_end_of_its_connection_until_the_broker_hangs_up() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::connect(broker.address()?).await?;
    let connection = broker.connection().await?;

    // The connection is open and nothing arrives on it, so the client still waits.
    let mut closed = pin!(client.closed());
    assert!(poll!(&mut closed).is_pending());
    connection.hang_up();
    let closed = soon(closed).await?;

    assert_eq!(closed, Ok(()));
    // The next request opens a new connection.
    client.send_metadata().await?;
    let asked = broker.connection().await?.request().await?;
    assert_eq!(asked.api().known(), Some(ApiKey::Metadata));
    Ok(())
}

#[tokio::test]
async fn a_response_that_arrives_where_the_connection_should_end_is_unreadable_and_stays_for_its_request()
-> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let led_by_broker_1 = MetadataResponse::default().with_controller_id(BrokerId(1));

    let sent = client.send_metadata().await?;
    let mut connection = broker.connection().await?;
    connection.answer_next(&led_by_broker_1).await?;
    let closed = soon(client.closed()).await?;
    // The connection is still open, and the response is still there for the request it answers.
    let metadata = soon(client.receive_response(&sent)).await??;

    assert!(
        matches!(closed, Err(ClientError::UnreadableAnswer { .. })),
        "{closed:?}"
    );
    assert_eq!(metadata.controller_id, BrokerId(1));
    Ok(())
}

#[tokio::test]
async fn a_client_without_a_connection_has_no_end_to_wait_for() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let closed = soon(client.closed()).await?;

    assert_eq!(closed, Ok(()));
    Ok(())
}

#[tokio::test]
async fn bytes_that_are_no_request_go_out_as_they_are() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    client.send_bytes(NO_REQUEST).await?;
    let mut connection = broker.connection().await?;
    let arrived = connection.bytes(NO_REQUEST.len()).await?;

    assert_eq!(arrived, NO_REQUEST);
    // The client counted no request, so it sees the broker hang up on those bytes without a response.
    connection.hang_up();
    assert_eq!(soon(client.closed()).await?, Ok(()));
    Ok(())
}
