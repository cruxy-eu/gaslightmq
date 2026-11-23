//! What a call fails with when the client or its connection fails.

use gaslightmq_kafka_client_api::{ApiVersions, ClientError, Producer, RawKafkaClient};
use gaslightmq_wire::messages::{ApiVersionsRequest, ProduceResponse};
use gaslightmq_wire::{Partition, Record};

use crate::support::{PlayedBroker, soon};

/// A version that `ApiVersions` does not have.
const NO_SUCH_VERSION: i16 = 99;

#[tokio::test]
async fn a_broker_that_hangs_up_is_a_lost_connection_and_the_next_request_opens_a_new_one() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let sent = client.send_request(0, &ApiVersionsRequest::default()).await?;
    let mut connection = broker.connection().await?;
    connection.request().await?;
    connection.hang_up();
    let lost = soon(client.receive_response(&sent)).await?;

    let again = client.send_request(0, &ApiVersionsRequest::default()).await?;
    let mut reopened = broker.connection().await?;
    reopened.answer_next(&PlayedBroker::offered()).await?;
    let response = soon(client.receive_response(&again)).await??;

    assert!(matches!(lost, Err(ClientError::ConnectionLost { .. })), "{lost:?}");
    assert_eq!(response.api_keys, PlayedBroker::offered().api_keys);
    Ok(())
}

#[tokio::test]
async fn an_address_that_nobody_listens_on_is_unreachable() -> anyhow::Result<()> {
    let nobody = PlayedBroker::listening().await?.gone()?;
    let client = RawKafkaClient::new(nobody);

    let answer = soon(client.api_versions(ApiVersionsRequest::default())).await?;

    assert!(matches!(answer, Err(ClientError::Unreachable { .. })), "{answer:?}");
    Ok(())
}

#[tokio::test]
async fn a_request_that_does_not_encode_at_its_version_is_unwritable() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let sent = client
        .send_request(NO_SUCH_VERSION, &ApiVersionsRequest::default())
        .await;

    assert!(matches!(sent, Err(ClientError::UnwritableRequest { .. })), "{sent:?}");
    Ok(())
}

#[tokio::test]
async fn the_response_to_another_request_is_an_unreadable_answer() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let first = client.send_request(0, &ApiVersionsRequest::default()).await?;
    client.send_request(0, &ApiVersionsRequest::default()).await?;
    let mut connection = broker.connection().await?;
    connection.request().await?;
    let asked_second = connection.request().await?;
    connection.answer(&asked_second, &PlayedBroker::offered()).await?;
    let answer = soon(client.receive_response(&first)).await?;

    assert_eq!(
        answer,
        Err(ClientError::unreadable_answer(
            "the response to request 2 arrived where the response to request 1 is next"
        ))
    );
    Ok(())
}

#[tokio::test]
async fn bytes_that_are_no_response_are_an_unreadable_answer() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let sent = client.send_request(0, &ApiVersionsRequest::default()).await?;
    let mut connection = broker.connection().await?;
    connection.request().await?;
    connection.send_frame_of_no_bytes().await?;
    let answer = soon(client.receive_response(&sent)).await?;

    assert_eq!(answer, Err(ClientError::unreadable_answer("frame of zero bytes")));
    Ok(())
}

#[tokio::test]
async fn a_response_without_the_partition_of_a_send_is_an_unreadable_answer() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let placed = [Record::new("placed")];

    let (sent, answered) = soon(async {
        tokio::join!(client.send("orders", Partition::new(0), &placed), async {
            let mut connection = broker.connection().await?;
            connection.answer_next(&ProduceResponse::default()).await?;
            // The connection stays open until the client has read the answer.
            anyhow::Ok(connection)
        })
    })
    .await?;
    answered?;

    assert_eq!(sent, Err(ClientError::unreadable_answer("no result for orders-0")));
    Ok(())
}
