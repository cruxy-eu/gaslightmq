//! A request, and the response that comes back.

use gaslightmq_kafka_client_api::{ClientError, Produce, Producer, RawKafkaClient};
use gaslightmq_wire::messages::{ApiVersionsRequest, BrokerId, MetadataRequest, MetadataResponse, ProduceRequest};
use gaslightmq_wire::{Api, ApiKey, Partition, Record, ResponseError};

use crate::support::{PlayedBroker, soon};

#[tokio::test]
async fn a_request_goes_out_at_the_version_it_is_given_and_gets_its_response() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);

    let sent = client.send_request(2, &ApiVersionsRequest::default()).await?;
    let mut connection = broker.connection().await?;
    let request = connection.request().await?;
    connection.answer(&request, &PlayedBroker::offered()).await?;
    let response = soon(client.receive_response(&sent)).await??;

    assert_eq!(request.api(), Api::new(ApiKey::ApiVersions, 2));
    assert_eq!(response.api_keys, PlayedBroker::offered().api_keys);
    Ok(())
}

#[tokio::test]
async fn a_second_request_goes_out_before_the_first_is_answered() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let led_by_broker_1 = MetadataResponse::default().with_controller_id(BrokerId(1));

    let first = client.send_request(0, &ApiVersionsRequest::default()).await?;
    let second = client.send_request(1, &MetadataRequest::default()).await?;
    // Both requests are at the broker, on one connection, and neither has an answer yet.
    let mut connection = broker.connection().await?;
    let asked_first = connection.request().await?;
    let asked_second = connection.request().await?;
    connection.answer(&asked_first, &PlayedBroker::offered()).await?;
    connection.answer(&asked_second, &led_by_broker_1).await?;
    let versions = soon(client.receive_response(&first)).await??;
    let metadata = soon(client.receive_response(&second)).await??;

    assert_eq!(asked_first.api().known(), Some(ApiKey::ApiVersions));
    assert_eq!(asked_second.api().known(), Some(ApiKey::Metadata));
    assert_eq!(versions.api_keys, PlayedBroker::offered().api_keys);
    assert_eq!(metadata.controller_id, BrokerId(1));
    Ok(())
}

#[tokio::test]
async fn an_error_code_stays_in_a_response_and_is_the_failure_of_a_send() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let refusal = PlayedBroker::refusing_to_write("orders", ResponseError::NotLeaderOrFollower);
    let placed = [Record::new("placed")];

    // The same answer of the broker, twice: to a Produce request, and to a producer that sends a record.
    let (response, connection) = soon(async {
        tokio::join!(client.produce(ProduceRequest::default()), async {
            let mut connection = broker.connection().await?;
            connection.answer_next(&refusal).await?;
            anyhow::Ok(connection)
        })
    })
    .await?;
    let mut connection = connection?;
    let (sent, answered) = soon(async {
        tokio::join!(
            client.send("orders", Partition::new(0), &placed),
            connection.answer_next(&refusal)
        )
    })
    .await?;
    answered?;

    let refused_partition = &response?.responses[0].partition_responses[0];
    assert_eq!(refused_partition.error_code, ResponseError::NotLeaderOrFollower.code());
    assert_eq!(
        sent,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );
    Ok(())
}
