//! A request, and the response that comes back.

use anyhow::Context;
use gaslightmq_kafka_client_api::{ClientError, Produce, Producer, RawClientConfiguration, RawKafkaClient};
use gaslightmq_kernel::{ClientId, GroupId, TopicPartition};
use gaslightmq_wire::messages::{
    ApiVersionsRequest, BrokerId, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse,
    ProduceRequest,
};
use gaslightmq_wire::{Api, ApiKey, Partition, Record, RecordBatch, ResponseError};

use crate::support::{PlayedBroker, soon};

/// The `acks` of a Produce that the broker does not answer.
const NO_ACKS: i16 = 0;

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

#[tokio::test]
async fn a_heartbeat_and_a_metadata_request_go_out_without_waiting_for_a_response() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let group: GroupId = "analytics-etl".parse()?;
    let led_by_broker_1 = MetadataResponse::default().with_controller_id(BrokerId(1));

    let heartbeat = client.send_heartbeat(&group).await?;
    let metadata = client.send_metadata().await?;
    // Both requests are at the broker, on one connection, and neither has an answer yet.
    let mut connection = broker.connection().await?;
    let asked_first = connection.request().await?;
    let asked_second = connection.request().await?;
    connection.answer(&asked_first, &HeartbeatResponse::default()).await?;
    connection.answer(&asked_second, &led_by_broker_1).await?;
    let beat = soon(client.receive_response(&heartbeat)).await?;
    let metadata = soon(client.receive_response(&metadata)).await??;

    let asked_first: HeartbeatRequest = asked_first.body()?;
    assert_eq!(asked_first.group_id.as_str(), group.as_str());
    assert_eq!(asked_second.api().known(), Some(ApiKey::Metadata));
    assert_eq!(beat, Ok(HeartbeatResponse::default()));
    assert_eq!(metadata.controller_id, BrokerId(1));
    Ok(())
}

#[tokio::test]
async fn an_unacknowledged_send_asks_for_no_response_and_the_next_response_answers_the_next_request()
-> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let client = RawKafkaClient::new(broker.address()?);
    let partition: TopicPartition = "orders.eu/3".parse()?;
    let group: GroupId = "analytics-etl".parse()?;
    let placed = [Record::new("placed")];

    client.send_unacknowledged(&partition, &placed).await?;
    let heartbeat = client.send_heartbeat(&group).await?;
    let mut connection = broker.connection().await?;
    let asked_first = connection.request().await?;
    // The broker sends no response to the Produce, and answers the Heartbeat behind it.
    connection.answer_next(&HeartbeatResponse::default()).await?;
    let beat = soon(client.receive_response(&heartbeat)).await?;

    let produce: ProduceRequest = asked_first.body()?;
    let written = &produce.topic_data[0].partition_data[0];
    let batches = RecordBatch::decode_all(written.records.as_ref().context("the Produce carries records")?)?;
    assert_eq!(produce.acks, NO_ACKS);
    assert_eq!(produce.topic_data[0].name.as_str(), partition.topic().as_str());
    assert_eq!(written.index, i32::from(partition.partition()));
    // The broker reads the values the client sent. A record that was decoded also has an offset and a timestamp.
    let values_sent: Vec<_> = placed.iter().map(|record| &record.value).collect();
    let values_read: Vec<_> = batches[0].records.iter().map(|record| &record.value).collect();
    assert_eq!(values_read, values_sent);
    assert_eq!(beat, Ok(HeartbeatResponse::default()));
    Ok(())
}

#[tokio::test]
async fn every_request_carries_the_client_id_of_the_configuration() -> anyhow::Result<()> {
    let broker = PlayedBroker::listening().await?;
    let order_ingest: ClientId = "order-ingest".parse()?;
    let mut configuration = RawClientConfiguration::new(broker.address()?);
    configuration.client_id = order_ingest.clone();
    let named = RawKafkaClient::new(configuration);
    let unnamed = RawKafkaClient::new(broker.address()?);

    named.send_metadata().await?;
    let of_the_named = broker.connection().await?.request().await?;
    unnamed.send_metadata().await?;
    let of_the_unnamed = broker.connection().await?.request().await?;

    assert_eq!(of_the_named.client_id(), Some(order_ingest.as_str()));
    // A client that is configured with none sends the empty client ID, which is Kafka's default.
    assert_eq!(of_the_unnamed.client_id(), Some(""));
    Ok(())
}
