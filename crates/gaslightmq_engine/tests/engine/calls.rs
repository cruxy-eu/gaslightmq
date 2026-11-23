//! A call travels from the socket of the client through the pipeline to the backend, and its answer travels back.

use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::{ProduceResult, Producer, ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_listener::prelude::*;
use gaslightmq_wire::{Frame, Partition, Record};

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn a_request_goes_to_the_backend_and_the_truth_comes_back() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);

    let written = producer
        .send(orders.as_str(), partition, &[Record::new("placed")])
        .await;

    let truth = ProduceResult {
        partition,
        offset: ScriptedBackend::written_at(),
    };
    assert_eq!(written, Ok(truth));
    // The backend took the request under the call of the pipeline: the first call of the first connection.
    let first_call_of_the_first_connection: CallId = "#1.1".parse()?;
    assert_eq!(
        proxy.backend().calls_taken_so_far(),
        [first_call_of_the_first_connection]
    );
    let record = proxy.next_record().await?;
    assert_eq!(record.id(), first_call_of_the_first_connection);
    assert_eq!(record.verdict().to_string(), "pass");
    assert_eq!(record.explanation().failure().map(Failure::message), None);
    Ok(())
}

#[tokio::test]
async fn the_requests_of_a_connection_reach_the_backend_in_order_before_any_answer() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;
    proxy.backend().hold_answers();

    let first = client.send_metadata().await?;
    let second = client.send_heartbeat(&group).await?;
    let third = client.send_metadata().await?;

    // The engine does not wait for one answer before it starts the next call.
    let calls_of_the_connection: [CallId; 3] = ["#1.1".parse()?, "#1.2".parse()?, "#1.3".parse()?];
    assert_eq!(proxy.backend().calls_taken(3).await?, calls_of_the_connection);
    assert!(proxy.leaves_unanswered(client.receive_response(&first)).await);

    // The answers leave in the order of the requests.
    proxy.backend().release_answers();
    EngineUnderTest::soon(client.receive_response(&first)).await??;
    EngineUnderTest::soon(client.receive_response(&second)).await??;
    EngineUnderTest::soon(client.receive_response(&third)).await??;
    Ok(())
}

#[tokio::test]
async fn a_request_without_a_response_goes_to_the_backend_and_nothing_comes_back() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let partition: TopicPartition = "orders.eu/3".parse()?;
    let group: GroupId = "analytics-etl".parse()?;

    // The client asks for no acknowledgement of its records, so the cluster sends no response.
    client.send_unacknowledged(&partition, &[Record::new("placed")]).await?;
    let heartbeat = client.send_heartbeat(&group).await?;

    // The first response on the connection answers the second request.
    EngineUnderTest::soon(client.receive_response(&heartbeat)).await??;
    let produce_and_heartbeat: [CallId; 2] = ["#1.1".parse()?, "#1.2".parse()?];
    assert_eq!(proxy.backend().calls_taken_so_far(), produce_and_heartbeat);
    assert_eq!(proxy.next_record().await?.delivered(), &Delivered::Nothing);
    Ok(())
}

#[tokio::test]
async fn every_connection_is_a_session_of_its_own() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let first = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let second = RawKafkaClient::new(proxy.address_of("broker 2".parse()?)?);
    let group: GroupId = "analytics-etl".parse()?;

    first.heartbeat(group.as_str()).await?;
    second.heartbeat(group.as_str()).await?;
    first.heartbeat(group.as_str()).await?;

    // The endpoints of one proxy number their connections together, and every connection numbers its own calls.
    let first_call_of_the_first: CallId = "#1.1".parse()?;
    let first_call_of_the_second: CallId = "#2.1".parse()?;
    let second_call_of_the_first: CallId = "#1.2".parse()?;
    assert_eq!(
        proxy.backend().calls_taken_so_far(),
        [
            first_call_of_the_first,
            first_call_of_the_second,
            second_call_of_the_first
        ]
    );
    Ok(())
}

#[tokio::test]
async fn the_backend_hears_of_every_connection_that_it_got_a_request_of() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let group: GroupId = "analytics-etl".parse()?;
    // The first connection never sends a request, and the two after it send one each.
    let silent = RawKafkaClient::connect(proxy.address_of(Endpoint::Bootstrap)?).await?;
    let second = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let third = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    assert_eq!(second.heartbeat(group.as_str()).await, Ok(()));

    drop(silent);
    drop(second);
    assert_eq!(third.heartbeat(group.as_str()).await, Ok(()));
    drop(third);

    // The backend never held anything for the connection that sent nothing, so it hears of the other two alone.
    let second_and_third: [ConnectionNumber; 2] = ["#2".parse()?, "#3".parse()?];
    assert_eq!(proxy.backend().connections_closed(2).await?, second_and_third);
    Ok(())
}

#[tokio::test]
async fn a_client_that_leaves_ends_its_calls_in_flight_without_an_answer() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    proxy.backend().hold_answers();
    client.send_metadata().await?;
    proxy.backend().calls_taken(1).await?;

    drop(client);

    // The record says that the call got no answer, and the backend lets go of the connection.
    let its_connection: ConnectionNumber = "#1".parse()?;
    let record = proxy.next_record().await?;
    assert_eq!(record.connection(), its_connection);
    assert_eq!(record.delivered(), &Delivered::Nothing);
    assert_eq!(proxy.backend().connections_closed(1).await?, [its_connection]);
    Ok(())
}

#[tokio::test]
async fn bytes_that_are_no_request_close_the_connection() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    proxy.backend().hold_answers();
    client.send_metadata().await?;
    proxy.backend().calls_taken(1).await?;

    // The start of a frame that holds no bytes, which is no request.
    let frame_of_no_bytes = Frame::length_prefix(0);
    client.send_bytes(&frame_of_no_bytes).await?;

    // The proxy sends nothing back, and closes the connection.
    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));
    // The call in flight ends with the session, and has its record.
    assert_eq!(proxy.next_record().await?.delivered(), &Delivered::Nothing);
    let its_connection: ConnectionNumber = "#1".parse()?;
    assert_eq!(proxy.backend().connections_closed(1).await?, [its_connection]);
    Ok(())
}

#[tokio::test]
async fn a_frame_over_the_limit_of_the_engine_closes_the_connection() -> anyhow::Result<()> {
    let limit = FrameSizeLimit::new(64)?;
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.socket_request_max_bytes = limit;
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let one_byte_over_the_limit = u32::try_from(limit.bytes())? + 1;

    // The start of a frame that announces that many bytes, and none of them.
    client
        .send_bytes(&Frame::length_prefix(one_byte_over_the_limit))
        .await?;

    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));
    assert_eq!(proxy.backend().calls_taken_so_far(), Vec::<CallId>::new());
    Ok(())
}
