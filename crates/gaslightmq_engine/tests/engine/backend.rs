//! What a backend answers decides how a call ends, and a backend without an answer ends the session.

use gaslightmq_backend::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::ProtocolConsumer;
use gaslightmq_kernel::prelude::*;

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn a_failure_of_the_backend_ends_the_session() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut client = proxy.socket_on("broker 2".parse()?).await?;
    proxy.backend().fail_calls_with(BackendFailure::ConnectionLost {
        reason: "the broker closed it".to_owned(),
    });

    client.send_metadata().await?;

    // The client gets no response that the cluster did not send: it finds its connection closed.
    client.wait_until_closed().await?;
    let record = proxy.next_record().await?;
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some(
            "the backend has no answer for call #1.1: \
             the connection to the cluster ended before the answer arrived: the broker closed it"
        )
    );
    // The backend hears that the connection is gone.
    let its_connection: ConnectionNumber = "#1".parse()?;
    assert_eq!(proxy.backend().connections_closed(1).await?, [its_connection]);
    Ok(())
}

#[tokio::test]
async fn a_failure_ends_every_call_of_the_session_and_no_other_session() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let group: GroupId = "analytics-etl".parse()?;
    let other = proxy.client_on(Endpoint::Bootstrap)?;
    assert_eq!(other.heartbeat(group.as_str()).await, Ok(()));
    proxy.next_record().await?;
    let mut failing = proxy.socket_on(Endpoint::Bootstrap).await?;
    proxy.backend().hold_answers();
    failing.send_metadata().await?;
    failing.send_heartbeat(&group).await?;
    proxy.backend().calls_taken(3).await?;

    proxy.backend().fail_calls_with(BackendFailure::Unreachable {
        reason: "kafka-1.staging:9092 refused the connection".to_owned(),
    });
    proxy.backend().release_answers();

    // Both calls of the session end with it, and the record of each says why.
    failing.wait_until_closed().await?;
    let failing_connection: ConnectionNumber = "#2".parse()?;
    for _ in 0..2 {
        let record = proxy.next_record().await?;
        assert_eq!(record.connection(), failing_connection);
        assert!(record.explanation().failure().is_some(), "{record:?}");
    }
    // The session next to it goes on, once the backend reaches its cluster again.
    proxy.backend().answer_truthfully();
    assert_eq!(other.heartbeat(group.as_str()).await, Ok(()));
    Ok(())
}

#[tokio::test]
async fn an_answer_that_is_no_response_closes_the_connection() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut client = proxy.socket_on(Endpoint::Bootstrap).await?;
    let group: GroupId = "analytics-etl".parse()?;
    proxy.backend().answer_with_what_is_no_response();

    client.send_heartbeat(&group).await?;

    // The backend did not go away. The proxy has no answer to send, so it closes the connection itself.
    client.wait_until_closed().await?;
    let record = proxy.next_record().await?;
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);
    assert_eq!(
        record.explanation().failure().map(Failure::message),
        Some("the backend answered with what is no response: response header is cut short at 3 bytes")
    );
    let its_connection: ConnectionNumber = "#1".parse()?;
    assert_eq!(proxy.backend().connections_closed(1).await?, [its_connection]);
    Ok(())
}

#[tokio::test]
async fn a_backend_that_never_answers_keeps_the_call_in_flight_until_the_client_gives_up() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut client = proxy.socket_on(Endpoint::Bootstrap).await?;
    proxy.backend().hold_answers();

    client.send_metadata().await?;
    proxy.backend().calls_taken(1).await?;
    client.receive_nothing().await?;
    drop(client);

    // The engine drops the call, and tells the backend that the connection ended.
    let its_connection: ConnectionNumber = "#1".parse()?;
    assert_eq!(proxy.backend().connections_closed(1).await?, [its_connection]);
    assert_eq!(proxy.next_record().await?.delivered(), &Delivered::Nothing);
    Ok(())
}
