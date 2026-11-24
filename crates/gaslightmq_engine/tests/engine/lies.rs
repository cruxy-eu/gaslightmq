//! A client experiences the lies of the armed rules: a delay, a refusal, a silence and a hang-up.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, TimeSpan};
use gaslightmq_field::prelude::*;
use gaslightmq_kafka_client_api::{ClientError, Producer, ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_wire::{Partition, Record, ResponseError};

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn a_delayed_answer_leaves_when_the_clock_reaches_its_instant() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    proxy.arm(&Rule::new(
        "delay-1s".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_secs(1)),
    ))?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);
    let mut written = tokio::spawn(async move {
        let placed = [Record::new("placed")];
        producer.send(orders.as_str(), partition, &placed).await
    });

    // The backend answers at once, and the answer waits for its timer.
    proxy.backend().calls_taken(1).await?;
    proxy.advance(TimeSpan::from_millis(999));
    assert!(proxy.leaves_unanswered(&mut written).await);

    // The clock reaches the instant, and the answer leaves. It is the truth: a delay changes when, and not what.
    proxy.advance(TimeSpan::from_millis(1));
    written.await??;
    let record = proxy.next_record().await?;
    assert_eq!(record.verdict().to_string(), "+1 s");
    assert_eq!(record.timing().client_waited(), Some(Duration::from_secs(1)));
    Ok(())
}

#[tokio::test]
async fn a_delayed_request_reaches_the_backend_when_the_clock_reaches_its_instant() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let late = TimingLie::new()
        .with_delay(TimeSpan::from_secs(45))
        .at(Stage::BeforeForwarding);
    proxy.arm(&Rule::new(
        "late-heartbeat".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        late,
    ))?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;
    let mut beat = tokio::spawn(async move { member.heartbeat(group.as_str()).await });

    // The request arrives, and the backend does not see it.
    assert!(proxy.leaves_unanswered(&mut beat).await);
    assert_eq!(proxy.backend().calls_taken_so_far(), Vec::<CallId>::new());

    // The delay counts from the moment the request arrived.
    proxy.advance(TimeSpan::from_secs(45));
    assert_eq!(beat.await?, Ok(()));
    let the_heartbeat = CallId::FIRST;
    assert_eq!(proxy.backend().calls_taken_so_far(), [the_heartbeat]);
    Ok(())
}

#[tokio::test]
async fn two_delays_end_in_the_order_of_their_instants() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    proxy.arm(&Rule::new(
        "slow-produce".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        TimingLie::new().with_delay(TimeSpan::from_secs(3)),
    ))?;
    proxy.arm(&Rule::new(
        "slow-heartbeat".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        TimingLie::new().with_delay(TimeSpan::from_secs(1)),
    ))?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);
    let group: GroupId = "analytics-etl".parse()?;
    let mut written = tokio::spawn(async move {
        let placed = [Record::new("placed")];
        producer.send(orders.as_str(), partition, &placed).await
    });
    let beat = tokio::spawn(async move { member.heartbeat(group.as_str()).await });
    proxy.backend().calls_taken(2).await?;

    // Every session waits for its own timer on the one clock of the engine.
    proxy.advance(TimeSpan::from_secs(1));
    assert_eq!(beat.await?, Ok(()));
    assert!(proxy.leaves_unanswered(&mut written).await);

    proxy.advance(TimeSpan::from_secs(2));
    written.await??;
    Ok(())
}

#[tokio::test]
async fn a_refused_request_never_reaches_the_backend() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    proxy.arm(&Rule::new(
        "leader-flap".parse()?,
        Conditions::from(Condition::is(ApiKey::PRODUCE)),
        RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
    ))?;
    let producer = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let orders: TopicName = "orders.eu".parse()?;
    let partition = Partition::new(3);

    let written = producer
        .send(orders.as_str(), partition, &[Record::new("placed")])
        .await;

    assert_eq!(
        written,
        Err(ClientError::Refused {
            error: ResponseError::NotLeaderOrFollower
        })
    );
    assert_eq!(proxy.backend().calls_taken_so_far(), Vec::<CallId>::new());
    let record = proxy.next_record().await?;
    assert_eq!(record.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
    Ok(())
}

#[tokio::test]
async fn a_hang_up_closes_the_connection_and_the_client_connects_again() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let disconnect = proxy.arm(&Rule::new(
        "disconnect".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        HangUpLie::new(),
    ))?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;

    // The answer before the hang-up still arrives, and then the connection is gone.
    let metadata = client.send_metadata().await?;
    client.send_heartbeat(&group).await?;
    EngineUnderTest::soon(client.receive_response(&metadata)).await??;
    assert_eq!(EngineUnderTest::soon(client.closed()).await?, Ok(()));

    // The backend never saw the heartbeat, and it hears that the connection ended.
    let the_metadata_call = CallId::FIRST;
    let the_heartbeat_that_hung_up = the_metadata_call.next();
    let the_first_connection: ConnectionNumber = "#1".parse()?;
    assert_eq!(proxy.backend().calls_taken_so_far(), [the_metadata_call]);
    assert_eq!(proxy.backend().connections_closed(1).await?, [the_first_connection]);
    assert_eq!(proxy.next_record().await?.id(), the_metadata_call);
    let record = proxy.next_record().await?;
    assert_eq!(record.id(), the_heartbeat_that_hung_up);
    assert_eq!(record.verdict().to_string(), "hang-up");
    assert_eq!(record.delivered(), &Delivered::ConnectionClosed);

    // The client connects again, and gets a new session. Without the rule, its heartbeat goes through.
    proxy.engine().rules().disarm(disconnect.name())?;
    let again = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    assert_eq!(again.heartbeat(group.as_str()).await, Ok(()));
    // The proxy goes on counting its calls, and the new session is on a new connection.
    let the_heartbeat_of_the_new_session = the_heartbeat_that_hung_up.next();
    assert_eq!(
        proxy.backend().calls_taken_so_far(),
        [the_metadata_call, the_heartbeat_of_the_new_session]
    );
    assert_eq!(
        proxy.backend().connections_of_the_calls_taken_so_far(),
        [the_first_connection, ConnectionNumber::new(2)]
    );
    Ok(())
}

#[tokio::test]
async fn a_silence_never_answers_and_the_connection_stays_open() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    proxy.arm(&Rule::new(
        "blackhole".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        LieDefinition::Silence,
    ))?;
    let client = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;

    let heartbeat = client.send_heartbeat(&group).await?;
    client.send_metadata().await?;

    // The record of the silenced call leaves at once, and so does the one of the request behind it.
    assert_eq!(proxy.next_record().await?.verdict().to_string(), "silence");
    assert_eq!(proxy.next_record().await?.delivered(), &Delivered::Nothing);
    // However long the client waits, nothing comes: the session is stalled.
    proxy.advance(TimeSpan::from_secs(3600));
    assert!(proxy.leaves_unanswered(client.receive_response(&heartbeat)).await);
    assert_eq!(proxy.backend().calls_taken_so_far(), Vec::<CallId>::new());
    Ok(())
}
