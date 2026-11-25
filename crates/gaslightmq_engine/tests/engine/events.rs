//! The engine hands over what happens to every session, the record of every call, and every incident that a rule
//! fired, in the order in which they happened.

use anyhow::Context;
use gaslightmq_call::prelude::*;
use gaslightmq_engine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kafka_client_api::{ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn an_incident_that_a_rule_fires_leaves_the_engine_before_the_record_of_its_call() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let payments: GroupId = "payments".parse()?;
    proxy.arm(&Rule::new(
        "rebalance-payments".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        IncidentDefinition::Rebalance {
            group: Some(payments.clone()),
        },
    ))?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);

    member.heartbeat(payments.as_str()).await?;

    // The session opens, and its first request says who its client is, before anything of the call leaves.
    let the_member = ConnectionNumber::new(1);
    let EngineEvent::Session(SessionEvent::Opened(session)) = proxy.next_event().await? else {
        anyhow::bail!("the session opens before anything else happens to it");
    };
    assert_eq!(session.connection(), the_member);
    let identified = proxy.next_event().await?;
    assert!(
        matches!(&identified, EngineEvent::Session(SessionEvent::Identified { connection, .. }) if *connection == the_member),
        "{identified:?}"
    );
    let EngineEvent::IncidentFired(incident) = proxy.next_event().await? else {
        anyhow::bail!("the rule fired an incident before the record of its call left the session");
    };
    assert_eq!(incident, Incident::Rebalance { group: payments });
    // The call passes through unchanged, and its record follows.
    let EngineEvent::CallRecorded(record) = proxy.next_event().await? else {
        anyhow::bail!("the record of the call follows the incident");
    };
    let the_heartbeat = CallId::FIRST;
    assert_eq!(record.id(), the_heartbeat);
    assert_eq!(record.verdict().to_string(), "pass");
    // The engine hands the incident over, and applies nothing: the backend only saw the call.
    assert_eq!(proxy.backend().calls_taken_so_far(), [the_heartbeat]);
    Ok(())
}

#[tokio::test]
async fn every_listener_receives_every_event_of_every_session_in_one_order() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut watching = proxy.engine().live_events();
    let mut keeping = proxy.engine().every_event();
    let group: GroupId = "analytics-etl".parse()?;
    // Two members, one on each endpoint. Both stay connected, so neither session closes during the test.
    let members = [
        RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?),
        RawKafkaClient::new(proxy.address_of("broker 1".parse()?)?),
    ];

    for member in &members {
        member.heartbeat(group.as_str()).await?;
    }

    // Each session opens, says who its client is, and records its call, before the next member sends.
    let the_call_of_each_connection = [
        (CallId::FIRST, ConnectionNumber::new(1)),
        (CallId::FIRST.next(), ConnectionNumber::new(2)),
    ];
    let mut watched = Vec::new();
    let mut kept = Vec::new();
    for _ in 0..the_call_of_each_connection.len() * 3 {
        watched.push(seen(&watching.next().await?));
        kept.push(seen(&keeping.next().await.context("the engine runs")?));
    }
    let expected: Vec<String> = the_call_of_each_connection
        .iter()
        .flat_map(|(call, connection)| {
            [
                format!("{connection} opened"),
                format!("{connection} identified"),
                format!("{connection} recorded {call}"),
            ]
        })
        .collect();
    // Whoever watches and whoever keeps see the same events, in the same order.
    assert_eq!(watched, expected);
    assert_eq!(kept, expected);
    Ok(())
}

/// What an event says, in a few words: `#1 opened`, `#1 identified`, `#1 recorded 1`.
fn seen(event: &EngineEvent) -> String {
    match event {
        EngineEvent::Session(SessionEvent::Opened(session)) => format!("{} opened", session.connection()),
        EngineEvent::Session(SessionEvent::Identified { connection, .. }) => format!("{connection} identified"),
        EngineEvent::Session(SessionEvent::SoftwareNamed { connection, .. }) => {
            format!("{connection} named its software")
        }
        EngineEvent::Session(SessionEvent::Closed { connection, by }) => format!("{connection} closed by {by}"),
        EngineEvent::CallRecorded(record) => format!("{} recorded {}", record.connection(), record.id()),
        EngineEvent::IncidentFired(incident) => format!("incident {incident:?}"),
        other => format!("{other:?}"),
    }
}

#[tokio::test]
async fn a_slow_recorder_gets_every_event_while_a_live_listener_misses_the_oldest() -> anyhow::Result<()> {
    let backlog = EventBacklog::new(2)?;
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.event_backlog = backlog;
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let mut watching = proxy.engine().live_events();
    let mut recording = proxy.engine().every_event();
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;

    // Neither listener takes an event while the session records five calls, which no listener holds up.
    let five_calls_of_one_connection: Vec<CallId> =
        std::iter::successors(Some(CallId::FIRST), |call| Some(call.next()))
            .take(5)
            .collect();
    for _ in &five_calls_of_one_connection {
        member.heartbeat(group.as_str()).await?;
    }

    // The session opened and said who its client is, and recorded five calls: seven events.
    // The live listener is five events behind its backlog of two: it learns that, and goes on with the newest.
    assert_eq!(watching.next().await.err(), Some(LiveEventsError::Missed { events: 5 }));
    let the_newest_two = &five_calls_of_one_connection[3..];
    for call in the_newest_two {
        let EngineEvent::CallRecorded(record) = watching.next().await? else {
            anyhow::bail!("every event of this session is the record of a call");
        };
        assert_eq!(record.id(), *call);
    }
    // The recorder is as slow, and misses nothing: the engine kept every event for it.
    let opened = recording.next().await;
    assert!(
        matches!(opened, Some(EngineEvent::Session(SessionEvent::Opened(_)))),
        "{opened:?}"
    );
    let identified = recording.next().await;
    assert!(
        matches!(identified, Some(EngineEvent::Session(SessionEvent::Identified { .. }))),
        "{identified:?}"
    );
    for call in five_calls_of_one_connection {
        let Some(EngineEvent::CallRecorded(record)) = recording.next().await else {
            anyhow::bail!("every event of this session is the record of a call");
        };
        assert_eq!(record.id(), call);
    }
    Ok(())
}
