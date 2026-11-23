//! The engine hands over the record of every call, and every incident that a rule fired.

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
        "Rebalance payments".parse()?,
        Conditions::from(Condition::is(ApiKey::HEARTBEAT)),
        IncidentDefinition::Rebalance {
            group: Some(payments.clone()),
        },
    ))?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);

    member.heartbeat(payments.as_str()).await?;

    let EngineEvent::IncidentFired(incident) = proxy.next_event().await? else {
        anyhow::bail!("the rule fired an incident before anything else left the session");
    };
    assert_eq!(incident, Incident::Rebalance { group: payments });
    // The call passes through unchanged, and its record follows.
    let EngineEvent::CallRecorded(record) = proxy.next_event().await? else {
        anyhow::bail!("the record of the call follows the incident");
    };
    let the_heartbeat: CallId = "#1.1".parse()?;
    assert_eq!(record.id(), the_heartbeat);
    assert_eq!(record.verdict().to_string(), "pass");
    // The engine hands the incident over, and applies nothing: the backend only saw the call.
    assert_eq!(proxy.backend().calls_taken_so_far(), [the_heartbeat]);
    Ok(())
}

#[tokio::test]
async fn every_listener_receives_the_records_of_every_session() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let mut watching = proxy.engine().live_events();
    let mut keeping = proxy.engine().every_event();
    let group: GroupId = "analytics-etl".parse()?;

    for endpoint in [Endpoint::Bootstrap, "broker 1".parse()?] {
        RawKafkaClient::new(proxy.address_of(endpoint)?)
            .heartbeat(group.as_str())
            .await?;
    }

    // Whoever watches and whoever keeps see the same records, in the same order.
    let first_call_of_each_connection = ["#1.1", "#2.1"];
    for call in first_call_of_each_connection {
        let EngineEvent::CallRecorded(record) = watching.next().await? else {
            anyhow::bail!("every event of these sessions is the record of a call");
        };
        assert_eq!(record.id(), call.parse::<CallId>()?);
    }
    for call in first_call_of_each_connection {
        let Some(EngineEvent::CallRecorded(record)) = keeping.next().await else {
            anyhow::bail!("every event of these sessions is the record of a call");
        };
        assert_eq!(record.id(), call.parse::<CallId>()?);
    }
    Ok(())
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
    let five_calls_of_one_connection = ["#1.1", "#1.2", "#1.3", "#1.4", "#1.5"];
    for _ in five_calls_of_one_connection {
        member.heartbeat(group.as_str()).await?;
    }

    // The live listener is three events behind its backlog of two: it learns that, and goes on with the newest.
    assert_eq!(watching.next().await.err(), Some(LiveEventsError::Missed { events: 3 }));
    let the_newest_two = &five_calls_of_one_connection[3..];
    for call in the_newest_two {
        let EngineEvent::CallRecorded(record) = watching.next().await? else {
            anyhow::bail!("every event of this session is the record of a call");
        };
        assert_eq!(record.id(), call.parse::<CallId>()?);
    }
    // The recorder is as slow, and misses nothing: the engine kept every event for it.
    for call in five_calls_of_one_connection {
        let Some(EngineEvent::CallRecorded(record)) = recording.next().await else {
            anyhow::bail!("every event of this session is the record of a call");
        };
        assert_eq!(record.id(), call.parse::<CallId>()?);
    }
    Ok(())
}
