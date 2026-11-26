//! The engine keeps the journal of its environment: it numbers what someone records, tells its time,
//! and hands every entry over with its events.

use gaslightmq_call::prelude::*;
use gaslightmq_clock::TimeSpan;
use gaslightmq_engine::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kafka_client_api::{ProtocolConsumer, RawKafkaClient};
use gaslightmq_kernel::prelude::*;

use crate::support::{EngineUnderTest, ScriptedBackend};

#[tokio::test]
async fn an_entry_of_the_journal_leaves_the_engine_between_the_calls_around_it() -> anyhow::Result<()> {
    let mut proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let member = RawKafkaClient::new(proxy.address_of(Endpoint::Bootstrap)?);
    let group: GroupId = "analytics-etl".parse()?;
    let rule: RecordedName = "slow-heartbeats".parse()?;

    // A call, then a change that someone records, then another call.
    member.heartbeat(group.as_str()).await?;
    let before = proxy.next_record().await?;
    let armed = Change::RuleArmed { rule };
    let entry = proxy
        .engine()
        .journal()
        .record(armed, ChangeSurface::Test, Some(Revision::new(2)));
    member.heartbeat(group.as_str()).await?;

    // Whoever keeps every event finds the entry where it happened: after the first call, and before the second.
    assert_eq!(before.id(), CallId::FIRST);
    let EngineEvent::Journaled(journaled) = proxy.next_event().await? else {
        anyhow::bail!("the entry leaves the engine before the record of the call after it");
    };
    assert_eq!(journaled, entry);
    let EngineEvent::CallRecorded(after) = proxy.next_event().await? else {
        anyhow::bail!("the record of the second call follows the entry");
    };
    assert_eq!(after.id(), CallId::FIRST.next());
    Ok(())
}

#[tokio::test]
async fn the_journal_tells_the_time_of_an_entry_on_the_clock_of_the_engine() -> anyhow::Result<()> {
    let proxy = EngineUnderTest::start(ScriptedBackend::with_three_brokers()).await?;
    let journal = proxy.engine().journal();
    let rule: RecordedName = "slow-heartbeats".parse()?;
    let armed_for = TimeSpan::from_secs(30);

    let armed = journal.record(
        Change::RuleArmed { rule: rule.clone() },
        ChangeSurface::Pad,
        Some(Revision::new(2)),
    );
    proxy.advance(armed_for);
    let disarmed = journal.record(
        Change::RuleDisarmed { rule },
        ChangeSurface::Pad,
        Some(Revision::new(3)),
    );

    assert_eq!(disarmed.at - armed.at, armed_for.duration());
    assert_eq!(journal.entries(), [armed, disarmed]);
    Ok(())
}

#[tokio::test]
async fn the_journal_keeps_as_many_entries_in_memory_as_the_configuration_says() -> anyhow::Result<()> {
    let two_entries = RecentJournalSize::new(2)?;
    let mut configuration = EngineUnderTest::configuration()?;
    configuration.recent_journal = two_entries;
    let proxy = EngineUnderTest::start_with(configuration, ScriptedBackend::with_three_brokers()).await?;
    let journal = proxy.engine().journal();
    let fired = || Change::RuleFiredByHand {
        rule: "slow-heartbeats".parse().expect("a rule name"),
    };

    let first = journal.record(fired(), ChangeSurface::Cli, None);
    let second = journal.record(fired(), ChangeSurface::Cli, None);
    let third = journal.record(fired(), ChangeSurface::Cli, None);

    // The first entry left the memory of the engine, and the count went on.
    assert_eq!(first.id, JournalEntryId::FIRST);
    assert_eq!(journal.entries(), [second.clone(), third.clone()]);
    // Whoever read up to the first entry reads on from the second.
    assert_eq!(journal.after(first.id), [second, third]);
    Ok(())
}
