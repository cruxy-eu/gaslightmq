//! A summary says what a stretch of a recording holds, so a reader knows whether to read it.

use std::collections::BTreeSet;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_wire::Request;
use gaslightmq_wire::messages::{FetchRequest, MetadataRequest, ProduceRequest};

#[test]
fn a_summary_holds_the_ranges_the_counts_and_the_values_of_its_stretch() -> anyhow::Result<()> {
    // A Produce to `orders.eu/3` that failed, at 250 ms, and a Fetch from `payments.settled/0`, at 900 ms.
    let eu: TopicPartition = "orders.eu/3".parse()?;
    let produced_at = Instant::ZERO + Duration::from_millis(250);
    let produce = Call::new(
        CallId::new(41)?,
        Fields::new()
            .with(ConnectionNumber::new(7))
            .with(ApiKey::PRODUCE)
            .with_part(PartFields::new(eu.clone()).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
            .with(Outcome::Ok),
        ResponseSummary::new(Answer::Ok)
            .with_part(eu, ErrorCode::NOT_LEADER_OR_FOLLOWER)
            .into(),
        Explanation::none(),
        CallTiming::arrived(produced_at).delivered(produced_at + Duration::from_millis(5))?,
    )?;
    let settled: TopicPartition = "payments.settled/0".parse()?;
    let fetched_at = Instant::ZERO + Duration::from_millis(900);
    let fetch = Call::new(
        CallId::new(44)?,
        Fields::new()
            .with(ConnectionNumber::new(8))
            .with(ApiKey::FETCH)
            .with_learned(GroupId::new("order-processors")?)
            .with_part(PartFields::new(settled).succeeded())
            .with(Outcome::Ok),
        ResponseSummary::new(Answer::Ok).into(),
        Explanation::none(),
        CallTiming::arrived(fetched_at),
    )?;
    let (produce_version, fetch_version) = (9, 12);
    let correlation_id = 1;
    let no_client_id = None;
    let produce_request = Request::new(
        produce_version,
        correlation_id,
        no_client_id,
        &ProduceRequest::default(),
    )?
    .into_frame();
    let fetch_request =
        Request::new(fetch_version, correlation_id, no_client_id, &FetchRequest::default())?.into_frame();
    // A journal entry between them, at 600 ms, and a session that closed at the end, at 2 s.
    let armed = JournalEntry {
        id: JournalEntryId::new(119)?,
        at: Instant::ZERO + Duration::from_millis(600),
        revision: None,
        from: ChangeSource::new(ChangeSurface::Ui),
        change: Change::RuleArmed {
            rule: "leader-flap".parse()?,
        },
    };
    let closed = RecordedSessionEvent {
        connection: ConnectionNumber::new(7),
        at: Instant::ZERO + Duration::from_secs(2),
        change: SessionChange::Closed { by: Closer::Client },
    };
    // `recordings.summary_values`: how many values of one field a summary lists.
    let listed = 256;

    let mut summary = Summary::empty();
    summary.add(
        &Entry::call(produce, CallFrames::new(produce_request), RecordingMode::Full)?,
        listed,
    );
    summary.add(&Entry::journal(&armed), listed);
    summary.add(
        &Entry::call(fetch, CallFrames::new(fetch_request), RecordingMode::Full)?,
        listed,
    );
    summary.add(&Entry::session(&closed), listed);

    assert_eq!(
        summary.instants(),
        Some((produced_at, Instant::ZERO + Duration::from_secs(2)))
    );
    assert_eq!(
        (summary.calls(), summary.journal_entries(), summary.session_events()),
        (2, 1, 1)
    );
    assert_eq!(summary.call_ids(), Some((CallId::new(41)?, CallId::new(44)?)));
    assert_eq!(summary.journal_ids(), Some((armed.id, armed.id)));

    // A field lists the values of the call as a whole and of each of its parts, each once.
    let topics: BTreeSet<FieldValue> = BTreeSet::from([
        TopicName::new("orders.eu")?.into(),
        TopicName::new("payments.settled")?.into(),
    ]);
    assert_eq!(summary.values(Field::Topic), Some(&SummaryValues::Listed(topics)));
    let outcomes: BTreeSet<FieldValue> = BTreeSet::from([Outcome::Ok.into(), Outcome::Error.into()]);
    assert_eq!(summary.values(Field::Outcome), Some(&SummaryValues::Listed(outcomes)));
    let groups: BTreeSet<FieldValue> = BTreeSet::from([GroupId::new("order-processors")?.into()]);
    assert_eq!(summary.values(Field::Group), Some(&SummaryValues::Listed(groups)));
    // A field that no call of the stretch has is not in the summary.
    assert_eq!(summary.values(Field::TransactionalId), None);

    // A layout keeps the summary as a message, and reads it back.
    assert_eq!(Summary::decode(EncodingVersion::CURRENT, &summary.encode())?, summary);
    Ok(())
}

#[test]
fn a_field_with_more_values_than_a_summary_lists_says_any() -> anyhow::Result<()> {
    // Three calls on three connections, in a summary that lists two values of a field.
    let listed = 2;
    let mut summary = Summary::empty();
    for connection in [7, 8, 9] {
        let call = Call::new(
            CallId::new(connection)?,
            Fields::new()
                .with(ConnectionNumber::new(connection))
                .with(ApiKey::METADATA)
                .with(Outcome::Ok),
            Delivered::Nothing,
            Explanation::none(),
            CallTiming::arrived(Instant::ZERO),
        )?;
        let api_version = 12;
        let correlation_id = 1;
        let no_client_id = None;
        let request =
            Request::new(api_version, correlation_id, no_client_id, &MetadataRequest::default())?.into_frame();
        summary.add(
            &Entry::call(call, CallFrames::new(request), RecordingMode::Full)?,
            listed,
        );
    }

    assert_eq!(summary.values(Field::Connection), Some(&SummaryValues::Any));
    // Every call has the one API, so the field still lists it.
    let apis: BTreeSet<FieldValue> = BTreeSet::from([ApiKey::METADATA.into()]);
    assert_eq!(summary.values(Field::Api), Some(&SummaryValues::Listed(apis)));
    assert_eq!(Summary::decode(EncodingVersion::CURRENT, &summary.encode())?, summary);
    Ok(())
}

#[test]
fn two_summaries_add_up_to_the_summary_of_both_stretches() -> anyhow::Result<()> {
    let listed = 256;
    let closed_at = |millis: u64, connection: u64| RecordedSessionEvent {
        connection: ConnectionNumber::new(connection),
        at: Instant::ZERO + Duration::from_millis(millis),
        change: SessionChange::Closed { by: Closer::Client },
    };
    let armed_as = |id: u64, millis: u64| -> anyhow::Result<JournalEntry> {
        Ok(JournalEntry {
            id: JournalEntryId::new(id)?,
            at: Instant::ZERO + Duration::from_millis(millis),
            revision: None,
            from: ChangeSource::new(ChangeSurface::Cli),
            change: Change::RuleArmed {
                rule: "leader-flap".parse()?,
            },
        })
    };
    let entries_of_the_first_stretch = [Entry::session(&closed_at(100, 7)), Entry::journal(&armed_as(119, 200)?)];
    let entries_of_the_second_stretch = [Entry::journal(&armed_as(120, 300)?), Entry::session(&closed_at(400, 8))];

    let mut first = Summary::empty();
    let mut second = Summary::empty();
    let mut both = Summary::empty();
    for entry in &entries_of_the_first_stretch {
        first.add(entry, listed);
        both.add(entry, listed);
    }
    for entry in &entries_of_the_second_stretch {
        second.add(entry, listed);
        both.add(entry, listed);
    }

    first.merge(&second, listed);

    assert_eq!(first, both);
    assert_eq!(
        first.instants(),
        Some((
            Instant::ZERO + Duration::from_millis(100),
            Instant::ZERO + Duration::from_millis(400)
        ))
    );
    assert_eq!(
        first.journal_ids(),
        Some((JournalEntryId::new(119)?, JournalEntryId::new(120)?))
    );

    // A stretch without an entry adds nothing.
    first.merge(&Summary::empty(), listed);
    assert_eq!(first, both);
    Ok(())
}
