//! What goes into a recording comes back: every kind of entry, at its position, as the type that went in.

use std::num::NonZeroU64;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant, TimeSpan};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_wire::Request;
use gaslightmq_wire::messages::FetchRequest;

#[test]
fn a_call_comes_back_with_every_line_of_its_explanation() -> anyhow::Result<()> {
    // A Fetch for two partitions that four rules touched.
    let eu: TopicPartition = "orders.eu/3".parse()?;
    let settled: TopicPartition = "payments.settled/0".parse()?;
    let both = Target::of_parts([eu.clone().into(), settled.clone().into()]).expect("two parts");
    let only_eu = Target::of_parts([eu.clone().into()]).expect("one part");
    let only_settled = Target::of_parts([settled.clone().into()]).expect("one part");
    let rule = |name: &str| name.parse::<RecordedRuleName>();

    // `leader-flap` fired a refusal on both partitions, and lost one of them to `slow-fetch`.
    let die = DieCheck::new(374_213, 400_000)?;
    let window = WindowCheck::new(
        TimeSpan::from_secs(10),
        Some(TimeSpan::from_secs(40)),
        TimeSpan::from_secs(12),
    )?;
    let checks = TriggerChecks::new().with_count(true).with_die(die).with_window(window);
    let three_firings = NonZeroU64::new(3).expect("a limit of three");
    let fired = TriggerExplanation::new(TriggerDecision::Fired, NonZeroU64::new(7), checks)?
        .with_budget(BudgetCheck::new(2, three_firings)?);
    let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(both.clone());
    let shadowed = vec![LostTarget::new(only_settled, rule("slow-fetch")?)];
    let fate = LieFate::new(refusal, Some(only_eu), shadowed, Vec::new())?;
    let flapped = RuleExplanation::lied(rule("leader-flap")?, Priority::new(0), both.clone(), fired, fate)?;

    // `slow-fetch` fired a delay by hand, which GaslightMQ could not tell.
    let delay = TimingLie::new()
        .with_delay(TimeSpan::from_millis(750))
        .with_jitter(TimeSpan::from_millis(50));
    let by_hand = TriggerExplanation::new(TriggerDecision::FiredByHand, NonZeroU64::new(12), TriggerChecks::new())?;
    let untold = LieFate::all_applied(LieDefinition::from(delay).on(Target::Call))
        .untold(Failure::new("the truth of the backend did not decode"));
    let slowed = RuleExplanation::lied(rule("slow-fetch")?, Priority::new(1), both.clone(), by_hand, untold)?;

    // `broker-down` fired an incident, and `refuse-every-2nd` held.
    let fired_again = TriggerExplanation::new(TriggerDecision::Fired, NonZeroU64::new(1), TriggerChecks::new())?;
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::new(1)?,
        failover: true,
    };
    let downed = RuleExplanation::fired_incidents(
        rule("broker-down")?,
        Priority::new(2),
        Target::Call,
        fired_again,
        vec![down],
    )?;
    let disagreed = TriggerChecks::new().with_count(false);
    let held = TriggerExplanation::new(TriggerDecision::Held, NonZeroU64::new(5), disagreed)?
        .with_reason("the count did not agree");
    let waited = RuleExplanation::held(rule("refuse-every-2nd")?, Priority::new(3), both, held)?;
    let explanation = Explanation::new([flapped, slowed, downed, waited])?
        .with_failure(Failure::new("the truth of the backend did not decode"));

    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with(ApiKey::FETCH)
        .with_part(PartFields::new(eu.clone()).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .with_part(PartFields::new(settled).succeeded())
        .with(Outcome::Ok);
    let delivered = ResponseSummary::new(Answer::Ok).with_part(eu, ErrorCode::NOT_LEADER_OR_FOLLOWER);
    let timing = CallTiming::arrived(Instant::ZERO);
    let call = Call::new(CallId::new(42)?, fields, delivered.into(), explanation, timing)?;
    let api_version = 17;
    let correlation_id = 9;
    let request = Request::new(api_version, correlation_id, Some("analytics"), &FetchRequest::default())?;

    let entry = Entry::call(call.clone(), CallFrames::new(request.into_frame()), RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, None);

    let Some((_, Ok(EntryView::Call(view)))) = batch.entries().next() else {
        anyhow::bail!("the batch holds the call");
    };
    assert_eq!(view.to_call()?, call);

    // A reader that wants no type reads the same explanation where it lies.
    let lines: Vec<(&str, bool)> = view
        .explanation()
        .rules()
        .map(|line| (line.rule(), line.fired()))
        .collect();
    assert_eq!(
        lines,
        [
            ("leader-flap", true),
            ("slow-fetch", true),
            ("broker-down", true),
            ("refuse-every-2nd", false)
        ]
    );
    assert_eq!(
        view.explanation().failure(),
        Some("the truth of the backend did not decode")
    );
    assert_eq!(view.explanation().to_explanation()?.verdict(), call.verdict());
    Ok(())
}

#[test]
fn a_call_comes_back_with_its_fields_its_moments_and_what_the_real_cluster_answered() -> anyhow::Result<()> {
    // A Fetch for two partitions on connection 7, whose group the client context provides,
    // and whose answer the overlay of the backend changed.
    let eu: TopicPartition = "orders.eu/3".parse()?;
    let settled: TopicPartition = "payments.settled/0".parse()?;
    let fields = Fields::new()
        .with(ConnectionNumber::new(7))
        .with("10.0.4.27:51234".parse::<ClientAddress>()?)
        .with(Endpoint::Broker(BrokerId::new(2)?))
        .with(ClientId::new("analytics")?)
        .with(ClientSoftware::new("apache-kafka-java", "3.9.0")?)
        .with(ApiKey::FETCH)
        .with(ApiVersion::new(17)?)
        .with_learned(GroupId::new("order-processors")?)
        .with_part(PartFields::new(eu.clone()).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .with_part(PartFields::new(settled.clone()).succeeded())
        .with(Outcome::Ok)
        .with(Latency::from_millis(12));
    let arrived = Instant::ZERO + Duration::from_millis(250);
    let timing = CallTiming::arrived(arrived)
        .forwarded(arrived + Duration::from_micros(400))?
        .answered(arrived + Duration::from_millis(12))?
        .delivered(arrived + Duration::from_millis(762))?;
    let delivered = ResponseSummary::new(Answer::Ok).with_part(eu, ErrorCode::NOT_LEADER_OR_FOLLOWER);
    let upstream = ResponseSummary::new(Answer::Ok).with_part(settled, ErrorCode::OFFSET_OUT_OF_RANGE);
    let call =
        Call::new(CallId::new(42)?, fields, delivered.into(), Explanation::none(), timing)?.with_upstream(upstream);
    let api_version = 17;
    let correlation_id = 9;
    let request = Request::new(api_version, correlation_id, Some("analytics"), &FetchRequest::default())?;

    let entry = Entry::call(call.clone(), CallFrames::new(request.into_frame()), RecordingMode::Full)?;
    let encoded = BatchEncoder::new().encode(entry);
    let mut entries = BatchBuffer::with_room(encoded.entry().len());
    entries.write(encoded.entry());
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, None);

    let Some((_, Ok(EntryView::Call(view)))) = batch.entries().next() else {
        anyhow::bail!("the batch holds the call");
    };
    assert_eq!(view.to_call()?, call);

    // A reader that wants no type reads the same fields where they lie: a topic as text, a partition as a number.
    let parts: Vec<(&str, Option<u32>)> = view
        .fields()
        .parts()
        .map(|part| (part.topic(), part.partition()))
        .collect();
    assert_eq!(parts, [("orders.eu", Some(3)), ("payments.settled", Some(0))]);
    let learned: Vec<Option<&str>> = view.fields().learned().map(|value| value.text()).collect();
    assert_eq!(learned, [Some("order-processors")]);
    let api_version = view.fields().values().find(|value| value.field() == Field::ApiVersion);
    assert_eq!(api_version.and_then(|value| value.number()), Some(17));
    assert_eq!(view.fields().to_fields()?, *call.fields());
    assert_eq!(view.arrived_at(), arrived);
    Ok(())
}

#[test]
fn a_session_event_of_every_kind_comes_back_as_it_went_in() -> anyhow::Result<()> {
    let connection = ConnectionNumber::new(7);
    let software = ClientSoftware::new("apache-kafka-java", "3.9.0")?;
    let opened = SessionChange::Opened {
        address: "10.0.4.27:51234".parse()?,
        endpoint: Endpoint::Bootstrap,
        client_id: None,
        software: None,
    };
    let identified = SessionChange::Identified {
        client_id: Some(ClientId::new("order-ingest")?),
    };
    let named = SessionChange::SoftwareNamed { software };
    let closed = SessionChange::Closed { by: Closer::Backend };
    let changes = [opened, identified, named, closed];
    let events: Vec<SessionEvent> = (0u64..)
        .zip(changes)
        .map(|(millis, change)| SessionEvent {
            connection,
            at: Instant::ZERO + Duration::from_millis(millis),
            change,
        })
        .collect();

    // The entries stand one after the other in the buffer of their batch.
    let mut batch_encoder = BatchEncoder::new();
    let encoded: Vec<EncodedEntry> = events
        .iter()
        .map(|event| batch_encoder.encode(Entry::session(event)))
        .collect();
    let room = encoded.iter().map(|entry| entry.entry().len()).sum();
    let mut entries = BatchBuffer::with_room(room);
    for entry in &encoded {
        entries.write(entry.entry());
    }
    let first = Position::new(40);
    let batch = Batch::checked(EncodingVersion::CURRENT, first, entries, None);

    let mut read = Vec::new();
    for (position, entry) in batch.entries() {
        let EntryView::Session(view) = entry? else {
            anyhow::bail!("the entry is a session event");
        };
        assert_eq!(view.connection(), connection);
        read.push((position, view.to_session_event()?));
    }
    let expected: Vec<(Position, SessionEvent)> = (40..).map(Position::new).zip(events).collect();
    assert_eq!(read, expected);
    Ok(())
}

#[test]
fn a_journal_entry_comes_back_as_it_went_in() -> anyhow::Result<()> {
    // A broker went down without a failover, which left a partition without a leader.
    let down = Incident::BrokerGoesDown {
        broker: BrokerId::new(1)?,
        failover: false,
    };
    let leaderless = Consequence::Leaderless {
        partition: "orders.eu/3".parse()?,
    };
    let applied = JournalEntry {
        id: JournalEntryId::new(119)?,
        at: Instant::ZERO + Duration::from_secs(30),
        revision: None,
        from: ChangeSource::new(ChangeSurface::Pad).with_cause(ChangeCause::Redo),
        change: Change::IncidentApplied {
            incident: ExpandedIncident::from_parts(down, vec![leaderless]),
        },
    };
    let turned = JournalEntry {
        id: JournalEntryId::new(120)?,
        at: Instant::ZERO + Duration::from_secs(31),
        revision: Some(Revision::new(8)),
        from: ChangeSource::new(ChangeSurface::Ui),
        change: Change::ParameterChanged {
            rule: "leader-flap".parse()?,
            parameter: Parameter::Chance,
            old: "100%".to_owned(),
            new: "40%".to_owned(),
        },
    };

    let mut batch_encoder = BatchEncoder::new();
    let encoded = [
        batch_encoder.encode(Entry::journal(&applied)),
        batch_encoder.encode(Entry::journal(&turned)),
    ];
    let room = encoded.iter().map(|entry| entry.entry().len()).sum();
    let mut entries = BatchBuffer::with_room(room);
    for entry in &encoded {
        entries.write(entry.entry());
    }
    let batch = Batch::checked(EncodingVersion::CURRENT, Position::FIRST, entries, None);

    let mut read = Vec::new();
    for (_, entry) in batch.entries() {
        let EntryView::Journal(view) = entry? else {
            anyhow::bail!("the entry is a journal entry");
        };
        assert_eq!(view.at(), view.to_journal_entry()?.at);
        read.push((view.id()?, view.to_journal_entry()?));
    }
    assert_eq!(read, [(applied.id, applied), (turned.id, turned)]);
    Ok(())
}
