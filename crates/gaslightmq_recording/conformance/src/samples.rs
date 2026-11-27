//! The recording that the check writes: a session, its calls with their frames, and the journal entries between them.

use std::num::NonZeroU64;

use bytes::Bytes;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::prelude::*;
use gaslightmq_journal::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_recording::prelude::*;
use gaslightmq_wire::messages::ProduceRequest;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Record, RecordBatch, Request};

/// The connection that every call of the check runs on.
const CONNECTION: u64 = 7;
/// The bytes of the value of the record that every Produce of the check carries.
/// With them, a hundred calls are more than a reader may hold, and more than a few batches.
const RECORD_VALUE: usize = 1024;

/// One thing that the check writes into a recording, as its type: what a reader gets back.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Sample {
    Session(SessionEvent),
    Call { call: Box<Call>, frames: CallFrames },
    Journal(JournalEntry),
}

impl Sample {
    /// The entry that a recorder makes of the sample, in a full recording.
    pub(crate) fn entry(&self) -> Entry {
        match self {
            Sample::Session(event) => Entry::session(event),
            Sample::Call { call, frames } => Entry::call((**call).clone(), frames.clone(), RecordingMode::Full)
                .expect("a call whose lies the encoding has a stored type for"),
            Sample::Journal(entry) => Entry::journal(entry),
        }
    }

    /// The ID of the call, when the sample is one.
    pub(crate) fn call_id(&self) -> Option<CallId> {
        match self {
            Sample::Call { call, .. } => Some(call.id()),
            Sample::Session(_) | Sample::Journal(_) => None,
        }
    }
}

/// What a recording of `environment` that started at `started` is.
pub(crate) fn description(environment: &str, started: &str) -> Description {
    Description {
        environment: environment.parse().expect("the name of an environment"),
        started: started.parse().expect("an instant in UTC"),
        clock: Instant::ZERO + Duration::from_secs(42),
        mode: RecordingMode::Full,
        backend: RecordedBackend::Simulated,
        written_by: "0.1.0".to_owned(),
        armed: vec!["leader-flap".parse().expect("the name of a rule")],
        scenario: Some(ActiveScenario {
            scenario: "black-friday".parse().expect("the name of a scenario"),
            step: Some("warm-up".parse().expect("the name of a step")),
        }),
        journal_after: Some(JournalEntryId::FIRST),
    }
}

/// The recording that most behaviours are checked with.
pub(crate) fn orders_sim() -> Description {
    description("orders-sim", "2026-11-21T14:02:11.527Z")
}

/// When the entry with that number happened: a millisecond after the one before.
fn at(number: u64) -> Instant {
    Instant::ZERO + Duration::from_secs(42) + Duration::from_millis(number)
}

fn opened() -> Sample {
    Sample::Session(SessionEvent {
        connection: ConnectionNumber::new(CONNECTION),
        at: at(0),
        change: SessionChange::Opened {
            address: "10.0.4.27:51234".parse().expect("the address of a client"),
            endpoint: Endpoint::Bootstrap,
            client_id: Some(ClientId::new("order-ingest").expect("a client ID")),
            software: None,
        },
    })
}

fn closed(number: u64) -> Sample {
    Sample::Session(SessionEvent {
        connection: ConnectionNumber::new(CONNECTION),
        at: at(number),
        change: SessionChange::Closed { by: Closer::Client },
    })
}

fn armed(number: u64) -> Sample {
    Sample::Journal(JournalEntry {
        id: JournalEntryId::FIRST.next(),
        at: at(number),
        revision: Some(Revision::new(7)),
        from: ChangeSource::new(ChangeSurface::Ui),
        change: Change::RuleArmed {
            rule: "slow-fetch".parse().expect("the name of a rule"),
        },
    })
}

/// A Produce of one record to `orders.eu/3`, which the rule `leader-flap` refused, and the frames of it.
fn produce(id: u64, number: u64) -> Sample {
    let partition: TopicPartition = "orders.eu/3".parse().expect("a topic partition");
    let target = Target::of_parts([partition.clone().into()]).expect("one part");
    let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(target.clone());
    let fired = TriggerExplanation::new(TriggerDecision::Fired, Some(NonZeroU64::MIN), TriggerChecks::new())
        .expect("a match that fired");
    let rule = "leader-flap".parse().expect("the name of a rule");
    let flapped = RuleExplanation::lied(rule, Priority::HIGHEST, target, fired, LieFate::all_applied(refusal))
        .expect("a rule that fired tells its lie");
    let fields = Fields::new()
        .with(ConnectionNumber::new(CONNECTION))
        .with(ClientId::new("order-ingest").expect("a client ID"))
        .with(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition.clone()).without_truth())
        .without_truth();
    let delivered = ResponseSummary::new(Answer::Ok).with_part(partition, ErrorCode::NOT_LEADER_OR_FOLLOWER);
    let call = Call::new(
        CallId::new(id).expect("a call ID"),
        fields,
        delivered.into(),
        Explanation::new([flapped]).expect("one line for each rule"),
        CallTiming::arrived(at(number))
            .delivered(at(number))
            .expect("delivered no earlier than it arrived"),
    )
    .expect("the record of a call on one connection");

    let value = Bytes::from(vec![b'o'; RECORD_VALUE]);
    let records = RecordBatch::new(vec![Record::new(value).with_key("order-42")])
        .encode()
        .expect("a batch of one record");
    let data = PartitionProduceData::default()
        .with_index(3)
        .with_records(Some(records));
    let topic = TopicProduceData::default()
        .with_name(StrBytes::from_static_str("orders.eu").into())
        .with_partition_data(vec![data]);
    let body = ProduceRequest::default().with_acks(-1).with_topic_data(vec![topic]);
    let api_version = 9;
    let correlation_id = i32::try_from(id).expect("a correlation ID");
    let request = Request::new(api_version, correlation_id, Some("order-ingest"), &body).expect("a Produce request");
    let refused = request
        .response(&gaslightmq_wire::messages::ProduceResponse::default())
        .expect("a Produce response");
    let frames = CallFrames::new(request.into_frame()).with_delivered(refused.into_frame());
    Sample::Call {
        call: Box::new(call),
        frames,
    }
}

/// A session that opens, `calls` Produce calls with a journal entry behind the first of them,
/// and the session that closes: the traffic of the recording of the check, in the order in which it ended.
pub(crate) fn traffic(calls: u64) -> Vec<Sample> {
    let mut samples = vec![opened()];
    for id in 1..=calls {
        samples.push(produce(id, samples.len() as u64));
        if id == 1 {
            samples.push(armed(samples.len() as u64));
        }
    }
    samples.push(closed(samples.len() as u64));
    samples
}
