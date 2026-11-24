//! What the tests share: an engine in miniature, calls, rules, and a backend that answers truthfully.

use std::sync::Arc;

use bytes::Bytes;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Clock, Duration, Instant, VirtualClock};
use gaslightmq_field::prelude::*;
use gaslightmq_incident::Incident;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_pipeline::prelude::*;
use gaslightmq_rule::prelude::*;
use gaslightmq_selector::prelude::*;
use gaslightmq_session::prelude::*;
use gaslightmq_trigger::prelude::*;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::metadata_response::MetadataResponseTopic;
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    self, FetchRequest, FetchResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey as KnownApi, FrameDecoder, Record, RecordBatch, Request, Response};
use gaslightmq_wire_edit::prelude::*;

/// The connection of every session of these tests, unless a test opens another one.
pub const CONNECTION: u64 = 7;

/// The seed of every rule of these tests.
pub const SEED: u64 = 42;

/// The offset the backend gives every partition it writes.
pub const WRITTEN_AT: i64 = 100;

pub const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
pub const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;
pub const REBALANCING: ErrorCode = ErrorCode::REBALANCE_IN_PROGRESS;

// The engine in miniature.

/// One pipeline with what an engine holds next to it: a virtual clock and the handles of its proxy.
pub struct Driver {
    pub pipeline: Pipeline,
    pub clock: VirtualClock,
    pub shared: SharedBySessions,
}

impl Driver {
    /// A pipeline with those rules armed, in priority order, and with the refusal registered next to the lies of every build.
    pub fn with(rules: impl IntoIterator<Item = Rule>) -> Self {
        Self::telling(Lies::of_every_build().with(Refusal).unwrap(), rules)
    }

    /// A pipeline of a build that tells `lies`.
    pub fn telling(lies: Lies, rules: impl IntoIterator<Item = Rule>) -> Self {
        let shared = SharedBySessions::new(
            ClientContexts::new(Duration::from_secs(60)),
            CallIds::new(),
            KnownTopics::new(),
            Arc::new(lies),
        );
        Self::sharing(shared, CONNECTION, snapshot(rules, Instant::ZERO))
    }

    /// A pipeline of another connection of the same proxy.
    pub fn sharing(shared: SharedBySessions, connection: u64, snapshot: Arc<Snapshot>) -> Self {
        let clock = VirtualClock::new();
        let connection = Connection::new(
            ConnectionNumber::new(connection),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Broker(BrokerId::from(2)),
        );
        let ordinal = ConnectionOrdinal::new(connection.number().value()).unwrap();
        let pipeline = Pipeline::open(connection, ordinal, shared.clone(), snapshot, clock.now());
        Self {
            pipeline,
            clock,
            shared,
        }
    }

    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// The client sends `request`.
    pub fn send(&mut self, request: &Request) -> Seen {
        let bytes = request.frame().to_bytes();
        self.handle(Input::ClientBytes(&bytes))
    }

    /// The backend answers the request that the pipeline forwarded, truthfully.
    pub fn answer(&mut self, (call, request): &(CallId, Request)) -> Seen {
        self.answer_with(*call, truth(request))
    }

    pub fn answer_with(&mut self, call: CallId, response: Response) -> Seen {
        self.handle(Input::BackendAnswered { call, response })
    }

    /// The clock reaches `at`, and the timer runs out.
    pub fn fire(&mut self, (timer, at): (TimerId, Instant)) -> Seen {
        self.clock.advance_to(at);
        self.handle(Input::TimerFired(timer))
    }

    /// The engine publishes a snapshot with those rules, armed now.
    pub fn publish(&mut self, rules: impl IntoIterator<Item = Rule>) -> Seen {
        let snapshot = snapshot(rules, self.now());
        self.handle(Input::SnapshotPublished(snapshot))
    }

    pub fn handle(&mut self, input: Input<'_>) -> Seen {
        Seen(self.pipeline.handle(input, self.clock.now()).into_iter().collect())
    }
}

/// The snapshot of those rules, armed at `now`, in priority order.
pub fn snapshot(rules: impl IntoIterator<Item = Rule>, now: Instant) -> Arc<Snapshot> {
    let armed = rules.into_iter().map(|rule| {
        let resolved = rule.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();
        resolved.arm(Seed::new(SEED), now)
    });
    Arc::new(Snapshot::new(armed).unwrap())
}

/// What one `handle` returned.
#[derive(Debug)]
pub struct Seen(pub Vec<Output>);

impl Seen {
    /// The outputs by kind, in order.
    pub fn kinds(&self) -> Vec<&'static str> {
        let kind = |output: &Output| match output {
            Output::Forward { .. } => "forward",
            Output::Send(_) => "send",
            Output::SetTimer { .. } => "set timer",
            Output::Close => "close",
            Output::Emit(_) => "emit",
            Output::Fire(_) => "fire",
        };
        self.0.iter().map(kind).collect()
    }

    pub fn forwards(&self) -> Vec<(CallId, Request)> {
        let forward = |output: &Output| match output {
            Output::Forward { call, request } => Some((*call, request.clone())),
            _ => None,
        };
        self.0.iter().filter_map(forward).collect()
    }

    /// The one request that the pipeline forwarded.
    pub fn forwarded(&self) -> (CallId, Request) {
        let [forward] = self.forwards().try_into().unwrap();
        forward
    }

    pub fn sends(&self) -> Vec<Bytes> {
        let send = |output: &Output| match output {
            Output::Send(bytes) => Some(bytes.clone()),
            _ => None,
        };
        self.0.iter().filter_map(send).collect()
    }

    /// The one response that the pipeline sent, as the client reads it: the answer to `request`.
    pub fn sent(&self, request: &Request) -> Response {
        let [bytes] = self.sends().try_into().unwrap();
        let mut frames = FrameDecoder::new();
        frames.extend(&bytes);
        let frame = frames.next_frame().unwrap().unwrap();
        assert_eq!(frames.buffered(), 0, "the pipeline sends one whole response");
        Response::decode(request.api(), frame).unwrap()
    }

    pub fn timers(&self) -> Vec<(TimerId, Instant)> {
        let timer = |output: &Output| match output {
            Output::SetTimer { id, at } => Some((*id, *at)),
            _ => None,
        };
        self.0.iter().filter_map(timer).collect()
    }

    /// The one timer that the pipeline asked for.
    pub fn timer(&self) -> (TimerId, Instant) {
        let [timer] = self.timers().try_into().unwrap();
        timer
    }

    pub fn calls(&self) -> Vec<Call> {
        let call = |output: &Output| match output {
            Output::Emit(call) => Some(Call::clone(call)),
            _ => None,
        };
        self.0.iter().filter_map(call).collect()
    }

    /// The one call record that the pipeline emitted.
    pub fn emitted(&self) -> Call {
        let [call] = self.calls().try_into().unwrap();
        call
    }

    pub fn incidents(&self) -> Vec<Incident> {
        let incident = |output: &Output| match output {
            Output::Fire(incident) => Some(incident.clone()),
            _ => None,
        };
        self.0.iter().filter_map(incident).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// Rules.

fn name(text: &str) -> RuleName {
    text.parse().unwrap()
}

/// A rule that lies on every call of `api`.
pub fn on_api(rule: &str, api: ApiKey, lie: impl Into<LieDefinition>) -> Rule {
    Rule::new(name(rule), Conditions::from(Condition::is(api)), lie.into())
}

/// A rule that lies on the partitions of the topics that match `topics`, whatever the API.
pub fn on_topic(rule: &str, topics: &str, lie: impl Into<LieDefinition>) -> Rule {
    let topic = Condition::parse(Field::Topic, topics).unwrap();
    Rule::new(name(rule), Conditions::from(topic), lie.into())
}

/// A rule that lies on one partition.
pub fn on_partition(rule: &str, topic: &str, partition: &str, lie: impl Into<LieDefinition>) -> Rule {
    let conditions = Conditions::new()
        .with(Condition::parse(Field::Topic, topic).unwrap())
        .with(Condition::parse(Field::Partition, partition).unwrap());
    Rule::new(name(rule), conditions, lie.into())
}

/// A rule that lies on the calls of `api` that the backend answered without an error. It waits for the truth.
pub fn on_success(rule: &str, api: ApiKey, lie: impl Into<LieDefinition>) -> Rule {
    let conditions = Conditions::new()
        .with(Condition::is(api))
        .with(Condition::is(Outcome::Ok));
    Rule::new(name(rule), conditions, lie.into())
}

pub fn refusal(error: ErrorCode) -> RefusalLie {
    RefusalLie::new(error)
}

pub fn late_refusal(error: ErrorCode) -> RefusalLie {
    RefusalLie::new(error).at(Stage::AfterResponse)
}

pub fn part(text: &str) -> Part {
    text.parse().unwrap()
}

// Topics.

/// The topics of these tests.
const TOPICS: [&str; 2] = ["orders.eu", "payments.settled"];

/// The ID of a topic: every byte is the position of the topic among [`TOPICS`], from one.
pub fn topic_id(name: &str) -> TopicId {
    TopicId::new(id_bytes(name)).unwrap()
}

fn id_bytes(name: &str) -> [u8; 16] {
    let position = TOPICS.iter().position(|topic| *topic == name).unwrap();
    [u8::try_from(position + 1).unwrap(); 16]
}

/// The ID of a topic as Kafka messages hold it.
fn wire_id<Id: TryFrom<Vec<u8>>>(name: &str) -> Id
where
    Id::Error: std::fmt::Debug,
{
    id_bytes(name).to_vec().try_into().unwrap()
}

fn wire_name(name: &str) -> messages::TopicName {
    StrBytes::from_string(name.to_owned()).into()
}

/// The partitions by topic, in the order the texts name them.
fn by_topic(partitions: &[&str]) -> Vec<(String, Vec<i32>)> {
    let mut topics: Vec<(String, Vec<i32>)> = Vec::new();
    for partition in partitions {
        let partition: TopicPartition = partition.parse().unwrap();
        let name = partition.topic().as_str();
        let index = i32::from(partition.partition());
        match topics.iter_mut().find(|(topic, _)| topic == name) {
            Some((_, indexes)) => indexes.push(index),
            None => topics.push((name.to_owned(), vec![index])),
        }
    }
    topics
}

// Requests.

/// A Produce for those partitions, each with one record, that waits for every replica.
pub fn produce(correlation_id: i32, partitions: &[&str]) -> Request {
    produce_with_acks(correlation_id, partitions, -1)
}

pub fn produce_with_acks(correlation_id: i32, partitions: &[&str], acks: i16) -> Request {
    let records = RecordBatch::new([Record::new("a record")]).encode().unwrap();
    let topics = by_topic(partitions).into_iter().map(|(name, indexes)| {
        let partitions = indexes.into_iter().map(|index| {
            PartitionProduceData::default()
                .with_index(index)
                .with_records(Some(records.clone()))
        });
        TopicProduceData::default()
            .with_name(wire_name(&name))
            .with_partition_data(partitions.collect())
    });
    let body = ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(30_000)
        .with_topic_data(topics.collect());
    Request::new(9, correlation_id, Some("order-ingest"), &body).unwrap()
}

/// A Fetch of version 13 for those partitions, which names its topics by ID alone.
pub fn fetch_by_id(correlation_id: i32, partitions: &[&str]) -> Request {
    let topics = by_topic(partitions).into_iter().map(|(name, indexes)| {
        let partitions = indexes
            .into_iter()
            .map(|index| FetchPartition::default().with_partition(index));
        FetchTopic::default()
            .with_topic_id(wire_id(&name))
            .with_partitions(partitions.collect())
    });
    let body = FetchRequest::default().with_topics(topics.collect());
    Request::new(13, correlation_id, Some("order-ingest"), &body).unwrap()
}

/// A Metadata request for those topics.
pub fn metadata(correlation_id: i32, topics: &[&str]) -> Request {
    let topics = topics
        .iter()
        .map(|name| MetadataRequestTopic::default().with_name(Some(wire_name(name))));
    let body = MetadataRequest::default().with_topics(Some(topics.collect()));
    Request::new(12, correlation_id, Some("order-ingest"), &body).unwrap()
}

/// A Heartbeat of a member of the group `analytics-etl`.
pub fn heartbeat(correlation_id: i32) -> Request {
    let body = HeartbeatRequest::default()
        .with_group_id(StrBytes::from_static_str("analytics-etl").into())
        .with_member_id(StrBytes::from_static_str("consumer-1-4f2a"));
    Request::new(4, correlation_id, Some("order-ingest"), &body).unwrap()
}

/// A `JoinGroup` of a client that has no member ID yet.
pub fn join_group(correlation_id: i32) -> Request {
    let body = JoinGroupRequest::default().with_group_id(StrBytes::from_static_str("analytics-etl").into());
    Request::new(9, correlation_id, Some("order-ingest"), &body).unwrap()
}

// The backend.

/// What a backend answers `request`, when nothing is wrong with the cluster.
pub fn truth(request: &Request) -> Response {
    match request.api().known() {
        Some(KnownApi::Produce) => {
            let asked: ProduceRequest = request.body().unwrap();
            let topics = asked.topic_data.iter().map(|topic| {
                let partitions = topic.partition_data.iter().map(|partition| {
                    PartitionProduceResponse::default()
                        .with_index(partition.index)
                        .with_base_offset(WRITTEN_AT)
                });
                TopicProduceResponse::default()
                    .with_name(topic.name.clone())
                    .with_partition_responses(partitions.collect())
            });
            let body = ProduceResponse::default().with_responses(topics.collect());
            request.response(&body).unwrap()
        }
        Some(KnownApi::Fetch) => {
            let asked: FetchRequest = request.body().unwrap();
            let topics = asked.topics.iter().map(|topic| {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionData::default().with_partition_index(partition.partition));
                FetchableTopicResponse::default()
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions.collect())
            });
            let body = FetchResponse::default().with_responses(topics.collect());
            request.response(&body).unwrap()
        }
        Some(KnownApi::Metadata) => {
            let asked: MetadataRequest = request.body().unwrap();
            let topics = asked.topics.iter().flatten().map(|topic| {
                let name = topic.name.clone().unwrap();
                MetadataResponseTopic::default()
                    .with_topic_id(wire_id(name.as_str()))
                    .with_name(Some(name))
            });
            let body = MetadataResponse::default().with_topics(topics.collect());
            request.response(&body).unwrap()
        }
        Some(KnownApi::Heartbeat) => request.response(&HeartbeatResponse::default()).unwrap(),
        Some(KnownApi::JoinGroup) => {
            let body = JoinGroupResponse::default().with_member_id(StrBytes::from_static_str("consumer-1-4f2a"));
            request.response(&body).unwrap()
        }
        other => panic!("the backend of these tests has no answer for {other:?}"),
    }
}

// Reading requests, responses and records.

/// The parts that `request` names, by someone who knows every topic of these tests by its ID.
pub fn asked(request: &Request) -> Vec<String> {
    let names: TopicNames = TOPICS
        .into_iter()
        .map(|name| (topic_id(name), TopicName::new(name).unwrap()))
        .collect();
    let decoded = EditableRequest::decode(request, &names).unwrap();
    decoded.parts().iter().map(ToString::to_string).collect()
}

/// What `response` tells the client about each part of `request`, in the order of the response: `orders.eu/3 ok`.
pub fn told(request: &Request, response: &Response) -> Vec<String> {
    let names: TopicNames = TOPICS
        .into_iter()
        .map(|name| (topic_id(name), TopicName::new(name).unwrap()))
        .collect();
    let decoded = EditableRequest::decode(request, &names).unwrap();
    let answered = decoded.parts_of(response).unwrap();
    let line = |part: &PartFields| match part.own(Field::ErrorCode) {
        Some(Reading::Present(code)) => format!("{} {code}", part.part()),
        _ => format!("{} ok", part.part()),
    };
    answered.iter().map(line).collect()
}

/// The lines of the explanation of `call`, one per rule.
pub fn explained(call: &Call) -> Vec<String> {
    call.explanation().iter().map(ToString::to_string).collect()
}

/// What the rules saw as the truth of `part` in `call`: `ok`, an error code, or `none`.
pub fn truth_of(call: &Call, part: &str) -> String {
    let part = part.parse::<Part>().unwrap();
    let fields = call.parts().iter().find(|fields| *fields.part() == part).unwrap();
    match (
        call.read_in(fields, Field::Outcome),
        call.read_in(fields, Field::ErrorCode),
    ) {
        (Reading::Present(_), Reading::Present(code)) => code.iter().next().unwrap().to_string(),
        (Reading::Present(outcome), _) => outcome.iter().next().unwrap().to_string(),
        _ => "none".to_owned(),
    }
}
