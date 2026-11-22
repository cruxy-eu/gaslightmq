//! What the tests share: calls, a backend that answers them truthfully, and the stages of one call.
//!
//! [`walk`] is the part of a pipeline that these tests need.
//! It asks the refusals that apply, in order, and it does what each effect says.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie::prelude::*;
use gaslightmq_lie_api::prelude::*;
use gaslightmq_lie_refusal::prelude::*;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    self, FetchRequest, FetchResponse, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Record, RecordBatch, Request, Response};
use gaslightmq_wire_edit::prelude::*;

/// The offset the backend gives every partition it writes.
pub const WRITTEN_AT: i64 = 100;

/// The high watermark the backend reads for every partition.
pub const HIGH_WATERMARK: i64 = 250;

/// The correlation ID of every request.
pub const CORRELATION_ID: i32 = 7;

// Targets and lies.

pub fn part(text: &str) -> Part {
    text.parse().unwrap()
}

pub fn parts(texts: &[&str]) -> Target {
    Target::of_parts(texts.iter().map(|text| part(text))).unwrap()
}

/// The target of one part.
pub fn only(part: &Part) -> Target {
    Target::of_parts([part.clone()]).unwrap()
}

/// A refusal with `error` on `target`, before forwarding.
pub fn refusal(error: ErrorCode, target: Target) -> TargetedLie {
    LieDefinition::from(RefusalLie::new(error)).on(target)
}

pub fn every_lie() -> Lies {
    Lies::of_every_build().with(Refusal).unwrap()
}

// Topics.

/// The topics of these tests.
const TOPICS: [&str; 3] = ["orders.eu", "payments.settled", "audit.log"];

/// The ID of a topic: every byte is the position of the topic among [`TOPICS`], from one.
pub fn topic_id(name: &str) -> TopicId {
    TopicId::new(id_bytes(name)).unwrap()
}

fn id_bytes(name: &str) -> [u8; 16] {
    let position = TOPICS.iter().position(|topic| *topic == name).unwrap();
    [u8::try_from(position + 1).unwrap(); 16]
}

/// The ID of a topic as Kafka messages hold it.
/// The messages have a UUID type of their own, which converts from bytes.
fn wire_id<Id: TryFrom<Vec<u8>>>(name: &str) -> Id
where
    Id::Error: std::fmt::Debug,
{
    id_bytes(name).to_vec().try_into().unwrap()
}

fn wire_name(name: &str) -> messages::TopicName {
    StrBytes::from_string(name.to_owned()).into()
}

/// The name of every topic of these tests by its ID.
pub fn topic_names() -> TopicNames {
    TOPICS
        .into_iter()
        .map(|name| (topic_id(name), TopicName::new(name).unwrap()))
        .collect()
}

/// The partitions by topic, in the order the texts name them: `orders.eu/3` and `orders.eu/4` are one topic.
fn by_topic(partitions: &[&str]) -> Vec<(String, Vec<i32>)> {
    let mut topics: Vec<(String, Vec<i32>)> = Vec::new();
    for text in partitions {
        let partition: TopicPartition = text.parse().unwrap();
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
pub fn produce(partitions: &[&str]) -> Request {
    produce_with_acks(partitions, -1)
}

pub fn produce_with_acks(partitions: &[&str], acks: i16) -> Request {
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
    Request::new(9, CORRELATION_ID, Some("order-ingest"), &body).unwrap()
}

/// A Fetch for those partitions, which names its topics by name.
pub fn fetch(partitions: &[&str]) -> Request {
    fetch_at(11, partitions)
}

/// A Fetch for those partitions, which names its topics by ID alone.
pub fn fetch_by_id(partitions: &[&str]) -> Request {
    fetch_at(13, partitions)
}

fn fetch_at(version: i16, partitions: &[&str]) -> Request {
    let topics = by_topic(partitions).into_iter().map(|(name, indexes)| {
        let partitions = indexes
            .into_iter()
            .map(|index| FetchPartition::default().with_partition(index));
        let topic = FetchTopic::default().with_partitions(partitions.collect());
        if version >= 13 {
            topic.with_topic_id(wire_id(&name))
        } else {
            topic.with_topic(wire_name(&name))
        }
    });
    let body = FetchRequest::default().with_topics(topics.collect());
    Request::new(version, CORRELATION_ID, Some("analytics-etl"), &body).unwrap()
}

/// A Metadata request for those topics.
pub fn metadata(topics: &[&str]) -> Request {
    let topics = topics
        .iter()
        .map(|name| MetadataRequestTopic::default().with_name(Some(wire_name(name))));
    let body = MetadataRequest::default().with_topics(Some(topics.collect()));
    Request::new(9, CORRELATION_ID, Some("order-ingest"), &body).unwrap()
}

/// A Metadata request that names no topic, and so asks for every topic.
pub fn metadata_for_every_topic(version: i16) -> Request {
    let body = MetadataRequest::default().with_topics(None);
    Request::new(version, CORRELATION_ID, Some("order-ingest"), &body).unwrap()
}

/// A request of an API without parts.
pub fn heartbeat() -> Request {
    let body = HeartbeatRequest::default().with_group_id(StrBytes::from_static_str("analytics-etl").into());
    Request::new(4, CORRELATION_ID, Some("analytics-etl"), &body).unwrap()
}

/// One request of each API with parts that the tests take through both stages, each with three parts.
pub fn calls_with_parts() -> [Request; 3] {
    [
        produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]),
        fetch(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]),
        metadata(&["orders.eu", "payments.settled", "audit.log"]),
    ]
}

/// The request with its body decoded, by someone who knows every topic of these tests by its ID.
pub fn decoded(request: &Request) -> EditableRequest {
    EditableRequest::decode(request, &topic_names()).unwrap()
}

/// The three parts of one of the [`calls_with_parts`], in the order of the request.
pub fn three_parts(request: &Request) -> [Part; 3] {
    decoded(request).parts().to_vec().try_into().unwrap()
}

// The backend.

/// The truth: every partition is written at [`WRITTEN_AT`] or read up to [`HIGH_WATERMARK`],
/// every topic has one partition, and every Heartbeat succeeds.
pub fn backend(request: &Request) -> Response {
    match request.api().known() {
        Some(ApiKey::Produce) => {
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
            request
                .response(&ProduceResponse::default().with_responses(topics.collect()))
                .unwrap()
        }
        Some(ApiKey::Fetch) => {
            let asked: FetchRequest = request.body().unwrap();
            let topics = asked.topics.iter().map(|topic| {
                let partitions = topic.partitions.iter().map(|partition| {
                    PartitionData::default()
                        .with_partition_index(partition.partition)
                        .with_high_watermark(HIGH_WATERMARK)
                });
                FetchableTopicResponse::default()
                    .with_topic(topic.topic.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(partitions.collect())
            });
            request
                .response(&FetchResponse::default().with_responses(topics.collect()))
                .unwrap()
        }
        Some(ApiKey::Metadata) => {
            let asked: MetadataRequest = request.body().unwrap();
            let topics = asked.topics.iter().flatten().map(|topic| {
                let partition = MetadataResponsePartition::default()
                    .with_partition_index(0)
                    .with_leader_id(1.into());
                MetadataResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(vec![partition])
            });
            request
                .response(&MetadataResponse::default().with_topics(topics.collect()))
                .unwrap()
        }
        Some(ApiKey::Heartbeat) => request.response(&HeartbeatResponse::default()).unwrap(),
        other => panic!("the backend of these tests does not answer {other:?}"),
    }
}

/// A backend that no call may reach.
pub fn no_backend(request: &Request) -> Response {
    panic!("the backend saw {}", request.api())
}

// What a request names, and what a response tells.

/// The parts a request names, in order.
pub fn named(request: &Request) -> Vec<String> {
    decoded(request).parts().iter().map(ToString::to_string).collect()
}

/// What a response tells about every part, in the order of the response:
/// `orders.eu/3 NOT_LEADER_OR_FOLLOWER` or `orders.eu/4 ok`.
pub fn told(request: &Request, response: &Response) -> Vec<String> {
    let told = decoded(request).parts_of(response).unwrap();
    told.iter()
        .map(|fields| match fields.own(Field::ErrorCode).and_then(Reading::present) {
            Some(error) => format!("{} {error}", fields.part()),
            None => format!("{} ok", fields.part()),
        })
        .collect()
}

/// Every part of `request` with `outcome`, in the order of the request: what [`told`] gives when all of them fare alike.
pub fn every_part(request: &Request, outcome: &str) -> Vec<String> {
    named(request).iter().map(|part| format!("{part} {outcome}")).collect()
}

// The stages of one call.

/// What happened to one call.
#[derive(Debug)]
pub struct Walked {
    /// What the backend saw, if it saw anything.
    pub forwarded: Option<Request>,
    /// What the client received.
    pub sent: Response,
}

/// Takes one call through both stages.
///
/// `before` holds the refusals that apply before forwarding, and `after` the ones that apply after the response.
/// Each is asked with what the ones before it left.
/// After the response, the refusals of that stage come first, and then the ones that asked to forward.
pub fn walk(
    before: &[TargetedLie],
    after: &[TargetedLie],
    request: &Request,
    backend: impl Fn(&Request) -> Response,
) -> Walked {
    let lies = every_lie();

    let mut left = request.clone();
    let mut answered = None;
    let mut asked_to_forward = Vec::new();
    for lie in before {
        match lies.before_forwarding(lie, &left).unwrap() {
            Effect::Forward(rest) => {
                left = rest;
                asked_to_forward.push(lie);
            }
            Effect::Answer(response) => answered = Some(response),
            other => panic!("{lie} asked to {other}"),
        }
    }

    let forwarded = answered.is_none().then(|| left.clone());
    let mut response = answered.unwrap_or_else(|| backend(&left));

    for lie in after.iter().chain(asked_to_forward) {
        match lies.after_response(lie, request, &response).unwrap() {
            Effect::Answer(other) => response = other,
            other => panic!("{lie} asked to {other}"),
        }
    }

    Walked {
        forwarded,
        sent: response,
    }
}
