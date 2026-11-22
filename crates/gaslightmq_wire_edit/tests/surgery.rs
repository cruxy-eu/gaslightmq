//! Surgery on single requests, with the responses read field by field.
//!
//! `parts.rs` runs every API with parts through the same steps.
//! These tests look at what the steps leave on the wire: which bytes the backend sees,
//! what stays of its response, and what an API does that the others do not.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::messages::fetch_request::{FetchPartition, FetchTopic};
use gaslightmq_wire::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use gaslightmq_wire::messages::metadata_request::MetadataRequestTopic;
use gaslightmq_wire::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use gaslightmq_wire::messages::offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics};
use gaslightmq_wire::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
};
use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{
    self, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest,
    HeartbeatResponse, MetadataRequest, MetadataResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest,
    ProduceResponse,
};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{Api, ApiKey as KnownApi, Frame, Record, RecordBatch, Request, Response, WireError};
use gaslightmq_wire_edit::prelude::*;

const NOT_LEADER: ErrorCode = ErrorCode::NOT_LEADER_OR_FOLLOWER;
const TIMED_OUT: ErrorCode = ErrorCode::REQUEST_TIMED_OUT;

fn part(text: &str) -> Part {
    text.parse().unwrap()
}

fn text(text: &'static str) -> StrBytes {
    StrBytes::from_static_str(text)
}

fn wire_name(name: &'static str) -> messages::TopicName {
    text(name).into()
}

fn decode(request: &Request) -> EditableRequest {
    EditableRequest::decode(request, &TopicNames::new()).unwrap()
}

fn holds(bytes: &[u8], needle: &str) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle.as_bytes())
}

/// A partition of a Produce with one record, whose value is `value`.
fn produced(index: i32, value: &'static str) -> PartitionProduceData {
    let records = RecordBatch::new([Record::new(value)]).encode().unwrap();
    PartitionProduceData::default()
        .with_index(index)
        .with_records(Some(records))
}

/// A Produce for `orders.eu/3`, `orders.eu/4` and `payments.settled/0`, each with a record that names its partition.
fn produce_request() -> Request {
    let orders = TopicProduceData::default()
        .with_name(wire_name("orders.eu"))
        .with_partition_data(vec![
            produced(3, "a record for orders.eu/3"),
            produced(4, "a record for orders.eu/4"),
        ]);
    let payments = TopicProduceData::default()
        .with_name(wire_name("payments.settled"))
        .with_partition_data(vec![produced(0, "a record for payments.settled/0")]);
    let body = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30_000)
        .with_topic_data(vec![orders, payments]);
    Request::new(9, 42, Some("order-ingest"), &body).unwrap()
}

/// One topic of a Produce response, with a base offset for each partition.
fn appended(name: &'static str, partitions: &[(i32, i64)]) -> TopicProduceResponse {
    let partitions = partitions.iter().map(|&(index, base_offset)| {
        PartitionProduceResponse::default()
            .with_index(index)
            .with_base_offset(base_offset)
    });
    TopicProduceResponse::default()
        .with_name(wire_name(name))
        .with_partition_responses(partitions.collect())
}

/// Every partition of a Produce response: topic, partition, error code and base offset.
fn produce_response(response: &Response) -> Vec<(String, i32, i16, i64)> {
    let body: ProduceResponse = response.body().unwrap();
    body.responses
        .iter()
        .flat_map(|topic| {
            topic
                .partition_responses
                .iter()
                .map(|partition| {
                    (
                        topic.name.to_string(),
                        partition.index,
                        partition.error_code,
                        partition.base_offset,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn the_records_of_a_removed_partition_never_reach_the_backend() {
    let request = produce_request();
    let asked = decode(&request);
    assert!(holds(request.frame().payload(), "a record for orders.eu/3"));

    let Remainder::Rest(forwarded) = asked.without([&part("orders.eu/3")]).unwrap() else {
        panic!("two partitions are left");
    };

    assert!(!holds(forwarded.frame().payload(), "a record for orders.eu/3"));
    assert!(holds(forwarded.frame().payload(), "a record for orders.eu/4"));
    assert!(holds(forwarded.frame().payload(), "a record for payments.settled/0"));

    // Everything else of the request is as the client sent it.
    let body: ProduceRequest = forwarded.body().unwrap();
    assert_eq!(body.acks, -1);
    assert_eq!(body.timeout_ms, 30_000);
    assert_eq!(forwarded.api(), request.api());
    assert_eq!(forwarded.correlation_id(), 42);
    assert_eq!(forwarded.client_id(), Some("order-ingest"));

    // The original request is untouched.
    assert_eq!(asked.request().frame(), request.frame());
}

#[test]
fn a_topic_leaves_the_request_with_its_last_partition() {
    let asked = decode(&produce_request());

    let Remainder::Rest(forwarded) = asked.without([&part("payments.settled/0")]).unwrap() else {
        panic!("two partitions are left");
    };

    let body: ProduceRequest = forwarded.body().unwrap();
    assert_eq!(body.topic_data.len(), 1);
    assert_eq!(body.topic_data[0].name.as_str(), "orders.eu");
    assert_eq!(body.topic_data[0].partition_data.len(), 2);
}

#[test]
fn a_refused_partition_joins_the_response_of_the_backend_in_the_order_of_the_request() {
    let asked = decode(&produce_request());
    let three = part("orders.eu/3");
    let Remainder::Rest(forwarded) = asked.without([&three]).unwrap() else {
        panic!("two partitions are left");
    };

    // The backend answers in an order of its own, and throttles the client.
    let truth = forwarded
        .response(
            &ProduceResponse::default()
                .with_throttle_time_ms(25)
                .with_responses(vec![
                    appended("payments.settled", &[(0, 700)]),
                    appended("orders.eu", &[(4, 120)]),
                ]),
        )
        .unwrap();

    let delivered = asked
        .merge(&truth, &Failures::none().with_part(three, NOT_LEADER))
        .unwrap();

    assert_eq!(
        produce_response(&delivered),
        [
            ("orders.eu".to_owned(), 3, NOT_LEADER.value(), -1),
            ("orders.eu".to_owned(), 4, 0, 120),
            ("payments.settled".to_owned(), 0, 0, 700),
        ]
    );
    let body: ProduceResponse = delivered.body().unwrap();
    assert_eq!(body.throttle_time_ms, 25);
    assert_eq!(delivered.correlation_id(), 42);
}

#[test]
fn a_partition_refused_after_the_response_loses_what_the_backend_said_about_it() {
    let request = produce_request();
    let asked = decode(&request);
    let truth = request
        .response(&ProduceResponse::default().with_responses(vec![
            appended("orders.eu", &[(3, 119), (4, 120)]),
            appended("payments.settled", &[(0, 700)]),
        ]))
        .unwrap();

    let delivered = asked
        .merge(&truth, &Failures::none().with_part(part("orders.eu/4"), TIMED_OUT))
        .unwrap();

    assert_eq!(
        produce_response(&delivered),
        [
            ("orders.eu".to_owned(), 3, 0, 119),
            ("orders.eu".to_owned(), 4, TIMED_OUT.value(), -1),
            ("payments.settled".to_owned(), 0, 0, 700),
        ]
    );
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        [
            PartFields::new(part("orders.eu/3")).succeeded(),
            PartFields::new(part("orders.eu/4")).failed(TIMED_OUT),
            PartFields::new(part("payments.settled/0")).succeeded(),
        ]
    );
}

#[test]
fn a_request_names_each_part_once() {
    let twice = TopicProduceData::default()
        .with_name(wire_name("orders.eu"))
        .with_partition_data(vec![produced(3, "one"), produced(3, "two")]);
    let request = Request::new(9, 1, None, &ProduceRequest::default().with_topic_data(vec![twice])).unwrap();

    assert_eq!(decode(&request).parts(), [part("orders.eu/3")]);
}

#[test]
fn a_partition_with_a_number_no_partition_has_is_no_part() {
    let topic = TopicProduceData::default()
        .with_name(wire_name("orders.eu"))
        .with_partition_data(vec![produced(-1, "nowhere"), produced(3, "somewhere")]);
    let request = Request::new(9, 1, None, &ProduceRequest::default().with_topic_data(vec![topic])).unwrap();
    let asked = decode(&request);

    assert_eq!(asked.parts(), [part("orders.eu/3")]);

    // It stays in the request, and it only fails with the whole call.
    let Remainder::Rest(forwarded) = asked.without(asked.parts()).unwrap() else {
        panic!("the partition without a part is left");
    };
    let body: ProduceRequest = forwarded.body().unwrap();
    assert_eq!(body.topic_data[0].partition_data.len(), 1);
    assert_eq!(body.topic_data[0].partition_data[0].index, -1);

    let whole = asked.error_response(&Failures::whole_call(TIMED_OUT)).unwrap();
    assert_eq!(
        produce_response(&whole),
        [
            ("orders.eu".to_owned(), -1, TIMED_OUT.value(), -1),
            ("orders.eu".to_owned(), 3, TIMED_OUT.value(), -1),
        ]
    );
}

/// The ID that a Fetch of version 13 names a topic by: sixteen times `byte`.
fn fetched_by_id(byte: u8, partitions: &[i32]) -> FetchTopic {
    let partitions = partitions
        .iter()
        .map(|index| FetchPartition::default().with_partition(*index));
    FetchTopic::default()
        .with_topic_id(vec![byte; 16].try_into().unwrap())
        .with_partitions(partitions.collect())
}

/// Every partition of a Fetch response: the first byte of the topic ID, the partition and the error code.
fn fetch_response(body: &FetchResponse) -> Vec<(u8, i32, i16)> {
    body.responses
        .iter()
        .flat_map(|topic| {
            let id = topic.topic_id.into_bytes()[0];
            topic
                .partitions
                .iter()
                .map(move |partition| (id, partition.partition_index, partition.error_code))
        })
        .collect()
}

#[test]
fn a_topic_named_by_its_id_is_a_part_when_its_name_is_known() {
    let body = FetchRequest::default().with_topics(vec![fetched_by_id(1, &[0, 1]), fetched_by_id(9, &[0])]);
    let request = Request::new(13, 42, None, &body).unwrap();
    let mut topics = TopicNames::new();
    topics.learn(TopicId::new([1; 16]).unwrap(), TopicName::new("orders.eu").unwrap());

    let asked = EditableRequest::decode(&request, &topics).unwrap();
    // The editable request keeps the names it needs.
    drop(topics);

    // Nobody can name the topic with the unknown ID, so it is no part.
    assert_eq!(asked.parts(), [part("orders.eu/0"), part("orders.eu/1")]);

    // It stays in the request when every part is removed.
    let Remainder::Rest(forwarded) = asked.without(asked.parts()).unwrap() else {
        panic!("the topic with the unknown ID is left");
    };
    let rest: FetchRequest = forwarded.body().unwrap();
    assert_eq!(rest.topics.len(), 1);
    assert_eq!(rest.topics[0].topic_id.into_bytes(), [9; 16]);

    // A failed part is in the response under the ID the client used.
    let failures = Failures::none().with_part(part("orders.eu/1"), NOT_LEADER);
    let failed: FetchResponse = asked.error_response(&failures).unwrap().body().unwrap();
    assert_eq!(fetch_response(&failed), [(1, 1, NOT_LEADER.value())]);
    assert_eq!(failed.error_code, 0);
    assert_eq!(failed.responses[0].partitions[0].high_watermark, -1);

    // The backend answers for the rest, and the client gets one response for everything it asked.
    let truth = forwarded
        .response(&FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic_id(vec![9; 16].try_into().unwrap())
                    .with_partitions(vec![PartitionData::default().with_high_watermark(55)]),
            ]))
        .unwrap();
    let all = Failures::none()
        .with_part(part("orders.eu/0"), NOT_LEADER)
        .with_part(part("orders.eu/1"), TIMED_OUT);
    let delivered = asked.merge(&truth, &all).unwrap();
    let body: FetchResponse = delivered.body().unwrap();
    assert_eq!(
        fetch_response(&body),
        [(1, 0, NOT_LEADER.value()), (1, 1, TIMED_OUT.value()), (9, 0, 0)]
    );
    assert_eq!(body.responses[1].partitions[0].high_watermark, 55);
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        [
            PartFields::new(part("orders.eu/0")).failed(NOT_LEADER),
            PartFields::new(part("orders.eu/1")).failed(TIMED_OUT),
        ]
    );
}

#[test]
fn a_whole_fetch_fails_in_the_response_itself_and_in_every_partition() {
    let body = FetchRequest::default().with_topics(vec![fetched_by_id(1, &[0]), fetched_by_id(9, &[0])]);
    let asked = decode(&Request::new(13, 42, None, &body).unwrap());

    let failed: FetchResponse = asked
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();

    assert_eq!(failed.error_code, TIMED_OUT.value());
    assert_eq!(
        fetch_response(&failed),
        [(1, 0, TIMED_OUT.value()), (9, 0, TIMED_OUT.value())]
    );
}

#[test]
fn a_whole_fetch_of_an_old_version_fails_in_every_partition_only() {
    let topic = FetchTopic::default()
        .with_topic(wire_name("orders.eu"))
        .with_partitions(vec![FetchPartition::default().with_partition(3)]);
    let request = Request::new(6, 42, None, &FetchRequest::default().with_topics(vec![topic])).unwrap();

    let failed: FetchResponse = decode(&request)
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();

    // Version 6 has no error code for the response itself.
    assert_eq!(failed.error_code, 0);
    assert_eq!(failed.responses[0].topic.as_str(), "orders.eu");
    assert_eq!(failed.responses[0].partitions[0].error_code, TIMED_OUT.value());
}

fn metadata_request(version: i16, topics: Option<&[&'static str]>) -> Request {
    let topics = topics.map(|names| {
        names
            .iter()
            .map(|name| MetadataRequestTopic::default().with_name(Some(wire_name(name))))
            .collect()
    });
    Request::new(version, 42, None, &MetadataRequest::default().with_topics(topics)).unwrap()
}

/// One topic of a Metadata response, with an ID of sixteen times `byte`.
fn described(name: &'static str, byte: u8) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_name(Some(wire_name(name)))
        .with_topic_id(vec![byte; 16].try_into().unwrap())
}

#[test]
fn the_parts_of_a_metadata_request_are_topics() {
    let request = metadata_request(12, Some(&["orders.eu", "payments.settled"]));
    let asked = decode(&request);
    let orders = part("orders.eu");
    assert_eq!(asked.parts(), [orders.clone(), part("payments.settled")]);

    let Remainder::Rest(forwarded) = asked.without([&orders]).unwrap() else {
        panic!("one topic is left");
    };
    assert_eq!(decode(&forwarded).parts(), [part("payments.settled")]);

    // The backend describes the cluster and the topic it saw. The refused topic joins it, in front.
    let truth = forwarded
        .response(
            &MetadataResponse::default()
                .with_brokers(vec![MetadataResponseBroker::default().with_node_id(2.into())])
                .with_topics(vec![described("payments.settled", 2)]),
        )
        .unwrap();
    let delivered = asked
        .merge(&truth, &Failures::none().with_part(orders.clone(), NOT_LEADER))
        .unwrap();

    let body: MetadataResponse = delivered.body().unwrap();
    assert_eq!(body.brokers.len(), 1);
    let topics: Vec<(&str, i16)> = body
        .topics
        .iter()
        .map(|topic| (topic.name.as_ref().unwrap().as_str(), topic.error_code))
        .collect();
    assert_eq!(topics, [("orders.eu", NOT_LEADER.value()), ("payments.settled", 0)]);
    assert_eq!(
        asked.parts_of(&delivered).unwrap(),
        [
            PartFields::new(orders).failed(NOT_LEADER),
            PartFields::new(part("payments.settled")).succeeded(),
        ]
    );
}

#[test]
fn a_topic_refused_after_the_response_replaces_the_topic_the_backend_described() {
    let request = metadata_request(12, Some(&["orders.eu", "payments.settled"]));
    let asked = decode(&request);
    // The backend names each topic with its ID, which the request did not.
    let truth = request
        .response(
            &MetadataResponse::default().with_topics(vec![described("orders.eu", 1), described("payments.settled", 2)]),
        )
        .unwrap();

    let delivered = asked
        .merge(&truth, &Failures::none().with_part(part("payments.settled"), TIMED_OUT))
        .unwrap();

    let body: MetadataResponse = delivered.body().unwrap();
    let topics: Vec<(&str, i16)> = body
        .topics
        .iter()
        .map(|topic| (topic.name.as_ref().unwrap().as_str(), topic.error_code))
        .collect();
    assert_eq!(topics, [("orders.eu", 0), ("payments.settled", TIMED_OUT.value())]);
}

#[test]
fn a_metadata_request_for_every_topic_has_no_parts() {
    let asked = decode(&metadata_request(12, None));
    assert_eq!(asked.parts(), []);
    assert!(matches!(asked.without([&part("orders.eu")]), Ok(Remainder::Whole)));

    // Version 12 has no place for the error of a request that names nothing.
    let failed: MetadataResponse = asked
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();
    assert_eq!(failed.topics, []);
    assert_eq!(failed.error_code, 0);

    // Version 13 has one.
    let failed: MetadataResponse = decode(&metadata_request(13, None))
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();
    assert_eq!(failed.error_code, TIMED_OUT.value());
}

#[test]
fn nothing_is_left_of_a_metadata_request_without_its_topics() {
    // On version 0 a request without topics asks for every topic, so it must not be forwarded.
    let asked = decode(&metadata_request(0, Some(&["orders.eu"])));

    assert!(matches!(asked.without(asked.parts()), Ok(Remainder::Nothing)));
}

/// One group of an `OffsetFetch` of version 8, which asks for some partitions or for every topic.
fn group(id: &'static str, topics: Option<&[(&'static str, &[i32])]>) -> OffsetFetchRequestGroup {
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|(name, partitions)| {
                OffsetFetchRequestTopics::default()
                    .with_name(wire_name(name))
                    .with_partition_indexes(partitions.to_vec())
            })
            .collect()
    });
    OffsetFetchRequestGroup::default()
        .with_group_id(text(id).into())
        .with_topics(topics)
}

/// Every partition of an `OffsetFetch` response of version 8: group, topic, partition, error code and offset.
fn offset_fetch_response(body: &OffsetFetchResponse) -> Vec<(String, String, i32, i16, i64)> {
    let mut partitions = Vec::new();
    for group in &body.groups {
        for topic in &group.topics {
            for partition in &topic.partitions {
                partitions.push((
                    group.group_id.to_string(),
                    topic.name.to_string(),
                    partition.partition_index,
                    partition.error_code,
                    partition.committed_offset,
                ));
            }
        }
    }
    partitions
}

#[test]
fn a_partition_that_several_groups_ask_for_leaves_and_fails_in_each_of_them() {
    let body = OffsetFetchRequest::default().with_groups(vec![
        group("analytics-etl", Some(&[("orders.eu", &[3, 4])])),
        group("billing", Some(&[("orders.eu", &[3])])),
        group("audit", None),
    ]);
    let request = Request::new(8, 42, None, &body).unwrap();
    let asked = decode(&request);
    let three = part("orders.eu/3");
    assert_eq!(asked.parts(), [three.clone(), part("orders.eu/4")]);

    let Remainder::Rest(forwarded) = asked.without([&three]).unwrap() else {
        panic!("a partition and a group that asks for everything are left");
    };

    // `billing` asked for nothing else, so it leaves. `audit` asks for every topic, so it stays.
    let rest: OffsetFetchRequest = forwarded.body().unwrap();
    let groups: Vec<&str> = rest.groups.iter().map(|group| group.group_id.as_str()).collect();
    assert_eq!(groups, ["analytics-etl", "audit"]);
    assert_eq!(rest.groups[0].topics.as_ref().unwrap()[0].partition_indexes, [4]);
    assert_eq!(rest.groups[1].topics, None);

    let committed = |name: &'static str, index: i32, offset: i64| {
        OffsetFetchResponseTopics::default()
            .with_name(wire_name(name))
            .with_partitions(vec![
                OffsetFetchResponsePartitions::default()
                    .with_partition_index(index)
                    .with_committed_offset(offset),
            ])
    };
    let answered = |id: &'static str, topics| {
        OffsetFetchResponseGroup::default()
            .with_group_id(text(id).into())
            .with_topics(topics)
    };
    let truth = forwarded
        .response(&OffsetFetchResponse::default().with_groups(vec![
            answered("audit", vec![committed("payments.settled", 0, 9)]),
            answered("analytics-etl", vec![committed("orders.eu", 4, 120)]),
        ]))
        .unwrap();

    let delivered: OffsetFetchResponse = asked
        .merge(&truth, &Failures::none().with_part(three, NOT_LEADER))
        .unwrap()
        .body()
        .unwrap();

    let etl = "analytics-etl".to_owned();
    let orders = "orders.eu".to_owned();
    assert_eq!(
        offset_fetch_response(&delivered),
        [
            (etl.clone(), orders.clone(), 3, NOT_LEADER.value(), -1),
            (etl, orders.clone(), 4, 0, 120),
            ("billing".to_owned(), orders, 3, NOT_LEADER.value(), -1),
            ("audit".to_owned(), "payments.settled".to_owned(), 0, 0, 9),
        ]
    );
    // Only partitions failed, so no group failed as a whole.
    assert!(delivered.groups.iter().all(|group| group.error_code == 0));
}

#[test]
fn a_group_that_asks_for_every_topic_keeps_the_request_alive_when_every_part_is_gone() {
    let body = OffsetFetchRequest::default().with_groups(vec![
        group("billing", Some(&[("orders.eu", &[3])])),
        group("audit", None),
    ]);
    let asked = decode(&Request::new(8, 42, None, &body).unwrap());
    assert_eq!(asked.parts(), [part("orders.eu/3")]);

    // The request still asks for something that is no part, so there is something left to forward.
    let Remainder::Rest(forwarded) = asked.without(asked.parts()).unwrap() else {
        panic!("the group that asks for every topic is left");
    };

    let rest: OffsetFetchRequest = forwarded.body().unwrap();
    let groups: Vec<&str> = rest.groups.iter().map(|group| group.group_id.as_str()).collect();
    assert_eq!(groups, ["audit"]);
}

#[test]
fn a_request_without_parts_only_fails_as_a_whole() {
    let request = Request::new(4, 42, None, &HeartbeatRequest::default()).unwrap();
    let asked = decode(&request);

    assert_eq!(asked.parts(), []);
    assert!(matches!(asked.without([&part("orders.eu/3")]), Ok(Remainder::Whole)));

    let of_a_part = Failures::none().with_part(part("orders.eu/3"), NOT_LEADER);
    assert_eq!(
        asked.error_response(&of_a_part).unwrap_err(),
        EditableRequestError::NothingFailed { api: request.api() }
    );
    assert_eq!(
        asked.error_response(&Failures::none()).unwrap_err().to_string(),
        "nothing that the Heartbeat v4 request asks failed"
    );

    let failed = asked.error_response(&Failures::whole_call(TIMED_OUT)).unwrap();
    let body: HeartbeatResponse = failed.body().unwrap();
    assert_eq!(body.error_code, TIMED_OUT.value());
    assert_eq!(failed.correlation_id(), 42);
    assert_eq!(asked.parts_of(&failed).unwrap(), []);

    // The failure of the whole call replaces whatever the backend answered.
    let truth = request
        .response(&HeartbeatResponse::default().with_throttle_time_ms(25))
        .unwrap();
    let merged = asked.merge(&truth, &Failures::whole_call(TIMED_OUT)).unwrap();
    assert_eq!(merged.frame(), failed.frame());
    assert_eq!(asked.merge(&truth, &of_a_part).unwrap().frame(), truth.frame());
}

#[test]
fn find_coordinator_fails_in_the_response_or_in_every_key_it_asks_for() {
    let one = FindCoordinatorRequest::default().with_key(text("analytics-etl"));
    let failed: FindCoordinatorResponse = decode(&Request::new(3, 1, None, &one).unwrap())
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();
    assert_eq!(failed.error_code, TIMED_OUT.value());
    assert_eq!((i32::from(failed.node_id), failed.port), (-1, -1));

    let several = FindCoordinatorRequest::default().with_coordinator_keys(vec![text("analytics-etl"), text("billing")]);
    let failed: FindCoordinatorResponse = decode(&Request::new(4, 1, None, &several).unwrap())
        .error_response(&Failures::whole_call(TIMED_OUT))
        .unwrap()
        .body()
        .unwrap();
    let keys: Vec<(&str, i16, i32)> = failed
        .coordinators
        .iter()
        .map(|coordinator| {
            (
                coordinator.key.as_str(),
                coordinator.error_code,
                i32::from(coordinator.node_id),
            )
        })
        .collect();
    assert_eq!(
        keys,
        [
            ("analytics-etl", TIMED_OUT.value(), -1),
            ("billing", TIMED_OUT.value(), -1)
        ]
    );
}

#[test]
fn a_request_of_an_unknown_api_cannot_be_edited() {
    // The header of API key 9999 at version 3: the key, the version, a correlation ID and no client ID.
    let frame = Frame::new(vec![0x27, 0x0F, 0, 3, 0, 0, 0, 7, 0xFF, 0xFF, 1, 2, 3]);
    let request = Request::decode(frame).unwrap();

    let error = EditableRequest::decode(&request, &TopicNames::new()).unwrap_err();

    assert_eq!(
        error,
        EditableRequestError::UnknownApi {
            api: Api::from_wire(9999, 3)
        }
    );
    assert_eq!(
        error.to_string(),
        "a request of api 9999 v3 cannot be edited: its messages are unknown"
    );
}

#[test]
fn a_request_whose_body_does_not_decode_cannot_be_edited() {
    let whole = produce_request();
    let payload = whole.frame().payload();
    let cut_short = Request::decode(Frame::new(payload.slice(..payload.len() - 10))).unwrap();

    let error = EditableRequest::decode(&cut_short, &TopicNames::new()).unwrap_err();

    assert!(matches!(
        &error,
        EditableRequestError::UnreadableRequest {
            api,
            error: WireError::Decode { .. }
        } if *api == Api::new(KnownApi::Produce, 9)
    ));
    assert!(
        error
            .to_string()
            .starts_with("the Produce v9 request cannot be read: cannot decode request body: "),
        "{error}"
    );
}

#[test]
fn a_response_to_another_request_does_not_merge() {
    let asked = decode(&produce_request());
    let failures = Failures::none().with_part(part("orders.eu/3"), NOT_LEADER);

    let other_id = Response::new(asked.api(), 43, &ProduceResponse::default()).unwrap();
    let error = asked.merge(&other_id, &failures).unwrap_err();
    assert_eq!(
        error.to_string(),
        "the Produce v9 response 43 does not answer the Produce v9 request 42"
    );
    assert_eq!(asked.parts_of(&other_id).unwrap_err(), error);

    let other_api = Response::new(Api::new(KnownApi::Fetch, 9), 42, &FetchResponse::default()).unwrap();
    assert_eq!(
        asked.merge(&other_api, &failures).unwrap_err(),
        EditableRequestError::ResponseOfAnotherCall {
            api: asked.api(),
            correlation_id: 42,
            answered_api: Api::new(KnownApi::Fetch, 9),
            answered_correlation_id: 42,
        }
    );
}

#[test]
fn a_response_whose_body_does_not_decode_does_not_merge() {
    let asked = decode(&produce_request());
    // The correlation ID, the tagged fields of the header, and a body that stops short.
    let truth = Response::decode(asked.api(), Frame::new(vec![0, 0, 0, 42, 0, 0xFF])).unwrap();

    let error = asked
        .merge(&truth, &Failures::none().with_part(part("orders.eu/3"), NOT_LEADER))
        .unwrap_err();

    assert!(matches!(error, EditableRequestError::UnreadableResponse { .. }));
    assert!(
        error
            .to_string()
            .starts_with("the Produce v9 response cannot be read: cannot decode response body: "),
        "{error}"
    );
    assert!(matches!(
        asked.parts_of(&truth),
        Err(EditableRequestError::UnreadableResponse { .. })
    ));
}
