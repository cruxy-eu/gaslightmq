//! The rules of the targeting spec that a source has to make possible, each as a plain function call.
//!
//! The `selector` crate owns conditions and matching.
//! This file has the smallest matcher that follows the spec, and it only speaks to a [`FieldSource`].
//! Whatever it can decide, a real selector can decide too.

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

/// A condition that tests one field against one value.
enum Condition {
    Is(FieldValue),
    IsNot(FieldValue),
}

#[derive(Debug, PartialEq)]
enum Verdict {
    Matched(Target),
    NotMatched,
    /// A condition tests a field that the call does not have yet.
    Undecided,
}

fn is(field: Field, text: &str) -> Condition {
    Condition::Is(field.parse_value(text).unwrap())
}

fn is_not(field: Field, text: &str) -> Condition {
    Condition::IsNot(field.parse_value(text).unwrap())
}

/// Whether any of the values equals `value`, or `None` while the field is pending.
fn any_equals(reading: Reading<FieldValues>, value: &FieldValue) -> Option<bool> {
    match reading {
        Reading::Present(values) => Some(values.contains(value)),
        Reading::Absent => Some(false),
        Reading::Pending => None,
    }
}

/// Conditions combine with "and".
/// `is not` tests every value of the call, and `is` tests one part at a time.
fn evaluate(source: &dyn FieldSource, conditions: &[Condition]) -> Verdict {
    let mut undecided = false;
    let mut positive = Vec::new();
    for condition in conditions {
        match condition {
            Condition::Is(value) => positive.push(value),
            Condition::IsNot(value) => match any_equals(source.read(value.field()), value) {
                Some(true) => return Verdict::NotMatched,
                Some(false) => {}
                None => undecided = true,
            },
        }
    }

    // `None` when a condition fails, and otherwise whether every condition could be decided.
    let passes = |read: &dyn Fn(Field) -> Reading<FieldValues>| -> Option<bool> {
        let mut decided = true;
        for value in &positive {
            match any_equals(read(value.field()), value) {
                Some(true) => {}
                Some(false) => return None,
                None => decided = false,
            }
        }
        Some(decided)
    };

    if source.parts().is_empty() {
        return match passes(&|field| source.read(field)) {
            None => Verdict::NotMatched,
            Some(decided) if decided && !undecided => Verdict::Matched(Target::Call),
            Some(_) => Verdict::Undecided,
        };
    }

    let mut matched = Vec::new();
    for part in source.parts() {
        match passes(&|field| source.read_in(part, field)) {
            None => {}
            Some(true) => matched.push(part.part().clone()),
            Some(false) => undecided = true,
        }
    }
    match Target::of_parts(matched) {
        _ if undecided => Verdict::Undecided,
        Some(target) => Verdict::Matched(target),
        None => Verdict::NotMatched,
    }
}

fn matched(parts: &[&str]) -> Verdict {
    Verdict::Matched(Target::of_parts(parts.iter().map(|part| part.parse().unwrap())).unwrap())
}

fn partition(text: &str) -> TopicPartition {
    text.parse().unwrap()
}

fn produce(partitions: &[&str]) -> Fields {
    let call = Fields::new()
        .with(ConnectionNumber::new(7))
        .with("10.0.4.27:51459".parse::<ClientAddress>().unwrap())
        .with("10.0.4.27".parse::<ClientHost>().unwrap())
        .with(Endpoint::Broker(BrokerId::from(2)))
        .with(ClientId::new("order-ingest").unwrap())
        .with(ApiKey::PRODUCE)
        .with(ApiVersion::from(11))
        .with(Acks::ALL);
    partitions
        .iter()
        .fold(call, |call, text| call.with_part(partition(text)))
}

#[test]
fn a_positive_condition_matches_when_any_value_matches() {
    let call = produce(&["orders.eu/1", "payments.settled/3"]);

    assert_eq!(
        evaluate(&call, &[is(Field::Topic, "payments.settled")]),
        matched(&["payments.settled/3"])
    );
    assert_eq!(evaluate(&call, &[is(Field::Topic, "orders.us")]), Verdict::NotMatched);
}

#[test]
fn is_not_matches_when_no_value_equals() {
    let both = produce(&["orders.eu/1", "payments.settled/3"]);
    let settled = produce(&["payments.settled/3"]);

    assert_eq!(
        evaluate(&both, &[is_not(Field::Topic, "orders.eu")]),
        Verdict::NotMatched
    );
    assert_eq!(
        evaluate(&settled, &[is_not(Field::Topic, "orders.eu")]),
        matched(&["payments.settled/3"])
    );
}

#[test]
fn conditions_on_topic_and_partition_apply_to_the_same_partition() {
    let conditions = [is(Field::Topic, "orders.eu"), is(Field::Partition, "3")];

    assert_eq!(
        evaluate(&produce(&["orders.eu/3"]), &conditions),
        matched(&["orders.eu/3"])
    );
    assert_eq!(
        evaluate(&produce(&["orders.eu/1", "payments.settled/3"]), &conditions),
        Verdict::NotMatched
    );
}

#[test]
fn the_partitions_that_satisfy_the_conditions_are_the_target() {
    let call = produce(&["orders.eu/3", "orders.eu/4", "payments.settled/0"]);

    assert_eq!(
        evaluate(&call, &[is(Field::Topic, "orders.eu")]),
        matched(&["orders.eu/3", "orders.eu/4"])
    );
}

#[test]
fn a_selector_that_tests_no_part_targets_every_part() {
    let call = produce(&["orders.eu/3", "payments.settled/0"]);

    assert_eq!(
        evaluate(&call, &[is(Field::Api, "Produce")]),
        matched(&["orders.eu/3", "payments.settled/0"])
    );
    assert_eq!(evaluate(&call, &[]), Verdict::Matched(call.whole_target()));
}

#[test]
fn on_a_request_without_partitions_the_target_is_the_whole_call() {
    let heartbeat = Fields::new()
        .with(ApiKey::HEARTBEAT)
        .with(GroupId::new("analytics-etl").unwrap());

    assert_eq!(
        evaluate(&heartbeat, &[is(Field::Group, "analytics-etl")]),
        Verdict::Matched(Target::Call)
    );
    assert_eq!(
        evaluate(&heartbeat, &[is(Field::Topic, "orders.eu")]),
        Verdict::NotMatched
    );
    assert_eq!(
        evaluate(&heartbeat, &[is_not(Field::Topic, "orders.eu")]),
        Verdict::Matched(Target::Call)
    );
}

#[test]
fn on_a_request_that_names_topics_only_the_target_is_a_set_of_topics() {
    let topic = |name: &str| TopicName::new(name).unwrap();
    let metadata = Fields::new()
        .with(ApiKey::METADATA)
        .with_part(topic("orders.eu"))
        .with_part(topic("payments.settled"));

    assert_eq!(
        evaluate(&metadata, &[is(Field::Topic, "orders.eu")]),
        matched(&["orders.eu"])
    );
    assert_eq!(evaluate(&metadata, &[is(Field::Partition, "3")]), Verdict::NotMatched);
}

#[test]
fn a_field_that_the_request_does_not_have_fails_a_positive_condition() {
    let fetch = Fields::new().with(ApiKey::FETCH).with_part(partition("orders.eu/3"));

    assert_eq!(evaluate(&fetch, &[is(Field::Acks, "-1")]), Verdict::NotMatched);
    assert_eq!(
        evaluate(&fetch, &[is_not(Field::Acks, "-1")]),
        matched(&["orders.eu/3"])
    );
}

#[test]
fn a_fetch_matches_its_group_once_the_client_named_the_group() {
    let conditions = [is(Field::Api, "Fetch"), is(Field::Group, "analytics-etl")];
    let fetch = Fields::new().with(ApiKey::FETCH).with_part(partition("orders.eu/3"));

    assert_eq!(evaluate(&fetch, &conditions), Verdict::NotMatched);

    let learned = fetch.with_learned(GroupId::new("analytics-etl").unwrap());
    assert_eq!(evaluate(&learned, &conditions), matched(&["orders.eu/3"]));
    assert_eq!(
        learned.read(Field::Group).present().map(|groups| groups.origin()),
        Some(Origin::ClientContext)
    );
}

#[test]
fn a_client_with_several_consumers_has_several_groups() {
    let fetch = Fields::new()
        .with(ApiKey::FETCH)
        .with_learned(GroupId::new("payments").unwrap())
        .with_learned(GroupId::new("analytics-etl").unwrap());

    assert_eq!(
        evaluate(&fetch, &[is(Field::Group, "payments")]),
        Verdict::Matched(Target::Call)
    );
    assert_eq!(
        evaluate(&fetch, &[is(Field::Group, "analytics-etl")]),
        Verdict::Matched(Target::Call)
    );
    assert_eq!(
        evaluate(&fetch, &[is_not(Field::Group, "payments")]),
        Verdict::NotMatched
    );
}

#[test]
fn a_request_that_carries_its_group_matches_that_group_only() {
    let commit = Fields::new()
        .with(ApiKey::OFFSET_COMMIT)
        .with_learned(GroupId::new("payments").unwrap())
        .with(GroupId::new("analytics-etl").unwrap());

    assert_eq!(
        evaluate(&commit, &[is(Field::Group, "analytics-etl")]),
        Verdict::Matched(Target::Call)
    );
    assert_eq!(evaluate(&commit, &[is(Field::Group, "payments")]), Verdict::NotMatched);
}

#[test]
fn a_scope_tests_connection_fields_only() {
    let call = produce(&["orders.eu/3"]);
    let one_client_instance = [is(Field::ClientId, "order-ingest"), is(Field::Host, "10.0.4.27")];
    let one_connection = [is(Field::Connection, "#7")];
    let one_endpoint = [is(Field::Endpoint, "broker 2")];
    let another_client = [is(Field::ClientId, "kafka-ui")];

    for scope in [&one_client_instance[..], &one_connection, &one_endpoint] {
        assert_eq!(evaluate(&call, scope), matched(&["orders.eu/3"]));
    }
    assert_eq!(evaluate(&call, &another_client), Verdict::NotMatched);
    for scope in [
        &one_client_instance[..],
        &one_connection,
        &one_endpoint,
        &another_client,
    ] {
        for condition in scope {
            let (Condition::Is(value) | Condition::IsNot(value)) = condition;
            assert_eq!(value.field().group(), FieldGroup::Connection);
        }
    }
}

#[test]
fn a_session_that_is_not_identified_has_no_client_id() {
    let opened = Fields::new()
        .with(ConnectionNumber::new(7))
        .with("10.0.4.27".parse::<ClientHost>().unwrap())
        .with(Endpoint::Bootstrap);

    assert_eq!(opened.read(Field::ClientId), Reading::Absent);
    assert_eq!(opened.read(Field::ClientSoftware), Reading::Absent);
    assert_eq!(
        evaluate(&opened, &[is(Field::ClientId, "order-ingest")]),
        Verdict::NotMatched
    );
    assert_eq!(
        evaluate(&opened, &[is(Field::Endpoint, "bootstrap")]),
        Verdict::Matched(Target::Call)
    );
}

#[test]
fn a_rule_that_tests_a_response_field_cannot_match_before_the_backend_answers() {
    let conditions = [is(Field::Topic, "orders.eu"), is(Field::Outcome, "ok")];
    let asked = produce(&["orders.eu/3", "payments.settled/0"]);

    assert_eq!(evaluate(&asked, &conditions), Verdict::Undecided);
    assert_eq!(
        evaluate(&asked, &[is_not(Field::ErrorCode, "NOT_LEADER_OR_FOLLOWER")]),
        Verdict::Undecided
    );
    assert!(conditions.iter().any(|condition| {
        let (Condition::Is(value) | Condition::IsNot(value)) = condition;
        value.field().group() == FieldGroup::Response
    }));

    let answered = asked.with(Outcome::Ok).with(Latency::from_millis(14));
    assert_eq!(evaluate(&answered, &conditions), matched(&["orders.eu/3"]));
}

#[test]
fn a_request_condition_that_fails_decides_before_the_backend_answers() {
    let asked = produce(&["payments.settled/0"]);

    assert_eq!(
        evaluate(&asked, &[is(Field::Topic, "orders.eu"), is(Field::Outcome, "ok")]),
        Verdict::NotMatched
    );
}

#[test]
fn a_condition_on_the_truth_applies_to_the_same_partition() {
    let answered = Fields::new()
        .with(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition("orders.eu/3")).succeeded())
        .with_part(PartFields::new(partition("orders.eu/4")).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .with_part(PartFields::new(partition("payments.settled/0")).succeeded())
        .with(Latency::from_millis(14));

    assert_eq!(
        evaluate(&answered, &[is(Field::Topic, "orders.eu"), is(Field::Outcome, "ok")]),
        matched(&["orders.eu/3"])
    );
    assert_eq!(
        evaluate(&answered, &[is(Field::ErrorCode, "NOT_LEADER_OR_FOLLOWER")]),
        matched(&["orders.eu/4"])
    );
    assert_eq!(
        evaluate(
            &answered,
            &[is(Field::Topic, "payments.settled"), is(Field::Outcome, "error")]
        ),
        Verdict::NotMatched
    );
}

#[test]
fn the_response_pass_skips_a_partition_that_was_refused_before_forwarding() {
    let answered = Fields::new()
        .with(ApiKey::PRODUCE)
        .with_part(PartFields::new(partition("orders.eu/3")).without_truth())
        .with_part(PartFields::new(partition("orders.eu/4")).succeeded());

    assert_eq!(
        evaluate(&answered, &[is(Field::Outcome, "ok")]),
        matched(&["orders.eu/4"])
    );
    assert_eq!(evaluate(&answered, &[is(Field::Outcome, "error")]), Verdict::NotMatched);
}

#[test]
fn a_call_that_a_lie_ended_before_forwarding_never_has_a_truth() {
    let silenced = produce(&["orders.eu/3"]).without_truth();

    assert_eq!(evaluate(&silenced, &[is(Field::Outcome, "ok")]), Verdict::NotMatched);
    assert_eq!(silenced.read(Field::Latency), Reading::Absent);
}

/// A call as another crate holds it: typed members, and the trait on top.
struct RecordedFetch {
    client_id: Option<ClientId>,
    groups: Vec<GroupId>,
    parts: Vec<PartFields>,
    truth: Option<(Outcome, Latency)>,
}

impl FieldSource for RecordedFetch {
    fn whole_call(&self, field: Field) -> Reading<FieldValues> {
        match field {
            Field::Api => Reading::Present(FieldValues::one(ApiKey::FETCH)),
            Field::ClientId => self.client_id.clone().map(FieldValues::one).into(),
            Field::Group => FieldValues::learned(self.groups.iter().cloned()).into(),
            Field::Outcome | Field::Latency | Field::ErrorCode => match self.truth {
                None => Reading::Pending,
                Some((outcome, _)) if field == Field::Outcome => Reading::Present(FieldValues::one(outcome)),
                Some((_, latency)) if field == Field::Latency => Reading::Present(FieldValues::one(latency)),
                Some(_) => Reading::Absent,
            },
            _ => Reading::Absent,
        }
    }

    fn parts(&self) -> &[PartFields] {
        &self.parts
    }
}

#[test]
fn a_source_of_another_shape_answers_like_the_same_call_as_plain_data() {
    let recorded = RecordedFetch {
        client_id: Some(ClientId::new("analytics").unwrap()),
        groups: vec![GroupId::new("analytics-etl").unwrap()],
        parts: vec![partition("orders.eu/3").into(), partition("orders.eu/4").into()],
        truth: Some((Outcome::Ok, Latency::from_millis(14))),
    };
    let plain = Fields::new()
        .with(ApiKey::FETCH)
        .with(ClientId::new("analytics").unwrap())
        .with_learned(GroupId::new("analytics-etl").unwrap())
        .with_part(partition("orders.eu/3"))
        .with_part(partition("orders.eu/4"))
        .with(Outcome::Ok)
        .with(Latency::from_millis(14));

    for field in Field::ALL {
        assert_eq!(recorded.read(field), plain.read(field), "{field}");
        for (recorded_part, plain_part) in recorded.parts().iter().zip(plain.parts()) {
            assert_eq!(
                recorded.read_in(recorded_part, field),
                plain.read_in(plain_part, field),
                "{field}"
            );
        }
    }
    assert_eq!(recorded.whole_target(), plain.whole_target());

    let conditions = [
        is(Field::Group, "analytics-etl"),
        is(Field::Partition, "4"),
        is(Field::Outcome, "ok"),
    ];
    assert_eq!(evaluate(&recorded, &conditions), matched(&["orders.eu/4"]));
    assert_eq!(evaluate(&recorded, &conditions), evaluate(&plain, &conditions));
}
