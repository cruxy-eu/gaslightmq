//! The `serde` form of every definition type, and the rule that reading applies.
//!
//! The forms are those of the YAML in `docs/specs/domains/targeting.md`, written here as JSON.

#![cfg(feature = "serde")]

use std::fmt::Debug;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_selector::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_test::{Configure, Readable, Token, assert_de_tokens, assert_de_tokens_error, assert_tokens};

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// `json` reads as the value, although the value writes something else.
fn reads<T: DeserializeOwned + PartialEq + Debug>(json: &str, value: &T) {
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value, "{json}");
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

fn name(text: &str) -> SelectorName {
    text.parse().unwrap()
}

fn client(id: &str) -> ClientId {
    ClientId::new(id).unwrap()
}

fn topic(text: &str) -> TopicName {
    TopicName::new(text).unwrap()
}

fn pattern(field: Field, text: &str) -> Condition {
    Condition::matches(field, text.parse().unwrap())
}

fn order_producers() -> Conditions {
    Conditions::new()
        .with(Condition::is(ApiKey::PRODUCE))
        .with(pattern(Field::Topic, "orders.*"))
}

// The examples of the spec

#[test]
fn a_selector_writes_its_name_and_its_conditions() {
    // name: Order producers
    // match:
    //   api: Produce
    //   topic: "orders.*"
    let selector = Selector::new(name("Order producers"), order_producers());

    writes(
        &selector,
        r#"{"name":"Order producers","match":{"api":"Produce","topic":"orders.*"}}"#,
    );
}

#[test]
fn a_selector_writes_the_name_of_the_selector_it_includes() {
    // name: Order producers from ingest
    // include: Order producers
    // match:
    //   client.id: order-ingest
    let selector = Selector::new(
        name("Order producers from ingest"),
        Condition::is(client("order-ingest")).into(),
    )
    .including(name("Order producers"));

    writes(
        &selector,
        r#"{"name":"Order producers from ingest","include":"Order producers","match":{"client.id":"order-ingest"}}"#,
    );
}

#[test]
fn a_rule_writes_a_selector_name_or_inline_conditions() {
    // selector: Order producers
    writes(&SelectorRef::Named(name("Order producers")), r#""Order producers""#);
    // selector: { api: Metadata }
    writes(
        &SelectorRef::Inline(Condition::is(ApiKey::METADATA).into()),
        r#"{"api":"Metadata"}"#,
    );
}

#[test]
fn a_scope_writes_its_conditions() {
    // scope:
    //   client.id: order-ingest
    writes(
        &Scope::client(client("order-ingest")),
        r#"{"client.id":"order-ingest"}"#,
    );
    writes(
        &Scope::client_instance(client("order-ingest"), "10.0.4.27".parse().unwrap()),
        r#"{"host":"10.0.4.27","client.id":"order-ingest"}"#,
    );
    // A connection number is the number 7, because `#7` starts a comment in YAML.
    writes(&Scope::connection(ConnectionNumber::new(7)), r#"{"connection":7}"#);
    writes(
        &Scope::endpoint(Endpoint::Broker(BrokerId::from(2))),
        r#"{"endpoint":"broker 2"}"#,
    );
    writes(&Scope::all(), "{}");
}

// The form of each operator

#[test]
fn is_writes_the_value() {
    writes(&Condition::is(ApiKey::PRODUCE), r#"{"api":"Produce"}"#);
    writes(&Condition::is(PartitionNumber::from(3)), r#"{"partition":3}"#);
    writes(&Condition::is(Acks::ALL), r#"{"acks":-1}"#);
    writes(&Condition::is(ApiVersion::from(11)), r#"{"api.version":11}"#);
    writes(&Condition::is(Outcome::Ok), r#"{"outcome":"ok"}"#);
    writes(
        &Condition::is(ErrorCode::NOT_LEADER_OR_FOLLOWER),
        r#"{"error.code":"NOT_LEADER_OR_FOLLOWER"}"#,
    );
    writes(&Condition::is(Latency::from_millis(14)), r#"{"latency":"14ms"}"#);
}

#[test]
fn is_any_of_writes_a_list_of_values() {
    writes(
        &Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP]).unwrap(),
        r#"{"api":["JoinGroup","SyncGroup"]}"#,
    );
    writes(
        &Condition::is_any_of([PartitionNumber::from(0), PartitionNumber::from(3)]).unwrap(),
        r#"{"partition":[0,3]}"#,
    );
    writes(
        &Condition::is_any_of([ApiKey::PRODUCE]).unwrap(),
        r#"{"api":["Produce"]}"#,
    );
}

#[test]
fn matches_writes_the_pattern() {
    writes(&pattern(Field::Topic, "orders.*"), r#"{"topic":"orders.*"}"#);
    writes(
        &pattern(Field::GroupInstanceId, "etl-?"),
        r#"{"group.instance.id":"etl-?"}"#,
    );
    writes(&pattern(Field::Api, "*Group*"), r#"{"api":"*Group*"}"#);
    writes(&pattern(Field::Partition, "1*"), r#"{"partition":"1*"}"#);
}

#[test]
fn is_not_writes_a_map_with_its_key() {
    writes(
        &Condition::is_not(client("kafka-ui")),
        r#"{"client.id":{"is-not":"kafka-ui"}}"#,
    );
    writes(
        &Condition::IsNot(ValueSet::new([client("kafka-ui"), client("kafka-exporter")]).unwrap()),
        r#"{"client.id":{"is-not":["kafka-exporter","kafka-ui"]}}"#,
    );
    writes(
        &Condition::is_not(PartitionNumber::from(3)),
        r#"{"partition":{"is-not":3}}"#,
    );
    reads(
        r#"{"client.id":{"is-not":["kafka-ui"]}}"#,
        &Condition::is_not(client("kafka-ui")),
    );
}

#[test]
fn every_operator_has_a_form_that_names_it() {
    reads(r#"{"api":{"is":"Produce"}}"#, &Condition::is(ApiKey::PRODUCE));
    reads(
        r#"{"api":{"is-any-of":["JoinGroup","SyncGroup"]}}"#,
        &Condition::is_any_of([ApiKey::JOIN_GROUP, ApiKey::SYNC_GROUP]).unwrap(),
    );
    reads(
        r#"{"api":{"is-any-of":"Produce"}}"#,
        &Condition::is_any_of([ApiKey::PRODUCE]).unwrap(),
    );
    reads(
        r#"{"topic":{"matches":"orders.*"}}"#,
        &pattern(Field::Topic, "orders.*"),
    );
    reads(r#"{"partition":{"is":3}}"#, &Condition::is(PartitionNumber::from(3)));
}

#[test]
fn a_value_that_reads_as_a_pattern_names_its_operator() {
    // A client ID can hold any character. This one is not a pattern.
    writes(&Condition::is(client("batch-*")), r#"{"client.id":{"is":"batch-*"}}"#);
    writes(
        &Condition::is_any_of([client("batch-*"), client("etl")]).unwrap(),
        r#"{"client.id":{"is-any-of":["batch-*","etl"]}}"#,
    );
    writes(
        &Condition::is_not(client("what?")),
        r#"{"client.id":{"is-not":"what?"}}"#,
    );
    assert_ne!(
        serde_json::from_str::<Condition>(r#"{"client.id":"batch-*"}"#).unwrap(),
        Condition::is(client("batch-*"))
    );
}

#[test]
fn a_pattern_without_a_wildcard_names_its_operator() {
    writes(
        &pattern(Field::Topic, "orders.eu"),
        r#"{"topic":{"matches":"orders.eu"}}"#,
    );
    assert_ne!(pattern(Field::Topic, "orders.eu"), Condition::is(topic("orders.eu")));
}

#[test]
fn every_condition_reads_back_from_what_it_writes() {
    let conditions = [
        Condition::is(ConnectionNumber::new(7)),
        Condition::is("10.0.4.27:51459".parse::<ClientAddress>().unwrap()),
        Condition::is(Endpoint::Bootstrap),
        Condition::is(ClientSoftware::new("librdkafka", "2.6.1").unwrap()),
        Condition::is(client("a?b")),
        Condition::is_any_of([topic("orders.eu"), topic("orders.us")]).unwrap(),
        Condition::is_any_of([GroupId::new("*").unwrap()]).unwrap(),
        pattern(Field::Connection, "#1?"),
        pattern(Field::Latency, "*ms"),
        pattern(Field::ClientId, "literal"),
        Condition::is_not(Outcome::Timeout),
        Condition::IsNot(ValueSet::new([Acks::ALL, Acks::LEADER]).unwrap()),
        Condition::is_not(MemberId::new("consumer-1-*").unwrap()),
        Condition::is(TransactionalId::new("payments-tx-1").unwrap()),
    ];

    for condition in conditions {
        let json = serde_json::to_string(&condition).unwrap();
        assert_eq!(serde_json::from_str::<Condition>(&json).unwrap(), condition, "{json}");

        let alone = Conditions::from(condition);
        assert_eq!(serde_json::to_string(&alone).unwrap(), json);
        assert_eq!(serde_json::from_str::<Conditions>(&json).unwrap(), alone, "{json}");
    }
}

// Conditions

#[test]
fn conditions_write_a_map_in_the_order_of_the_fields() {
    let conditions = order_producers()
        .with(Condition::is_not(client("kafka-ui")))
        .with(Condition::is_any_of([PartitionNumber::from(3), PartitionNumber::from(0)]).unwrap());

    writes(
        &conditions,
        r#"{"client.id":{"is-not":"kafka-ui"},"api":"Produce","topic":"orders.*","partition":[0,3]}"#,
    );
    reads(
        r#"{"partition":[3,0,3],"topic":"orders.*","api":"Produce","client.id":{"is-not":"kafka-ui"}}"#,
        &conditions,
    );
    writes(&Conditions::new(), "{}");
}

#[test]
fn a_field_has_one_condition() {
    rejects::<Conditions>(r#"{"api":"Produce","api":"Fetch"}"#, "api has two conditions");
}

#[test]
fn a_condition_alone_is_a_map_with_one_entry() {
    rejects::<Condition>("{}", "expected a map with one field and its condition");
    rejects::<Condition>(
        r#"{"api":"Produce","topic":"orders.*"}"#,
        "expected a map with one field and its condition",
    );
    rejects::<Condition>(r#""api""#, "expected a map with one field and its condition");
}

// What reading checks

#[test]
fn a_field_that_does_not_exist_is_an_error() {
    rejects::<Conditions>(r#"{"client.name":"order-ingest"}"#, "no field is named \"client.name\"");
    rejects::<Selector>(
        r#"{"name":"x","match":{"Topic":"orders.eu"}}"#,
        "no field is named \"Topic\"",
    );
}

#[test]
fn a_value_that_is_not_of_its_field_is_an_error() {
    // `Prodcue` is an error, and not a selector that never matches.
    rejects::<Conditions>(r#"{"api":"Prodcue"}"#, "api: ");
    rejects::<Conditions>(r#"{"api":"Prodcue"}"#, "Prodcue");
    rejects::<Conditions>(r#"{"api":["Produce","Prodcue"]}"#, "api: ");
    rejects::<Conditions>(r#"{"api":{"is-not":"Prodcue"}}"#, "api: ");
    rejects::<Conditions>(r#"{"topic":"orders/eu"}"#, "topic: ");
    rejects::<Conditions>(r#"{"partition":-1}"#, "partition: ");
    rejects::<Conditions>(r#"{"partition":"three"}"#, "partition: ");
    rejects::<Conditions>(r#"{"outcome":"okay"}"#, "outcome: ");
    rejects::<Conditions>(r#"{"group":""}"#, "group: ");
}

#[test]
fn a_number_is_a_value_of_a_field_that_holds_numbers() {
    reads(r#"{"partition":3}"#, &Condition::is(PartitionNumber::from(3)));
    reads(r#"{"partition":"3"}"#, &Condition::is(PartitionNumber::from(3)));
    reads(r#"{"connection":7}"#, &Condition::is(ConnectionNumber::new(7)));
    reads(r##"{"connection":"#7"}"##, &Condition::is(ConnectionNumber::new(7)));
    reads(r#"{"acks":-1}"#, &Condition::is(Acks::ALL));
    reads(r#"{"acks":"all"}"#, &Condition::is(Acks::ALL));
    reads(
        r#"{"connection":[7,8]}"#,
        &Condition::is_any_of([ConnectionNumber::new(7), ConnectionNumber::new(8)]).unwrap(),
    );
}

#[test]
fn a_number_is_also_the_text_of_its_digits() {
    // YAML reads `group: 42` as a number, and the group is called `42`.
    reads(r#"{"group":42}"#, &Condition::is(GroupId::new("42").unwrap()));
    reads(r#"{"client.id":{"is-not":7}}"#, &Condition::is_not(client("7")));
    reads(
        r#"{"topic":[2024,2025]}"#,
        &Condition::is_any_of([topic("2024"), topic("2025")]).unwrap(),
    );
    reads(r#"{"api":0}"#, &Condition::is(ApiKey::PRODUCE));
    rejects::<Conditions>(r#"{"outcome":1}"#, "outcome: ");
}

#[test]
fn a_pattern_inside_a_list_is_an_error() {
    rejects::<Conditions>(
        r#"{"topic":["orders.*","payments.settled"]}"#,
        "topic: \"orders.*\" is a pattern, and a list holds values",
    );
    rejects::<Conditions>(
        r#"{"client.id":["batch-?"]}"#,
        "client.id: \"batch-?\" is a pattern, and a list holds values",
    );
}

#[test]
fn a_condition_needs_a_value() {
    rejects::<Conditions>(r#"{"api":[]}"#, "a condition needs at least one value");
    rejects::<Conditions>(r#"{"api":{"is-not":[]}}"#, "a condition needs at least one value");
    rejects::<Conditions>(r#"{"api":{"is-any-of":[]}}"#, "a condition needs at least one value");
    rejects::<Conditions>(r#"{"topic":{"matches":""}}"#, "a pattern is empty");
    rejects::<Conditions>(r#"{"topic":""}"#, "topic: ");
}

#[test]
fn a_map_names_one_operator_that_exists() {
    rejects::<Conditions>(r#"{"api":{}}"#, "expected a map with one operator");
    rejects::<Conditions>(
        r#"{"api":{"is":"Produce","is-not":"Fetch"}}"#,
        "expected a map with one operator",
    );
    rejects::<Conditions>(
        r#"{"api":{"not":"Produce"}}"#,
        "unknown variant `not`, expected one of `is`, `is-any-of`, `matches`, `is-not`",
    );
    rejects::<Conditions>(r#"{"api":{"is not":"Produce"}}"#, "unknown variant `is not`");
    rejects::<Conditions>(r#"{"api":{"is":["Produce"]}}"#, "expected a value of api");
    rejects::<Conditions>(r#"{"topic":{"matches":["orders.*"]}}"#, "expected a string");
}

#[test]
fn anything_else_is_no_condition() {
    for json in [r#"{"api":true}"#, r#"{"api":null}"#, r#"{"api":1.5}"#] {
        rejects::<Conditions>(
            json,
            "expected a value of api, a pattern, a list of values, or a map with one operator",
        );
    }
    rejects::<Conditions>(r#"{"api":[true]}"#, "expected a value of api");
    rejects::<Conditions>(r#"{"api":[["Produce"]]}"#, "expected a value of api");
    rejects::<Conditions>(r#"["api"]"#, "expected a map of fields and their conditions");
}

// Scope, names and selectors

#[test]
fn a_scope_only_reads_connection_fields() {
    rejects::<Scope>(
        r#"{"client.id":"order-ingest","topic":"orders.*"}"#,
        "a scope only tests connection fields, and topic is a request field",
    );
    rejects::<Scope>(
        r#"{"group":"analytics-etl"}"#,
        "a scope only tests connection fields, and group is a learned field",
    );
    reads(
        r#"{"client.id":"order-*","host":{"is-not":"10.0.4.99"}}"#,
        &Scope::new(
            Conditions::new()
                .with(pattern(Field::ClientId, "order-*"))
                .with(Condition::is_not("10.0.4.99".parse::<ClientHost>().unwrap())),
        )
        .unwrap(),
    );
}

#[test]
fn a_name_and_a_pattern_write_their_text() {
    writes(&name("Order producers"), r#""Order producers""#);
    writes(&"orders.*".parse::<Glob>().unwrap(), r#""orders.*""#);
    rejects::<SelectorName>(r#""""#, "selector name \"\" is empty");
    rejects::<SelectorName>(r#"" x""#, "starts or ends with white space");
    rejects::<SelectorName>("7", "expected a string");
    rejects::<Glob>(r#""""#, "a pattern is empty");
}

#[test]
fn a_selector_without_conditions_writes_its_name_alone() {
    writes(
        &Selector::new(name("All clients"), Conditions::new()),
        r#"{"name":"All clients"}"#,
    );
    writes(
        &Selector::new(name("Alias"), Conditions::new()).including(name("Order producers")),
        r#"{"name":"Alias","include":"Order producers"}"#,
    );
    reads(
        r#"{"name":"All clients","match":{}}"#,
        &Selector::new(name("All clients"), Conditions::new()),
    );
    reads(
        r#"{"match":{"api":"Produce"},"name":"Produce"}"#,
        &Selector::new(name("Produce"), Condition::is(ApiKey::PRODUCE).into()),
    );
}

#[test]
fn a_selector_needs_a_name_and_knows_its_keys() {
    rejects::<Selector>(r#"{"match":{"api":"Produce"}}"#, "missing field `name`");
    rejects::<Selector>(r#"{"name":""}"#, "selector name \"\" is empty");
    rejects::<Selector>(r#"{"name":"x","includes":"y"}"#, "unknown field `includes`");
    rejects::<Selector>(r#"{"name":"x","include":["y","z"]}"#, "expected a string");
    rejects::<Selector>(r#"{"name":"x","conditions":{}}"#, "unknown field `conditions`");
}

#[test]
fn a_reference_is_text_or_a_map() {
    rejects::<SelectorRef>(
        "7",
        "expected the name of a selector, or a map of fields and their conditions",
    );
    rejects::<SelectorRef>(r#""""#, "selector name \"\" is empty");
    rejects::<SelectorRef>(r#"{"api":"Prodcue"}"#, "api: ");
    reads("{}", &SelectorRef::Inline(Conditions::new()));
}

#[test]
fn the_selectors_of_a_file_make_a_set() {
    let json = r#"[
        {"name": "Order producers", "match": {"api": "Produce", "topic": "orders.*"}},
        {"name": "Order producers from ingest", "include": "Order producers", "match": {"client.id": "order-ingest"}}
    ]"#;
    let selectors: Vec<Selector> = serde_json::from_str(json).unwrap();
    let set = SelectorSet::new(selectors).unwrap();

    assert_eq!(
        set.effective(&name("Order producers from ingest")).unwrap().to_string(),
        "api is Produce and topic matches orders.* and client.id is order-ingest"
    );
}

// Any format that describes itself

#[test]
fn the_forms_are_those_of_any_format_that_describes_itself() {
    assert_tokens(
        &Selector::new(name("Order producers"), order_producers())
            .including(name("Producers"))
            .readable(),
        &[
            Token::Struct {
                name: "Selector",
                len: 3,
            },
            Token::Str("name"),
            Token::Str("Order producers"),
            Token::Str("include"),
            Token::Some,
            Token::Str("Producers"),
            Token::Str("match"),
            Token::Map { len: Some(2) },
            Token::Str("api"),
            Token::Str("Produce"),
            Token::Str("topic"),
            Token::Str("orders.*"),
            Token::MapEnd,
            Token::StructEnd,
        ],
    );
    assert_de_tokens(
        &Condition::is_any_of([PartitionNumber::from(0), PartitionNumber::from(3)])
            .unwrap()
            .readable(),
        &[
            Token::Map { len: Some(1) },
            Token::Str("partition"),
            Token::Seq { len: Some(2) },
            Token::I64(3),
            Token::U8(0),
            Token::SeqEnd,
            Token::MapEnd,
        ],
    );
    assert_de_tokens(
        &Condition::is_not(Acks::ALL).readable(),
        &[
            Token::Map { len: Some(1) },
            Token::Str("acks"),
            Token::Map { len: Some(1) },
            Token::Str("is-not"),
            Token::I8(-1),
            Token::MapEnd,
            Token::MapEnd,
        ],
    );
    assert_de_tokens_error::<Readable<Conditions>>(
        &[
            Token::Map { len: Some(1) },
            Token::Str("topic"),
            Token::Seq { len: Some(0) },
            Token::SeqEnd,
        ],
        "a condition needs at least one value",
    );
}
