//! The `serde` form of every definition type, and the rule that reading applies.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;
use std::fmt::{self, Debug};

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use serde::de::{DeserializeOwned, DeserializeSeed, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_test::{Compact, Configure, Token, assert_de_tokens_error, assert_tokens};

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

/// One value of every field, with the JSON of its bare form.
fn examples() -> Vec<(FieldValue, &'static str)> {
    vec![
        (ConnectionNumber::new(7).into(), "7"),
        (
            "10.0.4.27:51459".parse::<ClientAddress>().unwrap().into(),
            r#""10.0.4.27:51459""#,
        ),
        ("10.0.4.27".parse::<ClientHost>().unwrap().into(), r#""10.0.4.27""#),
        (Endpoint::Broker(BrokerId::from(2)).into(), r#""broker 2""#),
        (ClientId::new("order-ingest").unwrap().into(), r#""order-ingest""#),
        (
            ClientSoftware::new("librdkafka", "2.6.1").unwrap().into(),
            r#""librdkafka 2.6.1""#,
        ),
        (ApiKey::PRODUCE.into(), r#""Produce""#),
        (ApiVersion::from(11).into(), "11"),
        (TopicName::new("orders.eu").unwrap().into(), r#""orders.eu""#),
        (PartitionNumber::from(3).into(), "3"),
        (Acks::ALL.into(), "-1"),
        (GroupId::new("analytics-etl").unwrap().into(), r#""analytics-etl""#),
        (MemberId::new("consumer-1-4f2a").unwrap().into(), r#""consumer-1-4f2a""#),
        (GroupInstanceId::new("etl-0").unwrap().into(), r#""etl-0""#),
        (
            TransactionalId::new("payments-tx-1").unwrap().into(),
            r#""payments-tx-1""#,
        ),
        (Outcome::Ok.into(), r#""ok""#),
        (ErrorCode::NOT_LEADER_OR_FOLLOWER.into(), r#""NOT_LEADER_OR_FOLLOWER""#),
        (Latency::from_millis(14).into(), r#""14ms""#),
    ]
}

#[test]
fn a_field_writes_its_name() {
    writes(&Field::ClientId, r#""client.id""#);
    writes(&Field::Topic, r#""topic""#);
    for field in Field::ALL {
        writes(&field, &format!("\"{field}\""));
    }
    rejects::<Field>(r#""client.name""#, "no field is named \"client.name\"");
    rejects::<Field>("3", "expected a string");
}

#[test]
fn a_field_is_the_key_of_a_map() {
    let mut selector = BTreeMap::new();
    selector.insert(Field::Api, "Produce".to_owned());
    selector.insert(Field::ClientId, "order-ingest".to_owned());

    writes(&selector, r#"{"client.id":"order-ingest","api":"Produce"}"#);
}

#[test]
fn a_field_group_writes_its_name() {
    writes(&FieldGroup::Connection, r#""connection""#);
    writes(&FieldGroup::Response, r#""response""#);
    rejects::<FieldGroup>(r#""truth""#, "unknown field group: \"truth\"");
}

#[test]
fn a_connection_number_writes_a_number_and_also_reads_its_text() {
    writes(&ConnectionNumber::new(7), "7");
    writes(&ConnectionNumber::new(u64::MAX), "18446744073709551615");
    reads(r##""#7""##, &ConnectionNumber::new(7));
    reads(r#""7""#, &ConnectionNumber::new(7));
    rejects::<ConnectionNumber>(
        "-7",
        "invalid value: integer `-7`, expected a connection number such as 7 or \"#7\"",
    );
    rejects::<ConnectionNumber>(r#""seven""#, "connection number \"seven\" does not have the form #N");
    rejects::<ConnectionNumber>("7.5", "expected a connection number");
}

#[test]
fn an_api_version_writes_a_number() {
    writes(&ApiVersion::from(11), "11");
    reads(r#""11""#, &ApiVersion::from(11));
    rejects::<ApiVersion>("40000", "API version 40000 is not between 0 and 32767");
    rejects::<ApiVersion>("-1", "invalid value: integer `-1`, expected an API version such as 11");
    rejects::<ApiVersion>(
        "70000",
        "invalid value: integer `70000`, expected an API version such as 11",
    );
}

#[test]
fn acks_writes_a_number_and_also_reads_all() {
    writes(&Acks::ALL, "-1");
    writes(&Acks::NONE, "0");
    writes(&Acks::LEADER, "1");
    reads(r#""all""#, &Acks::ALL);
    reads(r#""-1""#, &Acks::ALL);
    rejects::<Acks>(r#""none""#, "acks \"none\" does not have the form a whole number");
    rejects::<Acks>(
        "40000",
        "invalid value: integer `40000`, expected the acks of a Produce request",
    );
}

#[test]
fn every_other_value_type_writes_its_text_form() {
    writes(
        &"10.0.4.27:51459".parse::<ClientAddress>().unwrap(),
        r#""10.0.4.27:51459""#,
    );
    writes(
        &"[2001:db8::1]:51459".parse::<ClientAddress>().unwrap(),
        r#""[2001:db8::1]:51459""#,
    );
    writes(&"10.0.4.27".parse::<ClientHost>().unwrap(), r#""10.0.4.27""#);
    writes(&Endpoint::Bootstrap, r#""bootstrap""#);
    writes(&Endpoint::Broker(BrokerId::from(2)), r#""broker 2""#);
    writes(
        &ClientSoftware::new("apache-kafka-java", "3.8.0").unwrap(),
        r#""apache-kafka-java 3.8.0""#,
    );
    writes(&Outcome::Timeout, r#""timeout""#);
    writes(&Latency::from_millis(14), r#""14ms""#);
    reads(r#""14 ms""#, &Latency::from_millis(14));
    reads(
        r#""[::ffff:10.0.4.27]:51459""#,
        &"10.0.4.27:51459".parse::<ClientAddress>().unwrap(),
    );
}

#[test]
fn reading_a_value_type_applies_its_rule() {
    rejects::<ClientAddress>(
        r#""10.0.4.27""#,
        "client address \"10.0.4.27\" does not have the form an IP address and a port",
    );
    rejects::<ClientHost>(
        r#""localhost""#,
        "client host \"localhost\" does not have the form an IP address",
    );
    rejects::<Endpoint>(
        r#""broker""#,
        "endpoint \"broker\" does not have the form bootstrap or broker N",
    );
    rejects::<Endpoint>(r#""broker -1""#, "broker ID cannot be negative: -1");
    rejects::<ClientSoftware>(
        r#""librdkafka""#,
        "client software \"librdkafka\" does not have the form a name and a version",
    );
    rejects::<Outcome>(r#""delayed""#, "unknown outcome: \"delayed\"");
    rejects::<Latency>(
        r#""14""#,
        "latency \"14\" does not have the form a whole number and a unit",
    );
    rejects::<Outcome>("3", "expected a string");
}

#[test]
fn a_field_value_writes_a_map_with_its_field_and_its_bare_form() {
    writes(
        &FieldValue::from(TopicName::new("orders.eu").unwrap()),
        r#"{"topic":"orders.eu"}"#,
    );
    writes(&FieldValue::from(PartitionNumber::from(3)), r#"{"partition":3}"#);
    writes(&FieldValue::from(ApiKey::PRODUCE), r#"{"api":"Produce"}"#);
    writes(
        &FieldValue::from(ClientId::new("order-ingest").unwrap()),
        r#"{"client.id":"order-ingest"}"#,
    );
    for (value, bare) in examples() {
        writes(&value.clone(), &format!("{{\"{}\":{bare}}}", value.field()));
    }
}

#[test]
fn a_bare_value_has_the_form_of_its_own_type_and_its_field_reads_it_back() {
    for (value, bare) in examples() {
        assert_eq!(serde_json::to_string(&value.bare()).unwrap(), bare, "{}", value.field());

        let mut json = serde_json::Deserializer::from_str(bare);
        assert_eq!(
            value.field().deserialize(&mut json).unwrap(),
            value,
            "{}",
            value.field()
        );
    }
}

#[test]
fn a_field_value_names_one_field_and_a_value_of_that_field() {
    rejects::<FieldValue>("{}", "invalid length 0, expected a map with one field and its value");
    rejects::<FieldValue>(
        r#"{"topic":"orders.eu","partition":3}"#,
        "invalid length 2, expected a map with one field",
    );
    rejects::<FieldValue>(r#"{"client.name":"x"}"#, "no field is named \"client.name\"");
    rejects::<FieldValue>(
        r#"{"topic":"orders/eu"}"#,
        "topic name \"orders/eu\" has the invalid character '/' at position 6",
    );
    rejects::<FieldValue>(r#"{"api":"Prodcue"}"#, "unknown API key: \"Prodcue\"");
    rejects::<FieldValue>(r#"{"partition":-1}"#, "invalid value: integer `-1`, expected u32");
    rejects::<FieldValue>(r#"{"error.code":"NONE"}"#, "error code cannot be \"NONE\"");
    rejects::<FieldValue>(r#""orders.eu""#, "expected a map with one field and its value");
}

#[test]
fn an_api_or_an_error_without_a_name_writes_its_number() {
    writes(&FieldValue::from(ApiKey::new(4711)), r#"{"api":4711}"#);
    writes(
        &FieldValue::from(ErrorCode::new(4711).unwrap()),
        r#"{"error.code":4711}"#,
    );
    reads(r#"{"api":0}"#, &FieldValue::from(ApiKey::PRODUCE));
}

/// The conditions of a selector as a caller of this crate holds them: one value for each field.
/// Its `serde` form is what a selector in YAML writes under `match`.
#[derive(Debug, PartialEq)]
struct Conditions(Vec<FieldValue>);

impl Serialize for Conditions {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for value in &self.0 {
            map.serialize_entry(&value.field(), &value.bare())?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Conditions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByField;

        impl<'de> Visitor<'de> for ByField {
            type Value = Conditions;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of fields and values")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Conditions, A::Error> {
                let mut values = Vec::new();
                while let Some(field) = map.next_key::<Field>()? {
                    values.push(map.next_value_seed(field)?);
                }
                Ok(Conditions(values))
            }
        }

        deserializer.deserialize_map(ByField)
    }
}

#[test]
fn a_selector_writes_each_condition_as_a_field_and_a_bare_value() {
    let conditions = Conditions(vec![
        ApiKey::PRODUCE.into(),
        TopicName::new("orders.eu").unwrap().into(),
        PartitionNumber::from(3).into(),
        ClientId::new("order-ingest").unwrap().into(),
        ConnectionNumber::new(7).into(),
    ]);

    writes(
        &conditions,
        r#"{"api":"Produce","topic":"orders.eu","partition":3,"client.id":"order-ingest","connection":7}"#,
    );
    rejects::<Conditions>(r#"{"api":"Prodcue"}"#, "unknown API key: \"Prodcue\"");
    rejects::<Conditions>(r#"{"topic":3}"#, "expected a string");
}

#[test]
fn a_part_writes_its_text_form() {
    writes(&"orders.eu/3".parse::<Part>().unwrap(), r#""orders.eu/3""#);
    writes(&"orders.eu".parse::<Part>().unwrap(), r#""orders.eu""#);
    rejects::<Part>(r#""orders.eu/x""#, "partition");
    rejects::<Part>(r#""""#, "topic name is empty");
}

#[test]
fn a_target_writes_call_or_its_parts() {
    writes(&Target::Call, r#""call""#);
    writes(
        &Target::of_parts(["orders.eu/4".parse().unwrap(), "orders.eu/3".parse().unwrap()]).unwrap(),
        r#"{"parts":["orders.eu/3","orders.eu/4"]}"#,
    );
    writes(
        &Target::of_parts(["orders.eu".parse().unwrap()]).unwrap(),
        r#"{"parts":["orders.eu"]}"#,
    );
    rejects::<Target>(
        r#""partitions""#,
        "unknown variant `partitions`, expected `call` or `parts`",
    );
    rejects::<Target>(
        r#"{"parts":["orders eu"]}"#,
        "part: topic name \"orders eu\" has the invalid character ' ' at position 6",
    );
    rejects::<Target>(r#"{"parts":["orders/eu"]}"#, "partition number is not a number: \"eu\"");
}

#[test]
fn a_target_without_parts_does_not_read() {
    // A selector that matches no part does not match the call, so no target is empty.
    rejects::<Target>(r#"{"parts":[]}"#, "invalid length 0, expected at least one part");
    assert_de_tokens_error::<Target>(
        &[
            Token::NewtypeVariant {
                name: "Target",
                variant: "parts",
            },
            Token::Seq { len: Some(0) },
            Token::SeqEnd,
        ],
        "invalid length 0, expected at least one part",
    );
}

/// A compact format is one that people do not read, such as a binary one.
fn compact<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: T, tokens: &[Token]) {
    assert_tokens::<Compact<T>>(&value.compact(), tokens);
}

#[test]
fn a_format_that_people_do_not_read_gets_the_number_alone() {
    compact(ConnectionNumber::new(7), &[Token::U64(7)]);
    compact(ApiVersion::from(11), &[Token::U16(11)]);
    compact(Acks::ALL, &[Token::I16(-1)]);
    assert_de_tokens_error::<Compact<ApiVersion>>(
        &[Token::U16(40_000)],
        "API version 40000 is not between 0 and 32767",
    );
}

#[test]
fn a_field_value_keeps_its_form_in_a_format_that_people_do_not_read() {
    compact(
        FieldValue::from(ApiKey::PRODUCE),
        &[
            Token::Map { len: Some(1) },
            Token::Str("api"),
            Token::I16(0),
            Token::MapEnd,
        ],
    );
    compact(
        FieldValue::from(ConnectionNumber::new(7)),
        &[
            Token::Map { len: Some(1) },
            Token::Str("connection"),
            Token::U64(7),
            Token::MapEnd,
        ],
    );
    compact(
        FieldValue::from(Outcome::Ok),
        &[
            Token::Map { len: Some(1) },
            Token::Str("outcome"),
            Token::Str("ok"),
            Token::MapEnd,
        ],
    );
    compact(
        Target::Call,
        &[Token::UnitVariant {
            name: "Target",
            variant: "call",
        }],
    );
}
