//! The `serde` form of every type, and the rule that reading applies.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::collections::BTreeMap;
use std::fmt::Debug;

use gaslightmq_kernel::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_test::{Compact, Configure, Token, assert_de_tokens_error, assert_tokens};

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// Reading `json` fails with a message that holds `reason`.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
}

#[test]
fn a_name_or_an_id_writes_a_string() {
    writes(&TopicName::new("orders.eu").unwrap(), r#""orders.eu""#);
    writes(&ClientId::new("order-ingest").unwrap(), r#""order-ingest""#);
    writes(&ClientId::new("").unwrap(), r#""""#);
    writes(&GroupId::new("analytics-etl").unwrap(), r#""analytics-etl""#);
    writes(&MemberId::new("consumer-1-4f2a").unwrap(), r#""consumer-1-4f2a""#);
    writes(&GroupInstanceId::new("etl-0").unwrap(), r#""etl-0""#);
    writes(&TransactionalId::new("payments-tx-1").unwrap(), r#""payments-tx-1""#);
    writes(
        &TopicId::new(*b"foobarbazqux1234").unwrap(),
        r#""Zm9vYmFyYmF6cXV4MTIzNA""#,
    );
}

#[test]
fn a_number_writes_a_number() {
    writes(&PartitionNumber::from(3), "3");
    writes(&Offset::from(42), "42");
    writes(&Offset::try_from(i64::MAX).unwrap(), "9223372036854775807");
    writes(&BrokerId::from(2), "2");
    writes(&LeaderEpoch::from(7), "7");
    writes(&ProducerId::from(4000), "4000");
    writes(&ProducerEpoch::from(5), "5");
}

#[test]
fn a_position_writes_its_text_form() {
    writes(&"orders.eu/3".parse::<TopicPartition>().unwrap(), r#""orders.eu/3""#);
    writes(&"3@42".parse::<PartitionOffset>().unwrap(), r#""3@42""#);
    writes(
        &"orders.eu/3@42".parse::<TopicPartitionOffset>().unwrap(),
        r#""orders.eu/3@42""#,
    );
}

#[test]
fn reading_a_name_or_an_id_applies_its_rule() {
    rejects::<TopicName>(
        r#""orders/eu""#,
        "topic name \"orders/eu\" has the invalid character '/' at position 6",
    );
    rejects::<TopicName>(r#""""#, "topic name is empty");
    rejects::<TopicName>(r#""..""#, "topic name cannot be \"..\"");
    rejects::<TopicName>(
        &format!("\"{}\"", "a".repeat(250)),
        "topic name of 250 bytes exceeds the limit of 249",
    );
    rejects::<GroupId>(r#""""#, "group ID is empty");
    rejects::<MemberId>(r#""""#, "member ID is empty");
    rejects::<GroupInstanceId>(
        r#""etl 0""#,
        "group instance ID \"etl 0\" has the invalid character ' ' at position 3",
    );
    rejects::<TransactionalId>(r#""""#, "transactional ID is empty");
    rejects::<ClientId>(
        &format!("\"{}\"", "a".repeat(32_768)),
        "client ID of 32768 bytes exceeds the limit of 32767",
    );
    rejects::<TopicId>(r#""AAAAAAAAAAAAAAAAAAAAAA""#, "topic ID cannot be");
    rejects::<TopicId>(r#""Zm9v""#, "does not have the form 22 characters of URL-safe base64");
}

#[test]
fn reading_a_number_applies_its_rule() {
    rejects::<PartitionNumber>(
        "2147483648",
        "partition number 2147483648 exceeds the maximum of 2147483647",
    );
    rejects::<Offset>(
        "9223372036854775808",
        "offset 9223372036854775808 exceeds the maximum of 9223372036854775807",
    );
    rejects::<BrokerId>("2147483648", "broker ID 2147483648 exceeds the maximum of 2147483647");
    rejects::<LeaderEpoch>(
        "2147483648",
        "leader epoch 2147483648 exceeds the maximum of 2147483647",
    );
    rejects::<ProducerId>(
        "9223372036854775808",
        "producer ID 9223372036854775808 exceeds the maximum",
    );
    rejects::<ProducerEpoch>("32768", "producer epoch 32768 exceeds the maximum of 32767");
}

#[test]
fn reading_a_number_takes_no_negative_number_and_no_text() {
    rejects::<PartitionNumber>("-1", "invalid value");
    rejects::<Offset>("-1", "invalid value");
    rejects::<BrokerId>("-1", "invalid value");
    rejects::<LeaderEpoch>("-1", "invalid value");
    rejects::<ProducerId>("-1", "invalid value");
    rejects::<ProducerEpoch>("-1", "invalid value");
    rejects::<Offset>(r#""42""#, "invalid type");
    rejects::<PartitionNumber>("3.5", "invalid type");
}

#[test]
fn reading_a_position_applies_the_rule_of_every_part() {
    rejects::<TopicPartition>(
        r#""orders.eu""#,
        "topic partition \"orders.eu\" does not have the form topic/partition",
    );
    rejects::<TopicPartition>(
        r#""orders.*/3""#,
        "topic partition \"orders.*/3\": topic name \"orders.*\" has the invalid character '*' at position 7",
    );
    rejects::<TopicPartition>(r#""orders.eu/-1""#, "partition number cannot be negative: -1");
    rejects::<PartitionOffset>(r#""3""#, "does not have the form partition@offset");
    rejects::<PartitionOffset>(r#""3@x""#, "offset is not a number: \"x\"");
    rejects::<TopicPartitionOffset>(r#""orders.eu/3""#, "does not have the form topic/partition@offset");
    rejects::<TopicPartitionOffset>(r#""orders.eu/3@-1""#, "offset cannot be negative: -1");
    rejects::<TopicPartition>("3", "invalid type");
}

#[test]
fn a_position_is_a_key_of_a_map() {
    let committed: BTreeMap<TopicPartition, Offset> = BTreeMap::from([
        ("orders.eu/10".parse().unwrap(), Offset::from(7)),
        ("orders.eu/3".parse().unwrap(), Offset::from(42)),
    ]);

    writes(&committed, r#"{"orders.eu/3":42,"orders.eu/10":7}"#);
}

#[test]
fn a_name_and_a_number_are_keys_of_a_map() {
    writes(
        &BTreeMap::from([(TopicName::new("orders.eu").unwrap(), 6)]),
        r#"{"orders.eu":6}"#,
    );
    writes(
        &BTreeMap::from([(PartitionNumber::from(3), Offset::from(42))]),
        r#"{"3":42}"#,
    );
}

#[test]
fn a_definition_holds_value_objects() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct LeaderMoves {
        partition: TopicPartition,
        to: Option<BrokerId>,
    }

    let incident: LeaderMoves = serde_json::from_str(r#"{ "partition": "orders.eu/3", "to": 2 }"#).unwrap();

    assert_eq!(
        incident,
        LeaderMoves {
            partition: "orders.eu/3".parse().unwrap(),
            to: Some(BrokerId::from(2))
        }
    );
}

#[test]
fn an_api_key_and_an_error_code_write_their_name() {
    writes(&ApiKey::PRODUCE, r#""Produce""#);
    writes(&ApiKey::FIND_COORDINATOR, r#""FindCoordinator""#);
    writes(&ErrorCode::NOT_LEADER_OR_FOLLOWER, r#""NOT_LEADER_OR_FOLLOWER""#);
    writes(&ErrorCode::UNKNOWN_SERVER_ERROR, r#""UNKNOWN_SERVER_ERROR""#);
}

#[test]
fn an_api_key_and_an_error_code_without_a_name_write_their_number() {
    writes(&ApiKey::from(4711), "4711");
    writes(&ApiKey::from(-1), "-1");
    writes(&ErrorCode::try_from(999).unwrap(), "999");
    writes(&ErrorCode::try_from(-2).unwrap(), "-2");
}

#[test]
fn an_api_key_and_an_error_code_read_a_number_and_a_number_as_text() {
    for json in ["0", r#""0""#, r#""Produce""#] {
        assert_eq!(serde_json::from_str::<ApiKey>(json).unwrap(), ApiKey::PRODUCE, "{json}");
    }
    for json in ["6", r#""6""#, r#""NOT_LEADER_OR_FOLLOWER""#] {
        assert_eq!(
            serde_json::from_str::<ErrorCode>(json).unwrap(),
            ErrorCode::NOT_LEADER_OR_FOLLOWER,
            "{json}"
        );
    }
    assert_eq!(serde_json::from_str::<ApiKey>(r#""4711""#).unwrap(), ApiKey::from(4711));
    assert_eq!(
        serde_json::from_str::<ErrorCode>("-1").unwrap(),
        ErrorCode::UNKNOWN_SERVER_ERROR
    );
}

#[test]
fn reading_an_api_key_or_an_error_code_applies_its_rule() {
    rejects::<ApiKey>(r#""Prodcue""#, "unknown API key: \"Prodcue\"");
    rejects::<ApiKey>(r#""produce""#, "unknown API key: \"produce\"");
    rejects::<ApiKey>(r#""40000""#, "API key 40000 exceeds the maximum of 32767");
    rejects::<ApiKey>(
        "40000",
        "invalid value: integer `40000`, expected the name or the number of a Kafka API",
    );
    rejects::<ApiKey>(
        "-40000",
        "invalid value: integer `-40000`, expected the name or the number of a Kafka API",
    );
    rejects::<ApiKey>("1.5", "invalid type");
    rejects::<ApiKey>("true", "invalid type");
    rejects::<ErrorCode>(r#""NOT_LEADER""#, "unknown error code: \"NOT_LEADER\"");
    rejects::<ErrorCode>(r#""NONE""#, "error code cannot be \"NONE\"");
    rejects::<ErrorCode>(r#""0""#, "error code cannot be \"0\"");
    rejects::<ErrorCode>("0", "error code cannot be \"0\"");
    rejects::<ErrorCode>(
        "40000",
        "invalid value: integer `40000`, expected the name or the number of a Kafka error",
    );
}

#[test]
fn an_api_key_and_an_error_code_are_keys_of_a_map() {
    writes(
        &BTreeMap::from([(ApiKey::PRODUCE, 3), (ApiKey::from(4711), 1)]),
        r#"{"Produce":3,"4711":1}"#,
    );
    writes(
        &BTreeMap::from([
            (ErrorCode::NOT_LEADER_OR_FOLLOWER, 3),
            (ErrorCode::try_from(999).unwrap(), 1),
        ]),
        r#"{"NOT_LEADER_OR_FOLLOWER":3,"999":1}"#,
    );
}

#[test]
fn a_format_that_people_read_gets_the_name() {
    assert_tokens(&ApiKey::PRODUCE.readable(), &[Token::Str("Produce")]);
    assert_tokens(&ApiKey::from(4711).readable(), &[Token::I16(4711)]);
    assert_tokens(
        &ErrorCode::NOT_LEADER_OR_FOLLOWER.readable(),
        &[Token::Str("NOT_LEADER_OR_FOLLOWER")],
    );
    assert_tokens(&ErrorCode::try_from(999).unwrap().readable(), &[Token::I16(999)]);
}

#[test]
fn a_binary_format_gets_the_number() {
    assert_tokens(&ApiKey::PRODUCE.compact(), &[Token::I16(0)]);
    assert_tokens(&ApiKey::from(4711).compact(), &[Token::I16(4711)]);
    assert_tokens(&ErrorCode::NOT_LEADER_OR_FOLLOWER.compact(), &[Token::I16(6)]);
    assert_tokens(&ErrorCode::try_from(999).unwrap().compact(), &[Token::I16(999)]);
    assert_de_tokens_error::<Compact<ErrorCode>>(&[Token::I16(0)], "error code cannot be \"0\"");
}

#[test]
fn a_refusal_holds_an_error_code_and_no_error_is_none() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Refusal {
        api: ApiKey,
        error: Option<ErrorCode>,
    }

    writes(
        &Refusal {
            api: ApiKey::PRODUCE,
            error: Some(ErrorCode::REQUEST_TIMED_OUT),
        },
        r#"{"api":"Produce","error":"REQUEST_TIMED_OUT"}"#,
    );
    writes(
        &Refusal {
            api: ApiKey::from(4711),
            error: None,
        },
        r#"{"api":4711,"error":null}"#,
    );
}
