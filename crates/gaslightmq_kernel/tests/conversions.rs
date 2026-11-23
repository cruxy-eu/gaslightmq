//! Every type of a family has the same conversions.
//!
//! The bounds below are the contract.
//! A type that misses one conversion does not compile here.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::borrow::Borrow;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;

use gaslightmq_kernel::prelude::*;

/// What every value object offers.
trait Value: Debug + Display + Clone + Eq + Ord + Hash + Send + Sync + 'static {}

impl<T: Debug + Display + Clone + Eq + Ord + Hash + Send + Sync + 'static> Value for T {}

/// What the error of every value object offers.
trait ValueError: std::error::Error + Clone + PartialEq + Send + Sync + 'static {}

impl<E: std::error::Error + Clone + PartialEq + Send + Sync + 'static> ValueError for E {}

/// What every type with a text form offers: it reads what it prints, from borrowed and from owned text.
///
/// `E` is the error of the type, and every conversion that can fail returns it.
fn round_trips_as_text<T, E>(text: &str)
where
    T: Value + FromStr<Err = E> + for<'a> TryFrom<&'a str, Error = E> + TryFrom<String, Error = E> + Into<String>,
    E: ValueError,
{
    let value: T = text.parse().unwrap_or_else(|error| panic!("{text:?}: {error}"));

    assert_eq!(value.to_string(), text);
    assert_eq!(T::try_from(text).as_ref(), Ok(&value));
    assert_eq!(T::try_from(text.to_owned()).as_ref(), Ok(&value));
    assert_eq!(value.clone().into(), text);
}

/// What every name and ID offers on top of its text form.
fn is_text<T, E>(text: &str, too_long: usize)
where
    T: Value
        + FromStr<Err = E>
        + for<'a> TryFrom<&'a str, Error = E>
        + TryFrom<String, Error = E>
        + Into<String>
        + AsRef<str>
        + Borrow<str>
        + PartialEq<str>
        + for<'a> PartialEq<&'a str>
        + PartialEq<String>,
    str: PartialEq<T>,
    for<'a> &'a str: PartialEq<T>,
    String: PartialEq<T>,
    E: ValueError,
{
    round_trips_as_text::<T, E>(text);
    let value: T = text.parse().unwrap();

    assert!(same(value.as_ref(), text));
    assert!(same(value.borrow(), text));
    let owned = text.to_owned();
    assert!(value == *text && value == text && value == owned);
    assert!(*text == value && text == value && owned == value);

    // `Borrow<str>` promises the order and the hash of `str`.
    assert!(HashSet::from([value.clone()]).contains(text));
    assert!(BTreeSet::from([value.clone()]).contains(text));

    let long = "a".repeat(too_long);
    let error = long.parse::<T>().unwrap_err();
    let limit = format!("of {too_long} bytes exceeds the limit of {}", too_long - 1);
    assert!(error.to_string().ends_with(&limit), "{error}");
    assert!(error.source().is_none(), "the message is complete");
    assert_eq!(T::try_from(long.as_str()), Err(error.clone()));
    assert_eq!(T::try_from(long), Err(error));
}

/// Compares two texts outside the bounds of [`is_text`], which add other ways to compare a `&str`.
fn same(left: &str, right: &str) -> bool {
    left == right
}

/// What every number offers.
///
/// `U` is the unsigned type of the value, `I` the signed type Kafka writes, and `S` the type that always fits.
/// `E` is the error of the type, and every conversion that can fail returns it.
fn is_number<T, E, U, I, S>(minus_one: I, highest: I)
where
    T: Value + Copy + FromStr<Err = E> + TryFrom<U, Error = E> + TryFrom<I, Error = E> + From<S> + Into<U> + Into<I>,
    U: Copy + Debug + Display + PartialEq + TryFrom<I> + From<S> + Into<u64>,
    I: Copy + Debug + PartialEq + Into<i64>,
    S: From<u8>,
    E: ValueError,
{
    let seven = T::from(S::from(7));
    let unsigned: U = seven.into();
    let signed: I = seven.into();

    assert_eq!(unsigned, U::from(S::from(7)));
    assert_eq!(T::try_from(unsigned), Ok(seven));
    assert_eq!(T::try_from(signed), Ok(seven));
    assert_eq!(seven.to_string(), "7");
    assert_eq!("7".parse::<T>(), Ok(seven));
    assert!(T::from(S::from(7)) < T::from(S::from(8)));

    // The highest value is the highest number Kafka can write.
    let top = T::try_from(highest).unwrap();
    assert_eq!(Into::<I>::into(top), highest);
    assert_eq!(top.to_string().parse::<T>(), Ok(top));
    let top: U = top.into();
    let max: u64 = top.into();
    let too_large = (max + 1).to_string().parse::<T>().unwrap_err();
    let limit = format!("{} exceeds the maximum of {max}", max + 1);
    assert!(too_large.to_string().ends_with(&limit), "{too_large}");

    // No negative number is a value.
    let negative = "-1".parse::<T>().unwrap_err();
    assert!(negative.to_string().ends_with("cannot be negative: -1"), "{negative}");
    assert_eq!(T::try_from(minus_one), Err(negative));
    let not_a_number = "seven".parse::<T>().unwrap_err();
    assert!(
        not_a_number.to_string().ends_with("is not a number: \"seven\""),
        "{not_a_number}"
    );
}

#[test]
fn every_name_and_id_has_the_same_conversions() {
    is_text::<TopicName, TopicNameError>("orders.eu", 250);
    is_text::<ClientId, ClientIdError>("order-ingest", 32_768);
    is_text::<GroupId, GroupIdError>("analytics-etl", 32_768);
    is_text::<MemberId, MemberIdError>("consumer-1-4f2a", 32_768);
    is_text::<GroupInstanceId, GroupInstanceIdError>("etl-0", 250);
    is_text::<TransactionalId, TransactionalIdError>("payments-tx-1", 32_768);
}

#[test]
fn every_number_has_the_same_conversions() {
    is_number::<PartitionNumber, PartitionNumberError, u32, i32, u16>(-1, i32::MAX);
    is_number::<BrokerId, BrokerIdError, u32, i32, u16>(-1, i32::MAX);
    is_number::<LeaderEpoch, LeaderEpochError, u32, i32, u16>(-1, i32::MAX);
    is_number::<Offset, OffsetError, u64, i64, u32>(-1, i64::MAX);
    is_number::<ProducerId, ProducerIdError, u64, i64, u32>(-1, i64::MAX);
    is_number::<ProducerEpoch, ProducerEpochError, u16, i16, u8>(-1, i16::MAX);
}

#[test]
fn every_type_with_a_text_form_reads_what_it_prints() {
    round_trips_as_text::<TopicId, TopicIdError>("Zm9vYmFyYmF6cXV4MTIzNA");
    round_trips_as_text::<TopicPartition, TopicPartitionError>("orders.eu/3");
    round_trips_as_text::<PartitionOffset, PartitionOffsetError>("3@42");
    round_trips_as_text::<TopicPartitionOffset, TopicPartitionOffsetError>("orders.eu/3@42");
    round_trips_as_text::<ApiKey, ApiKeyError>("Produce");
    round_trips_as_text::<ApiKey, ApiKeyError>("4711");
    round_trips_as_text::<ErrorCode, ErrorCodeError>("NOT_LEADER_OR_FOLLOWER");
    round_trips_as_text::<ErrorCode, ErrorCodeError>("999");
}

#[test]
fn an_api_key_and_an_error_code_convert_from_and_to_the_number_kafka_writes() {
    let produce: ApiKey = 0_i16.into();
    let not_leader: ErrorCode = 6_i16.try_into().unwrap();

    assert_eq!(
        (produce, not_leader),
        (ApiKey::PRODUCE, ErrorCode::NOT_LEADER_OR_FOLLOWER)
    );
    assert_eq!((i16::from(produce), i16::from(not_leader)), (0, 6));
    assert_eq!((produce.value(), not_leader.value()), (0, 6));
    assert_eq!(
        (produce.name(), not_leader.name()),
        (Some("Produce"), Some("NOT_LEADER_OR_FOLLOWER"))
    );
    assert!(produce.is_known() && not_leader.is_known());
    assert!(ApiKey::known().any(|key| key == produce) && ErrorCode::known().any(|code| code == not_leader));
}

#[test]
fn an_error_code_reads_zero_as_none() {
    assert_eq!(ErrorCode::from_wire(0), None);
    assert_eq!(ErrorCode::to_wire(None), 0_i16);
    assert_eq!(
        ErrorCode::try_from(0),
        Err(ErrorCodeError::Reserved { value: "0".to_owned() })
    );
}

#[test]
fn every_number_with_a_sentinel_reads_minus_one_as_none() {
    assert_eq!(Offset::from_wire(-1), Ok(None));
    assert_eq!(BrokerId::from_wire(-1), Ok(None));
    assert_eq!(LeaderEpoch::from_wire(-1), Ok(None));
    assert_eq!(ProducerId::from_wire(-1), Ok(None));
    assert_eq!(ProducerEpoch::from_wire(-1), Ok(None));

    assert_eq!(Offset::to_wire(None), -1_i64);
    assert_eq!(BrokerId::to_wire(None), -1_i32);
    assert_eq!(LeaderEpoch::to_wire(None), -1_i32);
    assert_eq!(ProducerId::to_wire(None), -1_i64);
    assert_eq!(ProducerEpoch::to_wire(None), -1_i16);
}

#[test]
fn a_literal_needs_no_suffix() {
    assert_eq!(PartitionNumber::from(3).value(), 3);
    assert_eq!(Offset::from(42).value(), 42);
    assert_eq!(BrokerId::from(2).value(), 2);
    assert_eq!(LeaderEpoch::from(7).value(), 7);
    assert_eq!(ProducerId::from(4000).value(), 4000);
    assert_eq!(ProducerEpoch::from(5).value(), 5);
}

#[test]
fn an_error_is_a_std_error() {
    fn message_of<E: std::error::Error + Send + Sync + 'static>(error: E) -> String {
        error.to_string()
    }

    assert_eq!(message_of(TopicName::new("").unwrap_err()), "topic name is empty");
}
