//! Every type of a family has the same conversions.
//!
//! The bounds below are the contract.
//! A type that misses one conversion does not compile here.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;

use gaslightmq_kernel::prelude::*;

/// What every value object offers.
trait Value: Debug + Display + Clone + Eq + Ord + Hash + Send + Sync + 'static {}

impl<T: Debug + Display + Clone + Eq + Ord + Hash + Send + Sync + 'static> Value for T {}

/// What every type with a text form offers: it reads what it prints, from borrowed and from owned text.
fn round_trips_as_text<T>(text: &str)
where
    T: Value
        + FromStr<Err = KernelError>
        + for<'a> TryFrom<&'a str, Error = KernelError>
        + TryFrom<String, Error = KernelError>
        + Into<String>,
{
    let value: T = text.parse().unwrap_or_else(|error| panic!("{text:?}: {error}"));

    assert_eq!(value.to_string(), text);
    assert_eq!(T::try_from(text).as_ref(), Ok(&value));
    assert_eq!(T::try_from(text.to_owned()).as_ref(), Ok(&value));
    assert_eq!(value.clone().into(), text);
}

/// What every name and ID offers on top of its text form.
fn is_text<T>(text: &str, too_long: usize)
where
    T: Value
        + FromStr<Err = KernelError>
        + for<'a> TryFrom<&'a str, Error = KernelError>
        + TryFrom<String, Error = KernelError>
        + Into<String>
        + AsRef<str>
        + Borrow<str>
        + PartialEq<str>
        + for<'a> PartialEq<&'a str>
        + PartialEq<String>,
    str: PartialEq<T>,
    for<'a> &'a str: PartialEq<T>,
    String: PartialEq<T>,
{
    round_trips_as_text::<T>(text);
    let value: T = text.parse().unwrap();

    assert!(same(value.as_ref(), text));
    assert!(same(value.borrow(), text));
    let owned = text.to_owned();
    assert!(value == *text && value == text && value == owned);
    assert!(*text == value && text == value && owned == value);

    // `Borrow<str>` promises the order and the hash of `str`.
    assert_eq!(HashMap::from([(value.clone(), ())]).get(text), Some(&()));
    assert_eq!(BTreeMap::from([(value.clone(), ())]).get(text), Some(&()));

    let long = "a".repeat(too_long);
    assert!(matches!(long.parse::<T>(), Err(KernelError::TooLong { .. })));
    assert!(matches!(T::try_from(long.as_str()), Err(KernelError::TooLong { .. })));
    assert!(matches!(T::try_from(long), Err(KernelError::TooLong { .. })));
}

/// Compares two texts outside the bounds of [`is_text`], which add other ways to compare a `&str`.
fn same(left: &str, right: &str) -> bool {
    left == right
}

/// What every number offers.
///
/// `U` is the unsigned type of the value, `I` the signed type Kafka writes, and `S` the type that always fits.
fn is_number<T, U, I, S>(minus_one: I, highest: I)
where
    T: Value
        + Copy
        + FromStr<Err = KernelError>
        + TryFrom<U, Error = KernelError>
        + TryFrom<I, Error = KernelError>
        + From<S>
        + Into<U>
        + Into<I>,
    U: Copy + Debug + Display + PartialEq + TryFrom<I> + From<S> + Into<u64>,
    I: Copy + Debug + PartialEq + Into<i64>,
    S: From<u8>,
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
    assert_eq!((max + 1).to_string().parse::<T>(), Err(too_large::<T>(max + 1, max)));

    // No negative number is a value.
    assert!(matches!(T::try_from(minus_one), Err(KernelError::Negative { value: -1, .. })));
    assert!(matches!("-1".parse::<T>(), Err(KernelError::Negative { value: -1, .. })));
    assert!(matches!("seven".parse::<T>(), Err(KernelError::NotANumber { .. })));
}

fn too_large<T: FromStr<Err = KernelError>>(value: u64, max: u64) -> KernelError {
    match u64::MAX.to_string().parse::<T>() {
        Err(KernelError::TooLarge { what, .. }) => KernelError::TooLarge { what, value, max },
        _ => panic!("the highest u64 is too large for every number"),
    }
}

#[test]
fn every_name_and_id_has_the_same_conversions() {
    is_text::<TopicName>("orders.eu", 250);
    is_text::<ClientId>("order-ingest", 32_768);
    is_text::<GroupId>("analytics-etl", 32_768);
    is_text::<MemberId>("consumer-1-4f2a", 32_768);
    is_text::<GroupInstanceId>("etl-0", 250);
    is_text::<TransactionalId>("payments-tx-1", 32_768);
}

#[test]
fn every_number_has_the_same_conversions() {
    is_number::<PartitionNumber, u32, i32, u16>(-1, i32::MAX);
    is_number::<BrokerId, u32, i32, u16>(-1, i32::MAX);
    is_number::<LeaderEpoch, u32, i32, u16>(-1, i32::MAX);
    is_number::<Offset, u64, i64, u32>(-1, i64::MAX);
    is_number::<ProducerId, u64, i64, u32>(-1, i64::MAX);
    is_number::<ProducerEpoch, u16, i16, u8>(-1, i16::MAX);
}

#[test]
fn every_type_with_a_text_form_reads_what_it_prints() {
    round_trips_as_text::<TopicId>("Zm9vYmFyYmF6cXV4MTIzNA");
    round_trips_as_text::<TopicPartition>("orders.eu/3");
    round_trips_as_text::<PartitionOffset>("3@42");
    round_trips_as_text::<TopicPartitionOffset>("orders.eu/3@42");
    round_trips_as_text::<ApiKey>("Produce");
    round_trips_as_text::<ApiKey>("4711");
    round_trips_as_text::<ErrorCode>("NOT_LEADER_OR_FOLLOWER");
    round_trips_as_text::<ErrorCode>("999");
}

#[test]
fn an_api_key_and_an_error_code_convert_from_and_to_the_number_kafka_writes() {
    let produce: ApiKey = 0_i16.into();
    let not_leader: ErrorCode = 6_i16.try_into().unwrap();

    assert_eq!((produce, not_leader), (ApiKey::PRODUCE, ErrorCode::NOT_LEADER_OR_FOLLOWER));
    assert_eq!((i16::from(produce), i16::from(not_leader)), (0, 6));
    assert_eq!((produce.value(), not_leader.value()), (0, 6));
    assert_eq!((produce.name(), not_leader.name()), (Some("Produce"), Some("NOT_LEADER_OR_FOLLOWER")));
    assert!(produce.is_known() && not_leader.is_known());
    assert!(ApiKey::known().any(|key| key == produce) && ErrorCode::known().any(|code| code == not_leader));
}

#[test]
fn an_error_code_reads_zero_as_none() {
    assert_eq!(ErrorCode::from_wire(0), None);
    assert_eq!(ErrorCode::to_wire(None), 0_i16);
    assert!(matches!(ErrorCode::try_from(0), Err(KernelError::Reserved { .. })));
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
    let error: Box<dyn std::error::Error + Send + Sync> = Box::new(TopicName::new("").unwrap_err());

    assert_eq!(error.to_string(), "topic name is empty");
}
