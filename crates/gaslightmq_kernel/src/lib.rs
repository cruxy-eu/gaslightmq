//! The value objects of Kafka: names, IDs, numbers and positions, each as its own type.
//!
//! | Type | Is | Text form |
//! |---|---|---|
//! | [`TopicName`] | The name of a topic. | `orders.eu` |
//! | [`TopicId`] | The UUID of a topic. | `Zm9vYmFyYmF6cXV4MTIzNA` |
//! | [`PartitionNumber`] | The number of a partition within its topic. | `3` |
//! | [`Offset`] | A position in the log of a partition. | `42` |
//! | [`TopicPartition`] | One partition of one topic. | `orders.eu/3` |
//! | [`PartitionOffset`] | An offset in one partition of a topic that the context names. | `3@42` |
//! | [`TopicPartitionOffset`] | An offset in one partition of one topic. | `orders.eu/3@42` |
//! | [`BrokerId`] | The node ID of a broker. | `2` |
//! | [`LeaderEpoch`] | The epoch of a partition's leader. | `7` |
//! | [`ClientId`] | The `client.id` of a client. | `order-ingest` |
//! | [`GroupId`] | The ID of a consumer group. | `analytics-etl` |
//! | [`MemberId`] | The ID of a member of a group. | `consumer-1-4f2a` |
//! | [`GroupInstanceId`] | The `group.instance.id` of a static member. | `etl-0` |
//! | [`TransactionalId`] | The `transactional.id` of a producer. | `payments-tx-1` |
//! | [`ProducerId`] | The ID of an idempotent or transactional producer. | `4000` |
//! | [`ProducerEpoch`] | The epoch of a producer ID. | `5` |
//! | [`ApiKey`] | The key of a Kafka API. | `Produce` |
//! | [`ErrorCode`] | The code of a Kafka error. | `NOT_LEADER_OR_FOLLOWER` |
//!
//! A value of one of these types is always valid.
//! A constructor checks the raw value, and returns the error of its type for one that breaks the type's rule.
//! No other code has to check a topic name or an offset again.
//!
//! Every type has an error of its own, named after it: [`TopicNameError`], [`OffsetError`], [`ApiKeyError`].
//! The error holds only what can go wrong with that value.
//! A topic name is empty, too long, reserved or has an invalid character, and an offset is negative or too large.
//! A type made of parts says which part is wrong, and keeps the error of that part:
//! [`TopicPartitionError::InvalidPartition`] holds a [`PartitionNumberError`].
//! Its message names the whole text first, and then says what is wrong with the part.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! The [`prelude`] holds every type of the table and its error.
//! A crate that speaks in these types imports it whole, instead of one type at a time.
//!
//! ```
//! use gaslightmq_kernel::prelude::*;
//!
//! // Build a position from its parts.
//! let orders = TopicName::new("orders.eu").expect("a topic name");
//! let position = orders.partition(PartitionNumber::from(3)).at(Offset::from(42));
//! assert_eq!(position.to_string(), "orders.eu/3@42");
//!
//! // Or read it from its text form.
//! let parsed: TopicPartitionOffset = "orders.eu/3@42".parse().expect("a position");
//! assert_eq!(parsed, position);
//! assert_eq!(parsed.topic_partition(), &"orders.eu/3".parse::<TopicPartition>()?);
//!
//! // A value that breaks the rule never becomes a value object.
//! assert!(TopicName::new("orders/eu").is_err());
//! assert_eq!(TopicName::new("orders/eu").unwrap_err().to_string(), "topic name \"orders/eu\" has the invalid character '/' at position 6");
//! let error = "orders.eu/-1".parse::<TopicPartition>().unwrap_err();
//! assert!(matches!(error, TopicPartitionError::InvalidPartition { .. }));
//! assert_eq!(error.to_string(), "topic partition \"orders.eu/-1\": partition number cannot be negative: -1");
//! # Ok::<(), TopicPartitionError>(())
//! ```
//!
//! # Conversions
//!
//! The types of one family share one set of conversions.
//!
//! A name or an ID holds text:
//! [`TopicName`], [`ClientId`], [`GroupId`], [`MemberId`], [`GroupInstanceId`] and [`TransactionalId`].
//!
//! | From or to | Through |
//! |---|---|
//! | Text that can break the rule | `new`, [`FromStr`](std::str::FromStr), `TryFrom<&str>`, `TryFrom<String>` |
//! | Back to text | `as_str`, [`Display`](std::fmt::Display), `AsRef<str>`, `Borrow<str>`, `From<T> for String` |
//! | Comparison with text | `PartialEq` with `str`, `&str` and `String`, in both directions |
//!
//! A number holds an unsigned value, and Kafka writes it as a signed one:
//! [`PartitionNumber`], [`Offset`], [`BrokerId`], [`LeaderEpoch`], [`ProducerId`] and [`ProducerEpoch`].
//!
//! | From or to | Through |
//! |---|---|
//! | A number that can break the rule | `new`, `TryFrom` of the unsigned type, `TryFrom` of the signed type Kafka writes |
//! | A number that always fits | `From` of the widest unsigned type below the value's own |
//! | Back to a number | `value`, `From<T>` for the unsigned type, `From<T>` for the signed type Kafka writes |
//! | Text | [`FromStr`](std::str::FromStr) and [`Display`](std::fmt::Display), in decimal digits |
//!
//! Exactly one integer type converts with `From`, so a literal needs no suffix: `Offset::from(42)`.
//!
//! An [`ApiKey`] and an [`ErrorCode`] hold the `i16` Kafka writes, and Kafka gives most of them a name.
//!
//! | From or to | Through |
//! |---|---|
//! | A number | `new`, `value`, `From<T> for i16`, and `From<i16>` for an API key or `TryFrom<i16>` for an error code |
//! | A name | A constant such as [`ApiKey::PRODUCE`], `name`, `known` |
//! | Text | [`FromStr`](std::str::FromStr) and [`Display`](std::fmt::Display): the name, or the number without one |
//!
//! [`TopicId`] converts from and to `[u8; 16]` and `u128`.
//! [`TopicPartition`], [`PartitionOffset`] and [`TopicPartitionOffset`] convert from and to tuples of their parts.
//! These four, an [`ApiKey`] and an [`ErrorCode`] convert from and to their text form, as the names and IDs do.
//!
//! # What Kafka writes for "none"
//!
//! Kafka has no optional numbers.
//! It writes `-1` for "no offset", "no leader" or "unknown epoch".
//! Such a number is never a value of these types.
//! `from_wire` reads it as `None`, and `to_wire` writes `None` back.
//!
//! | Type | Kafka writes `None` as |
//! |---|---|
//! | [`Offset`], [`BrokerId`], [`LeaderEpoch`], [`ProducerId`], [`ProducerEpoch`] | `-1` |
//! | [`TopicId`] | The UUID of all zeros. |
//! | [`MemberId`] | An empty string. |
//! | [`ErrorCode`] | `0`, which Kafka calls `NONE`. |
//!
//! ```
//! use gaslightmq_kernel::prelude::*;
//!
//! let leader: Option<BrokerId> = BrokerId::from_wire(-1)?;
//! assert_eq!(leader, None);
//! assert_eq!(Offset::to_wire(Some(Offset::from(42))), 42);
//! # Ok::<(), BrokerIdError>(())
//! ```
//!
//! A [`PartitionNumber`] has no such number, and its `TryFrom<i32>` rejects every negative one.
//! A [`ClientId`], a [`GroupInstanceId`] and a [`TransactionalId`] are null on the wire when there is none,
//! which is an `Option` without help from this crate.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every type serializes as its text form, and a number as a number.
//! A selector or an incident in YAML writes `orders.eu/3` for a topic partition and `3` for a partition number.
//! Deserialization applies the same rule as the constructor.
//!
//! An [`ApiKey`] and an [`ErrorCode`] serialize as their name, such as `Produce`.
//! One without a name serializes as its number, and both forms read back.
//! A format that people do not read, such as a binary one, gets the number alone.
//!
//! # No IO
//!
//! The crate depends on no other crate of the workspace.
//! It has no async code, no IO, no clock and no randomness, and it compiles to WebAssembly.

mod api;
mod client;
mod cluster;
mod code;
mod error_code;
mod group;
mod names;
mod number;
mod offset;
mod partition;
mod producer;
mod text;
mod topic;

pub use api::{ApiKey, ApiKeyError};
pub use client::{ClientId, ClientIdError};
pub use cluster::{BrokerId, BrokerIdError, LeaderEpoch, LeaderEpochError};
pub use error_code::{ErrorCode, ErrorCodeError};
pub use group::{GroupId, GroupIdError, GroupInstanceId, GroupInstanceIdError, MemberId, MemberIdError};
pub use offset::{Offset, OffsetError};
pub use partition::{
    PartitionNumber, PartitionNumberError, PartitionOffset, PartitionOffsetError, TopicPartition, TopicPartitionError,
    TopicPartitionOffset, TopicPartitionOffsetError,
};
pub use producer::{
    ProducerEpoch, ProducerEpochError, ProducerId, ProducerIdError, TransactionalId, TransactionalIdError,
};
pub use topic::{TopicId, TopicIdError, TopicName, TopicNameError};

/// Every value object of this crate, and the error of each.
///
/// Import it whole wherever code speaks in these types.
/// No name in it is a name of the standard prelude, so the import shadows nothing.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let committed: Result<TopicPartitionOffset, TopicPartitionOffsetError> = "orders.eu/3@42".parse();
/// assert_eq!(committed?.offset(), Offset::from(42));
/// # Ok::<(), TopicPartitionOffsetError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        ApiKey, ApiKeyError, BrokerId, BrokerIdError, ClientId, ClientIdError, ErrorCode, ErrorCodeError, GroupId,
        GroupIdError, GroupInstanceId, GroupInstanceIdError, LeaderEpoch, LeaderEpochError, MemberId, MemberIdError,
        Offset, OffsetError, PartitionNumber, PartitionNumberError, PartitionOffset, PartitionOffsetError,
        ProducerEpoch, ProducerEpochError, ProducerId, ProducerIdError, TopicId, TopicIdError, TopicName,
        TopicNameError, TopicPartition, TopicPartitionError, TopicPartitionOffset, TopicPartitionOffsetError,
        TransactionalId, TransactionalIdError,
    };
}
