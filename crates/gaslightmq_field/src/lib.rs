//! The field vocabulary: field names, their values, and what anything with fields has to answer.
//!
//! Targeting has one predicate language for three jobs:
//! it filters recorded traffic, it targets calls with rules, and it indexes recordings into facets.
//! All three speak in the fields of this crate.
//!
//! | Type | Is |
//! |---|---|
//! | [`Field`] | The name of a field, such as `client.id`. |
//! | [`FieldGroup`] | Where a field comes from: the connection, the request, the client context or the response. |
//! | [`FieldValue`] | One value of one field, typed by the field. |
//! | [`FieldValues`] | The values of one field on one call, with their [`Origin`]. |
//! | [`FieldSource`] | Anything with fields: a call in the pipeline, a recorded call or a call a test wrote down. |
//! | [`Reading`] | What a source answers: a value, absent, or not known yet. |
//! | [`Part`] | One piece of a call that a lie can change on its own: a topic or a topic partition. |
//! | [`PartFields`] | A part as a source shows it, with the truth of that part. |
//! | [`Target`] | What a selector matched: some parts of a call, or the whole call. |
//! | [`Fields`] | The fields of one call as plain data. It is the simplest source. |
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//!
//! // A Produce for two topics, as a test writes it down.
//! let produce = Fields::new()
//!     .with(ClientId::new("order-ingest")?)
//!     .with(ApiKey::PRODUCE)
//!     .with(Acks::ALL)
//!     .with_part("orders.eu/3".parse::<TopicPartition>()?)
//!     .with_part("payments.settled/3".parse::<TopicPartition>()?);
//!
//! // A selector reads the text of its conditions as typed values: `api: Produce` and `topic: orders.eu`.
//! let api = Field::Api.parse_value("Produce")?;
//! let topic = Field::Topic.parse_value("orders.eu")?;
//!
//! // It tests a condition on the whole call against every value of the field.
//! let has = |values: Reading<FieldValues>, value: &FieldValue| {
//!     values.present().is_some_and(|values| values.contains(value))
//! };
//! assert!(has(produce.read(api.field()), &api));
//!
//! // It tests a condition on a part against each part, and the parts that pass are the target.
//! let matched = produce
//!     .parts()
//!     .iter()
//!     .filter(|part| has(produce.read_in(part, topic.field()), &topic))
//!     .map(|part| part.part().clone());
//! let target = Target::of_parts(matched).expect("the request names orders.eu");
//! assert_eq!(target.to_string(), "orders.eu/3");
//! # Ok::<(), FieldError>(())
//! ```
//!
//! # Values
//!
//! Every field holds its own type, and a value of that type is always valid.
//! The kernel has the types that Kafka itself knows, such as `TopicName`, `GroupId`, `ApiKey` and `ErrorCode`.
//! This crate adds the ones that only targeting needs.
//!
//! | Field | Type | Text form |
//! |---|---|---|
//! | `connection` | [`ConnectionNumber`] | `#7` |
//! | `address` | [`ClientAddress`] | `10.0.4.27:51459` |
//! | `host` | [`ClientHost`] | `10.0.4.27` |
//! | `endpoint` | [`Endpoint`] | `bootstrap`, `broker 2` |
//! | `client.id` | `ClientId` | `order-ingest` |
//! | `client.software` | [`ClientSoftware`] | `librdkafka 2.6.1` |
//! | `api` | `ApiKey` | `Produce` |
//! | `api.version` | [`ApiVersion`] | `11` |
//! | `topic` | `TopicName` | `orders.eu` |
//! | `partition` | `PartitionNumber` | `3` |
//! | `acks` | [`Acks`] | `-1` |
//! | `group` | `GroupId` | `analytics-etl` |
//! | `member.id` | `MemberId` | `consumer-1-4f2a` |
//! | `group.instance.id` | `GroupInstanceId` | `etl-0` |
//! | `transactional.id` | `TransactionalId` | `payments-tx-1` |
//! | `outcome` | [`Outcome`] | `ok`, `error`, `timeout` |
//! | `error.code` | `ErrorCode` | `NOT_LEADER_OR_FOLLOWER` |
//! | `latency` | [`Latency`] | `14ms` |
//!
//! The text form of a value reads back as an equal value.
//! It is what a glob pattern tests, what the UI shows and what a selector in YAML writes.
//! Patterns are not values, and they belong to whoever reads a selector.
//!
//! A successful answer has no `error.code`: the field is absent, and it is never `NONE`.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every definition type has a `serde` form.
//!
//! | Type | Form |
//! |---|---|
//! | [`Field`], [`FieldGroup`] | The name: `client.id`, `connection`. A field works as the key of a map. |
//! | [`ConnectionNumber`] | The number `7`. A format that people read also takes the text `#7`. |
//! | [`ApiVersion`], [`Acks`] | The number: `11`, `-1`. A format that people read also takes the text, such as `all`. |
//! | Every other value type | Its text form. |
//! | [`FieldValue`] | A map with one entry: `topic: orders.eu`. |
//! | `FieldValue::bare` | The form of the value's own type, for under a key that names the field. A [`Field`] reads it back as a seed. |
//! | [`Part`] | Its text form: `orders.eu/3`. |
//! | [`Target`] | The text `call`, or a map with the one entry `parts` and the list of parts. |
//!
//! A connection number in YAML is the number `7`, because `#7` starts a comment there.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_kernel` for the value objects of Kafka,
//! and on `gaslightmq_clock` for the time span that a [`Latency`] holds.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.

mod connection;
mod error;
mod field;
mod fields;
mod part;
mod request;
mod response;
#[cfg(feature = "serde")]
mod serde;
mod source;
mod text;
mod value;

pub use connection::{ClientAddress, ClientHost, ClientSoftware, ConnectionNumber, Endpoint};
pub use error::FieldError;
pub use field::{Field, FieldGroup};
pub use fields::Fields;
pub use part::{Part, PartFields, Target};
pub use request::{Acks, ApiVersion};
pub use response::{Latency, Outcome};
pub use source::{FieldSource, Reading};
pub use value::{CallValue, FieldValue, FieldValues, LearnedValue, Origin};

/// Every item of this crate.
///
/// Import it whole wherever code speaks in fields, next to the prelude of the kernel.
/// No name in it is a name of the standard prelude or of the kernel's prelude, so the import shadows nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let value: Result<FieldValue, FieldError> = Field::Topic.parse_value("orders.eu");
/// assert_eq!(value?, FieldValue::Topic(TopicName::new("orders.eu")?));
/// # Ok::<(), FieldError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Acks, ApiVersion, CallValue, ClientAddress, ClientHost, ClientSoftware, ConnectionNumber, Endpoint, Field,
        FieldError, FieldGroup, FieldSource, FieldValue, FieldValues, Fields, Latency, LearnedValue, Origin, Outcome,
        Part, PartFields, Reading, Target,
    };
}
