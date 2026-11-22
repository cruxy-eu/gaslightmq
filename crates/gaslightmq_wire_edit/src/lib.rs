//! Surgery on requests and responses: remove parts of a request, answer with an error, and merge responses.
//!
//! A lie often changes only a part of a call.
//! A refusal of `orders.eu/3` keeps that partition away from the backend,
//! lets the other partitions of the same Produce through,
//! and gives the client one response for all of them.
//! This crate does the cutting and the stitching, for every Kafka API.
//!
//! | Type | Is |
//! |---|---|
//! | [`EditableRequest`] | A request with its body decoded, ready for surgery. |
//! | [`Remainder`] | What is left of a request after parts were removed. |
//! | [`Failures`] | What a client is told failed: the whole call or some parts, each with an error code. |
//! | [`TopicNames`] | The topics that are known by ID, so a request that names a topic by ID still has named parts. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! The crate speaks in the [`Part`](gaslightmq_field::Part) of `gaslightmq_field`:
//! one partition of a topic, or a topic on an API that names no partitions.
//! That is the piece of a call that a lie changes on its own.
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
//! use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
//! use gaslightmq_wire::messages::{ProduceRequest, ProduceResponse};
//! use gaslightmq_wire::protocol::StrBytes;
//! use gaslightmq_wire_edit::prelude::*;
//!
//! // A client produces to two partitions of `orders.eu`.
//! let orders = || StrBytes::from_static_str("orders.eu").into();
//! let partition = |index| PartitionProduceData::default().with_index(index);
//! let topic = TopicProduceData::default().with_name(orders()).with_partition_data(vec![partition(3), partition(4)]);
//! let request = Request::new(9, 42, Some("order-ingest"), &ProduceRequest::default().with_topic_data(vec![topic]))?;
//!
//! let asked = EditableRequest::decode(&request, &TopicNames::new())?;
//! let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
//! assert_eq!(asked.parts(), [three.clone(), four.clone()]);
//!
//! // Before forwarding, a refusal removes `orders.eu/3`. The backend gets what is left.
//! let refused = Failures::none().with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER);
//! let Remainder::Rest(forwarded) = asked.without([&three])? else {
//!     panic!("one partition is left");
//! };
//!
//! // The backend appends the records of `orders.eu/4`, and says where.
//! let appended = PartitionProduceResponse::default().with_index(4).with_base_offset(120);
//! let answered = TopicProduceResponse::default().with_name(orders()).with_partition_responses(vec![appended]);
//! let truth = forwarded.response(&ProduceResponse::default().with_responses(vec![answered]))?;
//!
//! // After the response, the refusal joins the truth. The client gets one response for both partitions.
//! let delivered = asked.merge(&truth, &refused)?;
//! assert_eq!(
//!     asked.parts_of(&delivered)?,
//!     [PartFields::new(three).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER), PartFields::new(four).succeeded()],
//! );
//!
//! // A refusal of both partitions leaves nothing to forward, and the response never sees a backend.
//! assert!(matches!(asked.without(asked.parts())?, Remainder::Nothing));
//! let everything = Failures::whole_call(ErrorCode::REQUEST_TIMED_OUT);
//! assert_eq!(asked.error_response(&everything)?.correlation_id(), 42);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Every API fails as a whole
//!
//! [`EditableRequest::error_response`] answers a request of every API that `gaslightmq_wire` has messages for,
//! at every version of it.
//! When the [whole call](Failures::whole_call) fails, the error code stands wherever the response has a place for it:
//!
//! - in the response itself, on an API and a version that has an error code there,
//! - and in every entry that answers something the request asked for by name:
//!   a partition, a topic, a group, a coordinator key, a configuration resource and so on.
//!
//! A Fetch of version 7 or later fails in both places, and a Produce in each of its partitions.
//! A `DescribeGroups` fails in each group it asks for, and a `Heartbeat` in the response itself.
//! The other fields of the response hold what a broker writes when it has nothing to say, such as `-1` for an offset.
//!
//! A response can only say what its API has a place for.
//! A request that asks for nothing by name has a response without an error code,
//! when its API and version have none in the response itself.
//! A Metadata request for every topic is one, up to version 12.
//!
//! # APIs with parts
//!
//! On these APIs a request names [parts](gaslightmq_field::Part),
//! so parts can leave the request, fail alone, and join the response of the backend.
//!
//! | Parts | APIs |
//! |---|---|
//! | Partitions | Produce, Fetch, `ListOffsets`, `OffsetForLeaderEpoch`, `DeleteRecords`, `OffsetCommit`, `OffsetFetch`, `OffsetDelete`, `TxnOffsetCommit`, `AddPartitionsToTxn` |
//! | Topics | Metadata, `CreateTopics`, `DeleteTopics`, `CreatePartitions` |
//!
//! A request of any other API has no parts.
//! It only fails as a whole, nothing can be removed from it, and no part joins its response.
//! Some of those APIs do name partitions, such as `DescribeProducers` and `ElectLeaders`.
//! They are not on the list yet: the list holds what producers, consumers and transactions send,
//! and the administration of topics.
//!
//! A request with parts can still name none:
//! a Metadata request or an `OffsetFetch` that asks for every topic.
//!
//! When only parts fail, the response itself does not fail.
//! A client then sees what a broker says about one partition, such as "not the leader",
//! and not a failure of the whole request.
//!
//! # The order of a response
//!
//! A merged response names its parts in the order of the request, whoever answered them:
//! topics as the request names them, and partitions within their topic.
//! A response of the backend that names something the request did not ask for keeps it, after the others.
//! Nothing in the order tells a client which parts never reached the backend.
//!
//! # Topic IDs
//!
//! Newer versions of Fetch and Produce name a topic by its ID alone.
//! [`TopicNames`] maps IDs to names, and it learns them from the responses that carry both.
//! [`EditableRequest::decode`] takes the names,
//! so `orders.eu/3` is the same part whichever way a request names the topic.
//! A response names the topic the way its request did.
//!
//! # Errors
//!
//! Every type that can fail has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`EditableRequest`] | [`EditableRequestError`] | The API has no messages, a body does not decode or encode, a response belongs to another request, or nothing failed. |
//! | [`TopicNames`] | [`TopicNamesError`] | The response to learn from does not decode. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! A variant that holds the [`WireError`](gaslightmq_wire::WireError) of the codec writes it once, after what it was doing.
//!
//! [`Failures`] and [`Remainder`] hold any value, so they have no error.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_wire` for the codec, on `gaslightmq_field` for the parts of a call,
//! and on `gaslightmq_kernel` for topic names, topic IDs and error codes.
//! It has no async code, no IO, no clock and no randomness.
//! It works on requests and responses in memory, and it changes none of them: every result is a new value.

mod api;
mod body;
mod entry;
mod failures;
mod request;
mod topic_names;

pub use failures::Failures;
pub use request::{EditableRequest, EditableRequestError, Remainder};
pub use topic_names::{TopicNames, TopicNamesError};

/// Every item of this crate.
///
/// Import it whole wherever code does surgery, next to the preludes of the kernel and of the fields.
/// No name in it is a name of the standard prelude or of those two, so the import shadows nothing.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let failures = Failures::of_target(&Target::Call, ErrorCode::REQUEST_TIMED_OUT);
/// assert_eq!(failures.of_call(), Some(ErrorCode::REQUEST_TIMED_OUT));
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{EditableRequest, EditableRequestError, Failures, Remainder, TopicNames, TopicNamesError};
}
