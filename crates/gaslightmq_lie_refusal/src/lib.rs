//! Refusals for every API: the client is told that the request failed, before forwarding or after the response.
//!
//! A refusal has a definition and a behaviour.
//! The definition is the [`RefusalLie`](gaslightmq_lie::RefusalLie) of `gaslightmq_lie`: an error code and a stage.
//! This crate holds the behaviour, and no definition.
//!
//! | Type | Is |
//! |---|---|
//! | [`Refusal`] | The behaviour of a refusal: the [`Lie`](gaslightmq_lie_api::Lie) that the engine registers for the kind. |
//! | [`RefusedRequest`] | A request with what one refusal refuses of it. It does the work at both stages. |
//!
//! The [`prelude`] holds both, with the one [error](#errors).
//!
//! ```
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_lie_refusal::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
//! use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
//! use gaslightmq_wire::messages::{ProduceRequest, ProduceResponse};
//! use gaslightmq_wire::protocol::StrBytes;
//! use gaslightmq_wire_edit::prelude::*;
//!
//! // The engine registers the refusal next to the kinds of every build.
//! let lies = Lies::of_every_build().with(Refusal).expect("a kind that is new to the set");
//!
//! // A client produces to two partitions of `orders.eu`, and a rule refuses one of them.
//! let orders = || StrBytes::from_static_str("orders.eu").into();
//! let partition = |index| PartitionProduceData::default().with_index(index);
//! let topic = TopicProduceData::default().with_name(orders()).with_partition_data(vec![partition(3), partition(4)]);
//! let body = ProduceRequest::default().with_topic_data(vec![topic]);
//! let request = Request::new(9, 42, Some("order-ingest"), &body).expect("a request");
//! // The pipeline decodes the request once, with the topics it knows by ID.
//! let asked = EditableRequest::decode(&request, &TopicNames::new()).expect("a request that decodes");
//! let three = Target::of_parts(["orders.eu/3".parse().expect("a part")]).expect("one part");
//! let flap = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(three);
//!
//! // Before forwarding, the refusal asks to forward the request without `orders.eu/3`.
//! let Effect::Forward(forwarded) = lies.before_forwarding(&flap, &asked)? else {
//!     panic!("one partition is left to forward");
//! };
//! let left: ProduceRequest = forwarded.body().expect("a body that decodes");
//! assert_eq!(left.topic_data[0].partition_data.len(), 1);
//!
//! // The backend appends the records of `orders.eu/4`, and says where.
//! let appended = PartitionProduceResponse::default().with_index(4).with_base_offset(120);
//! let answered = TopicProduceResponse::default().with_name(orders()).with_partition_responses(vec![appended]);
//! let truth = forwarded.response(&ProduceResponse::default().with_responses(vec![answered])).expect("a response");
//!
//! // The pipeline asks the refusal again, and the client gets one response for both partitions.
//! let Effect::Answer(delivered) = lies.after_response(&flap, &asked, &truth)? else {
//!     panic!("the refusal puts its error into the response");
//! };
//! let body: ProduceResponse = delivered.body().expect("a body that decodes");
//! let told: Vec<(i32, i16)> = body.responses[0].partition_responses.iter().map(|p| (p.index, p.error_code)).collect();
//! assert_eq!(told, [(3, ErrorCode::NOT_LEADER_OR_FOLLOWER.value()), (4, 0)]);
//! # Ok::<(), LieError>(())
//! ```
//!
//! # A refusal refuses exactly its target
//!
//! The pipeline asks a refusal with the [target](gaslightmq_field::Target) it holds: the whole call, or some of its parts.
//! The refusal fails what the target covers, and nothing else.
//! When it cannot do exactly that, it [fails](#a-refusal-that-cannot-be-told) and changes nothing.
//!
//! | Target | On | The refusal |
//! |---|---|---|
//! | The whole call | Every API | Fails everything the request asks for, and the response itself where it has an error code. |
//! | Parts | An [API with parts](gaslightmq_wire_edit#apis-with-parts) | Fails those parts, and leaves the others to the backend. |
//! | Parts | Any other API | Cannot be told. |
//!
//! A request of an API without parts only fails as a whole.
//! A refusal that targets parts of such a request does not widen to the whole call:
//! that would fail more than its rule matched.
//! It does not pass in silence either: the verdict of the call would list a lie that nobody told.
//!
//! # Before forwarding
//!
//! The backend never sees what a refusal before forwarding refuses.
//!
//! - **The whole call.**
//!   The refusal [answers](gaslightmq_lie_api::Effect::Answer) with the response in which the call failed.
//! - **Some parts of several.**
//!   The refusal asks to [forward](gaslightmq_lie_api::Effect::Forward) the request without those parts.
//!   The backend answers for the rest, and the pipeline asks the refusal again [after the response](#after-the-response).
//! - **Every part that is left.**
//!   Nothing is left to forward, so the refusal answers with the response of its own parts.
//!
//! The refusal that removes the last part is the one that notices, and it answers.
//! Several refusals can share the parts of one request.
//! Each is asked with what the ones before it left: the first ones forward, and the last one answers.
//! The pipeline then asks the ones that forwarded again, with that answer as the response, and each adds its parts.
//!
//! # After the response
//!
//! After the response, a refusal writes its error over whatever the response says about its target.
//!
//! - **The whole call.**
//!   The answer is the response in which the call failed, and nothing of the truth is left.
//! - **Some parts.**
//!   Each of those parts has the error.
//!   It replaces what the backend answered for the part, and the part is added when the backend never saw it.
//!   Everything else stays as the response has it, and the parts come in the order of the request.
//!
//! A refusal after the response and a refusal that forwarded the rest of its request are therefore the same case.
//! The first replaces what the backend answered: the backend handled the request, and the client is told it failed.
//! The second adds what the backend never saw.
//!
//! Asking twice gives the same response as asking once.
//! The last refusal that is asked decides a part.
//! Refusals of different parts are asked in any order.
//! A refusal of the whole call writes over every part,
//! so the pipeline asks a refusal that keeps a part of its own after it.
//!
//! # A Produce that expects no response
//!
//! A Produce with `acks=0` has no response.
//! The refusal does not read `acks`: it builds the response as for any other Produce.
//! Whether a call has a response at all is not a question of one lie, so the pipeline answers it for every call.
//! Before forwarding, the refusal still keeps the refused records away from the backend, and the pipeline sends nothing.
//! After the response, there is no response to ask the refusal with.
//!
//! # A refusal that cannot be told
//!
//! A refusal tells the client that something failed, so it needs a place for the error code.
//! A response can only say what its API has a place for.
//! A request that asks for nothing by name has a response without an error code,
//! when its API and version have none in the response itself.
//! A Metadata request for every topic is one, up to version 12.
//! The response would tell the client that the cluster has no topics, which is another lie than the rule asked for.
//! Such a refusal [fails](gaslightmq_lie_api::LieError::NoPlaceForError), at both stages.
//!
//! A refusal of parts fails when the request [does not name](gaslightmq_lie_api::LieError::PartsNotInRequest) one of them.
//!
//! A lie that fails changes nothing.
//! The pipeline passes the call on untouched, and writes the error into the explanation of the call.
//!
//! # Errors
//!
//! The crate has no error type of its own.
//! It tells a lie, so [`Refusal`] and [`RefusedRequest`] return the [`LieError`](gaslightmq_lie_api::LieError) of the contract,
//! and no other error.
//! The [table](Refusal#a-refusal-that-cannot-be-told) of the behaviour says which error stands for what.
//! What the reader or the writer of a message said is the `reason` of the error, as text.
//! The message of the error is complete, so it has no [`source`](std::error::Error::source).
//!
//! # No `serde` feature
//!
//! Nothing in this crate is a definition, and nothing travels in a file or in a recording.
//! The definition of a refusal is data in `gaslightmq_lie`.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_lie` for the definition, on `gaslightmq_lie_api` for the contract,
//! on `gaslightmq_wire_edit` for the surgery, on `gaslightmq_wire` for the request and the response,
//! on `gaslightmq_field` for the target, and on `gaslightmq_kernel` for the error code.
//! A behaviour is a pure function of what it is given:
//! the crate has no async code, no IO, no clock and no randomness, and it keeps no state between calls.
//! It reaches the Kafka codec, so it does not compile to WebAssembly, and no crate that the UI shares depends on it.

mod refusal;
mod refused_request;

pub use refusal::Refusal;
pub use refused_request::RefusedRequest;

/// Every item of this crate.
///
/// Import it whole wherever code tells refusals, next to the preludes of the crates it builds on.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_lie_refusal::prelude::*;
///
/// let lies: Result<Lies, LiesError> = Lies::of_every_build().with(Refusal);
/// assert!(lies?.tells(LieKind::Refusal));
/// # Ok::<(), LiesError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Refusal, RefusedRequest};
}
