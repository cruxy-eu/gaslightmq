use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::{Api, Request, Response, WireError};

use crate::body::{self, Body, Left, MergeFlaw, TopicKey, Verdict};
use crate::{Failures, TopicNames};

/// A request with its body decoded, ready for surgery.
///
/// It knows the [parts](Part) the request names, and it does four things with them.
///
/// | Method | Gives |
/// |---|---|
/// | [`without`](Self::without) | The request without some parts: what is left to forward. |
/// | [`error_response`](Self::error_response) | A response nobody asked a backend for, in which the call or its parts failed. |
/// | [`merge`](Self::merge) | The response of the backend, with failed parts put into it. |
/// | [`parts_of`](Self::parts_of) | What a response says about each part. |
///
/// It works on every API that `gaslightmq_wire` has messages for.
/// The original request stays as it arrived, and [`request`](Self::request) gives it back.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
/// use gaslightmq_wire::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
/// use gaslightmq_wire::messages::{ListOffsetsRequest, ListOffsetsResponse};
/// use gaslightmq_wire::protocol::StrBytes;
/// use gaslightmq_wire_edit::prelude::*;
///
/// // A client asks for the end of two partitions.
/// let orders = || StrBytes::from_static_str("orders.eu").into();
/// let partition = |index| ListOffsetsPartition::default().with_partition_index(index);
/// let topic = ListOffsetsTopic::default().with_name(orders()).with_partitions(vec![partition(3), partition(4)]);
/// let request = Request::new(7, 42, Some("order-ingest"), &ListOffsetsRequest::default().with_topics(vec![topic]))?;
///
/// let asked = EditableRequest::decode(&request, &TopicNames::new())?;
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// assert_eq!(asked.parts(), [three.clone(), four.clone()]);
///
/// // A lie refuses one of them, so the backend only sees the other.
/// let Remainder::Rest(forwarded) = asked.without([&three])? else {
///     panic!("one partition is left");
/// };
/// assert_eq!(EditableRequest::decode(&forwarded, &TopicNames::new())?.parts(), [four.clone()]);
///
/// // The backend answers for that one.
/// let end = ListOffsetsPartitionResponse::default().with_partition_index(4).with_offset(120);
/// let answered = ListOffsetsTopicResponse::default().with_name(orders()).with_partitions(vec![end]);
/// let truth = forwarded.response(&ListOffsetsResponse::default().with_topics(vec![answered]))?;
///
/// // The client gets one response for both partitions, in the order it asked for them.
/// let failures = Failures::none().with_part(three.clone(), ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// let delivered = asked.merge(&truth, &failures)?;
/// assert_eq!(
///     asked.parts_of(&delivered)?,
///     [PartFields::new(three).failed(ErrorCode::NOT_LEADER_OR_FOLLOWER), PartFields::new(four).succeeded()],
/// );
/// let body: ListOffsetsResponse = delivered.body()?;
/// assert_eq!(body.topics[0].partitions[1].offset, 120);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Parts
///
/// A part is a partition on an API that names partitions, and a topic on an API that only names topics.
/// See [the APIs with parts](crate#apis-with-parts).
/// A request of any other API has no parts, and only fails as a whole.
///
/// A request that names a topic by its ID has that topic as a part when the [`TopicNames`] know the ID.
/// A topic with an unknown ID is no part: nobody can name it.
/// It stays in the request whatever is removed, and it only fails when the whole call fails.
#[derive(Debug, Clone)]
pub struct EditableRequest {
    request: Request,
    body: Arc<dyn Body>,
    /// The names of the topics that the request names by ID.
    topics: TopicNames,
    parts: Vec<Part>,
}

impl EditableRequest {
    /// Decode the body of `request`, and name its parts.
    ///
    /// `topics` gives a name to the topics that the request names by ID.
    /// The editable request keeps the names it needs, and it does not hold on to `topics`.
    ///
    /// # Errors
    ///
    /// [`EditableRequestError::UnknownApi`] when `gaslightmq_wire` has no messages for the API of the request,
    /// and [`EditableRequestError::UnreadableRequest`] when the body does not decode.
    pub fn decode(request: &Request, topics: &TopicNames) -> Result<Self, EditableRequestError> {
        let api = request.api();
        let known = api.known().ok_or(EditableRequestError::UnknownApi { api })?;
        let body =
            body::decode(known, request).map_err(|error| EditableRequestError::UnreadableRequest { api, error })?;

        let mut by_id = TopicNames::new();
        let mut parts = Vec::new();
        let mut seen = BTreeSet::new();
        body.each_part(api.version, &mut |topic, partition| {
            if let Some(id) = TopicId::from_wire(topic.id)
                && let Some(name) = topics.name_of(id)
            {
                by_id.learn(id, name.clone());
            }
            if let Some(part) = part_of(&by_id, topic, partition)
                && seen.insert(part.clone())
            {
                parts.push(part);
            }
        });

        Ok(Self {
            request: request.clone(),
            body,
            topics: by_id,
            parts,
        })
    }

    /// The request as it arrived.
    pub fn request(&self) -> &Request {
        &self.request
    }

    /// The API of the request.
    pub fn api(&self) -> Api {
        self.request.api()
    }

    /// The parts the request names, in the order it names them, each once.
    pub fn parts(&self) -> &[Part] {
        &self.parts
    }

    /// The request without `parts`: what is left to forward.
    ///
    /// A part that the request does not name changes nothing.
    /// A topic that lost its last partition leaves the request with it.
    ///
    /// # Errors
    ///
    /// [`EditableRequestError::UnwritableRequest`] when what is left does not encode.
    pub fn without<'a>(&self, parts: impl IntoIterator<Item = &'a Part>) -> Result<Remainder, EditableRequestError> {
        let removed: BTreeSet<&Part> = parts.into_iter().collect();
        if !self.parts.iter().any(|part| removed.contains(part)) {
            return Ok(Remainder::Whole);
        }
        let mut stays = |topic: TopicKey<'_>, partition: Option<i32>| {
            self.part(topic, partition).is_none_or(|part| !removed.contains(&part))
        };
        match self.body.without(&self.request, &mut stays) {
            Ok(Left::Everything) => Ok(Remainder::Whole),
            Ok(Left::Rest(rest)) => Ok(Remainder::Rest(rest)),
            Ok(Left::Nothing) => Ok(Remainder::Nothing),
            Err(error) => Err(EditableRequestError::UnwritableRequest { api: self.api(), error }),
        }
    }

    /// A response to the request in which what `failures` names failed.
    ///
    /// Nobody asks a backend for this response.
    /// It repeats the correlation ID of the request, and it has the shape of the request's API and version.
    ///
    /// - When the [whole call](Failures::of_call) fails, every topic, partition, group or other thing the request asks for
    ///   has the error code, and so has the response itself where the API has a place for it.
    ///   A part with an error of its own keeps that one.
    /// - When only [parts](Failures::parts) fail, the response holds those parts and nothing else.
    ///   That is the whole response when the request went nowhere because every part of it was [removed](Remainder::Nothing).
    ///
    /// A response can only say what its API has a place for.
    /// A request that asks for nothing by name, such as a Metadata request for every topic,
    /// has a response without an error code on most versions.
    ///
    /// # Errors
    ///
    /// [`EditableRequestError::NothingFailed`] when `failures` names nothing that the request asks,
    /// and [`EditableRequestError::UnwritableResponse`] when the response does not encode.
    pub fn error_response(&self, failures: &Failures) -> Result<Response, EditableRequestError> {
        if failures.of_call().is_none() && !self.fails_a_part(failures) {
            return Err(EditableRequestError::NothingFailed { api: self.api() });
        }
        let part = |topic: TopicKey<'_>, partition: Option<i32>| self.error_code(failures, topic, partition);
        let call = ErrorCode::to_wire(failures.of_call());
        self.body
            .failed(&self.request, &Verdict::new(call, &part))
            .map_err(|error| EditableRequestError::UnwritableResponse { api: self.api(), error })
    }

    /// The response of the backend, with what `failures` names put into it.
    ///
    /// `truth` answers this request, or what was [left](Self::without) of it.
    /// Every part that fails is in the result with its error code:
    /// it replaces what the backend said about that part, and it is added when the backend never saw the part.
    /// Everything else stays as the backend said it, and the parts come in the order of the request.
    ///
    /// When no part of the request fails, the result is `truth` as it arrived, byte for byte.
    /// When the [whole call](Failures::of_call) fails, nothing of the truth is left,
    /// and the result is the [error response](Self::error_response).
    ///
    /// # Errors
    ///
    /// [`EditableRequestError::ResponseOfAnotherCall`] when `truth` answers another request,
    /// [`EditableRequestError::UnreadableResponse`] when its body does not decode,
    /// and [`EditableRequestError::UnwritableResponse`] when the result does not encode.
    pub fn merge(&self, truth: &Response, failures: &Failures) -> Result<Response, EditableRequestError> {
        let api = self.api();
        self.check_answers(truth)?;
        if failures.of_call().is_some() {
            return self.error_response(failures);
        }
        if !self.fails_a_part(failures) {
            return Ok(truth.clone());
        }
        let part = |topic: TopicKey<'_>, partition: Option<i32>| self.error_code(failures, topic, partition);
        self.body
            .merged(&self.request, truth, &Verdict::new(0, &part))
            .map_err(|flaw| match flaw {
                MergeFlaw::UnreadableResponse(error) => EditableRequestError::UnreadableResponse { api, error },
                MergeFlaw::UnwritableResponse(error) => EditableRequestError::UnwritableResponse { api, error },
            })
    }

    /// The parts of `response`, a response to this request, each with what the response says about it.
    ///
    /// A part [succeeded](PartFields::succeeded) when its error code is zero, and it [failed](PartFields::failed) with any other.
    /// The parts come in the order of the response.
    /// A topic that the response names by an ID this request does not use is no part, as in the request.
    ///
    /// A response of an API without parts has none.
    /// The error of such a response as a whole is not something this crate reads.
    ///
    /// # Errors
    ///
    /// [`EditableRequestError::ResponseOfAnotherCall`] when `response` answers another request,
    /// and [`EditableRequestError::UnreadableResponse`] when its body does not decode.
    pub fn parts_of(&self, response: &Response) -> Result<Vec<PartFields>, EditableRequestError> {
        self.check_answers(response)?;
        let mut parts = Vec::new();
        self.body
            .each_answered_part(response, &mut |topic, partition, error_code| {
                if let Some(part) = self.part(topic, partition) {
                    let part = PartFields::new(part);
                    parts.push(match ErrorCode::from_wire(error_code) {
                        Some(error) => part.failed(error),
                        None => part.succeeded(),
                    });
                }
            })
            .map_err(|error| EditableRequestError::UnreadableResponse { api: self.api(), error })?;
        Ok(parts)
    }

    /// Checks that `response` answers this request: same API, same correlation ID.
    fn check_answers(&self, response: &Response) -> Result<(), EditableRequestError> {
        if response.api() == self.api() && response.correlation_id() == self.request.correlation_id() {
            Ok(())
        } else {
            Err(EditableRequestError::ResponseOfAnotherCall {
                api: self.api(),
                correlation_id: self.request.correlation_id(),
                answered_api: response.api(),
                answered_correlation_id: response.correlation_id(),
            })
        }
    }

    /// The part that an entry of the request is, if it has a name.
    fn part(&self, topic: TopicKey<'_>, partition: Option<i32>) -> Option<Part> {
        part_of(&self.topics, topic, partition)
    }

    /// The error code of an entry of the request.
    /// An entry that is no part only fails with the whole call.
    fn error_code(&self, failures: &Failures, topic: TopicKey<'_>, partition: Option<i32>) -> Option<i16> {
        match self.part(topic, partition) {
            Some(part) => failures.of(&part),
            None => failures.of_call(),
        }
        .map(ErrorCode::value)
    }

    /// Whether `failures` has an error for a part of this request.
    fn fails_a_part(&self, failures: &Failures) -> bool {
        self.parts.iter().any(|part| failures.of(part).is_some())
    }
}

/// The part that an entry of a request is: its topic by name, or by an ID that `topics` knows, and its partition.
fn part_of(topics: &TopicNames, topic: TopicKey<'_>, partition: Option<i32>) -> Option<Part> {
    let name = match TopicName::new(topic.name) {
        Ok(name) => name,
        Err(_) => topics.name_of(TopicId::from_wire(topic.id)?)?.clone(),
    };
    match partition {
        None => Some(Part::Topic(name)),
        Some(index) => Some(Part::Partition(name.partition(PartitionNumber::try_from(index).ok()?))),
    }
}

/// What is left of a request after [parts were removed](EditableRequest::without).
#[derive(Debug, Clone)]
pub enum Remainder {
    /// No part was removed. The request goes on as it arrived, byte for byte.
    Whole,
    /// Some parts were removed. This request names the others.
    ///
    /// It is encoded anew, for the same API and version, under the same correlation ID and client ID.
    /// Everything else of its body is as the client sent it.
    Rest(Request),
    /// Every part was removed. Nothing is left to forward.
    Nothing,
}

/// Why surgery on a request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EditableRequestError {
    /// There are no messages for the API of the request, so its body has no known shape.
    UnknownApi { api: Api },
    /// The body of the request does not decode.
    UnreadableRequest { api: Api, error: WireError },
    /// What is left of the request does not encode.
    UnwritableRequest { api: Api, error: WireError },
    /// The body of the response to merge does not decode.
    UnreadableResponse { api: Api, error: WireError },
    /// The response does not encode.
    UnwritableResponse { api: Api, error: WireError },
    /// The response to merge belongs to another request: its API or its correlation ID differs.
    ResponseOfAnotherCall {
        api: Api,
        correlation_id: i32,
        answered_api: Api,
        answered_correlation_id: i32,
    },
    /// The failures name nothing that the request asks, so there is no error to answer with.
    NothingFailed { api: Api },
}

impl fmt::Display for EditableRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownApi { api } => write!(f, "a request of {api} cannot be edited: its messages are unknown"),
            Self::UnreadableRequest { api, error } => write!(f, "the {api} request cannot be read: {error}"),
            Self::UnwritableRequest { api, error } => {
                write!(f, "what is left of the {api} request cannot be written: {error}")
            }
            Self::UnreadableResponse { api, error } => write!(f, "the {api} response cannot be read: {error}"),
            Self::UnwritableResponse { api, error } => write!(f, "the {api} response cannot be written: {error}"),
            Self::ResponseOfAnotherCall {
                api,
                correlation_id,
                answered_api,
                answered_correlation_id,
            } => write!(
                f,
                "the {answered_api} response {answered_correlation_id} does not answer the {api} request {correlation_id}"
            ),
            Self::NothingFailed { api } => write!(f, "nothing that the {api} request asks failed"),
        }
    }
}

/// The message is complete, so no variant has a source.
impl std::error::Error for EditableRequestError {}
