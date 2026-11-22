//! The answer message: how a call ended at a backend.

use gaslightmq_wire::{Response, WireError};

use crate::BackendFailure;

/// What a backend answers a call with.
///
/// | Answer | Says | The engine |
/// |---|---|---|
/// | [`Truth`](Self::Truth) | The cluster answered. | Reports the response to the pipeline, under the call of the request. |
/// | [`NoResponse`](Self::NoResponse) | The backend has the request, and the request has no response. | Reports nothing. The pipeline expects no answer either. |
/// | [`NotAResponse`](Self::NotAResponse) | The cluster answered, and the backend cannot read a response in it. | Reports that to the pipeline. The proxy closes the connection when that answer would be the next to leave. |
/// | [`Failed`](Self::Failed) | The backend has no answer, and none will come. | Reports the failure to the pipeline. The session ends at once, as closed by the backend. |
///
/// The answer names no call.
/// A backend behind the [`Backend`](crate::Backend) trait answers through the future of
/// [`call`](crate::Backend::call), so an answer cannot belong to another call than the one that was made.
/// A backend that answers in a later step pairs the answer with the
/// [call of the request](crate::BackendRequest::call) itself.
///
/// A backend never lies, so it has no answer that differs from the truth.
/// An answer that is no response and a failure are no lies either:
/// neither is a response, and the client never gets one made up from them.
///
/// The two differ in who closes the session.
/// An answer that is no response leaves the proxy without a way to keep the answers of the connection in order,
/// so the proxy closes it, whether the backend or the pipeline found that out.
/// A failure means that the backend lost the cluster, so the backend closed the session.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
///
/// let heartbeat = Request::new(4, 41, Some("order-ingest"), &HeartbeatRequest::default())?;
/// let answer = BackendAnswer::from(Truth::new(heartbeat.response(&HeartbeatResponse::default())?));
///
/// let BackendAnswer::Truth(truth) = answer else { unreachable!("the cluster answered") };
/// assert_eq!(truth.response().correlation_id(), 41);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub enum BackendAnswer {
    /// The cluster answered the request.
    Truth(Truth),
    /// The backend has the request, and the request has no response: a Produce with `acks=0`.
    ///
    /// The answer says that the request is on its way, and not that the cluster took it.
    ///
    /// It is the answer to a request that expects no response, and to no other request.
    /// The backend and the pipeline each read from the request whether a response comes, and they agree.
    /// When a backend gets it wrong, the engine cannot tell, and the call ends as its pipeline expects:
    ///
    /// - `NoResponse` for a request that expects a response leaves the call in flight, as a backend that never
    ///   answers does. The client gives up, and the session ends.
    /// - A [`Truth`](Self::Truth) for a request that expects none changes nothing: the pipeline does not wait for it.
    NoResponse,
    /// The cluster answered the request, and the backend cannot read a response in what it sent.
    /// `error` says what does not read.
    ///
    /// The client must not get such an answer.
    /// A response that reads, and that does not answer the request, is a [`Truth`](Self::Truth) all the same:
    /// the pipeline finds that out, and does the same with it.
    NotAResponse { error: WireError },
    /// The backend has no answer for the call, and none will come.
    ///
    /// The backend does not say whether the cluster got the request.
    Failed(BackendFailure),
}

impl From<Truth> for BackendAnswer {
    fn from(truth: Truth) -> Self {
        Self::Truth(truth)
    }
}

impl From<BackendFailure> for BackendAnswer {
    fn from(failure: BackendFailure) -> Self {
        Self::Failed(failure)
    }
}

/// The truth of a call: what the backend answered.
///
/// Rules match on it, and the explanation of a call compares the delivered response with it.
/// The [response](Self::response) answers the request as the backend got it:
/// it is of the same API, and it repeats the correlation ID of that request.
///
/// A backend in front of a real cluster lays an overlay over the answers of the cluster.
/// The overlay is part of the truth.
/// When it changed an answer, the truth keeps what the cluster answered next to it as
/// [`upstream`](Self::upstream), and the record of the call shows both.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::{MetadataRequest, MetadataResponse};
///
/// let metadata = Request::new(12, 5, Some("order-ingest"), &MetadataRequest::default())?;
/// let of_the_cluster = metadata.response(&MetadataResponse::default().with_controller_id(3.into()))?;
/// // The overlay hides broker 3, which an incident took down.
/// let with_the_overlay = metadata.response(&MetadataResponse::default().with_controller_id((-1).into()))?;
///
/// let truth = Truth::new(with_the_overlay).with_upstream(of_the_cluster);
///
/// assert_eq!(truth.response().body::<MetadataResponse>()?.controller_id, -1);
/// let upstream = truth.upstream().expect("the overlay changed the answer");
/// assert_eq!(upstream.body::<MetadataResponse>()?.controller_id, 3);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct Truth {
    response: Response,
    upstream: Option<Response>,
}

impl Truth {
    /// The truth that is this response.
    pub const fn new(response: Response) -> Self {
        Self {
            response,
            upstream: None,
        }
    }

    /// The real cluster answered `upstream`, and the overlay of the backend changed that answer into the truth.
    #[must_use]
    pub fn with_upstream(mut self, upstream: Response) -> Self {
        self.upstream = Some(upstream);
        self
    }

    /// The response that is the truth.
    pub const fn response(&self) -> &Response {
        &self.response
    }

    /// What the real cluster answered, when the overlay of the backend changed it.
    pub const fn upstream(&self) -> Option<&Response> {
        self.upstream.as_ref()
    }

    /// The response that is the truth, without what the real cluster answered.
    pub fn into_response(self) -> Response {
        self.response
    }

    /// The response that is the truth, and what the real cluster answered when the overlay changed it.
    pub fn into_parts(self) -> (Response, Option<Response>) {
        (self.response, self.upstream)
    }
}

/// A response that no overlay changed is the truth as it is.
impl From<Response> for Truth {
    fn from(response: Response) -> Self {
        Self::new(response)
    }
}
