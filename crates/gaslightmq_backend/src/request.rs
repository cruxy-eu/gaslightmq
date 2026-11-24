//! The request message: a request on its way to a backend.

use gaslightmq_call::CallId;
use gaslightmq_field::{ConnectionNumber, Endpoint};
use gaslightmq_wire::Request;

/// A request as a backend takes it: under the ID of its call, with its connection and the endpoint that is on.
///
/// The engine makes one from every request that a pipeline forwards.
/// A broker that runs alone gets the same message from whoever drives it.
///
/// | Part | Tells the backend |
/// |---|---|
/// | [`call`](Self::call) | Which call the request belongs to. An answer in a later step comes under this ID. |
/// | [`connection`](Self::connection) | Which connection the request arrived on. The ID of a call does not say that. |
/// | [`endpoint`](Self::endpoint) | Which endpoint that connection is on: `bootstrap`, or the broker the client meant to reach. |
/// | [`request`](Self::request) | What the client asked, or what the lies before forwarding left of it. |
///
/// A backend needs the connection, because the requests of one connection keep their order.
/// It needs the endpoint, because a request for `broker 2` is a request to that broker and to no other.
/// The endpoint of a connection never changes.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::HeartbeatRequest;
///
/// let heartbeat = Request::new(4, 41, Some("order-ingest"), &HeartbeatRequest::default()).expect("a request");
/// let call = CallId::FIRST;
/// let connection = ConnectionNumber::new(7);
/// let request = BackendRequest::new(call, connection, Endpoint::Broker(BrokerId::from(2)), heartbeat);
///
/// assert_eq!(request.call(), call);
/// assert_eq!(request.connection(), connection);
/// assert_eq!(request.endpoint().to_string(), "broker 2");
/// assert_eq!(request.request().correlation_id(), 41);
/// ```
#[derive(Debug, Clone)]
pub struct BackendRequest {
    call: CallId,
    connection: ConnectionNumber,
    endpoint: Endpoint,
    request: Request,
}

impl BackendRequest {
    /// The request of `call`, which arrived on `connection`, a connection to `endpoint`.
    pub const fn new(call: CallId, connection: ConnectionNumber, endpoint: Endpoint, request: Request) -> Self {
        Self {
            call,
            connection,
            endpoint,
            request,
        }
    }

    /// The call the request belongs to.
    ///
    /// A backend that answers in a later step keeps this ID, and answers under it.
    pub const fn call(&self) -> CallId {
        self.call
    }

    /// The connection the request arrived on.
    pub const fn connection(&self) -> ConnectionNumber {
        self.connection
    }

    /// The endpoint that the connection of the request is on.
    pub const fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// The request itself. It carries the correlation ID of the client.
    pub const fn request(&self) -> &Request {
        &self.request
    }

    /// The request itself, without the call, the connection and the endpoint.
    pub fn into_request(self) -> Request {
        self.request
    }
}
