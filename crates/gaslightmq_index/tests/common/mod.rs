//! What the tests of the index write call records down with: a client on a connection, and the requests it sends.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::sync::atomic::{AtomicU64, Ordering};

use gaslightmq_call::prelude::*;
use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

/// One request as a test writes it down: its API, and the parts it names with the truth of each.
///
/// The backend answers the call without an error, unless the test says otherwise.
pub struct Request {
    api: ApiKey,
    parts: Vec<PartFields>,
    outcome: Option<Outcome>,
}

impl Request {
    /// A request of `api` that names no topic.
    pub fn of(api: ApiKey) -> Request {
        Request {
            api,
            parts: Vec::new(),
            outcome: Some(Outcome::Ok),
        }
    }

    /// The request names `part`, such as `orders.eu/3`, and the backend answers that part without an error.
    pub fn ok(mut self, part: &str) -> Request {
        self.parts
            .push(PartFields::new(part.parse::<Part>().unwrap()).succeeded());
        self
    }

    /// The request names `part`, and the backend answers that part with `code`.
    pub fn failed(mut self, part: &str, code: ErrorCode) -> Request {
        self.parts
            .push(PartFields::new(part.parse::<Part>().unwrap()).failed(code));
        self
    }

    /// The backend does not answer the call in time.
    pub fn timed_out(mut self) -> Request {
        self.outcome = Some(Outcome::Timeout);
        self
    }

    /// The call never reaches the backend, so it has no truth.
    pub fn never_forwarded(mut self) -> Request {
        self.outcome = None;
        self
    }
}

/// The number of the next connection, across every test of the binary: each client gets one of its own.
static CONNECTIONS: AtomicU64 = AtomicU64::new(1);

/// The ID of the next call, across every test of the binary: an environment counts the calls of all its connections.
static CALLS: AtomicU64 = AtomicU64::new(1);

/// One client on one connection. Every request it sends is the next call of its environment.
pub struct Client {
    connection: ConnectionNumber,
    endpoint: Endpoint,
    id: ClientId,
    group: Option<GroupId>,
    transactional_id: Option<TransactionalId>,
}

impl Client {
    /// A client whose requests carry this client ID, on a connection of its own to the bootstrap endpoint.
    pub fn identified_as(id: &str) -> Client {
        Client {
            connection: ConnectionNumber::new(CONNECTIONS.fetch_add(1, Ordering::Relaxed)),
            endpoint: Endpoint::Bootstrap,
            id: ClientId::new(id).unwrap(),
            group: None,
            transactional_id: None,
        }
    }

    /// The client connected to `endpoint`, and not to the bootstrap endpoint.
    pub fn connected_to(mut self, endpoint: Endpoint) -> Client {
        self.endpoint = endpoint;
        self
    }

    /// The client context knows that the client is in this group, so every call of it has the group.
    pub fn in_group(mut self, group: &str) -> Client {
        self.group = Some(GroupId::new(group).unwrap());
        self
    }

    /// The client context knows that the client writes under this transactional ID.
    pub fn transacting_as(mut self, id: &str) -> Client {
        self.transactional_id = Some(TransactionalId::new(id).unwrap());
        self
    }

    /// The connection of the client.
    pub fn connection(&self) -> ConnectionNumber {
        self.connection
    }

    /// The fields that every call of the client has: the identity of its session, and what its context knows.
    fn session(&self) -> Fields {
        let identity = Fields::new()
            .with(self.connection)
            .with(self.endpoint)
            .with(self.id.clone());
        let in_group = self.group.iter().cloned().fold(identity, Fields::with_learned);
        self.transactional_id
            .iter()
            .cloned()
            .fold(in_group, Fields::with_learned)
    }

    /// The record of the next call of the client: `request`, which no rule touched.
    pub fn sends(&self, request: Request) -> Call {
        let id = CallId::new(CALLS.fetch_add(1, Ordering::Relaxed)).unwrap();

        let asked = self.session().with(request.api);
        let fields = match request.outcome {
            Some(outcome) => request.parts.into_iter().fold(asked, Fields::with_part).with(outcome),
            None => request
                .parts
                .into_iter()
                .map(PartFields::without_truth)
                .fold(asked, Fields::with_part)
                .without_truth(),
        };
        let arrived = CallTiming::arrived(Instant::ZERO);
        Call::new(id, fields, Delivered::Nothing, Explanation::none(), arrived).unwrap()
    }
}
