//! What the tests of the index write call records down with: a proxy, a client on a connection,
//! and the requests it sends.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::cell::Cell;
use std::rc::Rc;

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

/// The proxy of one test. It numbers the connections it accepts, and counts the calls of all of them together.
pub struct Proxy {
    /// How many connections the proxy accepted, which is the number of the last one.
    accepted: Cell<u64>,
    /// The ID of the next call, which every client of the proxy takes its calls from.
    next_call: Rc<Cell<CallId>>,
}

impl Proxy {
    /// A proxy that accepted no connection yet, and whose next call is its first.
    pub fn new() -> Proxy {
        Proxy {
            accepted: Cell::new(0),
            next_call: Rc::new(Cell::new(CallId::FIRST)),
        }
    }

    /// The proxy accepts a client whose requests carry this client ID,
    /// on a connection of its own to the bootstrap endpoint.
    pub fn accepts(&self, id: &str) -> Client {
        self.accepted.set(self.accepted.get() + 1);
        Client {
            connection: ConnectionNumber::new(self.accepted.get()),
            endpoint: Endpoint::Bootstrap,
            id: ClientId::new(id).unwrap(),
            group: None,
            transactional_id: None,
            next_call: Rc::clone(&self.next_call),
        }
    }
}

/// One client on one connection. Every request it sends is the next call of its proxy.
pub struct Client {
    connection: ConnectionNumber,
    endpoint: Endpoint,
    id: ClientId,
    group: Option<GroupId>,
    transactional_id: Option<TransactionalId>,
    next_call: Rc<Cell<CallId>>,
}

impl Client {
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
        let id = self.next_call.get();
        self.next_call.set(id.next());

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
