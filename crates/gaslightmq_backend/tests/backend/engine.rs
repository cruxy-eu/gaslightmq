//! What a pipeline forwards becomes a call on a backend, and whatever the backend answers goes back into the pipeline.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::executor::block_on;
use gaslightmq_backend::prelude::*;
use gaslightmq_call::prelude::*;
use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_incident::prelude::*;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::Lies;
use gaslightmq_pipeline::prelude::*;
use gaslightmq_rule::Snapshot;
use gaslightmq_session::prelude::*;
use gaslightmq_wire::messages::{HeartbeatRequest, MetadataRequest, ProduceRequest};
use gaslightmq_wire::{FrameDecoder, Request, Response};

use crate::cluster::{CLIENT, Cluster, broker};

const NOW: Instant = Instant::ZERO;

/// The session task of an engine in miniature: the pipeline of connection 7, and the backend of its proxy.
///
/// It waits for each answer before it performs the next output, which keeps the test short.
/// An engine starts a call and goes on.
struct Session<'a, B> {
    pipeline: Pipeline,
    endpoint: Endpoint,
    backend: &'a B,
    /// The responses that the client received, as bytes with their length in front.
    sent: Vec<Vec<u8>>,
    /// The records of the calls that ended.
    records: Vec<Call>,
}

impl<'a, B: Backend> Session<'a, B> {
    fn open(endpoint: Endpoint, backend: &'a B) -> Self {
        let shared = SharedBySessions::new(
            ClientContexts::new(Duration::from_secs(60)),
            KnownTopics::new(),
            Arc::new(Lies::of_every_build()),
        );
        let connection = Connection::new(ConnectionNumber::new(7), "10.0.4.27:51459".parse().unwrap(), endpoint);
        let rules = Arc::new(Snapshot::default());
        Self {
            pipeline: Pipeline::open(connection, ConnectionOrdinal::FIRST, shared, rules, NOW),
            endpoint,
            backend,
            sent: Vec::new(),
            records: Vec::new(),
        }
    }

    /// The client sends `request`, and the session performs whatever follows from it.
    async fn client_sends(&mut self, request: &Request) {
        let bytes = request.frame().to_bytes();
        let outputs = self.pipeline.handle(Input::ClientBytes(&bytes), NOW);
        self.perform(outputs).await;
    }

    /// The backend reported endpoints in which the endpoint of this session closed.
    async fn endpoint_closed(&mut self) {
        let reason = format!("endpoint `{}` accepts no connections any more", self.endpoint);
        let outputs = self.pipeline.handle(Input::BackendClosed { reason }, NOW);
        self.perform(outputs).await;
    }

    async fn perform(&mut self, outputs: Outputs) {
        let mut outputs: VecDeque<Output> = outputs.into_iter().collect();
        while let Some(output) = outputs.pop_front() {
            match output {
                Output::Forward { call, request } => {
                    let request = BackendRequest::new(call, self.endpoint, request);
                    // Every answer of a backend has its input, but the one that says there is nothing to report.
                    let input = match self.backend.call(request).await {
                        BackendAnswer::Truth(truth) => Input::BackendAnswered {
                            call,
                            response: truth.into_response(),
                        },
                        BackendAnswer::NoResponse => continue,
                        BackendAnswer::NotAResponse { reason } => {
                            Input::BackendAnswerUnreadable { call, error: reason }
                        }
                        BackendAnswer::Failed(failure) => Input::BackendFailed {
                            call,
                            failure: failure.to_string(),
                        },
                    };
                    outputs.extend(self.pipeline.handle(input, NOW));
                }
                Output::Send(bytes) => self.sent.push(bytes.to_vec()),
                Output::Emit(call) => self.records.push(*call),
                // The engine closes the transport, and the backend lets go of the connection.
                Output::Close => self.backend.close(self.pipeline.connection().number()).await,
                Output::SetTimer { .. } | Output::Fire(_) => {}
            }
        }
    }

    /// Who closed the session, or `None` while it is open.
    fn closed_by(&self) -> Option<ClosedBy> {
        self.pipeline.closed().map(|closed| closed.by())
    }

    /// What the record of each call says went wrong.
    fn failures(&self) -> Vec<Option<&str>> {
        let records = self.records.iter();
        records
            .map(|record| record.explanation().failure().map(Failure::message))
            .collect()
    }
}

/// The correlation ID of a response that the client received.
fn correlation_id(sent: &[u8]) -> i32 {
    let mut frames = FrameDecoder::new();
    frames.extend(sent);
    Response::peek_correlation_id(&frames.next_frame().unwrap().unwrap()).unwrap()
}

fn metadata() -> Request {
    Request::new(12, 41, Some(CLIENT), &MetadataRequest::default()).unwrap()
}

fn broker_2_goes_down() -> Incident {
    Incident::BrokerGoesDown {
        broker: BrokerId::from(2u16),
        failover: false,
    }
}

#[test]
fn a_request_that_a_pipeline_forwards_comes_back_as_the_truth() {
    let cluster = Cluster::default();
    let mut session = Session::open(Endpoint::Bootstrap, &cluster);

    block_on(session.client_sends(&metadata()));

    // The backend took the request under the call ID of the pipeline, and the client got the answer.
    assert_eq!(cluster.taken(), ["#7.1"]);
    assert_eq!(session.sent.len(), 1);
    assert_eq!(correlation_id(&session.sent[0]), 41);
    assert_eq!(session.failures(), [None]);
    assert_eq!(session.closed_by(), None);
}

#[test]
fn a_request_without_a_response_goes_to_the_backend_and_nothing_comes_back() {
    let cluster = Cluster::default();
    let mut session = Session::open(broker(2), &cluster);
    let produce = Request::new(9, 41, Some(CLIENT), &ProduceRequest::default().with_acks(0)).unwrap();

    block_on(session.client_sends(&produce));

    assert_eq!(cluster.taken(), ["#7.1"]);
    assert_eq!(session.sent, Vec::<Vec<u8>>::new());
    assert_eq!(session.closed_by(), None);
}

#[test]
fn a_failure_of_the_backend_ends_the_session_as_closed_by_the_backend() {
    let cluster = Cluster::default();
    // The session opened on the endpoint of broker 2 just before the broker went down.
    let mut session = Session::open(broker(2), &cluster);
    block_on(cluster.apply(broker_2_goes_down())).unwrap();

    block_on(session.client_sends(&metadata()));

    // The client gets no response that the cluster did not send: it finds its connection closed.
    assert_eq!(session.sent, Vec::<Vec<u8>>::new());
    assert_eq!(session.closed_by(), Some(ClosedBy::Backend));
    // The record of the call says why, in the words of the backend.
    assert_eq!(session.records[0].delivered(), &Delivered::ConnectionClosed);
    assert_eq!(
        session.failures(),
        [Some(
            "the backend has no answer for call #7.1: the cluster takes no requests on endpoint `broker 2`"
        )]
    );
    // The backend knows that the connection is gone.
    assert_eq!(cluster.closed(), [ConnectionNumber::new(7)]);
}

#[test]
fn an_answer_that_the_backend_cannot_read_ends_the_session_as_closed_by_the_proxy() {
    let cluster = Cluster::default();
    let mut session = Session::open(Endpoint::Bootstrap, &cluster);
    let heartbeat = Request::new(4, 41, Some(CLIENT), &HeartbeatRequest::default()).unwrap();

    block_on(session.client_sends(&heartbeat));

    // The backend did not go away. The proxy has no answer to send, so it closes the connection itself.
    assert_eq!(session.sent, Vec::<Vec<u8>>::new());
    assert_eq!(session.closed_by(), Some(ClosedBy::Proxy));
    assert_eq!(session.records[0].delivered(), &Delivered::ConnectionClosed);
    assert_eq!(
        session.failures(),
        [Some(
            "the backend answered with what is no response: response header is cut short at 3 bytes"
        )]
    );
    assert_eq!(cluster.closed(), [ConnectionNumber::new(7)]);
}

#[test]
fn a_session_ends_when_its_endpoint_closed_even_if_it_is_open_again() {
    let cluster = Cluster::default();
    let mut session = Session::open(broker(2), &cluster);
    let known = block_on(cluster.endpoints(&Endpoints::none()));

    // Broker 2 goes down and comes back before the engine asks for the endpoints again.
    block_on(cluster.apply(broker_2_goes_down())).unwrap();
    let back = Incident::BrokerComesBack {
        broker: BrokerId::from(2u16),
    };
    block_on(cluster.apply(back)).unwrap();
    let endpoints = block_on(cluster.endpoints(&known));

    // The engine closes the sessions on every endpoint that closed in between.
    let closed: Vec<Endpoint> = endpoints.closed_since(&known).collect();
    assert_eq!(closed, [session.endpoint]);
    block_on(session.endpoint_closed());

    assert_eq!(session.closed_by(), Some(ClosedBy::Backend));
    assert_eq!(cluster.closed(), [ConnectionNumber::new(7)]);
}
