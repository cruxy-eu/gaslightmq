//! The task of one session: it owns the transport, and drives the pipeline.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::Poll;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use gaslightmq_backend::{Backend, BackendAnswer, BackendRequest};
use gaslightmq_call::CallId;
use gaslightmq_clock::Instant;
use gaslightmq_listener::{ConnectionHandler, ReceiveError, SendError, Transport};
use gaslightmq_pipeline::{Input, Output, Outputs, Pipeline, SharedBySessions};
use gaslightmq_rule::Snapshot;
use gaslightmq_session::{Connection, ConnectionOrdinal, ConnectionOrdinals};
use gaslightmq_wire::{Frame, Request};
use tokio::sync::{mpsc, watch};

use crate::events::EventBus;
use crate::timers::Timers;
use crate::{ArmedRules, EngineEvent, WaitableClock};

/// The bytes of a frame that holds nothing, which are no request.
///
/// A transport takes the bytes of its client that are no frame for itself, so the pipeline never sees them.
/// The session reports these bytes instead: the pipeline then ends the session as it does for every client
/// whose bytes are no request, and the session records that the proxy closed it.
const NO_REQUEST: [u8; 4] = [0; 4];

/// What the sessions of one engine work with: the backend, the clock, and what they share.
pub(crate) struct Sessions<B, C> {
    pub backend: B,
    pub clock: Arc<C>,
    pub shared: SharedBySessions,
    pub rules: ArmedRules,
    pub events: EventBus,
    /// Which connection from its host to its endpoint each connection is.
    ordinals: Mutex<ConnectionOrdinals>,
    /// The instants at which a client context may end, because a session closed a while before.
    lingering: mpsc::UnboundedSender<Instant>,
}

impl<B: Backend, C: WaitableClock> Sessions<B, C> {
    /// What the sessions of an engine work with, and the instants at which [`expire_contexts`](Self::expire_contexts)
    /// looks for client contexts that ended.
    pub fn new(
        backend: B,
        clock: Arc<C>,
        shared: SharedBySessions,
        rules: ArmedRules,
        events: EventBus,
    ) -> (Self, mpsc::UnboundedReceiver<Instant>) {
        let (lingering, lingered) = mpsc::unbounded_channel();
        let sessions = Self {
            backend,
            clock,
            shared,
            rules,
            events,
            ordinals: Mutex::default(),
            lingering,
        };
        (sessions, lingered)
    }

    /// Ends every client context that no session has shared for the time the contexts linger.
    ///
    /// Every session that closes names the instant at which its context may end.
    /// The instants come in order, because the clock never goes back. It never returns.
    pub async fn expire_contexts(&self, mut lingered: mpsc::UnboundedReceiver<Instant>) {
        while let Some(at) = lingered.recv().await {
            self.clock.until(at).await;
            self.shared.contexts().expire(self.clock.now());
        }
    }

    /// Counts one more connection from the host of `connection` to its endpoint.
    fn ordinal(&self, connection: &Connection) -> ConnectionOrdinal {
        // The count is whole whenever the lock is free, also after a panic of the task that held it.
        let mut ordinals = self.ordinals.lock().unwrap_or_else(PoisonError::into_inner);
        ordinals.assign(connection.host(), connection.endpoint())
    }

    /// A session closed: its client context may end once it lingered.
    fn session_closed(&self) {
        let linger = self.shared.contexts().linger();
        let at = self.clock.now().checked_add(linger).unwrap_or(Instant::MAX);
        // Nobody expires the contexts of an engine that stopped.
        let _ = self.lingering.send(at);
    }
}

/// The sessions on one endpoint: the handler of the listener of that endpoint.
///
/// The listener calls it for every connection it accepts, in a task of that connection alone.
/// That task is the session: it lives until the connection ends.
pub(crate) struct EndpointSessions<B, C> {
    sessions: Arc<Sessions<B, C>>,
    ended_by_backend: Arc<OnceLock<String>>,
}

impl<B, C> EndpointSessions<B, C> {
    /// The handler of an endpoint that just opened, and the place for the reason why the backend ended it.
    ///
    /// Whoever closes the listener because of the backend says why first: the backend closed the endpoint,
    /// rejected its listener, or named other brokers, so that the listener moves.
    /// The pipeline of every session on the endpoint hears that reason.
    /// A listener that closes without one closes because the engine stops.
    pub fn new(sessions: Arc<Sessions<B, C>>) -> (Self, Arc<OnceLock<String>>) {
        let ended_by_backend = Arc::new(OnceLock::new());
        let handler = Self {
            sessions,
            ended_by_backend: Arc::clone(&ended_by_backend),
        };
        (handler, ended_by_backend)
    }
}

impl<B: Backend + 'static, C: WaitableClock> ConnectionHandler for EndpointSessions<B, C> {
    async fn handle(&self, transport: Transport) {
        Session::open(&self.sessions, &self.ended_by_backend, transport)
            .run()
            .await;
    }
}

/// One session: a transport and a pipeline, and what the pipeline waits for.
///
/// The task waits for the next thing to happen, reports it to the pipeline as an [`Input`],
/// and performs every [`Output`] that the pipeline returns, in order.
struct Session<'a, B, C> {
    of: &'a Sessions<B, C>,
    /// Why the backend ended the endpoint of the session, once it did.
    ended_by_backend: &'a OnceLock<String>,
    pipeline: Pipeline,
    transport: Transport,
    /// The snapshots of the armed rules that the pipeline did not get yet.
    published: watch::Receiver<Arc<Snapshot>>,
    /// The calls that the backend did not answer yet.
    calls: FuturesUnordered<BoxFuture<'a, (CallId, BackendAnswer)>>,
    timers: Timers,
    /// Whether the backend got a request of this connection, so it has something to let go of.
    forwarded: bool,
}

/// What a session waited for.
enum Happened {
    Published(Arc<Snapshot>),
    Answered(CallId, BackendAnswer),
    TimersDue,
    Received(Result<Frame, ReceiveError>),
}

/// How a session ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// The client closed the connection, or the connection broke. Nothing is left to say to the client.
    ClientLeft,
    /// The pipeline closed the session: a lie, the proxy or the backend ended it.
    Closed,
    /// The engine stops. The pipeline has no input for that, so the calls in flight get no record.
    EngineStopped,
}

impl<'a, B: Backend, C: WaitableClock> Session<'a, B, C> {
    /// A client connected: its session, with the pipeline of its connection on the rules that are armed now.
    fn open(of: &'a Sessions<B, C>, ended_by_backend: &'a OnceLock<String>, transport: Transport) -> Self {
        let connection = Connection::new(transport.number(), transport.address(), transport.endpoint());
        let ordinal = of.ordinal(&connection);
        let mut published = of.rules.published();
        let snapshot = Arc::clone(&published.borrow_and_update());
        let pipeline = Pipeline::open(connection, ordinal, of.shared.clone(), snapshot, of.clock.now());
        Self {
            of,
            ended_by_backend,
            pipeline,
            transport,
            published,
            calls: FuturesUnordered::new(),
            timers: Timers::default(),
            forwarded: false,
        }
    }

    /// Runs the session until its connection ends.
    async fn run(mut self) {
        let ending = loop {
            let happened = self.wait().await;
            if let Some(ending) = self.react(happened).await {
                break ending;
            }
        };
        self.end(ending).await;
    }

    /// Waits for the next thing to happen.
    ///
    /// When several things happened, a new snapshot comes first, so the next request is evaluated against it.
    /// Then come the answers of the backend and the timers, and the next frame of the client comes last:
    /// what is in flight goes on before the session takes more.
    async fn wait(&mut self) -> Happened {
        let next_timer = self.timers.next();
        let clock = &self.of.clock;
        let timer_is_due = async {
            match next_timer {
                Some(at) => clock.until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            Ok(()) = self.published.changed() => Happened::Published(Arc::clone(&self.published.borrow_and_update())),
            Some((call, answer)) = self.calls.next(), if !self.calls.is_empty() => Happened::Answered(call, answer),
            () = timer_is_due => Happened::TimersDue,
            received = self.transport.receive() => Happened::Received(received),
        }
    }

    /// Reports what happened to the pipeline, and performs what follows. Returns how the session ends, if it does.
    async fn react(&mut self, happened: Happened) -> Option<Ending> {
        match happened {
            Happened::Published(snapshot) => self.report(Input::SnapshotPublished(snapshot)).await,
            Happened::Answered(call, answer) => match answered(call, answer) {
                Some(input) => self.report(input).await,
                None => None,
            },
            Happened::TimersDue => {
                for timer in self.timers.due(self.of.clock.now()) {
                    let ending = self.report(Input::TimerFired(timer)).await;
                    if ending.is_some() {
                        return ending;
                    }
                }
                None
            }
            Happened::Received(Ok(frame)) => self.report(Input::ClientBytes(&frame.to_bytes())).await,
            Happened::Received(Err(ReceiveError::NotAFrame { .. })) => {
                let ending = self.report(Input::ClientBytes(&NO_REQUEST)).await;
                // The connection is of no use, whatever the pipeline made of it.
                ending.or(Some(Ending::Closed))
            }
            Happened::Received(Err(ReceiveError::ListenerClosed)) => match self.endpoint_ended() {
                Some(input) => self.report(input).await.or(Some(Ending::Closed)),
                None => Some(Ending::EngineStopped),
            },
            Happened::Received(Err(
                // The client closed the connection, or the connection broke: no further frame comes.
                ReceiveError::ClientClosed | ReceiveError::FrameCutShort { .. } | ReceiveError::ConnectionLost { .. },
            )) => {
                self.report(Input::ClientClosed).await;
                Some(Ending::ClientLeft)
            }
        }
    }

    /// Reports `input` to the pipeline, and performs every output it returns, in order.
    ///
    /// Performing an output can make something happen at once: a backend that answers on the spot,
    /// or a client that is gone. The session reports that too, and performs what follows behind the rest.
    /// Returns how the session ends, if it does.
    async fn report(&mut self, input: Input<'_>) -> Option<Ending> {
        let mut outputs: VecDeque<Output> = self.handle(input).into_iter().collect();
        let mut ending = None;
        while let Some(output) = outputs.pop_front() {
            match output {
                Output::Forward { call, request } => {
                    if let Some(input) = self.forward(call, request).await {
                        outputs.extend(self.handle(input));
                    }
                }
                // Nothing reaches a client once its session ends.
                Output::Send(_) if ending.is_some() => {}
                Output::Send(bytes) => match self.transport.send(&bytes).await {
                    Ok(()) => {}
                    // The send may have left a part of its bytes behind, so the connection takes no further response:
                    // the session ends here, and what the pipeline still asks to send stays unsent.
                    Err(SendError::ListenerClosed) => match self.endpoint_ended() {
                        Some(input) => {
                            outputs.extend(self.handle(input));
                            ending = Some(Ending::Closed);
                        }
                        None => ending = Some(Ending::EngineStopped),
                    },
                    // The connection takes no bytes, so the client is gone.
                    Err(SendError::ConnectionLost { .. }) => {
                        outputs.extend(self.handle(Input::ClientClosed));
                        ending = Some(Ending::ClientLeft);
                    }
                },
                Output::SetTimer { id, at } => self.timers.set(id, at),
                Output::Close => {
                    ending.get_or_insert(Ending::Closed);
                }
                Output::Emit(call) => {
                    self.of.events.publish(EngineEvent::CallRecorded(Arc::from(call)));
                    // The call may have spent the last match or the last firing of a rule.
                    self.of.rules.disarm_ended(self.of.clock.now());
                }
                Output::Fire(incident) => self.of.events.publish(EngineEvent::IncidentFired(incident)),
            }
        }
        ending
    }

    /// Hands one input to the pipeline, with the instant of the clock.
    fn handle(&mut self, input: Input<'_>) -> Outputs {
        self.pipeline.handle(input, self.of.clock.now())
    }

    /// Starts the call of `request` on the backend, and goes on without its answer.
    ///
    /// The call starts here, before the next output is performed,
    /// so the backend takes the requests of the connection in the order in which the pipeline forwarded them.
    /// An answer that is there at once comes back as the input it is.
    async fn forward(&mut self, call: CallId, request: Request) -> Option<Input<'static>> {
        self.forwarded = true;
        let backend = &self.of.backend;
        let request = BackendRequest::new(call, self.transport.number(), self.transport.endpoint(), request);
        let mut answer: BoxFuture<'a, (CallId, BackendAnswer)> =
            Box::pin(async move { (call, backend.call(request).await) });
        match futures::poll!(answer.as_mut()) {
            Poll::Ready((call, answer)) => answered(call, answer),
            Poll::Pending => {
                self.calls.push(answer);
                None
            }
        }
    }

    /// The listener of the session closed: what the pipeline hears of it.
    ///
    /// The backend ended the endpoint, or the engine stops, and the pipeline has an input for the first alone.
    fn endpoint_ended(&self) -> Option<Input<'static>> {
        let reason = self.ended_by_backend.get()?;
        Some(Input::BackendClosed { reason: reason.clone() })
    }

    /// Ends the session: the connection closes, and the backend lets go of it.
    async fn end(self, ending: Ending) {
        let Self {
            of,
            pipeline,
            transport,
            calls,
            forwarded,
            ..
        } = self;
        // Nobody waits for the answers any more, so the backend owes none.
        drop(calls);
        let connection = transport.number();
        let leave_the_client = async {
            match ending {
                Ending::ClientLeft => drop(transport),
                // The client reads what the session sent before it finds the connection closed.
                Ending::Closed | Ending::EngineStopped => transport.close().await,
            }
        };
        let leave_the_backend = async {
            if forwarded {
                of.backend.close(connection).await;
            }
        };
        tokio::join!(leave_the_client, leave_the_backend);
        // The state of the session goes with its task. A client that reconnects gets a new pipeline.
        drop(pipeline);
        of.session_closed();
    }
}

/// The input that an answer of the backend is, or `None` for the answer that says there is nothing to report.
fn answered(call: CallId, answer: BackendAnswer) -> Option<Input<'static>> {
    match answer {
        BackendAnswer::Truth(truth) => Some(Input::BackendAnswered {
            call,
            response: truth.into_response(),
        }),
        // The request has no response, and the pipeline expects none.
        BackendAnswer::NoResponse => None,
        BackendAnswer::NotAResponse { reason } => Some(Input::BackendAnswerUnreadable { call, reason }),
        BackendAnswer::Failed(failure) => Some(Input::BackendFailed {
            call,
            reason: failure.to_string(),
        }),
    }
}
