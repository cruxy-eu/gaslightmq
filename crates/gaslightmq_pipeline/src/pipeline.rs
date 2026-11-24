//! The request pipeline of one connection, as a state machine.

use std::sync::Arc;

use gaslightmq_call::prelude::*;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_lie::Stage;
use gaslightmq_rule::Snapshot;
use gaslightmq_session::prelude::*;
use gaslightmq_wire::{DecodeError, FrameDecoder, FrameError, Request, Response};

use crate::in_flight::{AtRequestGate, CallInFlight, Leaving, Settled};
use crate::untold::StageFailure;
use crate::{Input, Output, Outputs, SharedBySessions, TimerId};

/// The request pipeline of one connection: a state machine that decides what happens to each call.
///
/// The engine opens one pipeline per client connection.
/// It reports what happened as an [`Input`], with the current instant,
/// and it performs every [`Output`] that [`handle`](Self::handle) returns, in order.
/// The pipeline reads no socket, sets no timer and looks at no clock.
///
/// # The stages of a call
///
/// | Stage | When | The pipeline |
/// |---|---|---|
/// | 1 · Receive, 2 · Decode | [`Input::ClientBytes`] | Cuts the bytes into requests, and decodes each one once. |
/// | 3 · Identify | | Identifies the session, and tells the client context what the request names. |
/// | 4 · Match, 5 · Trigger | | Evaluates the request pass on the snapshot it received last. |
/// | 6 · Resolve | | Combines the lies, and hands out the incidents as [`Output::Fire`]. |
/// | 7 · Before forwarding | | Asks the lies that act now. A silence stalls the session, a hang-up closes it, a delay is an [`Output::SetTimer`]. |
/// | 8 · Backend | The request is at the head of the line. | Hands out what is left of the request as [`Output::Forward`]. |
/// | 9 · After the response | [`Input::BackendAnswered`] | Reads the truth, evaluates the response pass, and asks the lies that act now. |
/// | 10 · Send, 11 · Emit | The answer is at the head of the line. | Hands out the bytes as [`Output::Send`], and the record as [`Output::Emit`]. |
///
/// A call never holds the pipeline up.
/// While one call waits for a timer or for the backend, the pipeline takes the next request of the connection.
/// Requests still reach the backend in the order they arrived, and answers leave in that order.
///
/// # Rules change while calls are in flight
///
/// [`Input::SnapshotPublished`] replaces the snapshot for the calls that arrive from then on.
/// A call in flight keeps the snapshot it started with, so both of its passes see the same rules.
///
/// # How a session ends
///
/// | The session ends when | Closed by | The pipeline returns |
/// |---|---|---|
/// | [`Input::ClientClosed`] arrives. | The client | The record of every call in flight. |
/// | A hang-up acts. | A lie | The record of every call in flight, and [`Output::Close`]. |
/// | The bytes of the client are no request, a Produce does not say whether it expects a response, or the backend sends something that is no response to the request. | The proxy | The same. |
/// | [`Input::BackendFailed`] or [`Input::BackendClosed`] arrives. | The backend | The same. Every record says why the backend ended the session. |
///
/// An answer of the backend that is no response is a [`Response`] that does not answer the request,
/// or [`Input::BackendAnswerUnreadable`].
///
/// A call that was in flight when its session ended got no answer, and its record says so.
/// Its firings go back to their budgets.
/// A pipeline whose session ended takes no further input: every [`handle`](Self::handle) returns nothing.
///
/// # What the pipeline ignores
///
/// The engine cannot avoid every race: a backend answers a call whose session just closed,
/// and a timer fires for a call that left.
/// An input that names a call or a timer that the pipeline does not wait for changes nothing.
/// A call ID names one call of the environment, so the call of another connection is one the pipeline does not wait for.
///
/// A failure of the backend is the exception: [`Input::BackendFailed`] ends the session whichever call it names.
/// A Produce without a response has left by the time its failure arrives.
/// The engine reports a failure to the pipeline that forwarded the request, and to no other.
#[derive(Debug)]
pub struct Pipeline {
    session: Session<CallInFlight>,
    /// The snapshot that the next call starts with.
    snapshot: Arc<Snapshot>,
    shared: SharedBySessions,
    /// The bytes of the client that are no whole request yet.
    frames: FrameDecoder,
}

impl Pipeline {
    /// A client connected at `now`: the pipeline of its connection.
    ///
    /// `ordinal` says which connection from its host to its endpoint this one is.
    /// `shared` are the handles of the proxy, and `snapshot` holds the rules that are armed at this moment.
    /// Every request that arrives takes the ID of its call from the [`CallIds`] in `shared`.
    pub fn open(
        connection: Connection,
        ordinal: ConnectionOrdinal,
        shared: SharedBySessions,
        snapshot: Arc<Snapshot>,
        now: Instant,
    ) -> Self {
        Self {
            session: Session::open(
                connection,
                ordinal,
                shared.contexts().clone(),
                shared.call_ids().clone(),
                now,
            ),
            snapshot,
            shared,
            frames: FrameDecoder::new(),
        }
    }

    /// Takes one thing that happened at `now`, and returns what should happen, in order.
    pub fn handle(&mut self, input: Input<'_>, now: Instant) -> Outputs {
        let mut outputs = Outputs::default();
        if self.session.closed().is_some() {
            return outputs;
        }
        match input {
            Input::ClientBytes(bytes) => self.receive(bytes, now, &mut outputs),
            Input::BackendAnswered { call, response } => self.backend_answered(call, response, now, &mut outputs),
            Input::BackendAnswerUnreadable { call, reason } => {
                self.backend_answered_unreadably(call, reason, now, &mut outputs);
            }
            Input::BackendFailed { call, reason } => self.backend_failed(call, reason, now, &mut outputs),
            Input::BackendClosed { reason } => {
                let reason = StageFailure::BackendClosed { reason };
                self.close(ClosedBy::Backend, None, Some(&reason), now, &mut outputs);
            }
            Input::TimerFired(timer) => self.timer_fired(timer, now, &mut outputs),
            Input::SnapshotPublished(snapshot) => self.snapshot = snapshot,
            Input::ClientClosed => self.close(ClosedBy::Client, None, None, now, &mut outputs),
        }
        outputs
    }

    /// The connection of the session.
    pub const fn connection(&self) -> Connection {
        self.session.connection()
    }

    /// The identity fields of the session, as far as they are known.
    pub const fn identity(&self) -> &Identity {
        self.session.identity()
    }

    /// The key that tells the session from every other one, and that is the same in every run.
    pub const fn key(&self) -> SessionKey {
        self.session.key()
    }

    /// Where the session is in its life.
    pub const fn lifecycle(&self) -> Lifecycle {
        self.session.lifecycle()
    }

    /// Who closed the session and when, or `None` while it is open.
    pub const fn closed(&self) -> Option<Closed> {
        self.session.closed()
    }

    /// Whether a silence stopped the connection, and by which call.
    pub const fn stalled(&self) -> Option<Stalled> {
        self.session.stalled()
    }

    /// How many calls are in flight: their request arrived, and their answer did not leave yet.
    pub fn in_flight(&self) -> usize {
        self.session.calls().len()
    }

    /// The snapshot that the next call starts with.
    pub const fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snapshot
    }

    /// Stage 1: cuts the bytes of the client into requests.
    fn receive(&mut self, bytes: &[u8], now: Instant, outputs: &mut Outputs) {
        self.frames.extend(bytes);
        while self.session.closed().is_none() {
            // Without a frame, nothing says where the next request starts.
            // Without a request header, nothing says how to answer this one.
            // A broker closes such a connection, and so does the proxy: no lie and no backend did it.
            let frame = match self.frames.next_frame() {
                Ok(Some(frame)) => frame,
                Ok(None) => return,
                Err(FrameError::Empty | FrameError::TooLarge { .. }) => {
                    self.close(ClosedBy::Proxy, None, None, now, outputs);
                    return;
                }
            };
            match Request::decode(frame) {
                Ok(request) => self.arrive(request, now, outputs),
                Err(DecodeError { .. }) => self.close(ClosedBy::Proxy, None, None, now, outputs),
            }
        }
    }

    /// Stages 2 to 7 for one request.
    fn arrive(&mut self, request: Request, now: Instant, outputs: &mut Outputs) {
        let snapshot = Arc::clone(&self.snapshot);
        if self.session.stalled().is_some() {
            // A silence stopped the connection. The request never reaches the backend and never gets an answer,
            // so nothing happens to it: no rule is evaluated, the session takes no identity from it,
            // and the client context learns nothing from it.
            let call = CallInFlight::parked(request, &self.session, snapshot, &self.shared, now);
            let id = self.session.calls_mut().arrive(call, now);
            self.emit(id, Delivered::Nothing, outputs);
            return;
        }

        let mut call = CallInFlight::arrived(request, &mut self.session, snapshot, &self.shared, now);
        let arrival = call.evaluate(self.session.counters_mut(), &self.shared, now);
        outputs.extend(arrival.incidents.into_iter().map(Output::Fire));
        let id = self.session.calls_mut().arrive(call, now);

        if arrival.unanswerable {
            // A Produce that does not say its `acks` may get a response or not. Whichever the proxy assumed,
            // being wrong would stall or mix up every answer behind it, so it closes the connection, as a broker does.
            self.close(ClosedBy::Proxy, None, None, now, outputs);
        } else if arrival.silenced {
            // The call never gets an answer, and the session is stalled from now on.
            if self.session.calls_mut().silence(id, now).is_ok() {
                self.emit(id, Delivered::Nothing, outputs);
            }
        } else if let Some(at) = deadline(now, arrival.wait) {
            let timer = TimerId::new(id, Stage::BeforeForwarding);
            outputs.push(Output::SetTimer { id: timer, at });
        } else if self.session.calls_mut().release_request(id).is_ok() {
            self.advance(now, outputs);
        }
    }

    /// Stage 9 begins for the call that the backend answered.
    fn backend_answered(&mut self, call: CallId, response: Response, now: Instant, outputs: &mut Outputs) {
        let (calls, counters) = self.session.calls_and_counters_mut();
        let Some(in_flight) = calls.get_mut(call) else {
            return;
        };
        let Some(settled) = in_flight.content_mut().answered(response, now, counters, &self.shared) else {
            return;
        };
        self.settled(call, settled, now, outputs);
        self.advance(now, outputs);
    }

    /// The backend answered a call with something that is no response.
    /// The call has no answer, and the proxy closes the connection once that would be at the head of the line.
    fn backend_answered_unreadably(&mut self, call: CallId, reason: String, now: Instant, outputs: &mut Outputs) {
        let Some(in_flight) = self.session.calls_mut().get_mut(call) else {
            return;
        };
        let Some(settled) = in_flight.content_mut().answered_unreadably(reason, now) else {
            return;
        };
        self.settled(call, settled, now, outputs);
        self.advance(now, outputs);
    }

    /// The backend has no answer for a call that this pipeline forwarded, so the session ends.
    ///
    /// It ends whichever call the failure names: a Produce without a response has left by the time its failure arrives.
    fn backend_failed(&mut self, call: CallId, reason: String, now: Instant, outputs: &mut Outputs) {
        let failure = StageFailure::BackendFailed { call, reason };
        self.close(ClosedBy::Backend, Some(call), Some(&failure), now, outputs);
    }

    /// The wait of a call ended: its request or its answer joins the line.
    fn timer_fired(&mut self, timer: TimerId, now: Instant, outputs: &mut Outputs) {
        let id = timer.call();
        let calls = self.session.calls_mut();
        let Some(call) = calls.get(id).map(InFlightCall::content) else {
            return;
        };
        let released = match timer.stage() {
            Stage::BeforeForwarding => call.is_held_before_forwarding() && calls.release_request(id).is_ok(),
            Stage::AfterResponse => call.is_held_after_response() && calls.release_response(id).is_ok(),
        };
        if released {
            self.advance(now, outputs);
        }
    }

    /// Lets every request and every answer that is at the head of its line go on.
    ///
    /// Requests pass in the order they arrived, and so do answers.
    /// A call that is decided without the backend joins the line of answers at once, so the two lines feed each other.
    fn advance(&mut self, now: Instant, outputs: &mut Outputs) {
        loop {
            let mut moved = false;
            while let Some(id) = self.next_request() {
                moved = true;
                self.at_request_gate(id, now, outputs);
                if self.session.closed().is_some() {
                    return;
                }
            }
            while let Some(in_flight) = self.session.calls_mut().next_response() {
                moved = true;
                let id = in_flight.id();
                let mut call = in_flight.into_content();
                let (leaving, delivered) = call.leave(now);
                if let Leaving::Send(bytes) = &leaving {
                    outputs.push(Output::Send(bytes.clone()));
                }
                outputs.extend(call.record(id, delivered).map(Box::new).map(Output::Emit));
                if let Leaving::Close(by) = leaving {
                    self.close(by, None, None, now, outputs);
                    return;
                }
            }
            if !moved {
                return;
            }
        }
    }

    /// The call whose request is at the head of the line, when the pipeline let it go.
    fn next_request(&mut self) -> Option<CallId> {
        self.session.calls_mut().next_request().map(|(id, _)| id)
    }

    /// Stage 8 for the request at the head of the line: the backend gets it, or the call is decided without it.
    fn at_request_gate(&mut self, id: CallId, now: Instant, outputs: &mut Outputs) {
        let (calls, counters) = self.session.calls_and_counters_mut();
        let Some(in_flight) = calls.get_mut(id) else {
            return;
        };
        match in_flight.content_mut().at_request_gate(now, counters, &self.shared) {
            AtRequestGate::Forward(request) => outputs.push(Output::Forward { call: id, request }),
            AtRequestGate::Settled { forward, settled } => {
                outputs.extend(forward.map(|request| Output::Forward { call: id, request }));
                self.settled(id, settled, now, outputs);
            }
            AtRequestGate::HangUp => self.close(ClosedBy::Lie, Some(id), None, now, outputs),
            AtRequestGate::NotWaiting => {}
        }
    }

    /// Stage 9 ran for a call: the answer waits for its delay, or joins the line of answers.
    fn settled(&mut self, id: CallId, settled: Settled, now: Instant, outputs: &mut Outputs) {
        outputs.extend(settled.incidents.into_iter().map(Output::Fire));
        for value in settled.learned {
            value.teach(&self.session);
        }
        if settled.silenced {
            if self.session.calls_mut().silence(id, now).is_ok() {
                self.emit(id, Delivered::Nothing, outputs);
            }
        } else if let Some(at) = deadline(now, settled.wait) {
            let timer = TimerId::new(id, Stage::AfterResponse);
            outputs.push(Output::SetTimer { id: timer, at });
        } else {
            // The call awaits its response, so this only fails for a call that is gone.
            let _gone = self.session.calls_mut().release_response(id);
        }
    }

    /// Stage 11 for a call that stays in flight: a silenced call never leaves.
    fn emit(&mut self, id: CallId, delivered: Delivered, outputs: &mut Outputs) {
        let call = self.session.calls_mut().get_mut(id);
        let record = call.and_then(|call| call.content_mut().record(id, delivered));
        outputs.extend(record.map(Box::new).map(Output::Emit));
    }

    /// The session ends at `now`. No call in flight gets an answer, and each of them is recorded.
    ///
    /// `answered_by_closing` is the call that gets the closed connection as its answer, when it is still in flight:
    /// the call whose hang-up closes the connection, or the call that the backend has no answer for.
    /// `reason` is why the backend ended the session, which the record of every call in flight keeps.
    fn close(
        &mut self,
        by: ClosedBy,
        answered_by_closing: Option<CallId>,
        reason: Option<&StageFailure>,
        now: Instant,
        outputs: &mut Outputs,
    ) {
        for in_flight in self.session.close(by, now) {
            let id = in_flight.id();
            let delivered = if answered_by_closing == Some(id) {
                Delivered::ConnectionClosed
            } else {
                Delivered::Nothing
            };
            let mut call = in_flight.into_content();
            if let Some(reason) = reason {
                call.failed(reason.clone());
            }
            outputs.extend(call.record(id, delivered).map(Box::new).map(Output::Emit));
        }
        if by != ClosedBy::Client {
            outputs.push(Output::Close);
        }
    }
}

/// When a wait that starts at `now` ends, or `None` when the call does not wait.
fn deadline(now: Instant, wait: Duration) -> Option<Instant> {
    if wait.is_zero() {
        return None;
    }
    // A wait that runs past the end of the clock ends with it.
    Some(now.checked_add(wait).unwrap_or(Instant::from_nanos(u64::MAX)))
}
