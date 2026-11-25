//! What the engine reports to a pipeline, and what the pipeline asks the engine to do.

use std::sync::Arc;

use bytes::Bytes;
use gaslightmq_call::{Call, CallId};
use gaslightmq_clock::Instant;
use gaslightmq_incident::Incident;
use gaslightmq_rule::Snapshot;
use gaslightmq_wire::{Request, Response};

use crate::TimerId;

/// What happened on one connection. The engine reports it to the [`Pipeline`](crate::Pipeline).
///
/// | What happens | Input | Comes from |
/// |---|---|---|
/// | The client sends bytes. | [`ClientBytes`](Self::ClientBytes) | The transport |
/// | The backend answers a call. | [`BackendAnswered`](Self::BackendAnswered) | The backend |
/// | The backend answers a call with something that is no response. | [`BackendAnswerUnreadable`](Self::BackendAnswerUnreadable) | The backend |
/// | The backend has no answer for a call. | [`BackendFailed`](Self::BackendFailed) | The backend |
/// | The backend closes the session. | [`BackendClosed`](Self::BackendClosed) | The backend |
/// | A timer runs out. | [`TimerFired`](Self::TimerFired) | The engine's clock |
/// | A rule changes. | [`SnapshotPublished`](Self::SnapshotPublished) | The armed rules |
/// | The client closes the connection. | [`ClientClosed`](Self::ClientClosed) | The transport |
/// | Someone closes the session at the proxy. | [`ProxyClosed`](Self::ProxyClosed) | The control plane |
#[derive(Debug, Clone)]
pub enum Input<'a> {
    /// The client sent these bytes.
    /// They need not end where a request ends: the pipeline keeps what is left for the bytes that follow.
    ClientBytes(&'a [u8]),
    /// The backend answered the request that the pipeline handed out under `call`.
    ///
    /// The response answers the request as it was forwarded.
    /// The pipeline gives it the API and the correlation ID of the client's request before anything reads it.
    BackendAnswered { call: CallId, response: Response },
    /// The backend answered the request under `call` with something that it cannot hand over as a response.
    /// `reason` says why, in one line.
    ///
    /// The client must not get that answer.
    /// The proxy closes the connection once the answer would be at the head of the line, as for a
    /// [response](Self::BackendAnswered) that does not answer the request.
    BackendAnswerUnreadable { call: CallId, reason: String },
    /// The backend has no answer for the request that the pipeline handed out under `call`, and none will come.
    /// `reason` says why, in one line.
    ///
    /// The session ends, and the backend closed it.
    /// The record of every call in flight keeps the line.
    BackendFailed { call: CallId, reason: String },
    /// The backend closed the session: the broker of its endpoint went away.
    /// `reason` says what happened, in one line.
    ///
    /// The record of every call in flight keeps the line.
    BackendClosed { reason: String },
    /// The timer that the pipeline asked for under this ID ran out.
    TimerFired(TimerId),
    /// The armed rules changed.
    /// Every call that arrives from now on is evaluated against this snapshot,
    /// and a call in flight finishes on the one it started with.
    SnapshotPublished(Arc<Snapshot>),
    /// The client closed the connection, or gave up on it.
    ClientClosed,
    /// Someone closed the session at the proxy, through the control plane.
    /// The session ends as closed by the proxy, and the client finds its connection closed.
    ProxyClosed,
}

/// What should happen. The pipeline returns it, and the engine performs it.
///
/// | Output | The engine |
/// |---|---|
/// | [`Forward`](Self::Forward) | Hands the request to the backend, and reports the answer under the same call. |
/// | [`Send`](Self::Send) | Writes the bytes to the transport. |
/// | [`SetTimer`](Self::SetTimer) | Waits on its clock until the instant, and reports the timer. |
/// | [`Close`](Self::Close) | Closes the transport, which ends the session. |
/// | [`Emit`](Self::Emit) | Publishes the call record on the event bus. |
/// | [`Fire`](Self::Fire) | Passes the incident to the scenario runner. |
#[derive(Debug, Clone)]
pub enum Output {
    /// Hand `request` to the backend. The answer comes back as [`Input::BackendAnswered`] under `call`.
    ///
    /// The request is the client's, or what the lies before forwarding left of it.
    /// It carries the client's correlation ID.
    /// A Produce with `acks=0` has no answer, and the pipeline expects none.
    Forward { call: CallId, request: Request },
    /// Write these bytes to the client: one whole response, with its length in front.
    Send(Bytes),
    /// Report [`Input::TimerFired`] with `id` once the clock reaches `at`.
    SetTimer { id: TimerId, at: Instant },
    /// Close the connection to the client. The session has ended, and the pipeline takes no further input.
    Close,
    /// Publish the record of a call that ended.
    Emit(Box<Call>),
    /// Pass an incident that a rule fired to the scenario runner. It takes effect after the call that fired it.
    Fire(Incident),
}

/// What one [`handle`](crate::Pipeline::handle) asks for, in the order the engine performs it.
///
/// The order matters: responses leave in the order of their requests, and a record follows the bytes it describes.
///
/// ```
/// use gaslightmq_pipeline::prelude::*;
///
/// let outputs = Outputs::default();
/// assert!(outputs.is_empty());
/// assert_eq!(outputs.iter().count(), 0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Outputs(Vec<Output>);

impl Outputs {
    /// The outputs, in order.
    pub fn iter(&self) -> std::slice::Iter<'_, Output> {
        self.0.iter()
    }

    /// The outputs as a slice, in order.
    pub fn as_slice(&self) -> &[Output] {
        &self.0
    }

    /// How many outputs there are.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the pipeline asks for nothing.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds `output` after the others.
    pub(crate) fn push(&mut self, output: Output) {
        self.0.push(output);
    }

    /// Adds `outputs` after the others, in their order.
    pub(crate) fn extend(&mut self, outputs: impl IntoIterator<Item = Output>) {
        self.0.extend(outputs);
    }
}

impl IntoIterator for Outputs {
    type Item = Output;
    type IntoIter = std::vec::IntoIter<Output>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Outputs {
    type Item = &'a Output;
    type IntoIter = std::slice::Iter<'a, Output>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
