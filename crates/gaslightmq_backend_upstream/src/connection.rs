//! The connection to the cluster that stands for the connection of one client.

use std::sync::Arc;

use bytes::Bytes;
use gaslightmq_backend::{BackendAnswer, BackendFailure};
use gaslightmq_wire::{Frame, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::cluster::ClusterAddress;
use crate::in_flight::{Answered, InFlight, Reply};
use crate::socket::Socket;
use crate::watched::WatchedCluster;

/// The connection to the real cluster that carries the requests of one connection of a client.
///
/// A broker handles the requests of one connection one at a time.
/// On a shared connection, a Fetch that waits for records would hold up the requests of every other client.
/// Every connection of a client therefore has a connection of its own to the broker of its endpoint,
/// and the cluster sees each client come and go.
///
/// The connection is a task that owns the socket:
///
/// - It opens the socket when it starts, and requests wait in their order until then.
/// - It writes each request whole, in the order in which they were [sent](Self::send),
///   also when nobody waits for the answer any more.
/// - It reads while it writes, so a large request never stands in the way of a large response.
/// - It pairs each response with the oldest request in flight.
///
/// A connection that ended stays ended: every request in flight and every later one gets the same answer.
/// The backend does not open a new one behind the client's back, because a client whose broker goes away
/// loses its connection.
///
/// Letting go of the connection ends the task, and closes the socket at once.
#[derive(Debug)]
pub(crate) struct ClusterConnection {
    outbox: mpsc::UnboundedSender<Outgoing>,
    task: JoinHandle<()>,
}

/// A request on its way to the task, with whoever waits for its answer.
#[derive(Debug)]
struct Outgoing {
    request: Request,
    reply: Reply,
    answer: oneshot::Sender<BackendAnswer>,
}

impl ClusterConnection {
    /// What a call is told whose connection the backend let go of before the answer arrived.
    pub(crate) const LET_GO: &'static str = "the backend let go of the connection";

    /// Start a connection to the first of `to` that takes one. It answers through `cluster`.
    ///
    /// It starts a task, so it runs inside the runtime.
    pub(crate) fn open(to: Vec<ClusterAddress>, cluster: Arc<WatchedCluster>) -> Self {
        let (outbox, inbox) = mpsc::unbounded_channel();
        let task = tokio::spawn(carry(to, cluster, inbox));
        Self { outbox, task }
    }

    /// Hand `request` to the connection, and get what its answer arrives on.
    ///
    /// The request is in line once this returns, behind every request that was sent before it.
    /// Nothing here waits, so a caller that gives up later cannot leave half a request on the socket.
    pub(crate) fn send(&self, request: Request, reply: Reply) -> oneshot::Receiver<BackendAnswer> {
        let (answer, waiting) = oneshot::channel();
        // A task that ended takes no request. The request is dropped, and whoever waits learns that at once.
        let _ = self.outbox.send(Outgoing { request, reply, answer });
        waiting
    }
}

impl Drop for ClusterConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The task of one connection: open the socket, carry requests and answers, and answer everything after the end.
async fn carry(to: Vec<ClusterAddress>, cluster: Arc<WatchedCluster>, mut inbox: mpsc::UnboundedReceiver<Outgoing>) {
    let end = match Socket::connect(&to, cluster.configuration()).await {
        Ok(socket) => match Carrier::new(&cluster).carry(socket, &mut inbox).await {
            Some(end) => end,
            // The backend let go of the connection.
            None => return,
        },
        Err(unreachable) => unreachable.into(),
    };
    if matches!(end, BackendAnswer::Failed(_)) {
        // A broker that went away may have left the cluster.
        cluster.ask_again();
    }
    while let Some(outgoing) = inbox.recv().await {
        let _ = outgoing.answer.send(end.clone());
    }
}

/// A request that is being written.
#[derive(Debug)]
struct Writing {
    bytes: Bytes,
    written: usize,
    outgoing: Outgoing,
}

impl Writing {
    fn of(outgoing: Outgoing) -> Self {
        Self {
            bytes: outgoing.request.frame().to_bytes(),
            written: 0,
            outgoing,
        }
    }

    /// The bytes that the socket has not taken yet.
    fn rest(&self) -> &[u8] {
        self.bytes.get(self.written..).unwrap_or_default()
    }
}

/// What one open socket carries: the request that is being written, and the requests in flight.
struct Carrier<'a> {
    cluster: &'a WatchedCluster,
    writing: Option<Writing>,
    in_flight: InFlight<oneshot::Sender<BackendAnswer>>,
}

impl<'a> Carrier<'a> {
    const fn new(cluster: &'a WatchedCluster) -> Self {
        Self {
            cluster,
            writing: None,
            in_flight: InFlight::new(),
        }
    }

    /// Carry the requests of `inbox` over `socket`, and the answers back, until the socket is of no use any more.
    ///
    /// Returns the answer to everything that follows on this connection, or `None` when `inbox` closed first.
    /// Every request that was in flight has that answer already.
    async fn carry(mut self, socket: Socket, inbox: &mut mpsc::UnboundedReceiver<Outgoing>) -> Option<BackendAnswer> {
        let Socket {
            mut stream, mut frames, ..
        } = socket;
        let (mut from_cluster, mut to_cluster) = stream.split();
        let lost = |reason: String| BackendAnswer::from(BackendFailure::ConnectionLost { reason });
        let end = loop {
            match frames.next_frame() {
                Ok(Some(frame)) => match self.answered(frame) {
                    Some(end) => break end,
                    None => continue,
                },
                Ok(None) => {}
                Err(error) => {
                    break BackendAnswer::NotAResponse {
                        reason: error.to_string(),
                    };
                }
            }
            let rest = self.writing.as_ref().map_or(&[][..], Writing::rest);
            tokio::select! {
                outgoing = inbox.recv(), if self.writing.is_none() => match outgoing {
                    Some(outgoing) => self.writing = Some(Writing::of(outgoing)),
                    None => return None,
                },
                written = to_cluster.write(rest), if self.writing.is_some() => match written {
                    Ok(0) => break lost("the broker takes no more bytes".to_owned()),
                    Ok(written) => self.wrote(written),
                    Err(error) => break lost(error.to_string()),
                },
                read = from_cluster.read_buf(frames.buffer_mut()) => match read {
                    Ok(0) => break lost(Socket::CLOSED_BY_BROKER.to_owned()),
                    Ok(_) => {}
                    Err(error) => break lost(error.to_string()),
                },
            }
        };
        let unanswered = self.writing.take().map(|writing| writing.outgoing.answer);
        for waiter in self.in_flight.give_up().chain(unanswered) {
            let _ = waiter.send(end.clone());
        }
        Some(end)
    }

    /// The socket took `written` more bytes of the request that is being written.
    fn wrote(&mut self, written: usize) {
        let Some(mut writing) = self.writing.take() else {
            return;
        };
        writing.written += written;
        if !writing.rest().is_empty() {
            self.writing = Some(writing);
            return;
        }
        let Outgoing { request, reply, answer } = writing.outgoing;
        match reply {
            Reply::Comes => self.in_flight.sent(request, answer),
            // The request is on its way, and that is all a request without a response can say.
            Reply::Never => {
                let _ = answer.send(BackendAnswer::NoResponse);
            }
        }
    }

    /// The cluster sent `frame`. Returns the end of the connection, when the frame puts it out of step.
    fn answered(&mut self, frame: Frame) -> Option<BackendAnswer> {
        match self.in_flight.answered(frame) {
            Answered::Response {
                request,
                response,
                waiter,
            } => {
                // Nobody may wait any more. The overlay still reads the answer, for the addresses it tells.
                let _ = waiter.send(self.cluster.truth(&request, response));
                None
            }
            Answered::Unreadable { reason, waiter } => {
                let _ = waiter.send(BackendAnswer::NotAResponse { reason });
                None
            }
            Answered::OutOfStep { reason, waiter } => {
                let end = BackendAnswer::NotAResponse { reason };
                if let Some(waiter) = waiter {
                    let _ = waiter.send(end.clone());
                }
                Some(end)
            }
        }
    }
}
