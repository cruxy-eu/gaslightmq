//! The live stream of one client: its presence, its snapshot, the changes after it, and the calls it follows.

use std::convert::Infallible;
use std::pin::pin;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use gaslightmq_clock_waitable::WaitableClock;
use gaslightmq_control_api::call::Call;
use gaslightmq_control_api::clock::Instant;
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::surface::PresenceId;
use gaslightmq_host::CallSubscription;
use gaslightmq_workspace::WorkspaceContext;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;

use crate::attendance::{Attendance, NoPresence};
use crate::plane::{Change, ControlPlane, form};
use crate::presence::{Liveness, PingDue};
use crate::server::stopped;

/// The calls that one client follows: those of one environment that one selector matches.
struct Following {
    environment: EnvironmentName,
    calls: CallSubscription,
}

impl Following {
    /// The next call that the selector matches, as a message, or `None` once the environment stopped.
    /// A client that falls behind the calls misses the oldest of them, and follows on from there.
    async fn next(&mut self) -> Option<StreamMessage> {
        let call = self.calls.next().await?;
        Some(StreamMessage::Call {
            environment: self.environment.clone(),
            call: Call::clone(&call),
        })
    }
}

/// Why the stream of a client ended.
enum End {
    /// The client closed it, or its connection went away.
    Client,
    /// The client went silent: it answered no ping, or took no message, for a whole interval.
    /// It is silent since that instant, which the grace period of its presence counts from.
    Silent { since: Instant },
    /// The server stops.
    Stopping,
    /// The workspace could not be read for a snapshot. The text is what the store said.
    Unreadable(String),
    /// The client got no presence: the system gave no randomness for its ID. The text is what the system said.
    NoPresence(String),
}

/// The live stream of one client, from its presence and its snapshot until one side ends it.
pub(crate) struct ClientStream<C: WorkspaceContext, K> {
    plane: Arc<ControlPlane<C>>,
    stopping: watch::Receiver<bool>,
    attendance: Attendance<K>,
    /// The presence that the client comes back as, when it named one.
    comes_back_as: Option<PresenceId>,
}

impl<C: WorkspaceContext + 'static, K: WaitableClock> ClientStream<C, K> {
    /// The stream of a client of `plane`, which ends when `stopping` turns true.
    ///
    /// The client is new, or it comes back as the presence that it had on an earlier stream.
    pub(crate) fn new(
        plane: Arc<ControlPlane<C>>,
        stopping: watch::Receiver<bool>,
        attendance: Attendance<K>,
        comes_back_as: Option<PresenceId>,
    ) -> Self {
        Self {
            plane,
            stopping,
            attendance,
            comes_back_as,
        }
    }

    /// Sends the presence of the client and the snapshot, then every change and every call that the client follows,
    /// until one side ends the stream. Then it keeps the presence for its grace period, and ends it in the host
    /// unless the client comes back.
    pub(crate) async fn run(self, socket: WebSocket) {
        let (mut sender, mut receiver) = socket.split();
        let host = self.plane.host();
        let presence = match self.attendance.arrive(host, self.comes_back_as) {
            Ok(presence) => presence,
            Err(NoPresence::Stopping) => return close(sender, End::Stopping).await,
            Err(NoPresence::NoRandomness { reason }) => return close(sender, End::NoPresence(reason)).await,
        };
        let Err(end) = self.serve(presence, &mut sender, &mut receiver).await;
        // A client that went silent left when its silence was a whole interval long, whenever the server got to it.
        let left_at = match end {
            End::Silent { since } => since,
            _ => self.attendance.clock().now(),
        };
        // The server notes that the client left before it lets go of the connection,
        // so a client that closed its stream finds that noted once the connection is gone.
        let ends_at = self.attendance.leave(presence, left_at);
        // The connection closes behind this task: a client that takes no close frame holds up nothing.
        tokio::spawn(async move {
            close(sender, end).await;
            drop(receiver);
        });
        let Some(ends_at) = ends_at else {
            return;
        };
        let mut stopping = self.stopping.clone();
        tokio::select! {
            () = self.attendance.end_unless_back(host, presence, ends_at) => {}
            // A server that stops ends every presence itself.
            () = stopped(&mut stopping) => {}
        }
    }

    /// Serves the stream until it ends, and returns why it ended.
    async fn serve(
        &self,
        presence: PresenceId,
        sender: &mut SplitSink<WebSocket, Message>,
        receiver: &mut SplitStream<WebSocket>,
    ) -> Result<Infallible, End> {
        let clock = self.attendance.clock();
        let mut changes = self.plane.changes();
        let mut liveness = self.attendance.liveness();
        self.tell(sender, &StreamMessage::Present { presence }, &liveness)
            .await?;
        self.snapshot(sender, &liveness).await?;
        let mut following: Option<Following> = None;
        let mut stopping = self.stopping.clone();
        let mut ping_due = pin!(clock.until(liveness.next_ping()));
        loop {
            tokio::select! {
                // What the client sent comes before the ping that asks whether it sent anything.
                biased;
                () = stopped(&mut stopping) => return Err(End::Stopping),
                frame = receiver.next() => {
                    let Some(Ok(frame)) = frame else {
                        return Err(End::Client);
                    };
                    // Whatever the client sends says that it is there: the answer to a ping, or a subscription.
                    liveness.heard(clock.now());
                    match frame {
                        Message::Text(text) => {
                            let answer = match serde_json::from_str::<Subscription>(text.as_str()) {
                                Ok(subscription) => match self.follow(subscription.clone()).await {
                                    Ok(follows) => {
                                        following = follows;
                                        StreamMessage::Following { subscription }
                                    }
                                    Err(reason) => StreamMessage::SubscriptionRefused { reason },
                                },
                                Err(error) => StreamMessage::SubscriptionRefused {
                                    reason: format!("the frame is no subscription: {error}"),
                                },
                            };
                            if let StreamMessage::SubscriptionRefused { .. } = answer {
                                following = None;
                            }
                            // The calls that the client follows now come after this answer, and none of the others.
                            self.tell(sender, &answer, &liveness).await?;
                        }
                        Message::Binary(_) => {
                            following = None;
                            let refused = StreamMessage::SubscriptionRefused {
                                reason: "the frame is no subscription: a subscription is a text frame".to_owned(),
                            };
                            self.tell(sender, &refused, &liveness).await?;
                        }
                        // The WebSocket answers a ping itself.
                        Message::Ping(_) | Message::Pong(_) => {}
                        Message::Close(_) => return Err(End::Client),
                    }
                },
                () = &mut ping_due => {
                    match liveness.due(clock.now()) {
                        PingDue::Ping => self.deliver(sender, Message::Ping(Bytes::new()), &liveness).await?,
                        PingDue::Silent { since } => return Err(End::Silent { since }),
                    }
                    ping_due.set(clock.until(liveness.next_ping()));
                },
                change = changes.recv() => match change {
                    Ok(Change::Message(message)) => self.tell(sender, &message, &liveness).await?,
                    // The server missed changes of the host, so the client gets the whole state again.
                    Ok(Change::Resync) => self.snapshot(sender, &liveness).await?,
                    // The client missed changes, so it gets the whole state again, and the changes after it.
                    Err(RecvError::Lagged(_)) => {
                        changes = changes.resubscribe();
                        self.snapshot(sender, &liveness).await?;
                    }
                    Err(RecvError::Closed) => return Err(End::Stopping),
                },
                call = next_call(&mut following) => match call {
                    Some(message) => self.tell(sender, &message, &liveness).await?,
                    // The environment stopped, and the client follows no calls.
                    None => following = None,
                },
            }
        }
    }

    /// Sends the whole state of the process.
    async fn snapshot(&self, sender: &mut SplitSink<WebSocket, Message>, liveness: &Liveness) -> Result<(), End> {
        let state = self.plane.state().await.map_err(End::Unreadable)?;
        self.tell(sender, &StreamMessage::Snapshot(state), liveness).await
    }

    /// Sends one message as a text frame.
    async fn tell(
        &self,
        sender: &mut SplitSink<WebSocket, Message>,
        message: &StreamMessage,
        liveness: &Liveness,
    ) -> Result<(), End> {
        self.deliver(sender, Message::Text(form(message).into()), liveness)
            .await
    }

    /// Sends one frame to a client that takes it.
    ///
    /// # Errors
    ///
    /// [`End::Client`] when the client went away, and [`End::Silent`] when it does not take the frame for a whole
    /// interval: a client that reads nothing any more would hold the stream up for as long as its connection stays.
    async fn deliver(
        &self,
        sender: &mut SplitSink<WebSocket, Message>,
        frame: Message,
        liveness: &Liveness,
    ) -> Result<(), End> {
        let clock = self.attendance.clock();
        let taken_by = liveness.taken_by(clock.now());
        tokio::select! {
            // A client that is there takes the frame at once, and nobody waits for the clock.
            biased;
            sent = sender.send(frame) => sent.map_err(|_| End::Client),
            () = clock.until(taken_by) => Err(End::Silent { since: taken_by }),
        }
    }

    /// What the client follows from now on, or why it cannot follow it.
    async fn follow(&self, subscription: Subscription) -> Result<Option<Following>, String> {
        let followed = self.plane.follow(subscription).await?;
        Ok(followed.map(|(environment, calls)| Following { environment, calls }))
    }
}

/// The next call that the client follows, or `None` once its environment stopped.
/// A client that follows no calls waits forever.
async fn next_call(following: &mut Option<Following>) -> Option<StreamMessage> {
    match following {
        Some(following) => following.next().await,
        None => std::future::pending().await,
    }
}

/// Closes the stream of a client, with a close frame that says why when the server ends it.
async fn close(mut sender: SplitSink<WebSocket, Message>, end: End) {
    let frame = match end {
        End::Client => None,
        // A client that reads nothing gets no word: the connection closes when the server lets go of it.
        End::Silent { .. } => return,
        End::Stopping => Some(CloseFrame {
            code: close_code::AWAY,
            reason: "the control plane stops".into(),
        }),
        End::Unreadable(reason) => Some(CloseFrame {
            code: close_code::ERROR,
            reason: reason.into(),
        }),
        End::NoPresence(reason) => Some(CloseFrame {
            code: close_code::ERROR,
            reason: format!("the system gave no randomness for a presence: {reason}").into(),
        }),
    };
    // A client that went away hears nothing, and there is nothing else to tell it.
    let _ = match frame {
        Some(frame) => sender.send(Message::Close(Some(frame))).await,
        // The client closed the stream, and this answers its close.
        None => sender.close().await,
    };
}
