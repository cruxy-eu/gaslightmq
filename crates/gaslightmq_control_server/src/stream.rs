//! The live stream of one client: its snapshot, the changes after it, and the calls it follows.

use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use gaslightmq_control_api::call::Call;
use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_host::CallSubscription;
use gaslightmq_workspace::WorkspaceContext;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;

use crate::plane::{Change, ControlPlane, form};
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
    /// The server stops.
    Stopping,
    /// The workspace could not be read for a snapshot. The text is what the store said.
    Unreadable(String),
}

/// The live stream of one client, from its snapshot until one side ends it.
pub(crate) struct ClientStream<C: WorkspaceContext> {
    plane: Arc<ControlPlane<C>>,
    stopping: watch::Receiver<bool>,
}

impl<C: WorkspaceContext + 'static> ClientStream<C> {
    /// The stream of a client of `plane`, which ends when `stopping` turns true.
    pub(crate) fn new(plane: Arc<ControlPlane<C>>, stopping: watch::Receiver<bool>) -> Self {
        Self { plane, stopping }
    }

    /// Sends the snapshot, then every change and every call that the client follows, until one side ends the stream.
    pub(crate) async fn run(self, socket: WebSocket) {
        let (mut sender, mut receiver) = socket.split();
        let end = self.serve(&mut sender, &mut receiver).await;
        let close = match end {
            End::Client => None,
            End::Stopping => Some(CloseFrame {
                code: close_code::AWAY,
                reason: "the control plane stops".into(),
            }),
            End::Unreadable(reason) => Some(CloseFrame {
                code: close_code::ERROR,
                reason: reason.into(),
            }),
        };
        if let Some(close) = close {
            // A client that went away hears nothing, and there is nothing else to tell it.
            let _ = sender.send(Message::Close(Some(close))).await;
        }
    }

    async fn serve(&self, sender: &mut SplitSink<WebSocket, Message>, receiver: &mut SplitStream<WebSocket>) -> End {
        let mut changes = self.plane.changes();
        if let Err(end) = self.snapshot(sender).await {
            return end;
        }
        let mut following: Option<Following> = None;
        let mut stopping = self.stopping.clone();
        loop {
            tokio::select! {
                () = stopped(&mut stopping) => return End::Stopping,
                change = changes.recv() => match change {
                    Ok(Change::Message(message)) => {
                        if send(sender, &message).await.is_err() {
                            return End::Client;
                        }
                    }
                    // The server missed changes of the host, so the client gets the whole state again.
                    Ok(Change::Resync) => {
                        if let Err(end) = self.snapshot(sender).await {
                            return end;
                        }
                    }
                    // The client missed changes, so it gets the whole state again, and the changes after it.
                    Err(RecvError::Lagged(_)) => {
                        changes = changes.resubscribe();
                        if let Err(end) = self.snapshot(sender).await {
                            return end;
                        }
                    }
                    Err(RecvError::Closed) => return End::Stopping,
                },
                call = next_call(&mut following) => match call {
                    Some(message) => {
                        if send(sender, &message).await.is_err() {
                            return End::Client;
                        }
                    }
                    // The environment stopped, and the client follows no calls.
                    None => following = None,
                },
                frame = receiver.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
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
                        if send(sender, &answer).await.is_err() {
                            return End::Client;
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        following = None;
                        let refused = StreamMessage::SubscriptionRefused {
                            reason: "the frame is no subscription: a subscription is a text frame".to_owned(),
                        };
                        if send(sender, &refused).await.is_err() {
                            return End::Client;
                        }
                    }
                    // The WebSocket answers a ping itself.
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return End::Client,
                },
            }
        }
    }

    /// Sends the whole state of the process.
    async fn snapshot(&self, sender: &mut SplitSink<WebSocket, Message>) -> Result<(), End> {
        let state = self.plane.state().await.map_err(End::Unreadable)?;
        send(sender, &StreamMessage::Snapshot(state))
            .await
            .map_err(|()| End::Client)
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

/// Sends one message as a text frame.
///
/// # Errors
///
/// `()` when the client went away.
async fn send(sender: &mut SplitSink<WebSocket, Message>, message: &StreamMessage) -> Result<(), ()> {
    sender.send(Message::Text(form(message).into())).await.map_err(|_| ())
}
