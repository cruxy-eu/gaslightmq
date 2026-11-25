//! The live stream of one client.

use gaslightmq_control_api::{StreamMessage, Subscription};

use crate::StreamError;
use crate::exchange::{read_message, subscription_text};
use crate::transport::Socket;

/// The live stream of the control plane: the snapshot first, then every change and every call it follows.
///
/// A client that applies every message to the snapshot holds what the process holds.
/// A message is a state, and no difference: it may repeat what the client holds, and applying it again changes
/// nothing. A client that falls behind gets a new [snapshot](StreamMessage::Snapshot) in the middle of the stream,
/// and goes on from there. It misses the oldest calls instead, which no snapshot holds.
/// The stream ends at the first error: the client no longer holds what the process holds,
/// so it [opens](crate::ControlClient::live_stream) a new stream, which starts with a new snapshot.
pub struct LiveStream {
    socket: Socket,
    /// Why the stream ended, which every later call returns.
    ended: Option<StreamError>,
}

impl LiveStream {
    pub(crate) fn new(socket: Socket) -> Self {
        Self { socket, ended: None }
    }

    /// The next message. Waits as long as it takes.
    ///
    /// A caller that stops waiting loses no message: the next call returns it.
    ///
    /// # Errors
    ///
    /// [`StreamError::Unreachable`] when the stream broke or the control plane closed it,
    /// and [`StreamError::Unreadable`] when a message is no message of the live stream.
    /// The stream then ends, and every later call returns the same error.
    pub async fn next(&mut self) -> Result<StreamMessage, StreamError> {
        if let Some(error) = &self.ended {
            return Err(error.clone());
        }
        let message = match self.socket.receive().await {
            Ok(text) => read_message(&text),
            Err(error) => Err(error),
        };
        if let Err(error) = &message {
            self.ended = Some(error.clone());
        }
        message
    }

    /// Follows the calls that `subscription` names, in place of those the stream followed before.
    ///
    /// The control plane sends the calls from then on. When it cannot follow them, because the environment does not
    /// run or the selector does not exist, the stream follows no calls,
    /// and a [`SubscriptionRefused`](StreamMessage::SubscriptionRefused) message says why.
    /// When the environment stops, the stream follows no calls either,
    /// and [`EnvironmentStopped`](StreamMessage::EnvironmentStopped) says so.
    ///
    /// A stream that cannot send the subscription has broken, and its [next message](Self::next) says why.
    pub async fn follow(&mut self, subscription: Subscription) {
        if self.ended.is_some() {
            return;
        }
        if let Err(error) = self.socket.send(subscription_text(&subscription)).await {
            self.ended = Some(error);
        }
    }
}
