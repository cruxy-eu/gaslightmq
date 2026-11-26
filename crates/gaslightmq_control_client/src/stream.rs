//! The live stream of one client.

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::surface::PresenceId;
use gaslightmq_control_api::{StreamMessage, Subscription};

use crate::StreamError;
use crate::client::within;
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
///
/// # The presence of the client
///
/// A client that follows the live stream is there, and the control plane knows it as a [presence](Self::presence).
/// A request that [names the presence](gaslightmq_control_api::RequestSource::with_presence) adds holds that end
/// with it, so a pad never stays held for a client that is gone.
/// The presence outlives a stream that ends: the control plane keeps it for a grace period,
/// and a client that [comes back as it](crate::ControlClient::live_stream_as) within that time keeps its holds.
///
/// The control plane asks every client at an interval whether it is still there, with a ping.
/// A stream answers while its caller waits for its [next message](Self::next), and a browser answers by itself.
/// A caller on a native target that stops reading for longer than that interval is taken for gone:
/// the control plane ends the stream, and its next message is a [`StreamError::Unreachable`].
pub struct LiveStream {
    socket: Socket,
    /// The presence of the client, which the control plane named when the stream opened.
    presence: PresenceId,
    /// How long [`close`](Self::close) waits for the control plane.
    close_timeout: TimeSpan,
    /// Why the stream ended, which every later call returns.
    ended: Option<StreamError>,
}

impl LiveStream {
    pub(crate) fn new(socket: Socket, presence: PresenceId, close_timeout: TimeSpan) -> Self {
        Self {
            socket,
            presence,
            close_timeout,
            ended: None,
        }
    }

    /// The presence of the client of this stream.
    ///
    /// It is the presence that the client [asked for](crate::ControlClient::live_stream_as) when the control plane
    /// still held that one, and a new presence otherwise.
    pub fn presence(&self) -> PresenceId {
        self.presence
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

    /// Ends the stream, and tells the control plane so.
    ///
    /// The presence of the client stays for the grace period of the control plane, as after a stream that broke.
    /// On a native target, the call returns once the control plane let go of the stream, which it does after it
    /// noted that the client left, or once the [request timeout](crate::ControlClientConfiguration::request_timeout)
    /// passed. In a browser, the browser ends the stream behind the page, and the call returns at once.
    ///
    /// Letting go of the stream ends it too, without a word and without a wait.
    pub async fn close(self) {
        let _ = within(self.close_timeout, self.socket.close()).await;
    }
}
