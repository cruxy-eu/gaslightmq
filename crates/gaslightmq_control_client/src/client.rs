//! The client: it sends operations and opens the live stream.

use std::pin::pin;

use futures::future::{Either, select};
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::surface::PresenceId;
use gaslightmq_control_api::{Operation, RequestSource};

use crate::exchange::{OPERATIONS_PATH, RequestText, read_presence, stream_path};
use crate::transport::{self, Http, Socket};
use crate::{ControlClientConfiguration, LiveStream, SendError, StreamError};

/// A client of one control plane: it sends operations and opens live streams.
///
/// Every request says where it comes from. A client is one [surface](ControlClientConfiguration::surface),
/// and [`send`](Self::send) says so. A client that is several surfaces, as the UI is, or that takes back a change,
/// names the source of a request with [`send_from`](Self::send_from).
///
/// A clone shares the connections of the client, so every part of a program can hold one.
#[derive(Debug, Clone)]
pub struct ControlClient {
    configuration: ControlClientConfiguration,
    http: Http,
}

impl ControlClient {
    /// A client of the control plane that `configuration` names. It connects when it first sends.
    pub fn new(configuration: ControlClientConfiguration) -> Self {
        Self {
            configuration,
            http: Http::new(),
        }
    }

    pub fn configuration(&self) -> &ControlClientConfiguration {
        &self.configuration
    }

    /// Sends `operation`, and returns what the control plane answered: the answer of the operation, or its error.
    ///
    /// The request comes from the [surface](ControlClientConfiguration::surface) of the client.
    /// The error of the operation is the control plane's answer, so it arrives as itself, and the caller decides on
    /// it: it starts an environment that does not run, or picks another name for a rule.
    ///
    /// # Errors
    ///
    /// [`SendError::Unreachable`] when no answer came within the
    /// [request timeout](ControlClientConfiguration::request_timeout),
    /// [`SendError::Rejected`] when the control plane did not take the request,
    /// and [`SendError::Unreadable`] when the answer is no answer of the control plane to the operation.
    pub async fn send<O: Operation>(&self, operation: O) -> Result<Result<O::Answer, O::Error>, SendError> {
        self.send_from(self.configuration.surface, operation).await
    }

    /// Sends `operation` as a request that comes from `from`, and returns what the control plane answered,
    /// as [`send`](Self::send) does.
    ///
    /// `from` is a surface, or a surface with the cause of the request:
    /// an undo is the inverse of a change, sent with [`RequestCause::Undo`](gaslightmq_control_api::RequestCause::Undo).
    ///
    /// ```no_run
    /// use gaslightmq_control_api::prelude::*;
    /// use gaslightmq_control_client::{ControlClient, SendError};
    ///
    /// # async fn press_and_take_back(client: ControlClient, press: SendIntent, disarm: DisarmRule) -> Result<(), SendError> {
    /// // The UI is one client, and a press comes from its pads.
    /// let _ = client.send_from(RequestSurface::Pad, press).await?;
    /// // ⌘Z takes the press back: the rule disarms, and the request says why.
    /// let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
    /// let _ = client.send_from(undo, disarm).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The errors of [`send`](Self::send).
    pub async fn send_from<O: Operation>(
        &self,
        from: impl Into<RequestSource>,
        operation: O,
    ) -> Result<Result<O::Answer, O::Error>, SendError> {
        let request = RequestText::of(from.into(), operation);
        let url = self.configuration.address.http(OPERATIONS_PATH);
        let timeout = self.configuration.request_timeout;
        let reply = within(timeout, self.http.post(&url, request.body.clone()))
            .await
            .unwrap_or_else(|| {
                Err(SendError::Unreachable {
                    reason: no_answer(timeout),
                })
            })?;
        request.read::<O>(&reply)
    }

    /// Opens a live stream, and returns once the control plane took it and named the presence of the client.
    ///
    /// The client is a new [presence](LiveStream::presence): the control plane opens one for the stream.
    /// The [snapshot](gaslightmq_control_api::StreamMessage::Snapshot) is the first message that the stream
    /// [hands over](LiveStream::next), and every change comes after it. The stream follows no calls until it [follows](LiveStream::follow) some.
    ///
    /// # Errors
    ///
    /// [`StreamError::Unreachable`] when the control plane did not take the stream within the
    /// [request timeout](ControlClientConfiguration::request_timeout), and [`StreamError::Unreadable`] when
    /// something else than a control plane answered.
    pub async fn live_stream(&self) -> Result<LiveStream, StreamError> {
        self.open(None).await
    }

    /// Opens a live stream for a client that comes back as `presence`: the presence of the stream it had before.
    ///
    /// The control plane keeps a presence whose stream ended for a grace period.
    /// A client that comes back within it goes on as that presence, and the pads it holds stay held.
    /// A client that comes back later, or to another process, gets a new presence,
    /// and the holds of the one it had have ended: the [presence of the stream](LiveStream::presence) says which.
    ///
    /// ```no_run
    /// use gaslightmq_control_client::{ControlClient, StreamError};
    ///
    /// # async fn follow(client: ControlClient) -> Result<(), StreamError> {
    /// let stream = client.live_stream().await?;
    /// let presence = stream.presence();
    /// // The stream breaks, and the client opens a new one as the same presence.
    /// drop(stream);
    /// let again = client.live_stream_as(presence).await?;
    /// if again.presence() != presence {
    ///     // The control plane no longer held the presence: its holds have ended.
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The errors of [`live_stream`](Self::live_stream).
    pub async fn live_stream_as(&self, presence: PresenceId) -> Result<LiveStream, StreamError> {
        self.open(Some(presence)).await
    }

    /// Opens a live stream for a new client, or for one that comes back as `presence`.
    async fn open(&self, presence: Option<PresenceId>) -> Result<LiveStream, StreamError> {
        let url = self.configuration.address.websocket(&stream_path(presence));
        let timeout = self.configuration.request_timeout;
        let opening = async {
            let mut socket = Socket::open(&url).await?;
            let presence = read_presence(&socket.receive().await?)?;
            Ok(LiveStream::new(socket, presence, timeout))
        };
        within(timeout, opening).await.unwrap_or_else(|| {
            Err(StreamError::Unreachable {
                reason: no_answer(timeout),
            })
        })
    }
}

/// What `work` returns, or nothing once `span` passed.
pub(crate) async fn within<T>(span: TimeSpan, work: impl Future<Output = T>) -> Option<T> {
    match select(pin!(work), pin!(transport::sleep(span))).await {
        Either::Left((outcome, _)) => Some(outcome),
        Either::Right(((), _)) => None,
    }
}

/// Why a call got no answer within `span`.
fn no_answer(span: TimeSpan) -> String {
    format!("no answer within {span}")
}
