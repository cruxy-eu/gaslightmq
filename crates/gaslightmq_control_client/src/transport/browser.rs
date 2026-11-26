//! The browser's own fetch and WebSocket, from WebAssembly.

use std::pin::Pin;

use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, StreamExt};
use gaslightmq_control_api::clock::TimeSpan;
use gloo_net::http::Request;
use gloo_net::websocket::futures::WebSocket;
use gloo_net::websocket::{Message, State, WebSocketError};

use crate::exchange::{JSON, Reply, closed};
use crate::{SendError, StreamError};

/// Sends requests with the browser's fetch.
#[derive(Debug, Clone)]
pub(crate) struct Http;

impl Http {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Sends `body` to `url`, and reads the whole answer.
    pub(crate) async fn post(&self, url: &str, body: String) -> Result<Reply, SendError> {
        let unreachable = |error: gloo_net::Error| SendError::Unreachable {
            reason: error.to_string(),
        };
        let request = Request::post(url)
            .header("content-type", JSON)
            .body(body)
            .map_err(unreachable)?;
        let response = request.send().await.map_err(unreachable)?;
        let status = response.status();
        let body = response.text().await.map_err(unreachable)?;
        Ok(Reply { status, body })
    }
}

/// A WebSocket of the browser that is open, as text that goes out and text that comes in.
pub(crate) struct Socket {
    outgoing: SplitSink<WebSocket, Message>,
    incoming: SplitStream<WebSocket>,
}

impl Socket {
    /// Opens the WebSocket at `url`, and returns once the other side took it.
    pub(crate) async fn open(url: &str) -> Result<Self, StreamError> {
        let mut socket = WebSocket::open(url).map_err(|error| unreachable(&error))?;
        // The socket is ready once it stops connecting: it is open, or it failed.
        futures::future::poll_fn(|context| Pin::new(&mut socket).poll_ready(context))
            .await
            .map_err(|error| unreachable(&error))?;
        if !matches!(socket.state(), State::Open) {
            return Err(StreamError::Unreachable {
                reason: "the control plane did not take the live stream".to_owned(),
            });
        }
        let (outgoing, incoming) = socket.split();
        Ok(Self { outgoing, incoming })
    }

    pub(crate) async fn send(&mut self, text: String) -> Result<(), StreamError> {
        self.outgoing
            .send(Message::Text(text))
            .await
            .map_err(|error| unreachable(&error))
    }

    /// The next text that comes in. Waits as long as it takes, and loses nothing when the caller stops waiting.
    pub(crate) async fn receive(&mut self) -> Result<String, StreamError> {
        match self.incoming.next().await {
            Some(Ok(Message::Text(text))) => Ok(text),
            Some(Ok(Message::Bytes(_))) => Err(StreamError::Unreadable {
                reason: "the live stream sent bytes, and not text".to_owned(),
            }),
            Some(Err(WebSocketError::ConnectionClose(event))) => Err(closed(Some(event.code), &event.reason)),
            None => Err(closed(None, "")),
            Some(Err(error)) => Err(unreachable(&error)),
        }
    }

    /// Closes the socket. The browser tells the other side, behind the page.
    #[expect(
        clippy::unused_async,
        reason = "every transport closes a socket the same way, and the other one waits"
    )]
    pub(crate) async fn close(self) {
        // A socket that is let go of closes.
        drop(self);
    }
}

/// Waits for `span`, on a timer of the browser.
pub(crate) async fn sleep(span: TimeSpan) {
    let millis = u32::try_from(span.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

/// The stream is unreachable, for what the browser said.
fn unreachable<E: std::fmt::Display>(error: &E) -> StreamError {
    StreamError::Unreachable {
        reason: error.to_string(),
    }
}
