//! HTTP and the WebSocket from a native target, on Tokio.

use std::error::Error;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use gaslightmq_control_api::clock::TimeSpan;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::exchange::{JSON, Reply, closed};
use crate::{SendError, StreamError};

type Connection = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Sends requests over HTTP, on connections it keeps open between them.
#[derive(Debug, Clone)]
pub(crate) struct Http {
    client: reqwest::Client,
}

impl Http {
    pub(crate) fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Sends `body` to `url`, and reads the whole answer.
    pub(crate) async fn post(&self, url: &str, body: String) -> Result<Reply, SendError> {
        let unreachable = |error: reqwest::Error| SendError::Unreachable { reason: said(&error) };
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, JSON)
            .body(body)
            .send()
            .await
            .map_err(unreachable)?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(unreachable)?;
        Ok(Reply { status, body })
    }
}

/// A WebSocket that is open, as text that goes out and text that comes in.
pub(crate) struct Socket {
    outgoing: SplitSink<Connection, Message>,
    incoming: SplitStream<Connection>,
}

impl Socket {
    /// Opens the WebSocket at `url`, and returns once the other side took it.
    pub(crate) async fn open(url: &str) -> Result<Self, StreamError> {
        let (connection, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|error| match error {
                tungstenite::Error::Http(response) => StreamError::Unreadable {
                    reason: format!("the live stream got the status {}", response.status().as_u16()),
                },
                error => unreachable(&error),
            })?;
        let (outgoing, incoming) = connection.split();
        Ok(Self { outgoing, incoming })
    }

    pub(crate) async fn send(&mut self, text: String) -> Result<(), StreamError> {
        self.outgoing
            .send(Message::text(text))
            .await
            .map_err(|error| unreachable(&error))
    }

    /// The next text that comes in. Waits as long as it takes, and loses nothing when the caller stops waiting.
    pub(crate) async fn receive(&mut self) -> Result<String, StreamError> {
        loop {
            let message = self.incoming.next().await.ok_or_else(ended)?;
            match message.map_err(|error| unreachable(&error))? {
                Message::Text(text) => return Ok(text.as_str().to_owned()),
                Message::Binary(_) => {
                    return Err(StreamError::Unreadable {
                        reason: "the live stream sent bytes, and not text".to_owned(),
                    });
                }
                Message::Close(Some(frame)) => return Err(closed(Some(frame.code.into()), frame.reason.as_str())),
                Message::Close(None) => return Err(closed(None, "")),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    /// Tells the other side that the socket closes, and waits until the other side let go of the connection.
    pub(crate) async fn close(mut self) {
        if self.outgoing.send(Message::Close(None)).await.is_err() {
            return;
        }
        // The other side answers, and then closes the connection. What it still sent before goes by.
        while let Some(Ok(_)) = self.incoming.next().await {}
    }
}

/// Waits for `span`.
pub(crate) async fn sleep(span: TimeSpan) {
    tokio::time::sleep(span.duration()).await;
}

/// The live stream ended without a word.
fn ended() -> StreamError {
    StreamError::Unreachable {
        reason: "the live stream ended".to_owned(),
    }
}

/// The stream is unreachable, for what the library said.
fn unreachable<E: Error>(error: &E) -> StreamError {
    StreamError::Unreachable { reason: said(error) }
}

/// What the library said, with every cause it gives.
fn said<E: Error>(error: &E) -> String {
    let mut reason = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        let said = next.to_string();
        if !reason.ends_with(&said) {
            reason.push_str(": ");
            reason.push_str(&said);
        }
        cause = next.source();
    }
    reason
}
