//! A broker that the test plays.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use gaslightmq_wire::messages::api_versions_response::ApiVersion;
use gaslightmq_wire::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use gaslightmq_wire::messages::{ApiVersionsResponse, ProduceResponse, TopicName};
use gaslightmq_wire::protocol::{Encodable, StrBytes};
use gaslightmq_wire::{ApiKey, FrameDecoder, Request, ResponseError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long a test waits for what should happen at once.
const PATIENCE: Duration = Duration::from_secs(10);

/// The output of `future`. A test that would hang fails instead.
pub async fn soon<T>(future: impl Future<Output = T>) -> anyhow::Result<T> {
    tokio::time::timeout(PATIENCE, future)
        .await
        .context("what the test waits for happens within ten seconds")
}

/// A broker that the test plays by hand.
///
/// It listens on a free port of this machine.
/// The test takes each connection of a client, reads the requests on it, and decides what goes back, and when.
pub struct PlayedBroker {
    listener: TcpListener,
}

impl PlayedBroker {
    pub async fn listening() -> anyhow::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind("127.0.0.1:0").await?,
        })
    }

    /// The address a client connects to.
    pub fn address(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// The next connection of a client.
    pub async fn connection(&self) -> anyhow::Result<ClientConnection> {
        let (stream, _) = soon(self.listener.accept()).await??;
        Ok(ClientConnection {
            stream,
            frames: FrameDecoder::new(),
        })
    }

    /// Stops listening, and returns the address that nobody listens on any more.
    pub fn gone(self) -> anyhow::Result<SocketAddr> {
        self.address()
    }

    /// What the broker answers to `ApiVersions`: the one API it offers, with its versions.
    pub fn offered() -> ApiVersionsResponse {
        let api_versions = ApiVersion::default()
            .with_api_key(ApiKey::ApiVersions as i16)
            .with_min_version(0)
            .with_max_version(3);
        ApiVersionsResponse::default().with_api_keys(vec![api_versions])
    }

    /// What the broker answers to a Produce when it refuses the first partition of `topic` with `error`.
    pub fn refusing_to_write(topic: &str, error: ResponseError) -> ProduceResponse {
        let partition = PartitionProduceResponse::default()
            .with_index(0)
            .with_error_code(error.code());
        let topic = TopicProduceResponse::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_owned())))
            .with_partition_responses(vec![partition]);
        ProduceResponse::default().with_responses(vec![topic])
    }
}

/// The broker's end of a connection of a client.
pub struct ClientConnection {
    stream: TcpStream,
    /// The bytes of the client that no frame has taken yet.
    frames: FrameDecoder,
}

impl ClientConnection {
    /// The next request of the client.
    pub async fn request(&mut self) -> anyhow::Result<Request> {
        soon(async {
            loop {
                if let Some(frame) = self.frames.next_frame()? {
                    return Ok(Request::decode(frame)?);
                }
                let read = self.stream.read_buf(self.frames.buffer_mut()).await?;
                anyhow::ensure!(read > 0, "the client closed the connection before it sent a request");
            }
        })
        .await?
    }

    /// The next `count` bytes of the client, as they arrived, whether they are a request or not.
    pub async fn bytes(&mut self, count: usize) -> anyhow::Result<Vec<u8>> {
        soon(async {
            while self.frames.buffered() < count {
                let read = self.stream.read_buf(self.frames.buffer_mut()).await?;
                anyhow::ensure!(
                    read > 0,
                    "the client closed the connection before it sent {count} bytes"
                );
            }
            Ok(self.frames.buffer_mut().split_to(count).to_vec())
        })
        .await?
    }

    /// Answers `request` with `body`.
    pub async fn answer(&mut self, request: &Request, body: &impl Encodable) -> anyhow::Result<()> {
        let response = request.response(body)?;
        self.stream.write_all(&response.frame().to_bytes()).await?;
        Ok(())
    }

    /// Waits for the next request of the client, and answers it with `body`.
    pub async fn answer_next(&mut self, body: &impl Encodable) -> anyhow::Result<()> {
        let request = self.request().await?;
        self.answer(&request, body).await
    }

    /// Sends the start of a frame that holds no bytes, which is no response.
    pub async fn send_frame_of_no_bytes(&mut self) -> anyhow::Result<()> {
        self.stream.write_all(&0u32.to_be_bytes()).await?;
        Ok(())
    }

    /// Closes the connection.
    pub fn hang_up(self) {
        drop(self.stream);
    }
}
