//! One TCP connection to one broker, used for one request at a time.

use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use gaslightmq_wire::protocol::Request as KafkaRequest;
use gaslightmq_wire::{Frame, FrameDecoder, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::ClientError;

/// A connected stream, and the bytes read from it that no frame has taken yet.
struct Socket {
    stream: TcpStream,
    frames: FrameDecoder,
}

impl Socket {
    async fn connect(addr: &str) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            frames: FrameDecoder::new(),
        })
    }

    /// Send `request` and read its response.
    async fn exchange<R: KafkaRequest>(&mut self, request: &Request) -> Result<R::Response, ClientError> {
        self.stream.write_all(&request.frame().to_bytes()).await?;
        let response = Response::decode(request.api(), self.read_frame().await?)?;
        if response.correlation_id() != request.correlation_id() {
            return Err(ClientError::Protocol(format!(
                "correlation id {}, expected {}",
                response.correlation_id(),
                request.correlation_id()
            )));
        }
        Ok(response.body()?)
    }

    /// Read until one whole frame is there.
    async fn read_frame(&mut self) -> Result<Frame, ClientError> {
        loop {
            if let Some(frame) = self.frames.next_frame()? {
                return Ok(frame);
            }
            if self.stream.read_buf(self.frames.buffer_mut()).await? == 0 {
                return Err(ClientError::ConnectionFailed("connection closed by broker".into()));
            }
        }
    }
}

pub(crate) struct Connection {
    addr: String,
    client_id: String,
    socket: Mutex<Option<Socket>>,
    next_cid: AtomicI32,
}

impl Connection {
    pub fn new(addr: String, client_id: &str) -> Self {
        Self {
            addr,
            client_id: client_id.to_string(),
            socket: Mutex::new(None),
            next_cid: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Send a request and wait for its response. Connects lazily. On any
    /// failure the socket is dropped, so the next request starts clean.
    pub async fn request<R: KafkaRequest>(
        &self,
        version: i16,
        body: &R,
        timeout: Duration,
    ) -> Result<R::Response, ClientError> {
        let mut slot = self.socket.lock().await;
        let cid = self.next_cid.fetch_add(1, Ordering::Relaxed);
        let exchange = async {
            let request = Request::new(version, cid, Some(&self.client_id), body)?;
            let socket = match slot.as_mut() {
                Some(socket) => socket,
                None => slot.insert(Socket::connect(&self.addr).await?),
            };
            socket.exchange::<R>(&request).await
        };
        let result = match tokio::time::timeout(timeout, exchange).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Timeout),
        };
        if result.is_err() {
            *slot = None;
        }
        result
    }
}
