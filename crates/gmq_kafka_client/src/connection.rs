//! One TCP connection to one broker, used for one request at a time.

use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use gmq_wire::{decode_frame, decode_response, encode_frame, encode_request, peek_response_correlation_id};
use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::ClientError;

/// A connected stream and the bytes read from it that no frame has claimed yet.
struct Socket {
    stream: TcpStream,
    unread: BytesMut,
}

impl Socket {
    /// Read until one whole frame is there.
    async fn read_frame(&mut self) -> Result<Bytes, ClientError> {
        loop {
            if let Some(frame) = decode_frame(&mut self.unread).map_err(|e| ClientError::Protocol(e.to_string()))? {
                return Ok(frame);
            }
            if self.stream.read_buf(&mut self.unread).await? == 0 {
                return Err(ClientError::Io("connection closed by broker".into()));
            }
        }
    }
}

pub(crate) struct Connection {
    addr: String,
    client_id: StrBytes,
    socket: Mutex<Option<Socket>>,
    next_cid: AtomicI32,
}

impl Connection {
    pub fn new(addr: String, client_id: &str) -> Self {
        Self {
            addr,
            client_id: StrBytes::from_string(client_id.to_string()),
            socket: Mutex::new(None),
            next_cid: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Send a request and wait for its response. Connects lazily. On any
    /// failure the socket is dropped, so the next request starts clean.
    pub async fn request<Req: Encodable, Resp: Decodable>(
        &self,
        key: ApiKey,
        version: i16,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, ClientError> {
        let mut slot = self.socket.lock().await;
        let cid = self.next_cid.fetch_add(1, Ordering::Relaxed);
        let exchange = async {
            if slot.is_none() {
                let stream = TcpStream::connect(&self.addr).await?;
                let _ = stream.set_nodelay(true);
                *slot = Some(Socket { stream, unread: BytesMut::new() });
            }
            let socket = slot.as_mut().expect("connected above");
            let header = RequestHeader::default()
                .with_request_api_key(key as i16)
                .with_request_api_version(version)
                .with_correlation_id(cid)
                .with_client_id(Some(self.client_id.clone()));
            let payload = encode_request(&header, req).map_err(|e| ClientError::Protocol(e.to_string()))?;
            socket.stream.write_all(&encode_frame(&payload)).await?;
            let frame = socket.read_frame().await?;
            let got = peek_response_correlation_id(&frame).map_err(|e| ClientError::Protocol(e.to_string()))?;
            if got != cid {
                return Err(ClientError::Protocol(format!("correlation id {got}, expected {cid}")));
            }
            let (_, resp) = decode_response(frame, key as i16, version).map_err(|e| ClientError::Protocol(e.to_string()))?;
            Ok(resp)
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
