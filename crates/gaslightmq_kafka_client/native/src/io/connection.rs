//! The sockets of one role instance.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use gaslightmq_kernel::ClientId;
use gaslightmq_wire::{Frame, FrameDecoder, Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::address::BrokerAddress;
use crate::error::ClientError;
use crate::logic::call::Exchange;
use crate::logic::cluster::{ConnectionId, Destination};
use crate::logic::conversation::{self, Conversation, Greeting};

/// A connected stream, and the bytes read from it that no frame has taken yet.
struct Socket {
    stream: TcpStream,
    frames: FrameDecoder,
}

impl Socket {
    async fn connect(address: &BrokerAddress) -> Result<Self, ClientError> {
        let stream = TcpStream::connect((address.host(), address.port())).await?;
        // A request is small and waits for its response, so it leaves at once, as with Kafka's own clients.
        // A socket that refuses the option still works.
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            frames: FrameDecoder::new(),
        })
    }

    /// Send `request`, and read the frame that comes back.
    async fn exchange(&mut self, request: &Request) -> Result<Frame, ClientError> {
        self.stream.write_all(&request.frame().to_bytes()).await?;
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

/// A socket whose broker answered the greeting.
struct Open {
    socket: Socket,
    conversation: Conversation,
}

/// One connection of a role instance, used for one request at a time.
///
/// It opens when the first request needs it.
/// A failure closes it, and the next request opens it again, with a greeting first.
pub(crate) struct Connection {
    address: BrokerAddress,
    client_id: ClientId,
    open: tokio::sync::Mutex<Option<Open>>,
}

impl Connection {
    fn new(address: BrokerAddress, client_id: ClientId) -> Self {
        Self {
            address,
            client_id,
            open: tokio::sync::Mutex::new(None),
        }
    }

    /// Send the request that `encode` writes into the conversation of this connection,
    /// and return its response.
    ///
    /// The request waits until the one before it on this connection has its response.
    /// Whoever calls this bounds that wait: giving up while the request waits or is on its way is safe.
    /// The socket stays open only when the response arrived whole:
    /// a failure, a timeout or a caller that gives up closes it,
    /// so no later request reads a response that was meant for this one.
    pub async fn exchange(
        &self,
        exchange: &Exchange,
        encode: impl FnOnce(&mut Conversation) -> Result<Request, ClientError>,
    ) -> Result<Response, ClientError> {
        let mut slot = self.open.lock().await;
        let mut open = match slot.take() {
            Some(open) => open,
            None => self.open(exchange).await?,
        };
        let request = match encode(&mut open.conversation) {
            Ok(request) => request,
            Err(error) => {
                *slot = Some(open);
                return Err(error);
            }
        };
        let frame = within(exchange.answer_within, open.socket.exchange(&request))
            .await
            .unwrap_or(Err(ClientError::RequestTimedOut))?;
        let response = conversation::answer(&request, frame)?;
        *slot = Some(open);
        Ok(response)
    }

    /// Connect, and greet the broker, each within the time `exchange` gives it.
    async fn open(&self, exchange: &Exchange) -> Result<Open, ClientError> {
        let mut socket = within(exchange.connect_within, Socket::connect(&self.address))
            .await
            .unwrap_or_else(|| {
                Err(ClientError::ConnectionFailed(format!(
                    "no connection to {} within {:?}",
                    self.address, exchange.connect_within
                )))
            })?;
        let greeting = Greeting::new(self.client_id.clone())?;
        let frame = within(exchange.greet_within, socket.exchange(greeting.request()))
            .await
            .unwrap_or(Err(ClientError::RequestTimedOut))?;
        Ok(Open {
            socket,
            conversation: greeting.answered(frame)?,
        })
    }
}

/// The output of `future`, or `None` when it takes longer than `limit`.
async fn within<T>(limit: Duration, future: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout(limit, future).await.ok()
}

/// The connections of one role instance. No other instance uses them.
pub(crate) struct Connections {
    client_id: ClientId,
    connections: Mutex<HashMap<ConnectionId, Arc<Connection>>>,
}

impl Connections {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            connections: Mutex::default(),
        }
    }

    /// The connection of `destination`.
    /// A connection that leads to another address than the destination names is replaced.
    pub fn to(&self, destination: &Destination) -> Arc<Connection> {
        let mut connections = self.connections.lock().unwrap_or_else(PoisonError::into_inner);
        match connections.get(&destination.connection) {
            Some(connection) if connection.address == destination.address => Arc::clone(connection),
            _ => {
                let connection = Arc::new(Connection::new(destination.address.clone(), self.client_id.clone()));
                connections.insert(destination.connection, Arc::clone(&connection));
                connection
            }
        }
    }
}
