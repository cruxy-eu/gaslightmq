//! The client and the handlers of the tests.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_field::prelude::*;
use gaslightmq_listener::prelude::*;
use gaslightmq_wire::{Frame, FrameDecoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// The output of `future`. A test that would hang fails instead.
pub async fn soon<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("what the test waits for happens within ten seconds")
}

/// Whether `future` is still waiting after a moment in which a listener had the time to act.
pub async fn still_waits<T>(future: impl Future<Output = T>) -> bool {
    tokio::time::timeout(Duration::from_millis(50), future).await.is_err()
}

/// The configuration of a listener of `endpoint` on a free port of this machine.
pub fn on_a_free_port(endpoint: Endpoint) -> ListenerConfiguration {
    ListenerConfiguration::new("127.0.0.1:0".parse().expect("an address"), endpoint)
}

/// A bootstrap listener on a free port, with numbers of its own.
pub async fn listen(handler: impl ConnectionHandler) -> Listener {
    Listener::bind(on_a_free_port(Endpoint::Bootstrap), ConnectionNumbers::new(), handler)
        .await
        .expect("a free port takes a listener")
}

/// The frame with that payload.
pub fn frame(payload: &[u8]) -> Frame {
    Frame::new(payload.to_vec())
}

/// A client of a test: a socket, and the bytes read from it that no frame has taken yet.
pub struct Client {
    stream: TcpStream,
    frames: FrameDecoder,
}

impl Client {
    pub async fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).await.expect("the listener accepts");
        // Every write leaves at once, so a test decides how its bytes are cut.
        stream.set_nodelay(true).expect("a socket without delay");
        Self {
            stream,
            frames: FrameDecoder::new(),
        }
    }

    /// The address the client connects from, as the listener sees it.
    pub fn address(&self) -> ClientAddress {
        ClientAddress::new(self.stream.local_addr().expect("a connected socket has an address"))
    }

    /// Write `bytes` as they are, in one piece.
    pub async fn write(&mut self, bytes: &[u8]) {
        soon(self.stream.write_all(bytes)).await.expect("the listener reads");
    }

    /// Send one frame with that payload.
    pub async fn send(&mut self, payload: &[u8]) {
        self.write(&frame(payload).to_bytes()).await;
    }

    /// The next frame the listener sent, or `None` when the connection ended.
    pub async fn receive(&mut self) -> Option<Frame> {
        loop {
            if let Some(frame) = self.frames.next_frame().expect("the listener sends frames") {
                return Some(frame);
            }
            match soon(self.stream.read_buf(self.frames.buffer_mut())).await {
                Ok(0) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }

    /// As [`receive`](Self::receive), by a client that reads slower than the listener writes:
    /// a few kilobytes at a time, with a pause before each read.
    /// What the listener sent piles up on the way, on the client's side and on the listener's.
    pub async fn receive_slowly(&mut self) -> Option<Frame> {
        let mut piece = vec![0; 16 * 1024];
        loop {
            if let Some(frame) = self.frames.next_frame().expect("the listener sends frames") {
                return Some(frame);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            match soon(self.stream.read(&mut piece)).await {
                Ok(0) | Err(_) => return None,
                Ok(read) => self.frames.extend(&piece[..read]),
            }
        }
    }

    /// Send one frame, and return the payload of the answer as text.
    pub async fn ask(&mut self, payload: &[u8]) -> String {
        self.send(payload).await;
        let answer = self.receive().await.expect("an answer");
        String::from_utf8(answer.payload().to_vec()).expect("an answer in text")
    }
}

/// How one connection of an [`Echo`] ended.
#[derive(Debug)]
pub struct Ended {
    pub number: ConnectionNumber,
    pub address: ClientAddress,
    pub endpoint: Endpoint,
    /// How many frames the connection carried.
    pub frames: usize,
    pub why: ListenerError,
}

/// The connections of an [`Echo`] that ended, in the order they did.
pub struct Endings(mpsc::UnboundedReceiver<Ended>);

impl Endings {
    /// The next connection that ended.
    pub async fn next(&mut self) -> Ended {
        soon(self.0.recv()).await.expect("the handler lives")
    }
}

/// Sends every frame back to the client that sent it, and reports how each connection ended.
pub struct Echo {
    ended: mpsc::UnboundedSender<Ended>,
}

impl Echo {
    pub fn new() -> (Self, Endings) {
        let (ended, endings) = mpsc::unbounded_channel();
        (Self { ended }, Endings(endings))
    }
}

impl ConnectionHandler for Echo {
    async fn handle(&self, mut transport: Transport) {
        let mut frames = 0;
        let why = loop {
            match transport.receive().await {
                Ok(frame) => {
                    frames += 1;
                    // A client that left takes no answer, and the next `receive` says that it left.
                    let _sent = transport.send(&frame.to_bytes()).await;
                }
                Err(why) => break why,
            }
        };
        // A test that does not look at the endings has let go of them.
        let _reported = self.ended.send(Ended {
            number: transport.number(),
            address: transport.address(),
            endpoint: transport.endpoint(),
            frames,
            why,
        });
    }
}

/// Answers every frame with what the transport knows about its connection: `#1 from 127.0.0.1:51459 to bootstrap`.
pub struct WhoAmI;

impl ConnectionHandler for WhoAmI {
    async fn handle(&self, mut transport: Transport) {
        let facts = format!(
            "{} from {} to {}",
            transport.number(),
            transport.address(),
            transport.endpoint()
        );
        let answer = frame(facts.as_bytes()).to_bytes();
        while transport.receive().await.is_ok() {
            if transport.send(&answer).await.is_err() {
                return;
            }
        }
    }
}
