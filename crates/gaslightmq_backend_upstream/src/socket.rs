//! A connection to a server of the real cluster.

use gaslightmq_backend::BackendFailure;
use gaslightmq_wire::{Frame, FrameDecoder, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::cluster::ClusterAddress;
use crate::configuration::UpstreamBackendConfiguration;
use crate::error::UpstreamBackendError;

/// A connected stream to a server of the cluster, and the bytes read from it that no frame has taken yet.
#[derive(Debug)]
pub(crate) struct Socket {
    /// The server that took the connection.
    pub(crate) server: ClusterAddress,
    pub(crate) stream: TcpStream,
    pub(crate) frames: FrameDecoder,
}

impl Socket {
    /// What a broker did that closed its side of the connection.
    pub(crate) const CLOSED_BY_BROKER: &'static str = "the broker closed it";

    /// Connect to the first of `servers` that takes a connection.
    ///
    /// Each server gets the time of the `socket_connection_setup_timeout` of `configuration`.
    ///
    /// # Errors
    ///
    /// [`BackendFailure::Unreachable`] when none of them takes a connection in its time.
    /// Its reason names each server with what stood in the way.
    pub(crate) async fn connect(
        servers: &[ClusterAddress],
        configuration: &UpstreamBackendConfiguration,
    ) -> Result<Self, BackendFailure> {
        let within = configuration.socket_connection_setup_timeout;
        let mut refusals = Vec::new();
        for server in servers {
            match timeout(within.duration(), TcpStream::connect((server.host(), server.port()))).await {
                Ok(Ok(stream)) => {
                    // A request is small and waits for its response, so it leaves at once, as with Kafka's own clients.
                    // A socket that refuses the option still works.
                    let _ = stream.set_nodelay(true);
                    return Ok(Self {
                        server: server.clone(),
                        stream,
                        frames: FrameDecoder::with_max_len(configuration.response_max_bytes.bytes()),
                    });
                }
                Ok(Err(error)) => refusals.push(format!("{server}: {error}")),
                Err(_) => refusals.push(format!("{server}: no connection within {within}")),
            }
        }
        Err(BackendFailure::Unreachable {
            reason: refusals.join(", "),
        })
    }

    /// Send `request`, a question of the backend itself, and read the frame that comes back.
    ///
    /// Whoever gives up on this while it waits lets go of the socket too:
    /// the next frame of the socket may answer this request.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the connection ends first,
    /// and when the bytes that come back are no frame.
    pub(crate) async fn exchange(&mut self, request: &Request) -> Result<Frame, UpstreamBackendError> {
        let server = self.server.clone();
        let lost = |how: &dyn std::fmt::Display| {
            UpstreamBackendError::undiscovered(format_args!(
                "the connection to {server} ended before its answer arrived: {how}"
            ))
        };
        let written = self.stream.write_all(&request.frame().to_bytes()).await;
        written.map_err(|error| lost(&error))?;
        loop {
            match self.frames.next_frame() {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {}
                Err(error) => {
                    return Err(UpstreamBackendError::undiscovered(format_args!(
                        "{server} sent what is no frame: {error}"
                    )));
                }
            }
            let read = self.stream.read_buf(self.frames.buffer_mut()).await;
            if read.map_err(|error| lost(&error))? == 0 {
                return Err(lost(&Self::CLOSED_BY_BROKER));
            }
        }
    }
}
