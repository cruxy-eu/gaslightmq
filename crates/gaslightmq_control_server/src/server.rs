//! The server: one listener, the route of the operations and the route of the live stream.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::prelude::*;
use gaslightmq_host::Host;
use gaslightmq_workspace::WorkspaceContext;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::plane::{ControlPlane, Reply, form};
use crate::stream::ClientStream;
use crate::{ControlServerConfiguration, ControlServerError, RequestSizeLimit};

/// The path of the operations.
const OPERATIONS: &str = "/api/operations";

/// The path of the live stream.
const STREAM: &str = "/api/stream";

/// The HTTP and WebSocket server of the control plane, in front of one host.
///
/// It serves the operations of `gaslightmq_control_api` and the live stream, as
/// [the control plane over the network](crate#the-wire-form) describes them.
/// It runs from [`start`](Self::start) until someone [stops](Self::stop) it.
/// Whoever lets go of it without stopping it stops it a moment later, and waits for nothing.
///
/// The host stays with whoever made it: the server starts and stops environments of the workspace by their names,
/// and whoever made the host can start others beside it.
pub struct ControlServer {
    address: SocketAddr,
    stop_timeout: TimeSpan,
    stopping: watch::Sender<bool>,
    serving: JoinHandle<()>,
}

/// What every request of one server reaches.
struct Shared<C: WorkspaceContext> {
    plane: Arc<ControlPlane<C>>,
    request_limit: RequestSizeLimit,
    stopping: watch::Receiver<bool>,
}

impl<C: WorkspaceContext + 'static> Clone for Shared<C> {
    fn clone(&self) -> Self {
        Self {
            plane: Arc::clone(&self.plane),
            request_limit: self.request_limit,
            stopping: self.stopping.clone(),
        }
    }
}

impl ControlServer {
    /// Starts the server in front of `host`, on the address of `configuration`.
    ///
    /// The server answers requests from the moment the call returns, on the runtime that the caller runs on.
    ///
    /// # Errors
    ///
    /// [`ControlServerError::ListenerNotOpened`] when the server cannot listen on the address.
    pub async fn start<C: WorkspaceContext + 'static>(
        configuration: ControlServerConfiguration,
        host: Arc<Host<C>>,
    ) -> Result<Self, ControlServerError> {
        let not_opened = |reason: String| ControlServerError::ListenerNotOpened {
            address: configuration.listen,
            reason,
        };
        let listener = TcpListener::bind(configuration.listen)
            .await
            .map_err(|error| not_opened(error.to_string()))?;
        let address = listener.local_addr().map_err(|error| not_opened(error.to_string()))?;
        let (stopping, stopped) = watch::channel(false);
        let shared = Shared {
            plane: Arc::new(ControlPlane::new(host, configuration.stream_backlog)),
            request_limit: configuration.request_limit,
            stopping: stopped.clone(),
        };
        let routes = Router::new()
            .route(OPERATIONS, post(operation::<C>))
            .route(STREAM, get(stream::<C>))
            .with_state(shared);
        let mut until_stopped = stopped;
        let serving = tokio::spawn(async move {
            let serve = axum::serve(listener, routes).with_graceful_shutdown(async move {
                // A server whose stop signal went away stops too.
                let _ = until_stopped.wait_for(|stopping| *stopping).await;
            });
            // The listener fails only when the runtime ends, and the server ends with it.
            let _ = serve.await;
        });
        Ok(Self {
            address,
            stop_timeout: configuration.stop_timeout,
            stopping,
            serving,
        })
    }

    /// The address the server listens on.
    ///
    /// It names the port that the system picked, where the configuration asked for port 0.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stops the server, and waits until it has stopped.
    ///
    /// The server takes no new request, and every client of the live stream gets a close frame.
    /// The requests that are being answered get their answers, for as long as the
    /// [stop timeout](ControlServerConfiguration::stop_timeout). A request that takes longer ends without one,
    /// while its operation goes on in the host.
    /// The address is free once the call returns.
    pub async fn stop(self) {
        let Self {
            stopping,
            mut serving,
            stop_timeout,
            ..
        } = self;
        stopping.send_replace(true);
        if tokio::time::timeout(stop_timeout.duration(), &mut serving)
            .await
            .is_err()
        {
            serving.abort();
            // The task ends at its next wait, and the listener with it.
            let _ = serving.await;
        }
    }
}

/// Writes the address the server listens on.
impl fmt::Debug for ControlServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlServer")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Answers one request to the operations: reads its operation, and answers it.
async fn operation<C: WorkspaceContext + 'static>(State(shared): State<Shared<C>>, body: Body) -> Response {
    let reply = match to_bytes(body, shared.request_limit.bytes()).await {
        Err(error) => Reply::Refused(RequestError::Unreadable {
            reason: format!(
                "the body was not read, and the server takes {}: {error}",
                shared.request_limit
            ),
        }),
        Ok(bytes) => match serde_json::from_slice::<AnyOperation>(&bytes) {
            Ok(operation) => shared.plane.answer(operation).await,
            Err(error) => Reply::Refused(RequestError::Unreadable {
                reason: error.to_string(),
            }),
        },
    };
    let (status, body) = match reply {
        Reply::Answered(body) => (StatusCode::OK, body),
        Reply::Failed(body) => (StatusCode::UNPROCESSABLE_ENTITY, body),
        Reply::Refused(error @ RequestError::NotServed { .. }) => (StatusCode::NOT_IMPLEMENTED, form(&error)),
        Reply::Refused(error @ RequestError::Unreadable { .. }) => (StatusCode::BAD_REQUEST, form(&error)),
    };
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Opens the live stream of one client.
async fn stream<C: WorkspaceContext + 'static>(State(shared): State<Shared<C>>, upgrade: WebSocketUpgrade) -> Response {
    let client = ClientStream::new(shared.plane, shared.stopping);
    upgrade.on_upgrade(move |socket| client.run(socket))
}
