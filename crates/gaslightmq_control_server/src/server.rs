//! The server: one listener, the route of the operations and the route of the live stream.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use gaslightmq_clock_waitable::{WaitableClock, WallClock};
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::prelude::*;
use gaslightmq_host::Host;
use gaslightmq_workspace::WorkspaceContext;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::attendance::Attendance;
use crate::feed::Feed;
use crate::plane::{ControlPlane, Reply, form};
use crate::presence::{self, Presences};
use crate::stream::ClientStream;
use crate::ui;
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
    /// The task that ends every presence once the server stops.
    ending: JoinHandle<()>,
}

/// What every request of one server reaches.
struct Shared<C: WorkspaceContext, K> {
    plane: Arc<ControlPlane<C>>,
    request_limit: RequestSizeLimit,
    stopping: watch::Receiver<bool>,
    /// The presences of the clients of the live stream, on the clock that their grace period runs on.
    attendance: Attendance<K>,
}

impl<C: WorkspaceContext + 'static, K> Clone for Shared<C, K> {
    fn clone(&self) -> Self {
        Self {
            plane: Arc::clone(&self.plane),
            request_limit: self.request_limit,
            stopping: self.stopping.clone(),
            attendance: self.attendance.clone(),
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
        Self::start_on(configuration, host, WallClock::new()).await
    }

    /// Starts the server as [`start`](Self::start) does, on the clock it is handed.
    ///
    /// The [grace period](ControlServerConfiguration::presence_grace) of a presence runs on that clock,
    /// and so do the [pings](ControlServerConfiguration::ping_interval) that find a client which went silent.
    /// A process runs on real time, which [`start`](Self::start) picks.
    /// A test hands over a clock that it steps, so a grace period takes no time.
    ///
    /// ```no_run
    /// use std::sync::Arc;
    ///
    /// use gaslightmq_clock_waitable::SteppedClock;
    /// use gaslightmq_control_server::prelude::*;
    /// use gaslightmq_host::prelude::*;
    /// use gaslightmq_lie_api::prelude::*;
    /// use gaslightmq_workspace::prelude::*;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), ControlServerError> {
    /// // The binary reads the randomness of the system once, for the ID of the run. This one is written out.
    /// let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
    /// let host = Arc::new(Host::new(HostConfiguration::default(), MemoryContext::new(), Lies::of_every_build(), run));
    /// let configuration = ControlServerConfiguration::default();
    /// let grace = configuration.presence_grace;
    /// let clock = SteppedClock::new();
    /// let server = ControlServer::start_on(configuration, host, clock.clone()).await?;
    ///
    /// // A client whose stream ended stays away, and the test lets its grace period pass.
    /// clock.advance(grace.duration());
    /// # server.stop().await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`ControlServerError::ListenerNotOpened`] when the server cannot listen on the address.
    pub async fn start_on<C: WorkspaceContext + 'static, K: WaitableClock>(
        configuration: ControlServerConfiguration,
        host: Arc<Host<C>>,
        clock: K,
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
        let plane = Arc::new(ControlPlane::new(host, configuration.stream_backlog));
        // The feed follows the host from before the first client connects, so no change of the host goes past.
        tokio::spawn(Feed::new(Arc::clone(&plane), configuration.stats_interval, stopped.clone()).run());
        let attendance = Attendance::new(
            Presences::new(configuration.presence_grace),
            configuration.ping_interval,
            clock,
        );
        // No client comes back to a server that stopped, so every presence ends then, and its holds with it:
        // the host may live on, and nothing else would end them.
        let ending = tokio::spawn({
            let (plane, attendance, mut stopping) = (Arc::clone(&plane), attendance.clone(), stopped.clone());
            async move {
                self::stopped(&mut stopping).await;
                attendance.end_all(plane.host());
            }
        });
        let shared = Shared {
            plane,
            request_limit: configuration.request_limit,
            stopping: stopped.clone(),
            attendance,
        };
        let routes = Router::new()
            .route(OPERATIONS, post(operation::<C, K>))
            .route(STREAM, get(stream::<C, K>))
            .fallback(ui::serve)
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
            ending,
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
    /// Every presence ends, without a grace period: no client comes back to a server that stopped,
    /// so the pads that its clients hold are held no more in the host.
    /// The address is free once the call returns.
    pub async fn stop(self) {
        let Self {
            stopping,
            mut serving,
            ending,
            stop_timeout,
            ..
        } = self;
        stopping.send_replace(true);
        // The task ends every presence at once, and waits for nothing. One that panicked has nothing more to end.
        let _ = ending.await;
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

/// Answers one request to the operations: reads its operation and where it comes from, and answers it.
///
/// A body that holds no operation, that does not say where it comes from, or that claims a surface which only the
/// process names is no request, and gets a [`RequestError::Unreadable`].
async fn operation<C: WorkspaceContext + 'static, K: WaitableClock>(
    State(shared): State<Shared<C, K>>,
    body: Body,
) -> Response {
    let reply = match to_bytes(body, shared.request_limit.bytes()).await {
        Err(error) => Reply::Refused(RequestError::Unreadable {
            reason: format!(
                "the body was not read, and the server takes {}: {error}",
                shared.request_limit
            ),
        }),
        Ok(bytes) => match serde_json::from_slice::<OperationRequest>(&bytes) {
            Ok(request) => shared.plane.answer(request).await,
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

/// Opens the live stream of one client: a new one, or one that comes back as the presence that its query names.
async fn stream<C: WorkspaceContext + 'static, K: WaitableClock>(
    State(shared): State<Shared<C, K>>,
    RawQuery(query): RawQuery,
    upgrade: WebSocketUpgrade,
) -> Response {
    let comes_back_as = query.as_deref().and_then(presence::asked_for);
    let client = ClientStream::new(shared.plane, shared.stopping, shared.attendance, comes_back_as);
    upgrade.on_upgrade(move |socket| client.run(socket))
}

/// Waits until the server stops. A server whose stop signal went away stops too.
pub(crate) async fn stopped(stopping: &mut watch::Receiver<bool>) {
    let _ = stopping.wait_for(|stopping| *stopping).await;
}
