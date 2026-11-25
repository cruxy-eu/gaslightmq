//! The endpoints of one proxy: a listener for each one that the backend reports as open.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};

use futures::future::join_all;
use gaslightmq_backend::{Backend, Endpoints};
use gaslightmq_clock::{Clock, Instant, TimeSpan};
use gaslightmq_clock_waitable::WaitableClock;
use gaslightmq_environment::BrokerListenersError;
use gaslightmq_field::Endpoint;
use gaslightmq_listener::{ConnectionNumbers, Listener};
use tokio::sync::watch;
use tracing::error;

use crate::engine::Stopping;
use crate::session::{EndpointSessions, Sessions};
use crate::{EndpointOpenError, EngineConfiguration, EngineStartError};

/// Where each endpoint of a proxy listens at this moment.
pub(crate) type Listening = BTreeMap<Endpoint, SocketAddr>;

/// The endpoints of one proxy that accept connections, with the listener of each.
///
/// The backend says which endpoints its cluster has, and the environment says where each one listens.
/// This type keeps the two together: it closes the listener of an endpoint that closed, with every session on it,
/// opens a listener for an endpoint that opened, and tells the backend the address that each listener got.
pub(crate) struct OpenEndpoints<B, C> {
    sessions: Arc<Sessions<B, C>>,
    configuration: EngineConfiguration,
    /// The numbers of the connections of the proxy, whichever endpoint accepts them.
    numbers: ConnectionNumbers,
    /// What the backend reported last.
    known: Endpoints,
    open: BTreeMap<Endpoint, OpenEndpoint>,
    /// When the engine tries again to open the endpoints that have an address and no listener.
    /// `None` while every endpoint that can listen does.
    retry_at: Option<Instant>,
    listening: watch::Sender<Listening>,
}

/// The moment by which a stop is over, in real time. `None` when the time a stop may take is longer than a
/// runtime can count.
type StopBy = Option<tokio::time::Instant>;

/// The moment by which a stop that begins now is over, when it may take `stop_timeout`.
fn stop_by(stop_timeout: TimeSpan) -> StopBy {
    tokio::time::Instant::now().checked_add(stop_timeout.duration())
}

/// Waits for `ending` until `stop_by`, and returns what it gave, or `None` when it was not done by then.
async fn within<T>(stop_by: StopBy, ending: impl Future<Output = T>) -> Option<T> {
    match stop_by {
        Some(at) => tokio::time::timeout_at(at, ending).await.ok(),
        None => Some(ending.await),
    }
}

/// What stood in the way while the listeners followed a report of the backend.
#[derive(Debug)]
pub(crate) enum Unopened {
    /// An endpoint has an address, and no listener on it.
    Endpoint(EndpointOpenError),
    /// The listeners of the environment have no address for a broker endpoint.
    Brokers(BrokerListenersError),
}

impl Unopened {
    /// Whether another try can go better: an address can come free, and a backend can take a listener after all.
    /// Listeners that do not hold the brokers of the cluster stay as the environment wrote them.
    pub const fn can_be_tried_again(&self) -> bool {
        matches!(self, Self::Endpoint(_))
    }

    /// Writes what stood in the way to the log, for whoever runs the proxy.
    pub fn log(&self) {
        match self {
            Self::Endpoint(error) => error!(%error, "an endpoint of the proxy has no listener"),
            Self::Brokers(error) => error!(%error, "the listeners do not hold the brokers of the cluster"),
        }
    }
}

/// An engine that starts does not start with it.
impl From<Unopened> for EngineStartError {
    fn from(unopened: Unopened) -> Self {
        match unopened {
            Unopened::Endpoint(error) => Self::EndpointNotOpened { error },
            Unopened::Brokers(error) => Self::BrokersWithoutListener { error },
        }
    }
}

/// One endpoint that accepts connections.
struct OpenEndpoint {
    listener: Listener,
    /// The address that the listeners of the environment gave the endpoint when it opened.
    /// It names port 0 where the system picked the port, so it is not the address the listener got.
    assigned: SocketAddr,
    /// Why the backend ended the endpoint. Whoever closes the listener for the backend says so first,
    /// so the sessions on it know why they end.
    ended_by_backend: Arc<OnceLock<String>>,
}

impl OpenEndpoint {
    /// Closes the listener for `reason`, which the backend gave, and waits until every session on it has ended.
    async fn end(self, reason: String) {
        // Nobody else sets the reason, so it is still free.
        let _ = self.ended_by_backend.set(reason);
        self.listener.close().await;
    }
}

impl<B: Backend + 'static, C: WaitableClock> OpenEndpoints<B, C> {
    /// No endpoint is open yet, and the backend was not asked.
    pub fn none(
        sessions: Arc<Sessions<B, C>>,
        configuration: EngineConfiguration,
    ) -> (Self, watch::Receiver<Listening>) {
        let (listening, listened) = watch::channel(Listening::new());
        let endpoints = Self {
            sessions,
            configuration,
            numbers: ConnectionNumbers::new(),
            known: Endpoints::none(),
            open: BTreeMap::new(),
            retry_at: None,
            listening,
        };
        (endpoints, listened)
    }

    /// Waits until the backend reports other endpoints than the ones that are known, and follows them.
    ///
    /// Returns every endpoint that did not open, with the reason.
    pub async fn follow_the_backend(&mut self) -> Vec<Unopened> {
        let reported = self.sessions.backend.endpoints(&self.known).await;
        self.follow(reported).await
    }

    /// Follows the backend until the engine stops, and then closes every endpoint.
    ///
    /// | The task waits for | And then |
    /// |---|---|
    /// | A report of the backend that differs from the last one. | Makes the listeners follow it. |
    /// | The [retry backoff](EngineConfiguration::endpoint_retry_backoff), while an endpoint has an address and no listener. | Tries those endpoints again. |
    /// | The engine to stop. | Closes every endpoint, and ends. |
    ///
    /// A change that the listeners began to follow is followed to its end, so no listener is let go of halfway:
    /// every one is either open, and closes when the engine stops, or closed, with its sessions ended and its
    /// address free.
    ///
    /// A stop takes no longer than the [`stop_timeout`](EngineConfiguration::stop_timeout), from the moment the
    /// engine stops: for the change that the listeners still follow, and for every endpoint to close with its
    /// sessions. The task lets go of what is not done by then, and ends.
    pub async fn run(mut self, mut stopping: Stopping) {
        let stop_by = loop {
            let reported = {
                let clock = &self.sessions.clock;
                let retry_at = self.retry_at;
                let time_to_retry = async {
                    match retry_at {
                        Some(at) => clock.until(at).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    () = stopping.stopped() => break stop_by(self.configuration.stop_timeout),
                    reported = self.sessions.backend.endpoints(&self.known) => reported,
                    // The endpoints are the ones of the last report: only what has no listener changes.
                    () = time_to_retry => self.known.clone(),
                }
            };
            match self.follow_until_stopped(reported, &mut stopping).await {
                // Nobody waits for an endpoint that opens while the engine runs, so the log is where it says so.
                ControlFlow::Continue(failed) => {
                    for unopened in failed {
                        unopened.log();
                    }
                }
                ControlFlow::Break(stop_by) => break stop_by,
            }
        };
        // An endpoint that is not closed by then is let go of: it closes a moment later, and nobody waits for a
        // session on it that still waits for its client or for the backend.
        let _ = within(stop_by, self.close()).await;
    }

    /// Makes the listeners follow `reported`, and notes when to try again what did not open.
    ///
    /// Breaks when the engine stopped meanwhile, with the moment by which the stop is over.
    /// The change has until that moment to come to its end, and is let go of where it is then:
    /// a backend that never says whether it takes a listener must not keep an engine from stopping.
    async fn follow_until_stopped(
        &mut self,
        reported: Endpoints,
        stopping: &mut Stopping,
    ) -> ControlFlow<StopBy, Vec<Unopened>> {
        let tried_at = self.sessions.clock.now();
        let backoff = self.configuration.endpoint_retry_backoff.duration();
        let stop_timeout = self.configuration.stop_timeout;
        let failed = {
            let mut following = Box::pin(self.follow(reported));
            tokio::select! {
                biased;
                failed = &mut following => failed,
                () = stopping.stopped() => {
                    let stop_by = stop_by(stop_timeout);
                    // The engine stops whether the change came to its end or not.
                    let _ = within(stop_by, &mut following).await;
                    return ControlFlow::Break(stop_by);
                }
            }
        };
        // The wait counts from the try, so a try that took its time does not put the next one off.
        let retry = failed.iter().any(Unopened::can_be_tried_again);
        self.retry_at = retry.then(|| tried_at.checked_add(backoff).unwrap_or(Instant::MAX));
        ControlFlow::Continue(failed)
    }

    /// Closes every endpoint, and waits until every session on them has ended.
    ///
    /// The backend did not end them, so their sessions end without a word to their pipelines.
    pub async fn close(mut self) {
        let open = std::mem::take(&mut self.open);
        self.listening.send_replace(Listening::new());
        join_all(open.into_values().map(|open| open.listener.close())).await;
    }

    /// Makes the listeners follow `reported`.
    ///
    /// | The endpoint | Its listener |
    /// |---|---|
    /// | Closed since the last report. | Closes, with every session on it. |
    /// | Is open, and the listeners of the environment give it another address than before. | Moves: it closes, and opens on the new address. |
    /// | Accepts connections, and has no listener. | Opens. |
    ///
    /// An endpoint without a listener is one that opened since the last report, one that closed and opened again,
    /// one whose listener moves, and one that did not open before: its address was taken, or the backend rejected
    /// its listener. Every report is another try for those.
    ///
    /// The listeners close first, so their addresses are free for the ones that open.
    /// The backend hears where every listener that opened listens, and a listener that it rejects closes again.
    /// Returns every endpoint that did not open, with the reason.
    async fn follow(&mut self, reported: Endpoints) -> Vec<Unopened> {
        let closed: Vec<Endpoint> = reported.closed_since(&self.known).collect();
        self.known = reported;

        // The broker endpoints get their addresses together: a range gives its ports in the order of the broker IDs.
        let brokers = self.known.iter().filter_map(Endpoint::broker);
        let of_brokers = self.configuration.listeners.brokers().assign(brokers);
        let bootstrap = self.configuration.listeners.bootstrap();
        let assigned = |endpoint: Endpoint| -> Option<SocketAddr> {
            match endpoint.broker() {
                None => Some(bootstrap.into()),
                Some(broker) => of_brokers.as_ref().ok()?.get(&broker).map(|address| (*address).into()),
            }
        };
        // A broker that joined or left gives the brokers behind it another port of a range.
        let stays = |endpoint: &Endpoint| !closed.contains(endpoint);
        let gets_another_address = |(endpoint, open): (&Endpoint, &OpenEndpoint)| {
            assigned(*endpoint).is_some_and(|address| address != open.assigned)
        };
        let moved: Vec<Endpoint> = self
            .open
            .iter()
            .filter(|(endpoint, _)| stays(endpoint))
            .filter(|(endpoint, open)| gets_another_address((endpoint, open)))
            .map(|(endpoint, _)| *endpoint)
            .collect();

        let mut ending = Vec::new();
        for endpoint in &closed {
            let reason = format!("endpoint `{endpoint}` accepts no connections any more");
            ending.extend(self.open.remove(endpoint).map(|open| open.end(reason)));
        }
        for endpoint in &moved {
            let reason =
                format!("endpoint `{endpoint}` moves to another address, because the brokers of the cluster changed");
            ending.extend(self.open.remove(endpoint).map(|open| open.end(reason)));
        }
        // An endpoint that closed has no address, from before its sessions end.
        self.tell_where_the_endpoints_listen();
        // Each call returns once every session on the endpoint has ended, so the address is free again.
        join_all(ending).await;

        let without_listener = |endpoint: &Endpoint| !self.open.contains_key(endpoint);
        let to_open: Vec<Endpoint> = self.known.iter().filter(without_listener).collect();
        let mut failed = Vec::new();
        for endpoint in to_open {
            // An endpoint without an address is a broker that the listeners do not hold, which is reported below.
            let Some(address) = assigned(endpoint) else {
                continue;
            };
            match self.listen(endpoint, address).await {
                Ok(open) => {
                    self.open.insert(endpoint, open);
                }
                Err(error) => failed.push(Unopened::Endpoint(error)),
            }
        }
        if let Err(error) = of_brokers {
            failed.push(Unopened::Brokers(error));
        }

        self.tell_where_the_endpoints_listen();
        failed
    }

    /// Tells the engine the address of every endpoint that is open.
    fn tell_where_the_endpoints_listen(&self) {
        let listening = self
            .open
            .iter()
            .map(|(endpoint, open)| (*endpoint, open.listener.address()));
        self.listening.send_replace(listening.collect());
    }

    /// Opens the listener of `endpoint` on `address`, and tells the backend the address it got.
    ///
    /// Every connection that the listener accepts is a session of this engine.
    /// A listener that the backend rejects closes again: no client is told its address.
    async fn listen(&self, endpoint: Endpoint, address: SocketAddr) -> Result<OpenEndpoint, EndpointOpenError> {
        let (sessions, ended_by_backend) = EndpointSessions::new(Arc::clone(&self.sessions));
        let configuration = self.configuration.listener(address, endpoint);
        let listener = Listener::bind(configuration, self.numbers.clone(), sessions)
            .await
            .map_err(|error| EndpointOpenError::AddressUnavailable {
                endpoint,
                reason: error.to_string(),
            })?;
        let open = OpenEndpoint {
            listener,
            assigned: address,
            ended_by_backend,
        };
        let got = open.listener.address();
        match self.sessions.backend.listens(endpoint, got).await {
            Ok(()) => Ok(open),
            Err(rejection) => {
                let reason = rejection.to_string();
                open.end(reason.clone()).await;
                Err(EndpointOpenError::ListenerRejected { endpoint, reason })
            }
        }
    }
}
