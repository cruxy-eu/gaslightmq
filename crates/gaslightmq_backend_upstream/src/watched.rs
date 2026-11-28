//! The cluster of one backend, shared by everything that waits on it.

use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard, PoisonError};

use gaslightmq_backend::{BackendAnswer, BackendFailure, Endpoints, ListenerRejection};
use gaslightmq_field::Endpoint;
use gaslightmq_wire::{Request, Response};
use tokio::sync::{Notify, watch};

use crate::cluster::{Cluster, ClusterAddress, Route};
use crate::configuration::UpstreamBackendConfiguration;
use crate::error::UpstreamBackendError;

/// The [`Cluster`] of one backend, with what the tasks of the backend wait on.
///
/// The cluster decides, and never waits. This type adds the two waits around it:
///
/// - the engine waits for the [endpoints to change](Self::endpoints_since),
/// - and the task that asks the cluster for its brokers waits to be [asked again](Self::asked_again).
///
/// Every connection of the backend and that task share one of these.
/// The lock around the cluster is held for one decision, and never across a wait.
#[derive(Debug)]
pub(crate) struct WatchedCluster {
    configuration: UpstreamBackendConfiguration,
    cluster: Mutex<Cluster>,
    endpoints: watch::Sender<Endpoints>,
    outdated: Notify,
}

impl WatchedCluster {
    /// The cluster of `configuration`, of which nothing is known yet.
    pub(crate) fn new(configuration: UpstreamBackendConfiguration) -> Self {
        let cluster = Cluster::new(
            configuration.upstream.bootstrap(),
            configuration.advertised_host.clone(),
            configuration.advertised_listeners.clone(),
        );
        Self {
            configuration,
            cluster: Mutex::new(cluster),
            endpoints: watch::Sender::new(Endpoints::none()),
            outdated: Notify::new(),
        }
    }

    pub(crate) const fn configuration(&self) -> &UpstreamBackendConfiguration {
        &self.configuration
    }

    /// See [`Cluster::route`].
    pub(crate) fn route(&self, endpoint: Endpoint, request: &Request) -> Result<Route, BackendFailure> {
        self.cluster().route(endpoint, request)
    }

    /// See [`Cluster::truth`].
    ///
    /// An answer that names a broker without an endpoint, where every broker had one, asks the cluster again.
    pub(crate) fn truth(&self, request: &Request, upstream: Response) -> BackendAnswer {
        let mut cluster = self.cluster();
        let had_endpoints = !cluster.names_broker_without_endpoint();
        let answer = cluster.truth(request, upstream);
        if had_endpoints && cluster.names_broker_without_endpoint() {
            self.ask_again();
        }
        answer
    }

    /// See [`Cluster::listens`].
    pub(crate) fn listens(&self, endpoint: Endpoint, address: SocketAddr) -> Result<(), ListenerRejection> {
        self.cluster().listens(endpoint, address)
    }

    /// See [`Cluster::named`]. Whoever waits for the endpoints learns what changed.
    pub(crate) fn named(&self, answer: &Response) -> Result<(), UpstreamBackendError> {
        let mut cluster = self.cluster();
        cluster.named(answer)?;
        let endpoints = cluster.endpoints();
        self.endpoints.send_if_modified(|known| {
            let changed = known != endpoints;
            if changed {
                known.clone_from(endpoints);
            }
            changed
        });
        Ok(())
    }

    /// See [`Cluster::any_broker`].
    pub(crate) fn any_broker(&self) -> Vec<ClusterAddress> {
        self.cluster().any_broker()
    }

    /// What the backend knows about the brokers may be out of date, so it asks the cluster again.
    pub(crate) fn ask_again(&self) {
        self.outdated.notify_one();
    }

    /// Waits until someone [asks again](Self::ask_again). One wait takes every question that came before it.
    pub(crate) async fn asked_again(&self) {
        self.outdated.notified().await;
    }

    /// Waits until the endpoints differ from `known`, and returns them.
    pub(crate) async fn endpoints_since(&self, known: &Endpoints) -> Endpoints {
        let mut watching = self.endpoints.subscribe();
        let changed = watching.wait_for(|now| now != known).await;
        match changed.map(|now| now.clone()) {
            Ok(now) => now,
            // The cluster outlives everyone who waits on it, so its endpoints never go away.
            Err(_) => std::future::pending().await,
        }
    }

    fn cluster(&self) -> MutexGuard<'_, Cluster> {
        self.cluster.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
