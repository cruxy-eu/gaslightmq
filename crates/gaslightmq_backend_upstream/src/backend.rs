//! The Upstream backend.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use gaslightmq_backend::{
    Backend, BackendAnswer, BackendFailure, BackendRequest, Endpoints, IncidentRejection, ListenerRejection, Truth,
};
use gaslightmq_field::{ConnectionNumber, Endpoint};
use gaslightmq_incident::{ExpandedIncident, Incident, IncidentSupport, Requirements};
use gaslightmq_overlay::Overlay;
use tokio::task::JoinHandle;

use crate::cluster::Route;
use crate::configuration::UpstreamBackendConfiguration;
use crate::connection::ClusterConnection;
use crate::discovery::Discovery;
use crate::error::UpstreamBackendError;
use crate::watched::WatchedCluster;

/// The backend of a proxy in front of a real Kafka cluster.
///
/// It forwards each request to the real broker that the client meant to reach,
/// and answers with what that broker answered, seen through the [overlay](gaslightmq_overlay::Overlay).
///
/// | The engine | The backend |
/// |---|---|
/// | [Calls](Backend::call) with a request. | Asks the overlay where the request goes, and sends it on the connection to the cluster that stands for the connection of the client. The answer goes through the overlay again, and comes back as the truth. |
/// | Says that a connection [ended](Backend::close). | Closes its connection to the cluster, so the cluster sees the client leave. |
/// | Asks for the [endpoints](Backend::endpoints). | Answers with the bootstrap endpoint and one endpoint per broker of the cluster, and again whenever a broker comes or goes. |
/// | Says where it [listens](Backend::listens) for an endpoint. | Tells clients that address wherever the cluster tells the address of the broker: the advertised host, and the port of the listener. |
/// | Hands over an [incident](Backend::apply). | Rejects it. The overlay emulates no incident yet. |
///
/// The backend [starts](Self::start) once it knows the brokers of its cluster.
/// Letting go of it ends its tasks, and closes every connection to the cluster.
///
/// ```no_run
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_backend_upstream::prelude::*;
/// use gaslightmq_environment::prelude::*;
///
/// # async fn start() -> Result<(), UpstreamBackendError> {
/// let upstream = Upstream::new("kafka-1.staging:9092".parse().expect("a server address"));
/// let advertised_host = "gaslight.staging".parse().expect("a host name");
/// let configuration = UpstreamBackendConfiguration::new(upstream, advertised_host);
///
/// // The backend asks the bootstrap server which brokers the cluster has.
/// let backend = UpstreamBackend::start(configuration).await?;
///
/// // The engine opens a listener for each endpoint, on any free port, and says which address it got.
/// let endpoints = backend.endpoints(&Endpoints::none()).await;
/// let mut listeners = Vec::new();
/// for endpoint in endpoints.iter() {
///     let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.expect("a free port");
///     let address = listener.local_addr().expect("the address the listener got");
///     backend.listens(endpoint, address).await.expect("a port that no other listener has");
///     listeners.push(listener);
/// }
/// // From here on, a client that asks for Metadata is sent to `gaslight.staging`, and to the port of a listener.
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct UpstreamBackend {
    cluster: Arc<WatchedCluster>,
    /// The connection to the cluster of each connection of a client that sent a request.
    connections: Mutex<HashMap<ConnectionNumber, ClusterConnection>>,
    /// The task that keeps asking the cluster for its brokers.
    discovery: JoinHandle<()>,
}

impl UpstreamBackend {
    /// Ask the cluster of `configuration` for its brokers, and start the backend in front of them.
    ///
    /// The backend keeps asking from then on, in a task of its own, so this runs inside the runtime.
    ///
    /// # Errors
    ///
    /// - [`UpstreamBackendError::UnsupportedTls`] and [`UpstreamBackendError::UnsupportedSasl`] when the
    ///   environment asks for a connection that this backend cannot make yet.
    /// - [`UpstreamBackendError::ClusterUndiscovered`] when the cluster does not say which brokers it has:
    ///   no server takes a connection, or none answers in time or with an answer that reads.
    pub async fn start(configuration: UpstreamBackendConfiguration) -> Result<Self, UpstreamBackendError> {
        if configuration.upstream.tls().is_some() {
            return Err(UpstreamBackendError::UnsupportedTls);
        }
        if let Some(sasl) = configuration.upstream.sasl() {
            return Err(UpstreamBackendError::UnsupportedSasl {
                mechanism: sasl.mechanism(),
            });
        }
        let cluster = Arc::new(WatchedCluster::new(configuration));
        let mut discovery = Discovery::new(Arc::clone(&cluster));
        discovery.ask().await?;
        Ok(Self {
            cluster,
            connections: Mutex::default(),
            discovery: tokio::spawn(discovery.keep_asking()),
        })
    }

    /// What the backend is configured with.
    pub fn configuration(&self) -> &UpstreamBackendConfiguration {
        self.cluster.configuration()
    }

    fn connections(&self) -> std::sync::MutexGuard<'_, HashMap<ConnectionNumber, ClusterConnection>> {
        self.connections.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Backend for UpstreamBackend {
    /// Forwards the request to the cluster, and answers with the truth.
    ///
    /// Everything up to the request standing in line for its connection happens when the call starts,
    /// so the requests of one connection reach the cluster in the order in which their calls start.
    ///
    /// | What happens | The answer |
    /// |---|---|
    /// | The cluster answers. | [`BackendAnswer::Truth`], through the overlay. |
    /// | The overlay answers a Fetch that continues a fetch session. | [`BackendAnswer::Truth`], and the cluster never sees the request. |
    /// | The request is a Produce with `acks=0`. | [`BackendAnswer::NoResponse`], once the request is written. |
    /// | The cluster sends what is no response to the request. | [`BackendAnswer::NotAResponse`] |
    /// | The endpoint of the request accepts no connections. | [`BackendFailure::NoSuchEndpoint`] |
    /// | No connection to the broker opens. | [`BackendFailure::Unreachable`] |
    /// | The connection to the broker ends, now or earlier. | [`BackendFailure::ConnectionLost`] |
    /// | The overlay cannot read a request that it has to read, or cannot write the truth. | [`BackendFailure::ResponseNotBuilt`] |
    /// | The broker never answers. | None. The call waits until the engine drops it. |
    async fn call(&self, request: BackendRequest) -> BackendAnswer {
        let connection = request.connection();
        let route = self.cluster.route(request.endpoint(), request.request());
        let waiting = match route {
            Err(failure) => return failure.into(),
            Ok(Route::Answer(response)) => return Truth::new(response).into(),
            Ok(Route::Forward { to, request, reply }) => {
                let mut connections = self.connections();
                let open = connections
                    .entry(connection)
                    .or_insert_with(|| ClusterConnection::open(to, Arc::clone(&self.cluster)));
                open.send(request, reply)
            }
        };
        let let_go = |_| BackendFailure::ConnectionLost {
            reason: ClusterConnection::LET_GO.to_owned(),
        };
        waiting.await.unwrap_or_else(|error| let_go(error).into())
    }

    /// Closes the connection to the cluster that stands for `connection`, at once.
    ///
    /// A request of that connection that was not written yet is not sent.
    async fn close(&self, connection: ConnectionNumber) {
        self.connections().remove(&connection);
    }

    async fn endpoints(&self, known: &Endpoints) -> Endpoints {
        self.cluster.endpoints_since(known).await
    }

    /// Tells clients from now on that the broker of `endpoint` is at the advertised host, on the port of `address`.
    ///
    /// Until the engine says so, clients are not told about the broker:
    /// Metadata leaves it out, and a coordinator on it reads as not available.
    ///
    /// The backend decides at once, so the future is ready.
    ///
    /// # Errors
    ///
    /// [`ListenerRejection::Unadvertisable`] when the port of `address` is zero,
    /// and when another broker is advertised on that port:
    /// clients reach every listener under one host, so they could not tell the two apart.
    fn listens(
        &self,
        endpoint: Endpoint,
        address: SocketAddr,
    ) -> impl Future<Output = Result<(), ListenerRejection>> + Send {
        std::future::ready(self.cluster.listens(endpoint, address))
    }

    /// The support of the overlay, which emulates no incident yet.
    fn incident_support(&self) -> IncidentSupport {
        Overlay::incident_support()
    }

    /// Rejects the incident: the backend cannot change a real cluster, and the overlay emulates no incident yet.
    ///
    /// The backend rejects at once, so the future is ready.
    fn apply(&self, incident: Incident) -> impl Future<Output = Result<ExpandedIncident, IncidentRejection>> + Send {
        let unsupported = self.incident_support().check(&Requirements::from(&incident));
        std::future::ready(match unsupported {
            Err(error) => Err(error.into()),
            Ok(()) => Err(IncidentRejection::Impossible {
                incident: incident.kind(),
                reason: "the backend in front of a real cluster emulates no incident yet".to_owned(),
            }),
        })
    }
}

impl Drop for UpstreamBackend {
    fn drop(&mut self) {
        self.discovery.abort();
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_environment::{Sasl, SaslMechanism, SecretRef, Tls, Upstream};

    use super::*;

    fn configuration(upstream: Upstream) -> UpstreamBackendConfiguration {
        UpstreamBackendConfiguration::new(upstream, "gaslight.staging".parse().unwrap())
    }

    #[tokio::test]
    async fn a_cluster_behind_tls_is_rejected() {
        let upstream = Upstream::new("kafka-1.staging:9092".parse().unwrap()).with_tls(Tls::new());

        let error = UpstreamBackend::start(configuration(upstream)).await.unwrap_err();

        assert_eq!(error, UpstreamBackendError::UnsupportedTls);
    }

    #[tokio::test]
    async fn a_cluster_with_a_login_is_rejected() {
        let password = SecretRef::Variable("KAFKA_PASSWORD".parse().unwrap());
        let sasl = Sasl::new(SaslMechanism::ScramSha512, "gaslight".parse().unwrap(), password);
        let upstream = Upstream::new("kafka-1.staging:9092".parse().unwrap()).with_sasl(sasl);

        let error = UpstreamBackend::start(configuration(upstream)).await.unwrap_err();

        assert_eq!(
            error,
            UpstreamBackendError::UnsupportedSasl {
                mechanism: SaslMechanism::ScramSha512
            }
        );
    }
}
