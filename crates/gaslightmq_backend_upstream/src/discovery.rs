//! Asking the cluster for its brokers, again and again.

use std::sync::Arc;

use gaslightmq_wire::{Frame, Request, Response};
use tokio::time::{sleep, timeout};
use tracing::{debug, warn};

use crate::brokers::{BrokerQuery, MetadataVersion};
use crate::error::UpstreamBackendError;
use crate::socket::Socket;
use crate::watched::WatchedCluster;

/// The discovery of one cluster: it asks which brokers the cluster has, and tells the [`WatchedCluster`].
///
/// It asks on a connection of its own, which no client shares:
/// the bootstrap server first, and a broker it knows when that one takes no connection.
/// The connection stays open between two questions.
/// A question that fails on it is asked once more on a new one, because a broker closes a connection that is idle.
#[derive(Debug)]
pub(crate) struct Discovery {
    cluster: Arc<WatchedCluster>,
    query: BrokerQuery,
    connection: Option<Greeted>,
}

/// A socket whose broker answered the greeting.
#[derive(Debug)]
struct Greeted {
    socket: Socket,
    version: MetadataVersion,
}

impl Discovery {
    /// The discovery of `cluster`, which has asked nothing yet.
    pub(crate) fn new(cluster: Arc<WatchedCluster>) -> Self {
        let query = BrokerQuery::new(cluster.configuration().own_client_id());
        Self {
            cluster,
            query,
            connection: None,
        }
    }

    /// Ask the cluster once which brokers it has, and tell the watched cluster the answer.
    ///
    /// # Errors
    ///
    /// [`UpstreamBackendError::ClusterUndiscovered`] when the cluster does not say which brokers it has.
    /// The cluster is then not known any better than before.
    pub(crate) async fn ask(&mut self) -> Result<(), UpstreamBackendError> {
        let answer = self.answer().await?;
        self.cluster.named(&answer)
    }

    /// Ask whenever what the backend knows is too old or [may be out of date](WatchedCluster::ask_again), forever.
    ///
    /// A question that fails changes nothing, and the next one comes when someone asks again or the time is up.
    /// The cluster gets at most one question per `retry_backoff`.
    pub(crate) async fn keep_asking(mut self) {
        loop {
            let configuration = self.cluster.configuration();
            let (max_age, backoff) = (configuration.metadata_max_age, configuration.retry_backoff);
            // Whichever comes first: the wait is over either way.
            let _ = timeout(max_age.duration(), self.cluster.asked_again()).await;
            if let Err(error) = self.ask().await {
                warn!(%error, "the brokers of the cluster are not known any better than before");
            }
            sleep(backoff.duration()).await;
        }
    }

    /// The response that names the brokers, over the open connection or else over a new one.
    async fn answer(&mut self) -> Result<Response, UpstreamBackendError> {
        if let Some(mut open) = self.connection.take() {
            match self.asked(&mut open).await {
                Ok(answer) => {
                    self.connection = Some(open);
                    return Ok(answer);
                }
                Err(error) => debug!(%error, "the connection that asks for the brokers is of no use any more"),
            }
        }
        let mut new = self.greeted().await?;
        let answer = self.asked(&mut new).await?;
        self.connection = Some(new);
        Ok(answer)
    }

    /// Open a connection to a server that knows the cluster, and greet it.
    async fn greeted(&mut self) -> Result<Greeted, UpstreamBackendError> {
        let servers = self.cluster.any_broker();
        let mut socket = Socket::connect(&servers, self.cluster.configuration())
            .await
            .map_err(UpstreamBackendError::undiscovered)?;
        let greeting = self.query.greeting()?;
        let answer = self.exchange(&mut socket, &greeting).await?;
        let version = BrokerQuery::agreed(&greeting, answer)?;
        Ok(Greeted { socket, version })
    }

    /// Ask for the brokers on `connection`.
    async fn asked(&mut self, connection: &mut Greeted) -> Result<Response, UpstreamBackendError> {
        let question = self.query.brokers(connection.version)?;
        let answer = self.exchange(&mut connection.socket, &question).await?;
        BrokerQuery::named(&question, answer)
    }

    /// Send `request` on `socket`, and wait for its answer for the `request_timeout` of the backend.
    async fn exchange(&self, socket: &mut Socket, request: &Request) -> Result<Frame, UpstreamBackendError> {
        let after = self.cluster.configuration().request_timeout;
        match timeout(after.duration(), socket.exchange(request)).await {
            Ok(answer) => answer,
            Err(_) => Err(UpstreamBackendError::undiscovered(format_args!(
                "{} did not answer within {after}",
                socket.server
            ))),
        }
    }
}
