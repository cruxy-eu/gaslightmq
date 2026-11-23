//! How one role instance reaches the cluster.

use std::sync::{Mutex, MutexGuard, PoisonError};

use gaslightmq_clock::{Clock, TimeSpan};
use gaslightmq_kafka_client_api::ClientError;
use tracing::debug;

use crate::configuration::ClientConfiguration;
use crate::io::clock::MonotonicClock;
use crate::io::connection::Connections;
use crate::logic::attempts::Limits;
use crate::logic::call::{Call, Input, Operation, Step};
use crate::logic::cluster::ClusterView;

/// The connections, the clock and the cluster view of one role instance.
///
/// Every role instance builds its own, so two instances share no socket and no knowledge of the cluster.
pub(crate) struct ClusterAccess {
    configuration: ClientConfiguration,
    clock: MonotonicClock,
    view: Mutex<ClusterView>,
    connections: Connections,
}

impl ClusterAccess {
    pub fn new(configuration: ClientConfiguration) -> Self {
        Self {
            clock: MonotonicClock::new(),
            view: Mutex::new(ClusterView::new(
                configuration.bootstrap_servers.clone(),
                configuration.metadata_max_age.duration(),
            )),
            connections: Connections::new(configuration.client_id.clone()),
            configuration,
        }
    }

    /// Carry out `operation` as one call that may take `timeout`, with every retry,
    /// and that tries again `retries` times, or as often as its time allows when there is no number.
    ///
    /// # Errors
    ///
    /// The failure the call ended with.
    pub async fn call<O: Operation>(
        &self,
        operation: O,
        timeout: TimeSpan,
        retries: Option<u32>,
    ) -> Result<O::Output, ClientError> {
        self.carry_out(operation, timeout, retries).await.1
    }

    /// As [`call`](Self::call), and hand the operation back as the call left it, with what it learned.
    ///
    /// The [`Call`] decides every step. This loop sends, waits and reports back.
    /// An exchange ends at the instant the call names, also while its request still waits for its turn
    /// on a connection that another request holds.
    pub async fn carry_out<O: Operation>(
        &self,
        operation: O,
        timeout: TimeSpan,
        retries: Option<u32>,
    ) -> (O, Result<O::Output, ClientError>) {
        let limits = Limits::new(&self.configuration, timeout, retries);
        let mut call = Call::new(operation, limits, self.clock.now());
        let mut input = Input::Started;
        loop {
            let step = call.handle(input, &mut self.view(), self.clock.now());
            input = match step {
                Step::Exchange(exchange) => {
                    let connection = self.connections.to(&exchange.destination);
                    let answer = tokio::time::timeout(
                        self.clock.time_until(exchange.not_after),
                        connection.exchange(&exchange, |conversation| call.request(&exchange, conversation)),
                    )
                    .await
                    .unwrap_or(Err(ClientError::NoAnswer));
                    match answer {
                        Ok(response) => Input::Answered(exchange, response),
                        Err(error) => {
                            debug!(%error, address = %exchange.destination.address, "exchange failed");
                            Input::Failed(exchange, error)
                        }
                    }
                }
                Step::Wait { until } => {
                    self.clock.wait_until(until).await;
                    Input::Waited
                }
                Step::Done(result) => return (call.into_operation(), result),
            };
        }
    }

    /// The cluster view, for one step of a call. No lock is held while a call waits.
    fn view(&self) -> MutexGuard<'_, ClusterView> {
        self.view.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
