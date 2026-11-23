//! The producer.

use std::net::SocketAddr;

use gaslightmq_kafka_client_api::{ClientError, ProduceResult, Producer};
use gaslightmq_wire::{Offset, Partition, Record, RecordBatch};

use crate::configuration::{ProducerConfiguration, RoleNames};
use crate::io::access::ClusterAccess;
use crate::io::clock::milliseconds_since_1970;
use crate::logic::cluster::TopicPartition;
use crate::logic::produce::{ProduceRecords, stamped};

/// The names of the producers that are configured without a client ID.
static NAMES: RoleNames = RoleNames::new("producer");

/// A producer: it writes records to the leader of a partition.
///
/// It has its own connections, its own view of the cluster and its own `client.id`.
/// Two producers share nothing, and neither does a producer with a consumer or an admin.
///
/// A send that fails returns a [`ClientError`], through [`send`](Self::send) and through the [`Producer`] trait alike.
pub struct KafkaProducer {
    configuration: ProducerConfiguration,
    cluster: ClusterAccess,
}

impl KafkaProducer {
    /// A producer with `configuration`. It connects when it first sends.
    ///
    /// A producer that is configured without a client ID names itself `producer-1`,
    /// and the next one `producer-2`, as Kafka's Java producer does.
    pub fn new(mut configuration: ProducerConfiguration) -> Self {
        configuration.client.client_id = NAMES.client_id(&configuration.client.client_id);
        Self {
            cluster: ClusterAccess::new(configuration.client.clone()),
            configuration,
        }
    }

    /// What the producer runs with. Its client ID is the one the producer sends.
    pub fn configuration(&self) -> &ProducerConfiguration {
        &self.configuration
    }

    /// Write `records` to a partition, as one batch. Returns the offset of the first record.
    ///
    /// A record without a timestamp gets the time of the send, as Kafka's producer gives it.
    ///
    /// # Errors
    ///
    /// [`ClientError::UnwritableRequest`] when the records do not encode.
    /// [`ClientError::Refused`] when the broker answers with an error code that does not pass.
    /// Otherwise the failure of the client or of its connection,
    /// or [`ClientError::OutOfTime`] when retriable failures lasted until the delivery timeout.
    pub async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> Result<Offset, ClientError> {
        let produce = ProduceRecords {
            partition: TopicPartition::new(topic, partition),
            batch: RecordBatch::new(stamped(records, milliseconds_since_1970()))
                .encode()
                .map_err(ClientError::unwritable_request)?,
            acks: self.configuration.acks,
        };
        self.cluster
            .call(
                produce,
                self.configuration.delivery_timeout,
                Some(self.configuration.retries),
            )
            .await
    }
}

/// A producer with the default configuration, which starts from `bootstrap`.
impl From<SocketAddr> for KafkaProducer {
    fn from(bootstrap: SocketAddr) -> Self {
        Self::new(ProducerConfiguration::new(bootstrap))
    }
}

impl Producer for KafkaProducer {
    /// # Errors
    ///
    /// As [`KafkaProducer::send`].
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> Result<ProduceResult, ClientError> {
        let offset = KafkaProducer::send(self, topic, partition, records).await?;
        Ok(ProduceResult { partition, offset })
    }
}
