//! The orders service: it places an order by writing it to the topic of the orders.
//!
//! This is the application under test. It knows the bootstrap servers of its cluster and nothing about GaslightMQ:
//! its tests hand it the address of a proxy instead of the cluster.

use gaslightmq_kafka_client_api::ClientError;
use gaslightmq_kafka_client_native::{BootstrapServers, KafkaProducer, ProducerConfiguration};
use gaslightmq_wire::{Offset, Partition, Record, ResponseError};

/// What the service runs with.
#[derive(Debug, Clone)]
pub struct OrdersConfiguration {
    /// Default: `orders`.
    ///
    /// The topic that every order is written to.
    pub topic: String,
    /// Default: 0.
    ///
    /// The partition of the topic that every order is written to. The client of this example has no partitioner.
    pub partition: Partition,
    /// Default: Kafka's defaults.
    ///
    /// The producer that writes the orders: its bootstrap servers, its timeouts and its retries, as Kafka names them.
    pub producer: ProducerConfiguration,
}

impl OrdersConfiguration {
    /// The service with every default, in front of the cluster at `bootstrap_servers`.
    pub fn new(bootstrap_servers: impl Into<BootstrapServers>) -> Self {
        Self {
            topic: "orders".to_owned(),
            partition: Partition::new(0),
            producer: ProducerConfiguration::new(bootstrap_servers),
        }
    }
}

/// An order that a customer placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub id: String,
    pub item: String,
    /// When the customer placed it, in milliseconds since 1970.
    pub placed_at: i64,
}

impl Order {
    /// The record that the service writes for the order: the item, under the ID of the order.
    pub fn record(&self) -> Record {
        Record::new(self.item.clone())
            .with_key(self.id.clone())
            .with_timestamp(self.placed_at)
    }
}

/// The orders service.
pub struct Orders {
    topic: String,
    partition: Partition,
    producer: KafkaProducer,
}

impl Orders {
    pub fn new(configuration: OrdersConfiguration) -> Self {
        Self {
            topic: configuration.topic,
            partition: configuration.partition,
            producer: KafkaProducer::new(configuration.producer),
        }
    }

    /// Writes `order` to the topic of the orders, and returns its offset.
    ///
    /// # Errors
    ///
    /// - [`PlaceOrderError::NotAllowed`] when the cluster does not let the service write to the topic.
    /// - [`PlaceOrderError::Unconfirmed`] when the cluster did not confirm the order, and may hold it.
    /// - [`PlaceOrderError::NotPlaced`] when the order was not written.
    pub async fn place(&self, order: &Order) -> Result<Offset, PlaceOrderError> {
        self.producer
            .send(&self.topic, self.partition, &[order.record()])
            .await
            .map_err(|error| self.not_placed(order, error))
    }

    /// What it means for the service that the producer did not write `order`.
    fn not_placed(&self, order: &Order, error: ClientError) -> PlaceOrderError {
        match error {
            ClientError::Refused {
                error: ResponseError::TopicAuthorizationFailed,
            } => PlaceOrderError::NotAllowed {
                topic: self.topic.clone(),
            },
            // The request went out, and no answer said whether the cluster wrote the order.
            ClientError::Refused {
                error: ResponseError::RequestTimedOut,
            }
            | ClientError::NoAnswer
            | ClientError::ConnectionLost { .. }
            | ClientError::OutOfTime { .. } => PlaceOrderError::Unconfirmed {
                order: order.id.clone(),
                reason: error.to_string(),
            },
            other => PlaceOrderError::NotPlaced {
                order: order.id.clone(),
                reason: other.to_string(),
            },
        }
    }
}

/// Why the service did not place an order.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlaceOrderError {
    /// The cluster does not let the service write to the topic. Trying again does not help.
    #[error("the service may not write orders to the topic {topic}")]
    NotAllowed { topic: String },
    /// The cluster did not confirm the order, and may hold it all the same.
    /// Whoever places it again first checks that the cluster does not hold it.
    #[error("the cluster did not confirm the order {order}, and may hold it: {reason}")]
    Unconfirmed { order: String, reason: String },
    /// The cluster does not hold the order.
    #[error("the order {order} was not placed: {reason}")]
    NotPlaced { order: String, reason: String },
}
