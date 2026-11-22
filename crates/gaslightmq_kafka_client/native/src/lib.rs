//! Our own Kafka client, written against the traits in `gaslightmq_kafka_client_api`.
//!
//! | Type | Is | Implements |
//! |---|---|---|
//! | [`KafkaProducer`] | A producer: it writes records to a partition. | [`Producer`] |
//! | [`KafkaConsumer`] | A consumer: it reads records, and it is a member of consumer groups. | [`Consumer`], [`ProtocolConsumer`] |
//! | [`KafkaAdmin`] | An admin: it asks the cluster how it is set up. | [`Admin`] |
//!
//! Each of them follows the cluster as an application's client does:
//! it learns brokers and partition leaders from Metadata, sends each request to the right broker,
//! agrees on API versions with every connection, and retries retriable errors until its time runs out.
//!
//! # Nothing is shared
//!
//! Every producer, consumer and admin has its own connections, its own view of the cluster,
//! its own state and its own `client.id`.
//! A Fetch that waits for records holds up neither a Produce nor a Heartbeat.
//! A fault on a connection hits one role.
//! A Metadata response that one role receives moves the leaders of that role alone.
//!
//! # Configuration
//!
//! Each role has a configuration of its own:
//! [`ProducerConfiguration`], [`ConsumerConfiguration`] and [`AdminConfiguration`].
//! Each holds its own copy of [`ClientConfiguration`], the properties every role has.
//!
//! A property is a property of Kafka's own clients, with that property's name, meaning and default:
//! `request.timeout.ms` is the field `request_timeout`, and it is 30 seconds until someone sets it.
//! The documentation of each field names its Kafka property.
//! A property of Kafka that is missing names behaviour this client does not have.
//!
//! ```
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_kafka_client_native::{Acks, BootstrapServers, KafkaProducer, ProducerConfiguration};
//!
//! let bootstrap_servers: BootstrapServers = "localhost:9092".parse()?;
//! let mut configuration = ProducerConfiguration::new(bootstrap_servers);
//! configuration.acks = Acks::Leader;
//! configuration.client.client_id = "order-ingest".parse()?;
//! configuration.client.request_timeout = TimeSpan::from_secs(5);
//!
//! // The producer connects when it first sends.
//! let producer = KafkaProducer::new(configuration);
//! assert_eq!(producer.configuration().client.client_id, "order-ingest");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Failures
//!
//! The methods of a role itself report every failure as a [`ClientError`],
//! so a test can assert on how a client experiences a fault.
//! The traits have no error. [`Producer`], [`Admin`] and the group methods of [`ProtocolConsumer`]
//! carry the failure in their result, as the closest Kafka error.
//! [`Consumer`] panics on a failure.
//!
//! # Inside
//!
//! What a client decides is plain code in the `logic` module: no async, no socket and no clock.
//! The `io` module owns the sockets and the timers, and carries out what the logic decides.
//!
//! [`Producer`]: gaslightmq_kafka_client_api::Producer
//! [`Consumer`]: gaslightmq_kafka_client_api::Consumer
//! [`ProtocolConsumer`]: gaslightmq_kafka_client_api::ProtocolConsumer
//! [`Admin`]: gaslightmq_kafka_client_api::Admin

mod address;
mod admin;
mod configuration;
mod consumer;
mod error;
mod io;
mod logic;
mod producer;

pub use address::{BootstrapServers, BootstrapServersError, BrokerAddress, BrokerAddressError};
pub use admin::KafkaAdmin;
pub use configuration::{
    Acks, AdminConfiguration, AssignorName, AssignorNameError, ClientConfiguration, ConsumerConfiguration,
    IsolationLevel, PartitionAssignmentStrategy, PartitionAssignmentStrategyError, ProducerConfiguration,
};
pub use consumer::{JoinedGroup, KafkaConsumer};
pub use error::ClientError;
pub use producer::KafkaProducer;
