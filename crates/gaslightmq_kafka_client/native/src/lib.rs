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
//! agrees on API versions with every connection, and retries retriable failures until its time runs out.
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
//! use gaslightmq_kafka_client_native::{
//!     Acks, BootstrapServers, BootstrapServersError, KafkaProducer, ProducerConfiguration,
//! };
//!
//! let bootstrap_servers: BootstrapServers = "localhost:9092".parse()?;
//! let mut configuration = ProducerConfiguration::new(bootstrap_servers);
//! configuration.acks = Acks::Leader;
//! configuration.client.request_timeout = TimeSpan::from_secs(5);
//!
//! // The producer connects when it first sends.
//! let producer = KafkaProducer::new(configuration);
//! assert_eq!(producer.configuration().acks, Acks::Leader);
//! // It was configured without a client ID, so it named itself.
//! assert_eq!(producer.configuration().client.client_id, "producer-1");
//! # Ok::<(), BootstrapServersError>(())
//! ```
//!
//! # Failures
//!
//! Every call of a role returns a `Result` with a [`ClientError`], the error of `gaslightmq_kafka_client_api`,
//! through its own methods and through the traits alike. No call panics.
//! A test can therefore assert on how a client experiences a fault.
//!
//! A failure of the client never looks like an answer of the broker:
//!
//! | What happened | The call returns |
//! |---|---|
//! | The broker answered with an error code that does not pass. | [`ClientError::Refused`], with the error of that code. |
//! | A request got no answer within `request.timeout.ms`. | [`ClientError::NoAnswer`] |
//! | No connection opened, or not within `socket.connection.setup.timeout.ms`. | [`ClientError::Unreachable`] |
//! | The connection ended before the answer arrived. | [`ClientError::ConnectionLost`] |
//! | The answer is no response to the request, or does not hold what was asked for. | [`ClientError::UnreadableAnswer`] |
//! | The records or the request do not encode. | [`ClientError::UnwritableRequest`] |
//! | The broker speaks no version of the API that the client speaks. | [`ClientError::NoCommonVersion`] |
//! | The cluster names no leader for the partition. | [`ClientError::NoLeader`] |
//! | Retriable failures lasted until the time of the call ran out. | [`ClientError::OutOfTime`], with the last of them. |
//!
//! A failure is retriable when it may pass: a refusal that Kafka calls retriable, a request without an answer,
//! a connection that did not open or was lost, and a partition without a leader.
//! A retriable failure that used up the retries of the call is returned as it is.
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
pub use gaslightmq_kafka_client_api::ClientError;
pub use producer::KafkaProducer;
