//! What a producer, a consumer and an admin are configured with.
//!
//! A property is a property of Kafka's own clients: it has that property's name, meaning and default.
//! The documentation of each field names the Kafka property it is.
//! The Java client is the reference.
//! A property that Kafka does not have says that it is ours.
//!
//! A property is a public field, and its type only holds values that the property allows.
//! A time is a [`TimeSpan`], in the whole milliseconds that Kafka counts in.
//! A property of Kafka that is not here names behaviour that this client does not have.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::ClientId;

use crate::address::BootstrapServers;

/// What every role is configured with.
///
/// Each role's configuration holds a copy of its own, so two roles never share a property.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientConfiguration {
    /// Kafka: `bootstrap.servers`. Required.
    ///
    /// The addresses the role asks first, to learn which brokers the cluster has.
    pub bootstrap_servers: BootstrapServers,
    /// Kafka: `client.id`. Default: empty.
    ///
    /// The name the role sends in every request.
    /// A role without one names itself, as Kafka's Java clients do:
    /// `producer-1`, `consumer-1` or `adminclient-1`, with the next number for the next instance of the process.
    pub client_id: ClientId,
    /// Kafka: `request.timeout.ms`. Default: 30 seconds.
    ///
    /// How long one request waits for its response.
    /// The greeting on a new connection waits that long too.
    pub request_timeout: TimeSpan,
    /// Kafka: `socket.connection.setup.timeout.ms`. Default: 10 seconds.
    ///
    /// How long a socket may take to connect.
    /// Kafka's clients make it longer after each failure, up to `socket.connection.setup.timeout.max.ms`.
    /// This client waits the same time for every connection.
    pub socket_connection_setup_timeout: TimeSpan,
    /// Kafka: `retry.backoff.ms`. Default: 100 milliseconds.
    ///
    /// How long a call waits after its first retriable failure, before it tries again.
    /// The wait doubles with every further failure of the call,
    /// up to [`retry_backoff_max`](Self::retry_backoff_max).
    /// Kafka's clients also move each wait by up to 20% at random.
    /// This client does not, so a test sees the same waits on every run.
    pub retry_backoff: TimeSpan,
    /// Kafka: `retry.backoff.max.ms`. Default: 1 second.
    ///
    /// The longest a call waits between two tries.
    /// When [`retry_backoff`](Self::retry_backoff) is longer, every wait is this long.
    pub retry_backoff_max: TimeSpan,
    /// Kafka: `metadata.max.age.ms`. Default: 5 minutes.
    ///
    /// How long the role trusts the leader it learned for a partition.
    /// Kafka's clients ask again in the background when the time is over.
    /// This client asks again when the next request needs the leader.
    pub metadata_max_age: TimeSpan,
}

impl ClientConfiguration {
    /// Kafka's defaults, for a role that starts from `bootstrap_servers`.
    pub fn new(bootstrap_servers: impl Into<BootstrapServers>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: no_client_id(),
            request_timeout: TimeSpan::from_secs(30),
            socket_connection_setup_timeout: TimeSpan::from_secs(10),
            retry_backoff: TimeSpan::from_millis(100),
            retry_backoff_max: TimeSpan::from_secs(1),
            metadata_max_age: TimeSpan::from_secs(5 * 60),
        }
    }
}

/// The empty client ID, which is Kafka's default.
fn no_client_id() -> ClientId {
    ClientId::new("").expect("a client ID may be empty")
}

/// The names of the instances of one role that are configured without a client ID.
///
/// Kafka's Java clients name such an instance after its role and count on: `producer-1`, `producer-2`.
pub(crate) struct RoleNames {
    role: &'static str,
    named: AtomicU32,
}

impl RoleNames {
    /// The names of `role`, such as `producer`.
    pub const fn new(role: &'static str) -> Self {
        Self {
            role,
            named: AtomicU32::new(0),
        }
    }

    /// The client ID an instance sends: the one it is configured with, or the next name of its role.
    pub fn client_id(&self, configured: &ClientId) -> ClientId {
        if !configured.as_str().is_empty() {
            return configured.clone();
        }
        let number = self.named.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        ClientId::new(format!("{}-{number}", self.role))
            .expect("the name of a role and a number make a short client ID")
    }
}

/// The most retries Kafka allows, and its default: only the time of a call ends the retries.
const RETRIES_WITHOUT_END: u32 = 2_147_483_647;

/// How many replicas have a record before the broker confirms it.
///
/// Kafka: `acks`, with the values `all` (or `-1`) and `1`.
/// Kafka's `acks=0` is not here: the broker sends no response to it, and a producer returns the offset from one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Acks {
    /// Every in-sync replica. Kafka writes `all` or `-1`.
    #[default]
    All,
    /// The leader alone. Kafka writes `1`.
    Leader,
}

impl Acks {
    /// The number a Produce request carries.
    pub(crate) fn to_wire(self) -> i16 {
        match self {
            Self::All => -1,
            Self::Leader => 1,
        }
    }
}

/// What a [`KafkaProducer`](crate::KafkaProducer) is configured with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProducerConfiguration {
    /// The producer's own copy of the properties every role has.
    pub client: ClientConfiguration,
    /// Kafka: `acks`. Default: `all`.
    ///
    /// How many replicas have a record before the broker confirms it.
    pub acks: Acks,
    /// Kafka: `delivery.timeout.ms`. Default: 2 minutes.
    ///
    /// How long a send may take, with every retry.
    pub delivery_timeout: TimeSpan,
    /// Kafka: `retries`. Default: 2147483647.
    ///
    /// How often a send tries again after a retriable failure.
    /// A send that runs out of retries fails with its last error.
    /// The delivery timeout ends a send that has retries left.
    pub retries: u32,
}

impl ProducerConfiguration {
    /// Kafka's defaults, for a producer that starts from `bootstrap_servers`.
    pub fn new(bootstrap_servers: impl Into<BootstrapServers>) -> Self {
        Self {
            client: ClientConfiguration::new(bootstrap_servers),
            acks: Acks::All,
            delivery_timeout: TimeSpan::from_secs(2 * 60),
            retries: RETRIES_WITHOUT_END,
        }
    }
}

/// Which records a consumer reads from a partition that holds transactions.
///
/// Kafka: `isolation.level`, with the values `read_uncommitted` and `read_committed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IsolationLevel {
    /// Every record, also one of a transaction that is open or aborted.
    #[default]
    ReadUncommitted,
    /// Only records outside a transaction and records of a committed one.
    ReadCommitted,
}

impl IsolationLevel {
    /// The number a Fetch and a `ListOffsets` request carry.
    pub(crate) fn to_wire(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

/// The name of one way to divide partitions over the members of a group, such as `range`.
///
/// It is the name that travels in `JoinGroup`, and not the name of a Java class.
/// The name is never empty, and it has at most 32,767 bytes, the longest string a Kafka message carries.
///
/// ```
/// use gaslightmq_kafka_client_native::AssignorName;
///
/// let assignor: AssignorName = "roundrobin".parse()?;
/// assert_eq!(assignor.as_str(), "roundrobin");
/// # Ok::<(), gaslightmq_kafka_client_native::AssignorNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssignorName(String);

impl AssignorName {
    /// The most bytes an assignor name has.
    pub const MAX_LEN: usize = 32_767;

    /// Check `name` against the rule for an assignor name, and keep it.
    ///
    /// # Errors
    ///
    /// The [`AssignorNameError`] that says how `name` breaks the rule.
    pub fn new(name: impl Into<String>) -> Result<Self, AssignorNameError> {
        let name = name.into();
        if name.is_empty() {
            return Err(AssignorNameError::Empty);
        }
        if name.len() > Self::MAX_LEN {
            return Err(AssignorNameError::TooLong { len: name.len() });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AssignorName {
    type Err = AssignorNameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for AssignorName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a text is not an [`AssignorName`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AssignorNameError {
    /// The name has no characters.
    Empty,
    /// The name has more bytes than a Kafka message carries in one string.
    TooLong { len: usize },
}

impl fmt::Display for AssignorNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("assignor name cannot be empty"),
            Self::TooLong { len } => write!(
                f,
                "assignor name has {len} bytes, and the most is {}",
                AssignorName::MAX_LEN
            ),
        }
    }
}

impl std::error::Error for AssignorNameError {}

/// The assignors a consumer offers when it joins a group, the one it likes best first.
///
/// Kafka: `partition.assignment.strategy`.
/// Kafka's Java client names the classes of its assignors.
/// This list holds the names those assignors travel under, with commas between them in the text form.
/// There is always at least one.
///
/// ```
/// use gaslightmq_kafka_client_native::PartitionAssignmentStrategy;
///
/// let strategy: PartitionAssignmentStrategy = "roundrobin, range".parse()?;
/// assert_eq!(strategy.to_string(), "roundrobin,range");
/// assert_eq!(PartitionAssignmentStrategy::default().to_string(), "range,cooperative-sticky");
/// # Ok::<(), gaslightmq_kafka_client_native::PartitionAssignmentStrategyError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignmentStrategy {
    first: AssignorName,
    others: Vec<AssignorName>,
}

impl PartitionAssignmentStrategy {
    /// Keep `assignors`, in their order.
    ///
    /// # Errors
    ///
    /// [`PartitionAssignmentStrategyError::Empty`] when there is no assignor.
    pub fn new(assignors: impl IntoIterator<Item = AssignorName>) -> Result<Self, PartitionAssignmentStrategyError> {
        let mut assignors = assignors.into_iter();
        let first = assignors.next().ok_or(PartitionAssignmentStrategyError::Empty)?;
        Ok(Self {
            first,
            others: assignors.collect(),
        })
    }

    /// The assignors, the one the consumer likes best first.
    pub fn iter(&self) -> impl Iterator<Item = &AssignorName> {
        std::iter::once(&self.first).chain(&self.others)
    }
}

/// `range` and then `cooperative-sticky`, the assignors Kafka's Java consumer offers.
impl Default for PartitionAssignmentStrategy {
    fn default() -> Self {
        Self {
            first: AssignorName("range".to_owned()),
            others: vec![AssignorName("cooperative-sticky".to_owned())],
        }
    }
}

impl From<AssignorName> for PartitionAssignmentStrategy {
    fn from(assignor: AssignorName) -> Self {
        Self {
            first: assignor,
            others: Vec::new(),
        }
    }
}

impl FromStr for PartitionAssignmentStrategy {
    type Err = PartitionAssignmentStrategyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let assignors = text
            .split(',')
            .map(str::trim)
            .filter(|assignor| !assignor.is_empty())
            .map(|assignor| {
                assignor
                    .parse()
                    .map_err(|error| PartitionAssignmentStrategyError::InvalidAssignor {
                        input: text.to_owned(),
                        error,
                    })
            })
            .collect::<Result<Vec<AssignorName>, _>>()?;
        Self::new(assignors)
    }
}

impl fmt::Display for PartitionAssignmentStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.first)?;
        self.others.iter().try_for_each(|assignor| write!(f, ",{assignor}"))
    }
}

/// Why a list is not a [`PartitionAssignmentStrategy`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionAssignmentStrategyError {
    /// The list holds no assignor.
    Empty,
    /// One name of the text `input` is not an assignor name.
    InvalidAssignor { input: String, error: AssignorNameError },
}

impl fmt::Display for PartitionAssignmentStrategyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("partition assignment strategy needs at least one assignor"),
            Self::InvalidAssignor { input, error } => {
                write!(f, "partition assignment strategy `{input}`: {error}")
            }
        }
    }
}

/// The message says what is wrong with the name, so the error has no source.
impl std::error::Error for PartitionAssignmentStrategyError {}

/// What a [`KafkaConsumer`](crate::KafkaConsumer) is configured with.
///
/// A request carries a time in whole milliseconds and a size in bytes, each as a signed 32-bit number.
/// A property that is larger travels as the largest such number.
///
/// Kafka's consumer has no `retries`: a call tries again until its time is over.
/// It has a `group.id`, and this consumer does not: the caller names the group when it joins one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConsumerConfiguration {
    /// The consumer's own copy of the properties every role has.
    pub client: ClientConfiguration,
    /// Kafka: `default.api.timeout.ms`. Default: 1 minute.
    ///
    /// How long a call may take, with every retry.
    pub default_api_timeout: TimeSpan,
    /// Kafka: `fetch.max.wait.ms`. Default: 500 milliseconds.
    ///
    /// How long the broker holds a Fetch that has fewer bytes than [`fetch_min_bytes`](Self::fetch_min_bytes).
    pub fetch_max_wait: TimeSpan,
    /// Kafka: `fetch.min.bytes`. Default: 1.
    ///
    /// How many bytes the broker waits for before it answers a Fetch.
    pub fetch_min_bytes: u32,
    /// Kafka: `fetch.max.bytes`. Default: 52428800 (50 MiB).
    ///
    /// The most bytes one Fetch response carries.
    pub fetch_max_bytes: u32,
    /// Kafka: `max.partition.fetch.bytes`. Default: 1048576 (1 MiB).
    ///
    /// The most bytes one Fetch response carries of one partition.
    pub max_partition_fetch_bytes: u32,
    /// Kafka: `isolation.level`. Default: `read_uncommitted`.
    ///
    /// Which records the consumer reads from a partition that holds transactions.
    pub isolation_level: IsolationLevel,
    /// Kafka: `allow.auto.create.topics`. Default: true.
    ///
    /// Whether the broker may create a topic that it does not have, when the consumer asks about the topic.
    /// The broker only does so when its own `auto.create.topics.enable` allows it.
    pub allow_auto_create_topics: bool,
    /// Kafka: `session.timeout.ms`. Default: 45 seconds.
    ///
    /// How long the coordinator waits for a heartbeat before it removes the consumer from its group.
    pub session_timeout: TimeSpan,
    /// Kafka: `max.poll.interval.ms`. Default: 5 minutes.
    ///
    /// The consumer sends it as the rebalance timeout when it joins a group:
    /// how long the coordinator waits for every member to join again when the group rebalances.
    /// A `JoinGroup` waits that long for its response, on top of the request timeout.
    /// This consumer has no poll loop, so the property has no other effect.
    pub max_poll_interval: TimeSpan,
    /// Kafka: `partition.assignment.strategy`. Default: `range`, then `cooperative-sticky`.
    ///
    /// The assignors the consumer offers when it joins a group.
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
}

impl ConsumerConfiguration {
    /// Kafka's defaults, for a consumer that starts from `bootstrap_servers`.
    pub fn new(bootstrap_servers: impl Into<BootstrapServers>) -> Self {
        Self {
            client: ClientConfiguration::new(bootstrap_servers),
            default_api_timeout: TimeSpan::from_secs(60),
            fetch_max_wait: TimeSpan::from_millis(500),
            fetch_min_bytes: 1,
            fetch_max_bytes: 50 * 1024 * 1024,
            max_partition_fetch_bytes: 1024 * 1024,
            isolation_level: IsolationLevel::ReadUncommitted,
            allow_auto_create_topics: true,
            session_timeout: TimeSpan::from_secs(45),
            max_poll_interval: TimeSpan::from_secs(5 * 60),
            partition_assignment_strategy: PartitionAssignmentStrategy::default(),
        }
    }
}

/// What a [`KafkaAdmin`](crate::KafkaAdmin) is configured with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AdminConfiguration {
    /// The admin's own copy of the properties every role has.
    pub client: ClientConfiguration,
    /// Kafka: `default.api.timeout.ms`. Default: 1 minute.
    ///
    /// How long a call may take, with every retry.
    pub default_api_timeout: TimeSpan,
    /// Kafka: `retries`. Default: 2147483647.
    ///
    /// How often a call tries again after a retriable failure.
    /// A call that runs out of retries fails with its last error.
    /// The default API timeout ends a call that has retries left.
    pub retries: u32,
}

impl AdminConfiguration {
    /// Kafka's defaults, for an admin that starts from `bootstrap_servers`.
    pub fn new(bootstrap_servers: impl Into<BootstrapServers>) -> Self {
        Self {
            client: ClientConfiguration::new(bootstrap_servers),
            default_api_timeout: TimeSpan::from_secs(60),
            retries: RETRIES_WITHOUT_END,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    fn bootstrap() -> BootstrapServers {
        "localhost:9092".parse().unwrap()
    }

    #[test]
    fn a_role_without_a_client_id_names_itself_and_counts_on() {
        let names = RoleNames::new("producer");
        let configured = ClientId::new("order-ingest").unwrap();

        assert_eq!(names.client_id(&no_client_id()), "producer-1");
        assert_eq!(names.client_id(&configured), "order-ingest");
        assert_eq!(names.client_id(&no_client_id()), "producer-2");
    }

    #[test]
    fn a_property_of_one_role_leaves_the_other_roles_alone() {
        let mut producer = ProducerConfiguration::new(bootstrap());
        let consumer = ConsumerConfiguration::new(bootstrap());

        producer.client.request_timeout = TimeSpan::from_secs(1);

        assert_eq!(consumer.client.request_timeout, TimeSpan::from_secs(30));
    }

    #[test]
    fn the_defaults_are_those_of_kafkas_clients() {
        let producer = ProducerConfiguration::new(bootstrap());
        assert_eq!(producer.acks, Acks::All);
        assert_eq!(producer.delivery_timeout, TimeSpan::from_millis(120_000));
        assert_eq!(producer.retries, 2_147_483_647);
        assert_eq!(producer.client.client_id, "");
        assert_eq!(producer.client.request_timeout, TimeSpan::from_millis(30_000));
        assert_eq!(
            producer.client.socket_connection_setup_timeout,
            TimeSpan::from_millis(10_000)
        );
        assert_eq!(producer.client.retry_backoff, TimeSpan::from_millis(100));
        assert_eq!(producer.client.retry_backoff_max, TimeSpan::from_millis(1_000));
        assert_eq!(producer.client.metadata_max_age, TimeSpan::from_millis(300_000));

        let consumer = ConsumerConfiguration::new(bootstrap());
        assert_eq!(consumer.client, producer.client);
        assert_eq!(consumer.default_api_timeout, TimeSpan::from_millis(60_000));
        assert_eq!(consumer.fetch_max_wait, TimeSpan::from_millis(500));
        assert_eq!(consumer.fetch_min_bytes, 1);
        assert_eq!(consumer.fetch_max_bytes, 52_428_800);
        assert_eq!(consumer.max_partition_fetch_bytes, 1_048_576);
        assert_eq!(consumer.isolation_level, IsolationLevel::ReadUncommitted);
        assert!(consumer.allow_auto_create_topics);
        assert_eq!(consumer.session_timeout, TimeSpan::from_millis(45_000));
        assert_eq!(consumer.max_poll_interval, TimeSpan::from_millis(300_000));
        assert_eq!(
            consumer.partition_assignment_strategy.to_string(),
            "range,cooperative-sticky"
        );

        let admin = AdminConfiguration::new(bootstrap());
        assert_eq!(admin.client, producer.client);
        assert_eq!(admin.default_api_timeout, TimeSpan::from_millis(60_000));
        assert_eq!(admin.retries, 2_147_483_647);
    }

    #[test]
    fn acks_and_isolation_levels_travel_as_kafkas_numbers() {
        assert_eq!(Acks::All.to_wire(), -1);
        assert_eq!(Acks::Leader.to_wire(), 1);
        assert_eq!(IsolationLevel::ReadUncommitted.to_wire(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.to_wire(), 1);
    }

    #[test]
    fn an_assignor_name_has_one_to_many_bytes() {
        assert_eq!(AssignorName::new(""), Err(AssignorNameError::Empty));
        assert_eq!(
            AssignorName::new("a".repeat(32_768)),
            Err(AssignorNameError::TooLong { len: 32_768 })
        );
        assert_eq!(
            AssignorName::new("a".repeat(32_768)).unwrap_err().to_string(),
            "assignor name has 32768 bytes, and the most is 32767"
        );
        assert_eq!(AssignorName::new("sticky").unwrap().to_string(), "sticky");
    }

    #[test]
    fn a_strategy_keeps_its_assignors_in_their_order() {
        let strategy: PartitionAssignmentStrategy = "sticky, range".parse().unwrap();

        let names: Vec<&str> = strategy.iter().map(AssignorName::as_str).collect();
        assert_eq!(names, ["sticky", "range"]);
        assert_eq!(
            PartitionAssignmentStrategy::from(AssignorName::new("range").unwrap()).to_string(),
            "range"
        );
    }

    #[test]
    fn a_strategy_needs_one_assignor() {
        assert_eq!(
            PartitionAssignmentStrategy::new([]),
            Err(PartitionAssignmentStrategyError::Empty)
        );
        assert_eq!(
            " , ".parse::<PartitionAssignmentStrategy>(),
            Err(PartitionAssignmentStrategyError::Empty)
        );
    }

    #[test]
    fn a_strategy_names_the_assignor_that_is_wrong_once() {
        let text = format!("range,{}", "a".repeat(32_768));

        let error = text.parse::<PartitionAssignmentStrategy>().unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("partition assignment strategy `{text}`: assignor name has 32768 bytes, and the most is 32767")
        );
        assert!(error.source().is_none());
    }
}
