//! What an engine is configured with.
//!
//! A property is a public field, and its type only holds values that the property allows.
//! A property that a Kafka broker has keeps the name, the meaning and the default of that property,
//! and the documentation of the field names it.
//! A property that Kafka does not have says that it is ours.

use std::fmt;
use std::net::SocketAddr;

use gaslightmq_clock::TimeSpan;
use gaslightmq_environment::Listeners;
use gaslightmq_field::Endpoint;
use gaslightmq_journal::RecentJournalSize;
use gaslightmq_listener::{FrameSizeLimit, ListenerConfiguration};
use gaslightmq_trigger::Seed;

/// What one [`Engine`](crate::Engine) is configured with.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_engine::prelude::*;
/// use gaslightmq_environment::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let listeners = Listeners::new(
///     "0.0.0.0:9092".parse().expect("an address"),
///     "0.0.0.0:19092-19191".parse().expect("a range of ports"),
///     "gaslight.staging".parse().expect("a host"),
/// );
/// let mut configuration = EngineConfiguration::new(listeners.expect("listeners that do not meet"));
/// assert_eq!(configuration.client_context_linger, TimeSpan::from_secs(60));
///
/// configuration.seed = Seed::new(1337);
/// configuration.event_backlog = EventBacklog::new(64)?;
/// # Ok::<(), EventBacklogError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineConfiguration {
    /// Ours. Required.
    ///
    /// Where the proxy listens, as its environment says: the bootstrap endpoint, and the endpoint of every broker.
    /// The backend names the endpoints, and each one opens on the address that these listeners give it.
    /// The broker endpoints that accept connections get their addresses together,
    /// so a range gives its ports in the order of their broker IDs.
    pub listeners: Listeners,
    /// Ours. Default: 0.
    ///
    /// The seed of every die while no scenario is active.
    /// A rule that the engine [arms](crate::ArmedRules::arm) rolls its die from this seed,
    /// so the same seed makes the same matching calls fire in every run.
    pub seed: Seed,
    /// Ours. Default: 60 seconds.
    ///
    /// How long a client context stays after the last session of its client closed.
    /// A client that reconnects within that time finds what its earlier sessions learned, such as its group.
    /// The time runs on the clock of the engine.
    pub client_context_linger: TimeSpan,
    /// Ours. Default: 4096 events.
    ///
    /// How many events the engine keeps for a listener of its [live events](crate::Engine::live_events) that falls
    /// behind. A session never waits for a listener: a listener that is further behind misses the oldest events,
    /// and learns how many.
    /// The engine keeps at least that many: it rounds the number up to the next power of two.
    /// A listener of [every event](crate::Engine::every_event) has no backlog, because the engine drops nothing for it.
    pub event_backlog: EventBacklog,
    /// Ours. Default: 1000 entries.
    ///
    /// How many entries of its [journal](crate::Engine::journal) the engine keeps in memory.
    /// An entry that comes to a full journal pushes the oldest one out of memory.
    /// The journal is what someone sees who looks back at what changed, so it is as large as the stretch they want
    /// to look back on. Every entry it keeps is memory that the engine holds.
    pub recent_journal: RecentJournalSize,
    /// Kafka: `socket.request.max.bytes`. Default: 100 MiB.
    ///
    /// The largest frame a client may send, on every endpoint.
    /// A client that announces a larger one sends no request, and the proxy closes its connection.
    pub socket_request_max_bytes: FrameSizeLimit,
    /// Ours. Default: 100 milliseconds.
    ///
    /// How long an endpoint waits after it failed to accept a connection, before it accepts again.
    /// The wait is real time, whatever clock the engine runs on.
    pub accept_retry_backoff: TimeSpan,
    /// Ours. Default: 1 second.
    ///
    /// How long a connection that the proxy closes waits for the client to close its side.
    /// Until then the client can read everything that was sent to it.
    /// The wait is real time, whatever clock the engine runs on.
    pub close_timeout: TimeSpan,
    /// Ours. Default: 1 second.
    ///
    /// How long the engine waits after an endpoint of its backend did not get a listener, before it tries again:
    /// the address did not take one, or the backend rejected it.
    /// The wait counts from the moment the engine tried, and it runs on the clock of the engine.
    /// A report of the backend is another try at once, whatever is left of the wait.
    pub endpoint_retry_backoff: TimeSpan,
    /// Ours. Default: 5 seconds.
    ///
    /// How long a [stop](crate::Engine::stop) may take, from the moment it begins.
    /// In that time the listeners finish the change of the backend that they follow, every endpoint closes,
    /// and every session ends: it closes its connection, and the backend lets go of it.
    /// A backend that never answers would hold a stop up forever.
    /// After this time the engine lets go of what is left, and the stop returns:
    /// an endpoint that is still open closes a moment later, and nobody waits for a session that has not ended.
    /// The time is real time, whatever clock the engine runs on.
    pub stop_timeout: TimeSpan,
}

impl EngineConfiguration {
    /// The defaults, for an engine whose endpoints listen where `listeners` say.
    ///
    /// The properties of a listener start with the defaults of a listener.
    pub fn new(listeners: Listeners) -> Self {
        let of_a_listener = ListenerConfiguration::new(listeners.bootstrap().into(), Endpoint::Bootstrap);
        Self {
            listeners,
            seed: Seed::new(0),
            client_context_linger: TimeSpan::from_secs(60),
            event_backlog: EventBacklog::default(),
            recent_journal: RecentJournalSize::default(),
            socket_request_max_bytes: of_a_listener.socket_request_max_bytes,
            accept_retry_backoff: of_a_listener.accept_retry_backoff,
            close_timeout: of_a_listener.close_timeout,
            endpoint_retry_backoff: TimeSpan::from_secs(1),
            stop_timeout: TimeSpan::from_secs(5),
        }
    }

    /// What the listener of `endpoint` on `address` is configured with.
    pub(crate) fn listener(&self, address: SocketAddr, endpoint: Endpoint) -> ListenerConfiguration {
        let mut listener = ListenerConfiguration::new(address, endpoint);
        listener.socket_request_max_bytes = self.socket_request_max_bytes;
        listener.accept_retry_backoff = self.accept_retry_backoff;
        listener.close_timeout = self.close_timeout;
        listener
    }
}

/// How many events an engine keeps for a live listener that falls behind.
///
/// A backlog holds at least one event, and at most [`EventBacklog::MAX`].
/// The engine keeps at least that many events: it rounds the number up to the next power of two,
/// and sets the room for them aside when it starts.
/// The room is memory that the engine holds for as long as it runs, whether a listener falls behind or not,
/// which is why a backlog has a largest size.
///
/// ```
/// use gaslightmq_engine::prelude::*;
///
/// let backlog = EventBacklog::new(64)?;
/// assert_eq!(backlog.events(), 64);
/// assert_eq!(backlog.to_string(), "64 events");
/// assert_eq!(EventBacklog::new(0), Err(EventBacklogError::Zero));
/// # Ok::<(), EventBacklogError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventBacklog(usize);

impl EventBacklog {
    /// The largest backlog: 1 048 576 events, which an engine can still set the room aside for.
    pub const MAX: Self = Self(1 << 20);

    /// Check that `events` is a number of events a backlog can hold, and keep it.
    ///
    /// # Errors
    ///
    /// [`EventBacklogError::Zero`] when `events` is zero,
    /// and [`EventBacklogError::TooLarge`] when it is more than [`EventBacklog::MAX`].
    pub const fn new(events: usize) -> Result<Self, EventBacklogError> {
        if events == 0 {
            return Err(EventBacklogError::Zero);
        }
        if events > Self::MAX.0 {
            return Err(EventBacklogError::TooLarge { events });
        }
        Ok(Self(events))
    }

    /// The backlog as a plain number of events.
    pub const fn events(self) -> usize {
        self.0
    }
}

/// 4096 events.
impl Default for EventBacklog {
    fn default() -> Self {
        Self(4096)
    }
}

impl TryFrom<usize> for EventBacklog {
    type Error = EventBacklogError;

    fn try_from(events: usize) -> Result<Self, Self::Error> {
        Self::new(events)
    }
}

impl From<EventBacklog> for usize {
    fn from(backlog: EventBacklog) -> Self {
        backlog.events()
    }
}

/// Writes the number of events: `4096 events`.
impl fmt::Display for EventBacklog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} events", self.0)
    }
}

/// Why a number of events is not an [`EventBacklog`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventBacklogError {
    /// The backlog is zero, and a listener needs room for the event it is about to take.
    #[error("an event backlog holds at least one event")]
    Zero,
    /// The backlog is more than an engine can keep.
    #[error(
        "event backlog of {events} events is more than the {} that an engine can keep",
        EventBacklog::MAX
    )]
    TooLarge { events: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listeners() -> Listeners {
        Listeners::new(
            "0.0.0.0:9092".parse().unwrap(),
            "0.0.0.0:19092-19191".parse().unwrap(),
            "gaslight.staging".parse().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn a_configuration_starts_with_the_defaults() {
        let configuration = EngineConfiguration::new(listeners());

        assert_eq!(configuration.listeners, listeners());
        assert_eq!(configuration.seed, Seed::new(0));
        assert_eq!(configuration.client_context_linger, TimeSpan::from_secs(60));
        assert_eq!(configuration.event_backlog.events(), 4096);
        assert_eq!(configuration.recent_journal.entries(), 1000);
        assert_eq!(configuration.socket_request_max_bytes.bytes(), 100 * 1024 * 1024);
        assert_eq!(configuration.accept_retry_backoff, TimeSpan::from_millis(100));
        assert_eq!(configuration.close_timeout, TimeSpan::from_secs(1));
        assert_eq!(configuration.endpoint_retry_backoff, TimeSpan::from_secs(1));
        assert_eq!(configuration.stop_timeout, TimeSpan::from_secs(5));
    }

    #[test]
    fn every_listener_takes_the_properties_of_the_engine() {
        let mut configuration = EngineConfiguration::new(listeners());
        configuration.socket_request_max_bytes = FrameSizeLimit::new(1024).unwrap();
        configuration.accept_retry_backoff = TimeSpan::from_millis(5);
        configuration.close_timeout = TimeSpan::from_millis(250);
        let address: SocketAddr = "127.0.0.1:19093".parse().unwrap();
        let endpoint: Endpoint = "broker 2".parse().unwrap();

        let listener = configuration.listener(address, endpoint);

        assert_eq!(listener.address, address);
        assert_eq!(listener.endpoint, endpoint);
        assert_eq!(listener.socket_request_max_bytes.bytes(), 1024);
        assert_eq!(listener.accept_retry_backoff, TimeSpan::from_millis(5));
        assert_eq!(listener.close_timeout, TimeSpan::from_millis(250));
    }

    #[test]
    fn a_backlog_holds_at_least_one_event() {
        assert_eq!(EventBacklog::new(1).map(EventBacklog::events), Ok(1));
        assert_eq!(EventBacklog::new(1_048_576), Ok(EventBacklog::MAX));
        assert_eq!(EventBacklog::new(0), Err(EventBacklogError::Zero));
        assert_eq!(
            EventBacklog::new(1_048_577),
            Err(EventBacklogError::TooLarge { events: 1_048_577 })
        );
        assert_eq!(EventBacklog::try_from(0), Err(EventBacklogError::Zero));
        assert_eq!(usize::from(EventBacklog::try_from(512).unwrap()), 512);
    }

    #[test]
    fn each_backlog_error_says_what_is_wrong() {
        assert_eq!(
            EventBacklogError::Zero.to_string(),
            "an event backlog holds at least one event"
        );
        let too_large = EventBacklogError::TooLarge { events: 2_000_000 };
        assert_eq!(
            too_large.to_string(),
            "event backlog of 2000000 events is more than the 1048576 events that an engine can keep"
        );
    }
}
