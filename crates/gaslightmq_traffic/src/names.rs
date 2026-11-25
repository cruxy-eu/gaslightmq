//! The names that the calls of a traffic named: topics, consumer groups and client IDs.

use std::collections::BTreeMap;

use gaslightmq_call::Call;
use gaslightmq_field::{Field, FieldSource, FieldValue};
use gaslightmq_kernel::{ClientId, GroupId, TopicName};

/// The topics, the consumer groups and the client IDs that the calls of a traffic named.
///
/// A ⌘K sentence says them without the word for their kind: `delay produce orders.* order-ingest`.
/// [`Traffic::record`](crate::Traffic::record) notes the names of every call,
/// and [`Traffic::seen_names`](crate::Traffic::seen_names) gives them.
///
/// A traffic keeps as many names of each kind as its [configuration](crate::TrafficConfiguration::seen_names) says.
/// A new name that finds its kind full pushes out the name that was seen longest ago,
/// so the names that the traffic keeps are those of the clients that are still busy.
///
/// ```
/// use std::sync::Arc;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_traffic::prelude::*;
///
/// // `order-ingest` writes to `orders.eu`, on connection #7.
/// let fields = Fields::new()
///     .with(ConnectionNumber::new(7))
///     .with(ClientId::new("order-ingest").expect("a client ID"))
///     .with(ApiKey::PRODUCE)
///     .with_part("orders.eu/3".parse::<TopicPartition>().expect("a topic partition"))
///     .with(Outcome::Ok);
/// let arrived = CallTiming::arrived(Instant::ZERO);
/// let produce = Call::new(CallId::FIRST, fields, Delivered::Nothing, Explanation::none(), arrived)?;
///
/// let mut traffic = Traffic::new(TrafficConfiguration::new());
/// traffic.record(Arc::new(produce));
///
/// let seen = traffic.seen_names();
/// assert_eq!(seen.clients().map(ClientId::as_str).collect::<Vec<_>>(), ["order-ingest"]);
/// assert_eq!(seen.topics().map(TopicName::as_str).collect::<Vec<_>>(), ["orders.eu"]);
/// assert!(seen.groups().next().is_none());
/// # Ok::<(), CallError>(())
/// ```
#[derive(Debug, Clone)]
pub struct SeenNames {
    /// How many names of each kind are kept.
    limit: usize,
    topics: Recent<TopicName>,
    groups: Recent<GroupId>,
    clients: Recent<ClientId>,
}

impl SeenNames {
    /// No names yet, and room for `limit` names of each kind.
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            limit,
            topics: Recent::new(),
            groups: Recent::new(),
            clients: Recent::new(),
        }
    }

    /// Notes the names that `call` names, as the newest names of their kind.
    pub(crate) fn note(&mut self, call: &Call) {
        if self.limit == 0 {
            return;
        }
        for field in [Field::Topic, Field::Group, Field::ClientId] {
            let Some(values) = call.read(field).present() else {
                continue;
            };
            for value in &values {
                match value {
                    FieldValue::Topic(topic) => self.topics.see(topic, self.limit),
                    FieldValue::Group(group) => self.groups.see(group, self.limit),
                    FieldValue::ClientId(client) => self.clients.see(client, self.limit),
                    _ => {}
                }
            }
        }
    }

    /// The topics, in the order of their names.
    pub fn topics(&self) -> impl Iterator<Item = &TopicName> {
        self.topics.names()
    }

    /// The consumer groups, in the order of their names.
    pub fn groups(&self) -> impl Iterator<Item = &GroupId> {
        self.groups.names()
    }

    /// The client IDs, in their order.
    pub fn clients(&self) -> impl Iterator<Item = &ClientId> {
        self.clients.names()
    }
}

/// The names of one kind, each with the sighting it was last seen at.
#[derive(Debug, Clone)]
struct Recent<T> {
    /// The number of the last sighting of each name.
    last_seen: BTreeMap<T, u64>,
    /// Each name by the number of its last sighting, so the name that was seen longest ago comes first.
    by_sighting: BTreeMap<u64, T>,
    /// How many sightings there were: the number of the latest one.
    sightings: u64,
}

impl<T: Ord + Clone> Recent<T> {
    const fn new() -> Self {
        Self {
            last_seen: BTreeMap::new(),
            by_sighting: BTreeMap::new(),
            sightings: 0,
        }
    }

    /// `name` was seen now. A new name that makes more than `limit` pushes out the one that was seen longest ago.
    fn see(&mut self, name: &T, limit: usize) {
        self.sightings = self.sightings.wrapping_add(1);
        let now = self.sightings;
        if let Some(last) = self.last_seen.get_mut(name) {
            if let Some(known) = self.by_sighting.remove(last) {
                self.by_sighting.insert(now, known);
            }
            *last = now;
            return;
        }
        self.last_seen.insert(name.clone(), now);
        self.by_sighting.insert(now, name.clone());
        if self.last_seen.len() > limit
            && let Some((_, oldest)) = self.by_sighting.pop_first()
        {
            self.last_seen.remove(&oldest);
        }
    }

    /// The names, in their order.
    fn names(&self) -> impl Iterator<Item = &T> {
        self.last_seen.keys()
    }
}
