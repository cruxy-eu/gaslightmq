//! The client context: what the sessions of one client instance learned together.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use gaslightmq_clock::{Duration, Instant};
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

/// One client instance: a `client.id` with the host it connects from.
///
/// A connection number only lives as long as its connection.
/// The pair of `client.id` and `host` stays the same when a client reconnects,
/// and every connection of one Kafka client has it.
/// Sessions with the same pair share one [client context](ClientContexts).
///
/// A request can carry no `client.id` at all.
/// The clients of one host that send none are one client instance.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let host = "10.0.4.27".parse().expect("a client host");
/// let client = ClientInstance::new(Some(ClientId::new("order-ingest")?), host);
/// assert_eq!(client.to_string(), "order-ingest from 10.0.4.27");
/// # Ok::<(), ClientIdError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientInstance {
    client_id: Option<ClientId>,
    host: ClientHost,
}

impl ClientInstance {
    /// The client instance with that `client.id`, or without one, on that host.
    pub const fn new(client_id: Option<ClientId>, host: ClientHost) -> Self {
        Self { client_id, host }
    }

    /// The `client.id` the client sends, or `None` for a client that sends none.
    pub const fn client_id(&self) -> Option<&ClientId> {
        self.client_id.as_ref()
    }

    /// The IP address the client connects from.
    pub const fn host(&self) -> ClientHost {
        self.host
    }
}

/// Writes the `client.id` and the host: `order-ingest from 10.0.4.27`.
/// A client that sends no `client.id` is `no client.id from 10.0.4.27`.
impl fmt::Display for ClientInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.client_id {
            Some(client_id) => write!(f, "{client_id} from {}", self.host),
            None => write!(f, "no client.id from {}", self.host),
        }
    }
}

/// The values of one learned field, and the same values as a selector reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Learned<V> {
    values: BTreeSet<V>,
    /// The values with the client context as their origin, or `None` when there is none.
    as_field: Option<FieldValues>,
}

impl<V> Default for Learned<V> {
    fn default() -> Self {
        Self {
            values: BTreeSet::new(),
            as_field: None,
        }
    }
}

impl<V: LearnedValue + Clone + Ord> Learned<V> {
    /// These values and `value`, or `None` when `value` is one of them already.
    fn adding(&self, value: &V) -> Option<Self> {
        if self.values.contains(value) {
            return None;
        }
        let mut values = self.values.clone();
        values.insert(value.clone());
        let as_field = FieldValues::learned(values.iter().cloned());
        Some(Self { values, as_field })
    }
}

/// What a client context knows at one moment: the values of the learned fields.
///
/// Most connections of a Kafka client never say which group or transaction the client belongs to.
/// A consumer names its group on the connection to its coordinator, and fetches on other connections.
/// The context remembers what any session of the client learned:
///
/// | Learned field | Type |
/// |---|---|
/// | `group` | `GroupId` |
/// | `member.id` | `MemberId` |
/// | `group.instance.id` | `GroupInstanceId` |
/// | `transactional.id` | `TransactionalId` |
///
/// A field has several values when the client named several:
/// a client that runs two consumers under one `client.id` has two groups.
/// A context only adds values, and it forgets them all when it ends.
///
/// The values are a snapshot.
/// A call reads them once and works with them from start to finish,
/// whatever another session of the same client learns in the meantime.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let learned = LearnedFields::new()
///     .with(GroupId::new("payments")?)
///     .with(GroupId::new("analytics-etl")?);
///
/// let groups = learned.read(Field::Group).present().expect("the client named two groups");
/// assert_eq!(groups.to_string(), "analytics-etl, payments");
/// assert_eq!(groups.origin(), Origin::ClientContext);
/// assert_eq!(learned.read(Field::TransactionalId), Reading::Absent);
/// # Ok::<(), GroupIdError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LearnedFields {
    groups: Learned<GroupId>,
    member_ids: Learned<MemberId>,
    group_instance_ids: Learned<GroupInstanceId>,
    transactional_ids: Learned<TransactionalId>,
}

impl LearnedFields {
    /// What a context knows before any session learned something: no value of any field.
    pub fn new() -> Self {
        Self::default()
    }

    /// These values and `value`.
    #[must_use]
    pub fn with(self, value: impl LearnedValue) -> Self {
        self.adding(&value.into()).unwrap_or(self)
    }

    /// The values of `field` as a selector reads them, with the client context as their origin.
    ///
    /// A field that the context has no value of is absent, and so is every field that is not a learned field.
    pub fn read(&self, field: Field) -> Reading<FieldValues> {
        let values = match field {
            Field::Group => &self.groups.as_field,
            Field::MemberId => &self.member_ids.as_field,
            Field::GroupInstanceId => &self.group_instance_ids.as_field,
            Field::TransactionalId => &self.transactional_ids.as_field,
            _ => &None,
        };
        values.clone().into()
    }

    /// The groups the client named, in order.
    pub fn groups(&self) -> impl ExactSizeIterator<Item = &GroupId> {
        self.groups.values.iter()
    }

    /// The member IDs the client got when it joined a group, in order.
    pub fn member_ids(&self) -> impl ExactSizeIterator<Item = &MemberId> {
        self.member_ids.values.iter()
    }

    /// The `group.instance.id`s the client named as a static member, in order.
    pub fn group_instance_ids(&self) -> impl ExactSizeIterator<Item = &GroupInstanceId> {
        self.group_instance_ids.values.iter()
    }

    /// The `transactional.id`s the client named, in order.
    pub fn transactional_ids(&self) -> impl ExactSizeIterator<Item = &TransactionalId> {
        self.transactional_ids.values.iter()
    }

    /// Whether the context has no value of any field.
    pub fn is_empty(&self) -> bool {
        self.groups.values.is_empty()
            && self.member_ids.values.is_empty()
            && self.group_instance_ids.values.is_empty()
            && self.transactional_ids.values.is_empty()
    }

    /// Adds every value to `fields` as a value that the client context provides.
    ///
    /// A field that `fields` has a value of already keeps that value alone,
    /// because a request that carries a learned field provides it alone.
    #[must_use]
    pub fn provide(&self, fields: Fields) -> Fields {
        let fields = self.groups().cloned().fold(fields, Fields::with_learned);
        let fields = self.member_ids().cloned().fold(fields, Fields::with_learned);
        let fields = self.group_instance_ids().cloned().fold(fields, Fields::with_learned);
        self.transactional_ids().cloned().fold(fields, Fields::with_learned)
    }

    /// These values and `value`, or `None` when `value` is one of them already.
    fn adding(&self, value: &FieldValue) -> Option<Self> {
        let mut next = self.clone();
        match value {
            FieldValue::Group(group) => next.groups = self.groups.adding(group)?,
            FieldValue::MemberId(member_id) => next.member_ids = self.member_ids.adding(member_id)?,
            FieldValue::GroupInstanceId(instance_id) => {
                next.group_instance_ids = self.group_instance_ids.adding(instance_id)?;
            }
            FieldValue::TransactionalId(transactional_id) => {
                next.transactional_ids = self.transactional_ids.adding(transactional_id)?;
            }
            // Only the four types above are learned values, so no other value gets here.
            _ => return None,
        }
        Some(next)
    }
}

/// The low half of a context's state: how many sessions share the context right now.
const SESSIONS: u64 = 0xffff_ffff;
/// What one session that joins adds to a context's state: one session, and one join.
const JOIN: u64 = (1 << 32) | 1;
/// The state of a context that ended. No session joins it any more.
const ENDED: u64 = u64::MAX;

/// One client context, which the registry and every session of the client hold together.
///
/// Nothing in it is behind a lock.
/// A session swaps in a new snapshot of the learned fields, and counts itself in and out with atomic numbers.
#[derive(Debug)]
struct Shared {
    client: ClientInstance,
    learned: ArcSwap<LearnedFields>,
    /// The sessions that share the context in the low half, and how often a session joined in the high half.
    /// The joins tell a context that stayed idle from one that a session joined and left in the meantime.
    state: AtomicU64,
    /// The latest instant at which a session said that it left, in nanoseconds.
    last_left_at: AtomicU64,
    /// Whether a session went away without saying when.
    left_without_instant: AtomicBool,
}

impl Shared {
    /// A new context, which the session that makes it has joined already.
    fn joined(client: ClientInstance) -> Self {
        Self {
            client,
            learned: ArcSwap::default(),
            state: AtomicU64::new(JOIN),
            last_left_at: AtomicU64::new(0),
            left_without_instant: AtomicBool::new(false),
        }
    }

    /// Counts one more session in, unless the context ended.
    fn join(&self) -> bool {
        self.state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state != ENDED).then(|| state.wrapping_add(JOIN))
            })
            .is_ok()
    }

    fn has_ended(&self) -> bool {
        self.state.load(Ordering::Acquire) == ENDED
    }

    fn sessions(&self) -> u64 {
        match self.state.load(Ordering::Acquire) {
            ENDED => 0,
            state => state & SESSIONS,
        }
    }

    /// Ends the context when no session has shared it for `linger`.
    fn end_when_idle(&self, now: Instant, linger: Duration) -> bool {
        let state = self.state.load(Ordering::Acquire);
        if state == ENDED || state & SESSIONS != 0 {
            return false;
        }
        if self.left_without_instant.swap(false, Ordering::AcqRel) {
            // A session went away without saying when, so the context is idle from now on at the latest.
            self.last_left_at.fetch_max(now.as_nanos(), Ordering::AcqRel);
        }
        let idle_since = Instant::from_nanos(self.last_left_at.load(Ordering::Acquire));
        if now.duration_since(idle_since) < linger {
            return false;
        }
        // A session that joined in the meantime changed the state, even when it left again.
        self.state
            .compare_exchange(state, ENDED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// Every client context of one proxy: the handle through which sessions share what they learn.
///
/// The engine makes one of these, keeps it, and hands a clone to every session.
/// A clone is a second handle to the same contexts.
///
/// - A session [joins](Self::join) the context of its client instance when it is identified.
///   The first session of a client instance starts the context.
/// - The engine calls [`expire`](Self::expire) now and then, with the current instant.
///   A context ends when no session has shared it for the time the contexts linger.
///   A client that reconnects within that time finds what its earlier sessions learned.
///
/// No method waits: there is no lock, and no IO.
/// Sessions on different threads read, learn, join and leave at the same time,
/// and none of them holds another one up.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let contexts = ClientContexts::new(Duration::from_secs(60));
/// let host = "10.0.4.27".parse().expect("a client host");
/// let client = ClientInstance::new(Some(ClientId::new("analytics").expect("a client ID")), host);
///
/// // The consumer names its group on the connection to its coordinator.
/// let coordinator = contexts.join(&client);
/// assert!(coordinator.learn(GroupId::new("analytics-etl")?));
///
/// // It fetches on another connection, which shares the context.
/// let fetcher = contexts.join(&client);
/// assert_eq!(fetcher.learned().read(Field::Group).present().map(|groups| groups.to_string()), Some("analytics-etl".to_owned()));
///
/// // Both connections close. The context lingers, and then it ends.
/// let closed_at = Instant::ZERO + Duration::from_secs(5);
/// coordinator.leave(closed_at);
/// fetcher.leave(closed_at);
/// assert!(contexts.expire(closed_at + Duration::from_secs(59)).is_empty());
/// assert_eq!(contexts.expire(closed_at + Duration::from_secs(60)), [client]);
/// assert!(contexts.is_empty());
/// # Ok::<(), GroupIdError>(())
/// ```
#[derive(Debug, Clone)]
pub struct ClientContexts(Arc<Registry>);

#[derive(Debug)]
struct Registry {
    linger: Duration,
    /// The contexts by client instance. A change swaps in a new map.
    contexts: ArcSwap<BTreeMap<ClientInstance, Arc<Shared>>>,
}

impl ClientContexts {
    /// No context yet. A context ends when no session has shared it for `linger`.
    pub fn new(linger: Duration) -> Self {
        Self(Arc::new(Registry {
            linger,
            contexts: ArcSwap::default(),
        }))
    }

    /// How long a context stays after its last session left.
    pub fn linger(&self) -> Duration {
        self.0.linger
    }

    /// A session joins the context of `client`, and starts it when the client instance has none.
    ///
    /// The [`ClientContext`] it returns is that session's share of the context.
    /// The session [leaves](ClientContext::leave) when it closes.
    pub fn join(&self, client: &ClientInstance) -> ClientContext {
        loop {
            if let Some(shared) = self.0.contexts.load().get(client)
                && shared.join()
            {
                return ClientContext::of(Arc::clone(shared));
            }
            // The client instance has no context, or one that ended. A new one takes its place.
            let started = Arc::new(Shared::joined(client.clone()));
            self.0.contexts.rcu(|contexts| match contexts.get(client) {
                Some(running) if !running.has_ended() => Arc::clone(contexts),
                _ => {
                    let mut next = BTreeMap::clone(contexts);
                    next.insert(client.clone(), Arc::clone(&started));
                    Arc::new(next)
                }
            });
            // A context with a session never ends, so the new one is still there when it took the place.
            let took_the_place = self
                .0
                .contexts
                .load()
                .get(client)
                .is_some_and(|shared| Arc::ptr_eq(shared, &started));
            if took_the_place {
                return ClientContext::of(started);
            }
            // Another session started a context at the same moment. Join that one.
        }
    }

    /// What the context of `client` knows at this moment, or `None` when the client instance has no context.
    pub fn learned(&self, client: &ClientInstance) -> Option<Arc<LearnedFields>> {
        let contexts = self.0.contexts.load();
        let shared = contexts.get(client).filter(|shared| !shared.has_ended())?;
        Some(shared.learned.load_full())
    }

    /// How many sessions share the context of `client` at this moment.
    pub fn sessions(&self, client: &ClientInstance) -> u64 {
        self.0.contexts.load().get(client).map_or(0, |shared| shared.sessions())
    }

    /// Ends every context that no session has shared for the time the contexts linger,
    /// and returns the client instances they belonged to, in order.
    ///
    /// A session that closes says when it [leaves](ClientContext::leave), and the time counts from then.
    /// For a session that went away without saying so, the time counts from the next call of this method.
    pub fn expire(&self, now: Instant) -> Vec<ClientInstance> {
        let ended: Vec<Arc<Shared>> = self
            .0
            .contexts
            .load()
            .values()
            .filter(|shared| shared.end_when_idle(now, self.0.linger))
            .cloned()
            .collect();
        if ended.is_empty() {
            return Vec::new();
        }
        let has_ended = |shared: &Arc<Shared>| ended.iter().any(|ended| Arc::ptr_eq(ended, shared));
        self.0.contexts.rcu(|contexts| {
            let mut next = BTreeMap::clone(contexts);
            next.retain(|_, shared| !has_ended(shared));
            next
        });
        ended.iter().map(|shared| shared.client.clone()).collect()
    }

    /// How many client instances have a context.
    pub fn len(&self) -> usize {
        self.0.contexts.load().len()
    }

    /// Whether no client instance has a context.
    pub fn is_empty(&self) -> bool {
        self.0.contexts.load().is_empty()
    }

    /// Whether the two handles lead to the same contexts.
    pub fn is(&self, other: &ClientContexts) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// One session's share of the context of its client instance.
///
/// A session gets it from [`ClientContexts::join`], and holds it for as long as it is open.
/// Every session of the same client instance has a share of the same context:
/// what one of them [learns](Self::learn), all of them [read](Self::learned).
///
/// The share counts its session in.
/// [`leave`](Self::leave) counts it out, and says when.
/// A share that is dropped counts its session out too, and the context does not know since when.
#[derive(Debug)]
#[must_use = "a share that is dropped leaves its context at once"]
pub struct ClientContext {
    shared: Arc<Shared>,
    /// Whether the session said when it left.
    left: bool,
}

impl ClientContext {
    fn of(shared: Arc<Shared>) -> Self {
        Self { shared, left: false }
    }

    /// The client instance the context belongs to.
    pub fn client(&self) -> &ClientInstance {
        &self.shared.client
    }

    /// What the context knows at this moment.
    ///
    /// The values are a snapshot, and they stay as they are while the context learns more.
    pub fn learned(&self) -> Arc<LearnedFields> {
        self.shared.learned.load_full()
    }

    /// The session learned `value` from one of its calls, and every session of the client knows it from now on.
    ///
    /// Returns whether the context did not know the value yet.
    pub fn learn(&self, value: impl LearnedValue) -> bool {
        let value: FieldValue = value.into();
        let mut is_new = false;
        self.shared.learned.rcu(|learned| {
            let next = learned.adding(&value);
            is_new = next.is_some();
            next.map_or_else(|| Arc::clone(learned), Arc::new)
        });
        is_new
    }

    /// How many sessions share the context at this moment, this one included.
    pub fn sessions(&self) -> u64 {
        self.shared.sessions()
    }

    /// Whether the two are shares of the same context.
    pub fn shares_with(&self, other: &ClientContext) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// The session closed at `now`, and no longer shares the context.
    ///
    /// When it was the last session, the context lingers from `now` on.
    pub fn leave(mut self, now: Instant) {
        self.shared.last_left_at.fetch_max(now.as_nanos(), Ordering::AcqRel);
        self.left = true;
    }
}

impl Drop for ClientContext {
    fn drop(&mut self) {
        if !self.left {
            self.shared.left_without_instant.store(true, Ordering::Release);
        }
        // The share counted its session in, so the count is at least one.
        self.shared.state.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    const LINGER: Duration = Duration::from_secs(60);

    fn client(client_id: &str) -> ClientInstance {
        ClientInstance::new(Some(ClientId::new(client_id).unwrap()), "10.0.4.27".parse().unwrap())
    }

    fn group(id: &str) -> GroupId {
        GroupId::new(id).unwrap()
    }

    fn at(secs: u64) -> Instant {
        Instant::ZERO + Duration::from_secs(secs)
    }

    fn text(reading: Reading<FieldValues>) -> String {
        match reading {
            Reading::Present(values) => values.to_string(),
            Reading::Absent => "(absent)".to_owned(),
            Reading::Pending => "(pending)".to_owned(),
        }
    }

    #[test]
    fn a_client_instance_is_a_client_id_and_a_host() {
        let ingest = client("order-ingest");

        assert_eq!(ingest.client_id().map(ClientId::as_str), Some("order-ingest"));
        assert_eq!(ingest.host().to_string(), "10.0.4.27");
        assert_eq!(ingest.to_string(), "order-ingest from 10.0.4.27");
        assert_ne!(ingest, client("analytics"));
        assert_ne!(
            ingest,
            ClientInstance::new(ingest.client_id().cloned(), "10.0.4.28".parse().unwrap())
        );
    }

    #[test]
    fn a_client_without_a_client_id_is_not_a_client_with_an_empty_one() {
        let host: ClientHost = "10.0.4.27".parse().unwrap();
        let none = ClientInstance::new(None, host);
        let empty = ClientInstance::new(Some(ClientId::new("").unwrap()), host);

        assert_ne!(none, empty);
        assert_eq!(none.client_id(), None);
        assert_eq!(none.to_string(), "no client.id from 10.0.4.27");
        assert_eq!(empty.to_string(), " from 10.0.4.27");
    }

    #[test]
    fn learned_fields_start_without_a_value() {
        let learned = LearnedFields::new();

        assert!(learned.is_empty());
        for field in Field::ALL {
            assert_eq!(learned.read(field), Reading::Absent, "{field}");
        }
    }

    #[test]
    fn learned_fields_keep_every_value_once_and_in_order() {
        let learned = LearnedFields::new()
            .with(group("payments"))
            .with(group("analytics-etl"))
            .with(group("payments"))
            .with(MemberId::new("consumer-1-4f2a").unwrap())
            .with(GroupInstanceId::new("etl-0").unwrap())
            .with(TransactionalId::new("payments-tx-1").unwrap());

        assert!(!learned.is_empty());
        assert_eq!(text(learned.read(Field::Group)), "analytics-etl, payments");
        assert_eq!(text(learned.read(Field::MemberId)), "consumer-1-4f2a");
        assert_eq!(text(learned.read(Field::GroupInstanceId)), "etl-0");
        assert_eq!(text(learned.read(Field::TransactionalId)), "payments-tx-1");
        assert_eq!(
            learned.groups().map(GroupId::as_str).collect::<Vec<_>>(),
            ["analytics-etl", "payments"]
        );
        assert_eq!(learned.member_ids().len(), 1);
        assert_eq!(learned.group_instance_ids().len(), 1);
        assert_eq!(learned.transactional_ids().len(), 1);
    }

    #[test]
    fn learned_values_have_the_client_context_as_their_origin() {
        let learned = LearnedFields::new().with(group("payments"));

        let groups = learned.read(Field::Group).present().unwrap();
        assert_eq!(groups.origin(), Origin::ClientContext);
        assert_eq!(learned.read(Field::ClientId), Reading::Absent);
        assert_eq!(learned.read(Field::Outcome), Reading::Absent);
    }

    #[test]
    fn learned_fields_provide_what_a_request_does_not_carry() {
        let learned = LearnedFields::new()
            .with(group("payments"))
            .with(group("analytics-etl"))
            .with(TransactionalId::new("payments-tx-1").unwrap());

        let fetch = learned.provide(Fields::new().with(ApiKey::FETCH));
        assert_eq!(text(fetch.read(Field::Group)), "analytics-etl, payments");
        assert_eq!(text(fetch.read(Field::TransactionalId)), "payments-tx-1");

        // A request that names its group provides it alone.
        let heartbeat = learned.provide(Fields::new().with(ApiKey::HEARTBEAT).with(group("payments")));
        let groups = heartbeat.read(Field::Group).present().unwrap();
        assert_eq!(groups.to_string(), "payments");
        assert_eq!(groups.origin(), Origin::Call);
    }

    #[test]
    fn sessions_of_one_client_instance_share_a_context() {
        let contexts = ClientContexts::new(LINGER);
        let coordinator = contexts.join(&client("analytics"));
        let fetcher = contexts.join(&client("analytics"));
        let other = contexts.join(&client("order-ingest"));

        assert!(coordinator.shares_with(&fetcher));
        assert!(!coordinator.shares_with(&other));
        assert_eq!(coordinator.client(), &client("analytics"));
        assert_eq!(coordinator.sessions(), 2);
        assert_eq!(contexts.sessions(&client("analytics")), 2);
        assert_eq!(contexts.sessions(&client("order-ingest")), 1);
        assert_eq!(contexts.sessions(&client("kafka-ui")), 0);
        assert_eq!(contexts.len(), 2);
    }

    #[test]
    fn what_one_session_learns_every_session_of_the_client_reads() {
        let contexts = ClientContexts::new(LINGER);
        let coordinator = contexts.join(&client("analytics"));
        let fetcher = contexts.join(&client("analytics"));
        let other = contexts.join(&client("order-ingest"));

        assert!(coordinator.learn(group("analytics-etl")));
        assert!(!fetcher.learn(group("analytics-etl")));
        assert!(fetcher.learn(MemberId::new("consumer-1-4f2a").unwrap()));

        for session in [&coordinator, &fetcher] {
            assert_eq!(text(session.learned().read(Field::Group)), "analytics-etl");
            assert_eq!(text(session.learned().read(Field::MemberId)), "consumer-1-4f2a");
        }
        assert!(other.learned().is_empty());
        assert_eq!(
            contexts
                .learned(&client("analytics"))
                .map(|learned| learned.groups().len()),
            Some(1)
        );
        assert_eq!(contexts.learned(&client("kafka-ui")), None);
    }

    #[test]
    fn what_a_call_read_stays_as_it_is_while_the_context_learns_more() {
        let contexts = ClientContexts::new(LINGER);
        let session = contexts.join(&client("analytics"));
        session.learn(group("analytics-etl"));

        let read_by_a_call = session.learned();
        session.learn(group("payments"));

        assert_eq!(text(read_by_a_call.read(Field::Group)), "analytics-etl");
        assert_eq!(text(session.learned().read(Field::Group)), "analytics-etl, payments");
    }

    #[test]
    fn a_context_outlives_its_sessions_for_a_while() {
        let contexts = ClientContexts::new(LINGER);
        let session = contexts.join(&client("analytics"));
        session.learn(group("analytics-etl"));
        session.leave(at(10));

        assert_eq!(contexts.sessions(&client("analytics")), 0);
        assert_eq!(contexts.expire(at(69)), []);

        // The client reconnects in time, and finds its group.
        let reconnected = contexts.join(&client("analytics"));
        assert_eq!(text(reconnected.learned().read(Field::Group)), "analytics-etl");
        assert_eq!(contexts.expire(at(500)), []);
        drop(reconnected);
    }

    #[test]
    fn a_context_ends_when_its_last_session_has_been_gone_for_the_linger_time() {
        let contexts = ClientContexts::new(LINGER);
        let first = contexts.join(&client("analytics"));
        let second = contexts.join(&client("analytics"));
        first.learn(group("analytics-etl"));

        first.leave(at(10));
        assert_eq!(contexts.expire(at(500)), [], "a session is still open");

        second.leave(at(20));
        assert_eq!(contexts.expire(at(79)), []);
        assert_eq!(contexts.expire(at(80)), [client("analytics")]);
        assert!(contexts.is_empty());
        assert_eq!(contexts.learned(&client("analytics")), None);

        // A client that comes back later starts a new context, which knows nothing.
        let later = contexts.join(&client("analytics"));
        assert!(later.learned().is_empty());
        assert_eq!(later.sessions(), 1);
    }

    #[test]
    fn the_time_counts_from_the_session_that_left_last() {
        let contexts = ClientContexts::new(LINGER);
        let first = contexts.join(&client("analytics"));
        let second = contexts.join(&client("analytics"));

        // The sessions say when they left in the other order than they left.
        second.leave(at(20));
        first.leave(at(10));

        assert_eq!(contexts.expire(at(79)), []);
        assert_eq!(contexts.expire(at(80)), [client("analytics")]);
    }

    #[test]
    fn a_session_that_went_away_without_saying_when_is_gone_from_the_next_expiry() {
        let contexts = ClientContexts::new(LINGER);
        let session = contexts.join(&client("analytics"));
        session.learn(group("analytics-etl"));
        drop(session);

        assert_eq!(contexts.sessions(&client("analytics")), 0);
        assert_eq!(contexts.expire(at(100)), [], "the context is idle from here on");
        assert_eq!(contexts.expire(at(159)), []);
        assert_eq!(contexts.expire(at(160)), [client("analytics")]);
    }

    #[test]
    fn contexts_that_do_not_linger_end_at_the_next_expiry() {
        let contexts = ClientContexts::new(Duration::ZERO);
        let open = contexts.join(&client("order-ingest"));
        contexts.join(&client("analytics")).leave(at(10));
        drop(contexts.join(&client("kafka-ui")));

        assert_eq!(contexts.expire(at(10)), [client("analytics"), client("kafka-ui")]);
        assert_eq!(contexts.len(), 1);
        assert_eq!(open.sessions(), 1);
    }

    #[test]
    fn a_session_never_joins_a_context_that_ended() {
        let contexts = ClientContexts::new(Duration::ZERO);
        let session = contexts.join(&client("analytics"));
        let state = session.shared.state.load(Ordering::Acquire);
        // A context only ends without a session, so the test ends it by force.
        session.shared.state.store(ENDED, Ordering::Release);

        let next = contexts.join(&client("analytics"));

        assert!(!next.shares_with(&session));
        assert_eq!(next.sessions(), 1);
        assert_eq!(contexts.len(), 1);
        session.shared.state.store(state, Ordering::Release);
    }

    #[test]
    fn clones_lead_to_the_same_contexts() {
        let engine = ClientContexts::new(LINGER);
        let session = engine.clone();

        let joined = session.join(&client("analytics"));

        assert!(engine.is(&session));
        assert!(!engine.is(&ClientContexts::new(LINGER)));
        assert_eq!(engine.linger(), LINGER);
        assert_eq!(engine.len(), 1);
        assert_eq!(engine.sessions(&client("analytics")), 1);
        drop(joined);
    }

    #[test]
    fn sessions_on_many_threads_join_one_context_and_lose_no_value() {
        const THREADS: usize = 8;
        const VALUES: usize = 50;
        let contexts = ClientContexts::new(LINGER);

        let news: usize = thread::scope(|scope| {
            let sessions: Vec<_> = (0..THREADS)
                .map(|thread| {
                    let contexts = contexts.clone();
                    scope.spawn(move || {
                        let session = contexts.join(&client("analytics"));
                        let news = (0..VALUES)
                            .filter(|value| {
                                // Every thread learns its own groups, and the groups that all threads share.
                                let own = session.learn(group(&format!("group-{thread}-{value}")));
                                let shared = session.learn(group(&format!("shared-{value}")));
                                assert!(own);
                                shared
                            })
                            .count();
                        session.leave(at(10));
                        news
                    })
                })
                .collect();
            sessions.into_iter().map(|session| session.join().unwrap()).sum()
        });

        assert_eq!(news, VALUES, "every shared group is new to one session only");
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts.sessions(&client("analytics")), 0);
        let learned = contexts.learned(&client("analytics")).unwrap();
        assert_eq!(learned.groups().len(), THREADS * VALUES + VALUES);
    }

    #[test]
    fn joining_and_expiring_at_the_same_time_never_loses_an_open_session() {
        const ROUNDS: usize = 2_000;
        let contexts = ClientContexts::new(Duration::ZERO);

        thread::scope(|scope| {
            let expiring = contexts.clone();
            scope.spawn(move || {
                for round in 0..ROUNDS {
                    expiring.expire(at(round as u64));
                }
            });
            for _ in 0..4 {
                let contexts = contexts.clone();
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        let session = contexts.join(&client("analytics"));
                        let value = group(&format!("group-{round}"));
                        session.learn(value.clone());
                        // The context of an open session never ends, so the session reads what it learned.
                        assert!(session.learned().groups().any(|group| *group == value));
                        assert!(contexts.sessions(&client("analytics")) >= 1);
                        session.leave(at(round as u64));
                    }
                });
            }
        });

        contexts.expire(Instant::MAX);
        assert!(contexts.is_empty());
    }
}
