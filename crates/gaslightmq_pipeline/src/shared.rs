//! What the sessions of one proxy share.

use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;
use gaslightmq_kernel::prelude::*;
use gaslightmq_lie_api::Lies;
use gaslightmq_session::ClientContexts;
use gaslightmq_wire::Response;
use gaslightmq_wire_edit::{SurgeryError, TopicNames};

/// The topics that a proxy knows by ID, shared by all of its sessions.
///
/// Newer versions of Fetch and Produce name a topic by its ID alone.
/// A selector and a lie speak in topic names, so the pipeline needs the name behind an ID.
/// A cluster tells both in its Metadata, `CreateTopics` and `DescribeTopicPartitions` responses.
/// Every pipeline [learns](Self::learn_from) from the truth of those calls, before any lie changes it,
/// and every pipeline decodes its requests with what all of them learned.
///
/// The names belong to the cluster, and not to a client:
/// a client asks for Metadata on one connection and fetches on another, and every client sees the same IDs.
/// The engine therefore makes one handle per proxy, and hands a clone to every pipeline.
///
/// The handle has no lock, and none of its methods waits.
/// A change swaps in a new set of names, and a reader keeps the set it read.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_pipeline::prelude::*;
///
/// let orders = TopicName::new("orders.eu")?;
/// let id: TopicId = "Zm9vYmFyYmF6cXV4MTIzNA".parse()?;
///
/// let topics = KnownTopics::new();
/// let of_another_session = topics.clone();
/// topics.learn(id, orders.clone());
///
/// assert_eq!(of_another_session.names().name_of(id), Some(&orders));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct KnownTopics(Arc<ArcSwap<TopicNames>>);

impl KnownTopics {
    /// No topic is known yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The names that are known at this moment.
    ///
    /// The set is a snapshot: a call decodes its request with it, and works with it from start to finish.
    pub fn names(&self) -> Arc<TopicNames> {
        self.0.load_full()
    }

    /// Remembers that `id` is the topic `name`, for every session.
    pub fn learn(&self, id: TopicId, name: TopicName) {
        self.learn_all(&[(id, name)]);
    }

    /// Remembers every topic that `response` names with its ID, for every session.
    ///
    /// Metadata, `CreateTopics` and `DescribeTopicPartitions` responses name topics that way.
    /// A response of another API, or of a version without topic IDs, teaches nothing.
    /// It returns how many topics the response named with an ID.
    ///
    /// # Errors
    ///
    /// [`SurgeryError::UnreadableResponse`] when the body of the response does not decode.
    /// Nothing is learned then.
    pub fn learn_from(&self, response: &Response) -> Result<usize, SurgeryError> {
        let mut named = TopicNames::new();
        let count = named.learn_from(response)?;
        let named: Vec<(TopicId, TopicName)> = named.iter().map(|(id, name)| (id, name.clone())).collect();
        self.learn_all(&named);
        Ok(count)
    }

    /// Whether the two handles lead to the same names.
    pub fn is(&self, other: &KnownTopics) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Swaps in the names with `topics` among them, unless all of them are known already.
    fn learn_all(&self, topics: &[(TopicId, TopicName)]) {
        let is_new = |names: &TopicNames, (id, name): &(TopicId, TopicName)| names.name_of(*id) != Some(name);
        if !topics.iter().any(|topic| is_new(&self.0.load(), topic)) {
            return;
        }
        self.0.rcu(|names| {
            let mut next = TopicNames::clone(names);
            next.extend(topics.iter().cloned());
            next
        });
    }
}

/// What every session of one proxy shares: the handles that the engine passes to each [`Pipeline`](crate::Pipeline).
///
/// | Handle | Shares |
/// |---|---|
/// | [`ClientContexts`] | What the sessions of one client instance learned together, such as its group. |
/// | [`KnownTopics`] | The topics that the cluster named with their ID. |
/// | [`Lies`] | The kinds of lies of this build, one behaviour per kind. |
///
/// The engine makes one of these per proxy and hands a clone to every pipeline it opens.
/// The counters and budgets of the rules are shared too.
/// They travel inside the armed rules of a [`Snapshot`](gaslightmq_rule::Snapshot), so they need no handle here.
///
/// ```
/// use std::sync::Arc;
///
/// use gaslightmq_clock::Duration;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
/// use gaslightmq_pipeline::prelude::*;
/// use gaslightmq_session::prelude::*;
///
/// let shared = SharedBySessions::new(
///     ClientContexts::new(Duration::from_secs(60)),
///     KnownTopics::new(),
///     Arc::new(Lies::of_every_build()),
/// );
///
/// assert!(shared.lies().tells(LieKind::Silence));
/// assert!(shared.clone().topics().is(shared.topics()));
/// ```
#[derive(Clone)]
pub struct SharedBySessions {
    contexts: ClientContexts,
    topics: KnownTopics,
    lies: Arc<Lies>,
}

impl SharedBySessions {
    /// What the sessions of one proxy share.
    pub const fn new(contexts: ClientContexts, topics: KnownTopics, lies: Arc<Lies>) -> Self {
        Self { contexts, topics, lies }
    }

    /// The client contexts that the sessions of the proxy join.
    pub const fn contexts(&self) -> &ClientContexts {
        &self.contexts
    }

    /// The topics that the proxy knows by ID.
    pub const fn topics(&self) -> &KnownTopics {
        &self.topics
    }

    /// The lies that the pipelines of the proxy can tell.
    pub fn lies(&self) -> &Lies {
        &self.lies
    }
}

/// Writes the kinds of lies and how much is known, and not the contexts themselves.
impl fmt::Debug for SharedBySessions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedBySessions")
            .field("client_contexts", &self.contexts.len())
            .field("known_topics", &self.topics.names().len())
            .field("lies", &self.lies)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::Request;
    use gaslightmq_wire::messages::metadata_response::MetadataResponseTopic;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse};
    use gaslightmq_wire::protocol::StrBytes;

    use super::*;

    fn id(byte: u8) -> TopicId {
        TopicId::new([byte; 16]).unwrap()
    }

    fn name(text: &str) -> TopicName {
        TopicName::new(text).unwrap()
    }

    #[test]
    fn every_clone_of_the_handle_knows_what_one_of_them_learned() {
        let topics = KnownTopics::new();
        let other = topics.clone();
        assert!(topics.is(&other));
        assert!(!topics.is(&KnownTopics::new()));

        topics.learn(id(1), name("orders.eu"));

        assert_eq!(other.names().name_of(id(1)), Some(&name("orders.eu")));
        assert_eq!(other.names().id_of(&name("orders.eu")), Some(id(1)));
    }

    #[test]
    fn names_that_someone_read_stay_as_they_were() {
        let topics = KnownTopics::new();
        let before = topics.names();

        topics.learn(id(1), name("orders.eu"));

        assert!(before.is_empty());
        assert_eq!(topics.names().len(), 1);
    }

    #[test]
    fn what_is_known_already_swaps_in_nothing() {
        let topics = KnownTopics::new();
        topics.learn(id(1), name("orders.eu"));
        let known = topics.names();

        topics.learn(id(1), name("orders.eu"));
        assert!(Arc::ptr_eq(&known, &topics.names()));

        // A topic that was deleted and created again has a new ID, and that is news.
        topics.learn(id(2), name("orders.eu"));
        assert_eq!(topics.names().name_of(id(1)), None);
        assert_eq!(topics.names().id_of(&name("orders.eu")), Some(id(2)));
    }

    #[test]
    fn a_metadata_response_teaches_the_topics_it_names_with_an_id() {
        let topics = KnownTopics::new();
        let request = Request::new(12, 7, None, &MetadataRequest::default()).unwrap();
        let orders = MetadataResponseTopic::default()
            .with_name(Some(StrBytes::from_static_str("orders.eu").into()))
            .with_topic_id(vec![1; 16].try_into().unwrap());
        let response = request
            .response(&MetadataResponse::default().with_topics(vec![orders]))
            .unwrap();

        assert_eq!(topics.learn_from(&response), Ok(1));
        assert_eq!(topics.names().name_of(id(1)), Some(&name("orders.eu")));

        // A response of another API teaches nothing.
        let heartbeat = Request::new(4, 8, None, &HeartbeatRequest::default()).unwrap();
        let response = heartbeat.response(&HeartbeatResponse::default()).unwrap();
        assert_eq!(topics.learn_from(&response), Ok(0));
        assert_eq!(topics.names().len(), 1);
    }
}
