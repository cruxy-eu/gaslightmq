use std::collections::HashMap;

use gaslightmq_kernel::prelude::*;
use gaslightmq_wire::Response;
use gaslightmq_wire::messages::{
    ApiKey as KnownApi, CreateTopicsResponse, DescribeTopicPartitionsResponse, MetadataResponse,
};

use crate::SurgeryError;

/// The topics that are known by ID: which name each topic ID stands for.
///
/// Newer versions of Fetch, Produce and other requests name a topic by its ID alone.
/// A cluster tells the ID of a topic next to its name in a Metadata response,
/// so whoever sees those responses can [learn](Self::learn_from) the names.
/// An [`EditableRequest`](crate::EditableRequest) uses them to say which [parts](gaslightmq_field::Part) a request names.
///
/// A topic has one ID, and an ID has one name.
/// A topic that was deleted and created again has a new ID, and the old ID is forgotten.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_wire_edit::prelude::*;
///
/// let orders = TopicName::new("orders.eu").expect("a topic name");
/// let id: TopicId = "Zm9vYmFyYmF6cXV4MTIzNA".parse()?;
///
/// let mut topics = TopicNames::new();
/// topics.learn(id, orders.clone());
///
/// assert_eq!(topics.name_of(id), Some(&orders));
/// assert_eq!(topics.id_of(&orders), Some(id));
/// # Ok::<(), TopicIdError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicNames {
    names: HashMap<TopicId, TopicName>,
    ids: HashMap<TopicName, TopicId>,
}

impl TopicNames {
    /// No topic is known.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remembers that `id` is the topic `name`.
    ///
    /// What was known about that ID or that name before is forgotten.
    pub fn learn(&mut self, id: TopicId, name: TopicName) {
        if let Some(renamed) = self.names.insert(id, name.clone())
            && renamed != name
        {
            self.ids.remove(&renamed);
        }
        if let Some(replaced) = self.ids.insert(name, id)
            && replaced != id
        {
            self.names.remove(&replaced);
        }
    }

    /// Remembers every topic that `response` names with its ID.
    ///
    /// Metadata, `CreateTopics` and `DescribeTopicPartitions` responses name topics that way.
    /// A response of another API, or of a version without topic IDs, teaches nothing.
    /// It returns how many topics the response named with an ID.
    ///
    /// # Errors
    ///
    /// [`SurgeryError::UnreadableResponse`] when the body of the response does not decode.
    /// Nothing is learned then.
    pub fn learn_from(&mut self, response: &Response) -> Result<usize, SurgeryError> {
        let unreadable = |error| SurgeryError::unreadable_response(response.api(), &error);
        let named: Vec<(TopicId, TopicName)> = match response.api().known() {
            Some(KnownApi::Metadata) => response
                .body::<MetadataResponse>()
                .map_err(unreadable)?
                .topics
                .iter()
                .filter_map(|topic| identity(topic.name.as_deref()?, topic.topic_id.into_bytes()))
                .collect(),
            Some(KnownApi::CreateTopics) => response
                .body::<CreateTopicsResponse>()
                .map_err(unreadable)?
                .topics
                .iter()
                .filter(|topic| topic.error_code == 0)
                .filter_map(|topic| identity(&topic.name, topic.topic_id.into_bytes()))
                .collect(),
            Some(KnownApi::DescribeTopicPartitions) => response
                .body::<DescribeTopicPartitionsResponse>()
                .map_err(unreadable)?
                .topics
                .iter()
                .filter_map(|topic| identity(topic.name.as_deref()?, topic.topic_id.into_bytes()))
                .collect(),
            _ => Vec::new(),
        };
        let learned = named.len();
        for (id, name) in named {
            self.learn(id, name);
        }
        Ok(learned)
    }

    /// The name of the topic with `id`, if it is known.
    pub fn name_of(&self, id: TopicId) -> Option<&TopicName> {
        self.names.get(&id)
    }

    /// The ID of the topic `name`, if it is known.
    pub fn id_of(&self, name: &TopicName) -> Option<TopicId> {
        self.ids.get(name).copied()
    }

    /// How many topics are known.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether no topic is known.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// The known topics, in no order.
    pub fn iter(&self) -> impl Iterator<Item = (TopicId, &TopicName)> {
        self.names.iter().map(|(id, name)| (*id, name))
    }
}

/// Learns every pair.
impl Extend<(TopicId, TopicName)> for TopicNames {
    fn extend<I: IntoIterator<Item = (TopicId, TopicName)>>(&mut self, topics: I) {
        for (id, name) in topics {
            self.learn(id, name);
        }
    }
}

/// The topics of those pairs.
impl FromIterator<(TopicId, TopicName)> for TopicNames {
    fn from_iter<I: IntoIterator<Item = (TopicId, TopicName)>>(topics: I) -> Self {
        let mut names = Self::new();
        names.extend(topics);
        names
    }
}

/// A topic as a response names it, when it gives both a valid name and an ID.
fn identity(name: &str, id: [u8; 16]) -> Option<(TopicId, TopicName)> {
    Some((TopicId::from_wire(id)?, TopicName::new(name).ok()?))
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::messages::create_topics_response::CreatableTopicResult;
    use gaslightmq_wire::messages::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
    use gaslightmq_wire::messages::metadata_response::MetadataResponseTopic;
    use gaslightmq_wire::messages::{HeartbeatResponse, MetadataRequest};
    use gaslightmq_wire::protocol::StrBytes;
    use gaslightmq_wire::{Api, Frame};

    use super::*;

    fn name(text: &str) -> TopicName {
        TopicName::new(text).unwrap()
    }

    fn id(byte: u8) -> TopicId {
        TopicId::new([byte; 16]).unwrap()
    }

    fn metadata_topic(text: &'static str, byte: u8) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .with_name(Some(StrBytes::from_static_str(text).into()))
            .with_topic_id(vec![byte; 16].try_into().unwrap())
    }

    #[test]
    fn a_learned_topic_is_known_both_ways() {
        let mut topics = TopicNames::new();
        assert!(topics.is_empty());

        topics.learn(id(1), name("orders.eu"));

        assert_eq!(topics.len(), 1);
        assert_eq!(topics.name_of(id(1)), Some(&name("orders.eu")));
        assert_eq!(topics.id_of(&name("orders.eu")), Some(id(1)));
        assert_eq!(topics.name_of(id(2)), None);
        assert_eq!(topics.id_of(&name("payments.settled")), None);
    }

    #[test]
    fn a_topic_that_was_created_again_forgets_its_old_id() {
        let mut topics: TopicNames = [(id(1), name("orders.eu"))].into_iter().collect();

        topics.learn(id(2), name("orders.eu"));

        assert_eq!(topics.name_of(id(1)), None);
        assert_eq!(topics.name_of(id(2)), Some(&name("orders.eu")));
        assert_eq!(topics.id_of(&name("orders.eu")), Some(id(2)));
        assert_eq!(topics.len(), 1);
    }

    #[test]
    fn an_id_that_gets_another_name_forgets_its_old_name() {
        let mut topics: TopicNames = [(id(1), name("orders.eu"))].into_iter().collect();

        topics.learn(id(1), name("orders.us"));

        assert_eq!(topics.id_of(&name("orders.eu")), None);
        assert_eq!(topics.id_of(&name("orders.us")), Some(id(1)));
        assert_eq!(topics.iter().collect::<Vec<_>>(), [(id(1), &name("orders.us"))]);
    }

    #[test]
    fn learning_the_same_topic_again_changes_nothing() {
        let mut topics: TopicNames = [(id(1), name("orders.eu"))].into_iter().collect();
        let before = topics.clone();

        topics.learn(id(1), name("orders.eu"));

        assert_eq!(topics, before);
    }

    #[test]
    fn a_metadata_response_teaches_the_topics_it_names_with_an_id() {
        let body = MetadataResponse::default().with_topics(vec![
            metadata_topic("orders.eu", 1),
            metadata_topic("payments.settled", 2),
            // An unknown topic comes back without an ID.
            metadata_topic("missing", 0).with_error_code(3),
        ]);
        let response = Response::new(Api::new(KnownApi::Metadata, 12), 7, &body).unwrap();

        let mut topics = TopicNames::new();
        assert_eq!(topics.learn_from(&response), Ok(2));

        assert_eq!(topics.name_of(id(1)), Some(&name("orders.eu")));
        assert_eq!(topics.name_of(id(2)), Some(&name("payments.settled")));
        assert_eq!(topics.id_of(&name("missing")), None);
    }

    #[test]
    fn a_create_topics_response_teaches_the_topics_it_created() {
        let created = |text: &'static str, byte: u8| {
            CreatableTopicResult::default()
                .with_name(StrBytes::from_static_str(text).into())
                .with_topic_id(vec![byte; 16].try_into().unwrap())
        };
        let body = CreateTopicsResponse::default().with_topics(vec![
            created("orders.eu", 1),
            // A topic that was not created has no ID to learn, whatever the response says.
            created("payments.settled", 2).with_error_code(36),
        ]);
        let response = Response::new(Api::new(KnownApi::CreateTopics, 7), 7, &body).unwrap();

        let mut topics = TopicNames::new();
        assert_eq!(topics.learn_from(&response), Ok(1));

        assert_eq!(topics.name_of(id(1)), Some(&name("orders.eu")));
        assert_eq!(topics.name_of(id(2)), None);
    }

    #[test]
    fn a_describe_topic_partitions_response_teaches_the_topics_it_names_with_an_id() {
        let described = DescribeTopicPartitionsResponseTopic::default()
            .with_name(Some(StrBytes::from_static_str("orders.eu").into()))
            .with_topic_id(vec![1; 16].try_into().unwrap());
        let body = DescribeTopicPartitionsResponse::default().with_topics(vec![described]);
        let response = Response::new(Api::new(KnownApi::DescribeTopicPartitions, 0), 7, &body).unwrap();

        let mut topics = TopicNames::new();
        assert_eq!(topics.learn_from(&response), Ok(1));

        assert_eq!(topics.id_of(&name("orders.eu")), Some(id(1)));
    }

    #[test]
    fn a_metadata_response_of_a_version_without_topic_ids_teaches_nothing() {
        let body = MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default().with_name(Some(StrBytes::from_static_str("orders.eu").into())),
        ]);
        let response = Response::new(Api::new(KnownApi::Metadata, 9), 7, &body).unwrap();

        let mut topics = TopicNames::new();
        assert_eq!(topics.learn_from(&response), Ok(0));
        assert!(topics.is_empty());
    }

    #[test]
    fn a_response_of_an_api_without_topic_ids_teaches_nothing() {
        let request =
            gaslightmq_wire::Request::new(4, 7, None, &gaslightmq_wire::messages::HeartbeatRequest::default()).unwrap();
        let response = request.response(&HeartbeatResponse::default()).unwrap();

        let mut topics = TopicNames::new();
        assert_eq!(topics.learn_from(&response), Ok(0));
    }

    #[test]
    fn a_response_that_does_not_decode_teaches_nothing_and_says_why() {
        let request = gaslightmq_wire::Request::new(12, 7, None, &MetadataRequest::default()).unwrap();
        // A correlation ID, the tagged fields of the header, and a body that stops short.
        let frame = Frame::new(vec![0, 0, 0, 7, 0, 0xFF]);
        let response = Response::decode(request.api(), frame).unwrap();

        let mut topics: TopicNames = [(id(1), name("orders.eu"))].into_iter().collect();
        let error = topics.learn_from(&response).unwrap_err();

        assert!(matches!(error, SurgeryError::UnreadableResponse { .. }));
        assert!(
            error
                .to_string()
                .starts_with("the Metadata v12 response cannot be read: cannot decode response body: "),
            "{error}"
        );
        assert_eq!(topics.len(), 1);
    }
}
