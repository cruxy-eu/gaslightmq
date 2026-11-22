//! The APIs that ask about topics or change them: Metadata, `CreateTopics`, `DeleteTopics` and `CreatePartitions`.
//! Their parts are topics.

use gaslightmq_wire::messages::create_partitions_response::CreatePartitionsTopicResult;
use gaslightmq_wire::messages::create_topics_response::CreatableTopicResult;
use gaslightmq_wire::messages::delete_topics_response::DeletableTopicResult;
use gaslightmq_wire::messages::metadata_response::MetadataResponseTopic;
use gaslightmq_wire::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, MetadataRequest, MetadataResponse, TopicName,
};

use crate::body::{Editable, Keep, TopicKey, Verdict};
use crate::entry::{response, response_in_parts, topic_part};

/// The name of a topic that an entry may leave out, because it names the topic by ID.
fn name_or_empty(name: Option<&TopicName>) -> &str {
    name.map_or("", |name| name.as_str())
}

/// A Metadata request that asks for every topic names none, so it has no parts.
impl Editable for MetadataRequest {
    crate::api::response_has_error_code!();

    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        if let Some(topics) = &mut self.topics {
            topics.retain(|topic| {
                keep.topic(TopicKey::new(
                    name_or_empty(topic.name.as_ref()),
                    topic.topic_id.into_bytes(),
                ))
            });
        }
    }

    fn failed(&self, version: i16, verdict: &Verdict<'_>) -> MetadataResponse {
        let topics = self.topics.iter().flatten().filter_map(|topic| {
            let asked = TopicKey::new(name_or_empty(topic.name.as_ref()), topic.topic_id.into_bytes());
            let error_code = verdict.topic(asked)?;
            // A response can leave the name out since version 12.
            let name = topic.name.clone().or_else(|| (version < 12).then(TopicName::default));
            Some(
                MetadataResponseTopic::default()
                    .with_error_code(error_code)
                    .with_name(name)
                    .with_topic_id(topic.topic_id),
            )
        });
        // The response itself has an error code since version 13.
        let of_call = if version >= 13 { verdict.call() } else { 0 };
        MetadataResponse::default()
            .with_error_code(of_call)
            .with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(MetadataResponse, topics);
topic_part!(
    MetadataResponseTopic,
    |topic| TopicKey::new(name_or_empty(topic.name.as_ref()), topic.topic_id.into_bytes()),
    error_code
);

impl Editable for CreateTopicsRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain(|topic| keep.topic(TopicKey::named(&topic.name)));
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> CreateTopicsResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let error_code = verdict.topic(TopicKey::named(&topic.name))?;
            Some(
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(error_code),
            )
        });
        CreateTopicsResponse::default().with_topics(topics.collect())
    }

    response_in_parts!();
}

response!(CreateTopicsResponse, topics);
topic_part!(
    CreatableTopicResult,
    |topic| TopicKey::new(&topic.name, topic.topic_id.into_bytes()),
    error_code
);

/// Up to version 5 a `DeleteTopics` names its topics in one list, and since version 6 by name or by ID in another.
impl Editable for DeleteTopicsRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topic_names.retain(|name| keep.topic(TopicKey::named(name)));
        self.topics.retain(|topic| {
            keep.topic(TopicKey::new(
                name_or_empty(topic.name.as_ref()),
                topic.topic_id.into_bytes(),
            ))
        });
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DeleteTopicsResponse {
        let by_name = self.topic_names.iter().filter_map(|name| {
            let error_code = verdict.topic(TopicKey::named(name))?;
            Some(
                DeletableTopicResult::default()
                    .with_name(Some(name.clone()))
                    .with_error_code(error_code),
            )
        });
        let by_name_or_id = self.topics.iter().filter_map(|topic| {
            let asked = TopicKey::new(name_or_empty(topic.name.as_ref()), topic.topic_id.into_bytes());
            let error_code = verdict.topic(asked)?;
            Some(
                DeletableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_error_code(error_code),
            )
        });
        DeleteTopicsResponse::default().with_responses(by_name.chain(by_name_or_id).collect())
    }

    response_in_parts!();
}

response!(DeleteTopicsResponse, responses);
topic_part!(
    DeletableTopicResult,
    |topic| TopicKey::new(name_or_empty(topic.name.as_ref()), topic.topic_id.into_bytes()),
    error_code
);

impl Editable for CreatePartitionsRequest {
    fn retain_parts(&mut self, _version: i16, keep: &mut Keep<'_>) {
        self.topics.retain(|topic| keep.topic(TopicKey::named(&topic.name)));
    }

    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> CreatePartitionsResponse {
        let topics = self.topics.iter().filter_map(|topic| {
            let error_code = verdict.topic(TopicKey::named(&topic.name))?;
            Some(
                CreatePartitionsTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(error_code),
            )
        });
        CreatePartitionsResponse::default().with_results(topics.collect())
    }

    response_in_parts!();
}

response!(CreatePartitionsResponse, results);
topic_part!(
    CreatePartitionsTopicResult,
    |topic| TopicKey::named(&topic.name),
    error_code
);
