//! Error injection for APIs whose requests address topics or resources.
//!
//! The proxy never changes a request: it either forwards it as is or answers
//! it on its own. With a topic-scoped rule:
//! - Produce writes data, so a request touching the topic is answered with
//!   errors for every partition in it and never reaches the broker. Other
//!   topics batched into the same request fail with it.
//! - Metadata and DescribeConfigs only read. If the request is entirely in
//!   scope it is answered by the proxy; otherwise it is forwarded and only
//!   the entries in scope get the error in the response.

use anyhow::Result;
use bytes::Bytes;
use gaslightmq_proxy_api::catalog::api;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    BrokerId, DescribeConfigsRequest, DescribeConfigsResponse, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse,
};
use kafka_protocol::protocol::{Decodable, StrBytes};

/// Resource type of a topic in DescribeConfigs.
const RESOURCE_TOPIC: i8 = 2;

const INJECTED: &str = "injected by gaslightmq-proxy";

/// A decoded request of an API that supports error injection.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityRequest {
    Produce(ProduceRequest),
    Metadata(MetadataRequest),
    DescribeConfigs(DescribeConfigsRequest),
}

/// A response synthesized by the proxy.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SynthResponse {
    Produce(ProduceResponse),
    Metadata(MetadataResponse),
    DescribeConfigs(DescribeConfigsResponse),
}

/// How an error code gets applied to a request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ErrorPlan {
    /// Nothing in the request is in scope: forward it.
    NoMatch,
    /// Answer with this response; the request is not forwarded.
    Synthetic(SynthResponse),
    /// Forward the request, then set `code` on the response entries in
    /// scope. Only for read-only APIs.
    Mutate { code: i16, topic: Option<String> },
}

/// Cluster details needed to synthesize a Metadata response.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClusterInfo {
    pub brokers: Vec<MetadataResponseBroker>,
    pub cluster_id: Option<StrBytes>,
    pub controller_id: i32,
}

/// How much of a request a topic scope covers.
fn coverage(hits: usize, total: usize) -> Coverage {
    match hits {
        0 => Coverage::None,
        n if n == total => Coverage::All,
        _ => Coverage::Some,
    }
}

enum Coverage {
    None,
    Some,
    All,
}

impl EntityRequest {
    /// Decode the body of a request if its API supports error injection.
    pub fn decode(api_key: i16, api_version: i16, mut body: Bytes) -> Result<Option<Self>> {
        Ok(Some(match api_key {
            api::PRODUCE => Self::Produce(ProduceRequest::decode(&mut body, api_version)?),
            api::METADATA => Self::Metadata(MetadataRequest::decode(&mut body, api_version)?),
            api::DESCRIBE_CONFIGS => {
                Self::DescribeConfigs(DescribeConfigsRequest::decode(&mut body, api_version)?)
            }
            _ => return Ok(None),
        }))
    }

    /// Whether the request addresses `topic` (or anything at all, if `None`).
    pub fn touches(&self, topic: Option<&str>) -> bool {
        let Some(topic) = topic else { return true };
        match self {
            Self::Produce(r) => r.topic_data.iter().any(|t| t.name.as_str() == topic),
            Self::Metadata(r) => match &r.topics {
                None => true,
                Some(ts) => ts.iter().any(|t| t.name.as_ref().is_some_and(|n| n.as_str() == topic)),
            },
            Self::DescribeConfigs(r) => {
                r.resources.iter().any(|res| is_topic(res.resource_type, &res.resource_name, topic))
            }
        }
    }

    /// Work out how to fail this request with `code` for `topic`'s scope
    /// (everything, if `None`).
    pub fn plan_error(&self, code: i16, topic: Option<&str>, cluster: &ClusterInfo) -> ErrorPlan {
        let mutate = || ErrorPlan::Mutate { code, topic: topic.map(str::to_string) };
        match self {
            Self::Produce(req) => {
                if !self.touches(topic) {
                    return ErrorPlan::NoMatch;
                }
                ErrorPlan::Synthetic(SynthResponse::Produce(ProduceResponse::default().with_responses(
                    req.topic_data
                        .iter()
                        .map(|t| {
                            TopicProduceResponse::default()
                                .with_name(t.name.clone())
                                .with_topic_id(t.topic_id)
                                .with_partition_responses(
                                    t.partition_data
                                        .iter()
                                        .map(|p| {
                                            PartitionProduceResponse::default()
                                                .with_index(p.index)
                                                .with_error_code(code)
                                                .with_base_offset(-1)
                                                .with_error_message(Some(StrBytes::from_static_str(INJECTED)))
                                        })
                                        .collect(),
                                )
                        })
                        .collect(),
                )))
            }
            Self::Metadata(req) => {
                let Some(topics) = &req.topics else { return mutate() };
                let hits = topics
                    .iter()
                    .filter(|t| topic.is_none_or(|s| t.name.as_ref().is_some_and(|n| n.as_str() == s)))
                    .count();
                match coverage(hits, topics.len()) {
                    Coverage::None => ErrorPlan::NoMatch,
                    Coverage::Some => mutate(),
                    Coverage::All => ErrorPlan::Synthetic(SynthResponse::Metadata(
                        MetadataResponse::default()
                            .with_brokers(cluster.brokers.clone())
                            .with_cluster_id(cluster.cluster_id.clone())
                            .with_controller_id(BrokerId(cluster.controller_id))
                            .with_topics(
                                topics
                                    .iter()
                                    .map(|t| {
                                        MetadataResponseTopic::default()
                                            .with_error_code(code)
                                            .with_name(t.name.clone())
                                            .with_topic_id(t.topic_id)
                                    })
                                    .collect(),
                            ),
                    )),
                }
            }
            Self::DescribeConfigs(req) => {
                let hits = req
                    .resources
                    .iter()
                    .filter(|r| topic.is_none_or(|s| is_topic(r.resource_type, &r.resource_name, s)))
                    .count();
                match coverage(hits, req.resources.len()) {
                    Coverage::None => ErrorPlan::NoMatch,
                    Coverage::Some => mutate(),
                    Coverage::All => ErrorPlan::Synthetic(SynthResponse::DescribeConfigs(
                        DescribeConfigsResponse::default().with_results(
                            req.resources
                                .iter()
                                .map(|r| {
                                    DescribeConfigsResult::default()
                                        .with_error_code(code)
                                        .with_error_message(Some(StrBytes::from_static_str(INJECTED)))
                                        .with_resource_type(r.resource_type)
                                        .with_resource_name(r.resource_name.clone())
                                })
                                .collect(),
                        ),
                    )),
                }
            }
        }
    }
}

fn is_topic(resource_type: i8, name: &StrBytes, topic: &str) -> bool {
    resource_type == RESOURCE_TOPIC && name.as_str() == topic
}

/// Fail every topic in scope of an upstream Metadata response.
pub(crate) fn mutate_metadata(resp: &mut MetadataResponse, code: i16, topic: Option<&str>) {
    for t in &mut resp.topics {
        if topic.is_none_or(|s| t.name.as_ref().is_some_and(|n| n.as_str() == s)) {
            t.error_code = code;
            t.partitions.clear();
        }
    }
}

/// Fail every topic resource in scope of an upstream DescribeConfigs response.
pub(crate) fn mutate_describe_configs(resp: &mut DescribeConfigsResponse, code: i16, topic: Option<&str>) {
    for r in &mut resp.results {
        let in_scope = match topic {
            Some(s) => is_topic(r.resource_type, &r.resource_name, s),
            None => true,
        };
        if in_scope {
            r.error_code = code;
            r.error_message = Some(StrBytes::from_static_str(INJECTED));
            r.configs.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::describe_configs_response::DescribeConfigsResourceResult;
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

    fn name(s: &str) -> TopicName {
        TopicName(StrBytes::from_string(s.to_string()))
    }

    fn produce(topics: &[(&str, &[i32])]) -> EntityRequest {
        EntityRequest::Produce(ProduceRequest::default().with_acks(-1).with_topic_data(
            topics
                .iter()
                .map(|(t, ps)| {
                    TopicProduceData::default().with_name(name(t)).with_partition_data(
                        ps.iter().map(|p| PartitionProduceData::default().with_index(*p)).collect(),
                    )
                })
                .collect(),
        ))
    }

    fn produce_codes(plan: ErrorPlan) -> Vec<(String, i32, i16, i64)> {
        let ErrorPlan::Synthetic(SynthResponse::Produce(resp)) = plan else {
            panic!("expected synthetic produce response");
        };
        resp.responses
            .iter()
            .flat_map(|t| t.partition_responses.iter().map(|p| (t.name.to_string(), p.index, p.error_code, p.base_offset)))
            .collect()
    }

    #[test]
    fn produce_without_scope_fails_every_partition() {
        let req = produce(&[("a", &[0, 1]), ("b", &[0])]);
        assert_eq!(
            produce_codes(req.plan_error(6, None, &ClusterInfo::default())),
            [("a".into(), 0, 6, -1), ("a".into(), 1, 6, -1), ("b".into(), 0, 6, -1)]
        );
    }

    #[test]
    fn produce_touching_scoped_topic_fails_as_a_whole() {
        let req = produce(&[("a", &[0]), ("b", &[0])]);
        assert_eq!(
            produce_codes(req.plan_error(19, Some("b"), &ClusterInfo::default())),
            [("a".into(), 0, 19, -1), ("b".into(), 0, 19, -1)]
        );
    }

    #[test]
    fn produce_scoped_to_absent_topic_does_not_match() {
        let req = produce(&[("a", &[0])]);
        assert!(!req.touches(Some("z")));
        assert_eq!(req.plan_error(6, Some("z"), &ClusterInfo::default()), ErrorPlan::NoMatch);
    }

    #[test]
    fn metadata_for_all_topics_mutates_response() {
        let req = EntityRequest::Metadata(MetadataRequest::default().with_topics(None));
        assert!(req.touches(Some("anything")));
        assert_eq!(
            req.plan_error(5, Some("a"), &ClusterInfo::default()),
            ErrorPlan::Mutate { code: 5, topic: Some("a".into()) }
        );

        let mut resp = MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default().with_name(Some(name("a"))).with_partitions(vec![Default::default()]),
            MetadataResponseTopic::default().with_name(Some(name("b"))).with_partitions(vec![Default::default()]),
        ]);
        mutate_metadata(&mut resp, 5, Some("a"));
        assert_eq!((resp.topics[0].error_code, resp.topics[0].partitions.len()), (5, 0));
        assert_eq!((resp.topics[1].error_code, resp.topics[1].partitions.len()), (0, 1));
    }

    #[test]
    fn metadata_partly_in_scope_is_forwarded_and_mutated() {
        let req = EntityRequest::Metadata(MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(name("a"))),
            MetadataRequestTopic::default().with_name(Some(name("b"))),
        ])));
        assert_eq!(
            req.plan_error(3, Some("a"), &ClusterInfo::default()),
            ErrorPlan::Mutate { code: 3, topic: Some("a".into()) }
        );
    }

    #[test]
    fn metadata_fully_in_scope_is_synthesized_with_cluster_info() {
        let req = EntityRequest::Metadata(MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(name("a"))),
        ])));
        let cluster = ClusterInfo {
            brokers: vec![MetadataResponseBroker::default().with_node_id(BrokerId(4))],
            cluster_id: Some(StrBytes::from_static_str("c")),
            controller_id: 4,
        };
        let ErrorPlan::Synthetic(SynthResponse::Metadata(resp)) = req.plan_error(3, None, &cluster) else {
            panic!("expected synthetic metadata");
        };
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.controller_id, BrokerId(4));
        assert_eq!(resp.topics[0].error_code, 3);
        assert!(resp.topics[0].partitions.is_empty());
    }

    #[test]
    fn describe_configs_partly_in_scope_mutates_only_topic_resources() {
        let resource = |t: i8| {
            DescribeConfigsResource::default().with_resource_type(t).with_resource_name(StrBytes::from_static_str("a"))
        };
        let req = EntityRequest::DescribeConfigs(
            DescribeConfigsRequest::default().with_resources(vec![resource(RESOURCE_TOPIC), resource(4)]),
        );
        assert_eq!(
            req.plan_error(29, Some("a"), &ClusterInfo::default()),
            ErrorPlan::Mutate { code: 29, topic: Some("a".into()) }
        );

        let result = |t: i8| {
            DescribeConfigsResult::default()
                .with_resource_type(t)
                .with_resource_name(StrBytes::from_static_str("a"))
                .with_configs(vec![DescribeConfigsResourceResult::default()])
        };
        let mut resp = DescribeConfigsResponse::default().with_results(vec![result(RESOURCE_TOPIC), result(4)]);
        mutate_describe_configs(&mut resp, 29, Some("a"));
        assert_eq!((resp.results[0].error_code, resp.results[0].configs.len()), (29, 0));
        assert_eq!(resp.results[0].error_message.as_deref(), Some(INJECTED));
        assert_eq!((resp.results[1].error_code, resp.results[1].configs.len()), (0, 1));
    }
}
