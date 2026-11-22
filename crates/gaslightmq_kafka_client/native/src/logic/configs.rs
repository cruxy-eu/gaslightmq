//! Asking how a topic is configured.

use std::collections::BTreeMap;
use std::time::Duration;

use gaslightmq_wire::messages::describe_configs_request::DescribeConfigsResource;
use gaslightmq_wire::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use gaslightmq_wire::protocol::StrBytes;

use crate::error::ClientError;
use crate::logic::call::{Operation, Reading};
use crate::logic::cluster::Route;
use crate::logic::versions::Versions;

/// The `resource_type` of a topic in `DescribeConfigs`.
const TOPIC: i8 = 2;

/// Ask any broker for the configuration of a topic.
pub(crate) struct DescribeTopicConfigs {
    pub topic: String,
    /// The names of the configs to return. Empty asks for all of them.
    pub keys: Vec<String>,
}

impl Operation for DescribeTopicConfigs {
    type Request = DescribeConfigsRequest;
    type Output = BTreeMap<String, Option<String>>;

    const VERSIONS: Versions = Versions::new(1, 4);

    fn route(&self) -> Route {
        Route::AnyBroker
    }

    fn request(&self, _within: Duration) -> DescribeConfigsRequest {
        let keys =
            (!self.keys.is_empty()).then(|| self.keys.iter().map(|key| StrBytes::from_string(key.clone())).collect());
        DescribeConfigsRequest::default().with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(TOPIC)
                .with_resource_name(StrBytes::from_string(self.topic.clone()))
                .with_configuration_keys(keys),
        ])
    }

    fn read(
        &mut self,
        response: DescribeConfigsResponse,
    ) -> Result<Reading<BTreeMap<String, Option<String>>>, ClientError> {
        let described = response
            .results
            .iter()
            .find(|result| result.resource_type == TOPIC && result.resource_name.as_str() == self.topic)
            .ok_or_else(|| ClientError::Protocol(format!("no configs for topic {}", self.topic)))?;
        if let Some(error) = ClientError::broker(described.error_code) {
            return Err(error);
        }
        Ok(Reading::Done(
            described
                .configs
                .iter()
                .map(|config| (config.name.to_string(), config.value.as_ref().map(ToString::to_string)))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_wire::ResponseError;
    use gaslightmq_wire::messages::describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult};

    use super::*;

    fn describe(keys: &[&str]) -> DescribeTopicConfigs {
        DescribeTopicConfigs {
            topic: "orders".into(),
            keys: keys.iter().map(ToString::to_string).collect(),
        }
    }

    fn result(resource_type: i8, topic: &'static str) -> DescribeConfigsResult {
        DescribeConfigsResult::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_static_str(topic))
    }

    fn config(name: &'static str, value: Option<&'static str>) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult::default()
            .with_name(StrBytes::from_static_str(name))
            .with_value(value.map(StrBytes::from_static_str))
    }

    #[test]
    fn the_request_names_the_topic_and_the_configs_it_wants() {
        let some = describe(&["retention.ms"]).request(Duration::ZERO);
        let all = describe(&[]).request(Duration::ZERO);

        assert_eq!(some.resources[0].resource_type, 2);
        assert_eq!(some.resources[0].resource_name.as_str(), "orders");
        assert_eq!(
            some.resources[0].configuration_keys,
            Some(vec![StrBytes::from_static_str("retention.ms")])
        );
        assert_eq!(all.resources[0].configuration_keys, None);
        assert_eq!(describe(&[]).route(), Route::AnyBroker);
    }

    #[test]
    fn the_configs_of_the_topic_come_back_by_name() {
        let response = DescribeConfigsResponse::default().with_results(vec![
            result(4, "orders"),
            result(TOPIC, "orders").with_configs(vec![
                config("retention.ms", Some("604800000")),
                config("ssl.keystore.password", None),
            ]),
        ]);

        let read = describe(&[]).read(response).unwrap();

        let expected = BTreeMap::from([
            ("retention.ms".to_owned(), Some("604800000".to_owned())),
            ("ssl.keystore.password".to_owned(), None),
        ]);
        assert_eq!(read, Reading::Done(expected));
    }

    #[test]
    fn the_error_of_the_topic_is_the_failure_of_the_call() {
        let response = DescribeConfigsResponse::default().with_results(vec![
            result(TOPIC, "orders").with_error_code(ResponseError::UnknownTopicOrPartition.code()),
        ]);

        assert_eq!(
            describe(&[]).read(response),
            Err(ClientError::Broker {
                error: ResponseError::UnknownTopicOrPartition
            })
        );
    }

    #[test]
    fn a_response_without_the_topic_makes_no_sense() {
        let response = DescribeConfigsResponse::default().with_results(vec![result(TOPIC, "payments")]);

        assert_eq!(
            describe(&[]).read(response),
            Err(ClientError::Protocol("no configs for topic orders".into()))
        );
    }
}
