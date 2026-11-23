//! The admin.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use gaslightmq_kafka_client_api::{Admin, ClientError, TopicConfigs};

use crate::configuration::{AdminConfiguration, RoleNames};
use crate::io::access::ClusterAccess;
use crate::logic::configs::DescribeTopicConfigs;

/// The names of the admins that are configured without a client ID.
static NAMES: RoleNames = RoleNames::new("adminclient");

/// An admin: it asks the cluster how it is set up.
///
/// It has its own connections, its own view of the cluster and its own `client.id`.
/// Two admins share nothing, and neither does an admin with a producer or a consumer.
///
/// A call that fails returns a [`ClientError`],
/// through [`describe_topic_configs`](Self::describe_topic_configs) and through the [`Admin`] trait alike.
pub struct KafkaAdmin {
    configuration: AdminConfiguration,
    cluster: ClusterAccess,
}

impl KafkaAdmin {
    /// An admin with `configuration`. It connects when it first asks something.
    ///
    /// An admin that is configured without a client ID names itself `adminclient-1`,
    /// and the next one `adminclient-2`, as Kafka's Java admin client does.
    pub fn new(mut configuration: AdminConfiguration) -> Self {
        configuration.client.client_id = NAMES.client_id(&configuration.client.client_id);
        Self {
            cluster: ClusterAccess::new(configuration.client.clone()),
            configuration,
        }
    }

    /// What the admin runs with. Its client ID is the one the admin sends.
    pub fn configuration(&self) -> &AdminConfiguration {
        &self.configuration
    }

    /// The configuration of a topic. `keys` limits the result to those config names; empty means all.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] with the error code the cluster answers with for the topic, at once:
    /// a topic that the cluster does not have is `UNKNOWN_TOPIC_OR_PARTITION`, as with Kafka's admin client.
    /// Otherwise the failure of the client or of its connection,
    /// or [`ClientError::OutOfTime`] when requests stayed without an answer until the default API timeout.
    pub async fn describe_topic_configs(
        &self,
        topic: &str,
        keys: &[&str],
    ) -> Result<BTreeMap<String, Option<String>>, ClientError> {
        let describe = DescribeTopicConfigs {
            topic: topic.to_owned(),
            keys: keys.iter().map(ToString::to_string).collect(),
        };
        self.cluster
            .call(
                describe,
                self.configuration.default_api_timeout,
                Some(self.configuration.retries),
            )
            .await
    }
}

/// An admin with the default configuration, which starts from `bootstrap`.
impl From<SocketAddr> for KafkaAdmin {
    fn from(bootstrap: SocketAddr) -> Self {
        Self::new(AdminConfiguration::new(bootstrap))
    }
}

impl Admin for KafkaAdmin {
    /// # Errors
    ///
    /// As [`KafkaAdmin::describe_topic_configs`].
    async fn describe_topic_configs(&self, topic: &str, keys: &[&str]) -> Result<TopicConfigs, ClientError> {
        let configs = KafkaAdmin::describe_topic_configs(self, topic, keys).await?;
        Ok(TopicConfigs { configs })
    }
}
