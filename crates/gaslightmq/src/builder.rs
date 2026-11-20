use std::collections::HashMap;
use crate::broker::Broker;
use crate::chaos::ChaosBehavior;
use crate::generator::Generator;
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::Api;
use crate::topic::Topic;
use gaslightmq_scenario::TopicMode;

pub struct BrokerConfig {
    pub topics: HashMap<String, Box<dyn Topic>>,
    pub bind_addr: String,
    pub default_topic_mode: TopicMode,
}

pub struct BrokerBuilder {
    topics: HashMap<String, Box<dyn Topic>>,
    bind_addr: String,
    default_topic_mode: TopicMode,
}

impl BrokerBuilder {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            bind_addr: "127.0.0.1:9092".to_string(),
            default_topic_mode: TopicMode::Buffer,
        }
    }

    pub(crate) fn from_topics(topics: HashMap<String, Box<dyn Topic>>) -> Self {
        Self {
            topics,
            bind_addr: "127.0.0.1:9092".to_string(),
            default_topic_mode: TopicMode::Buffer,
        }
    }

    pub fn default_topic_mode(mut self, mode: TopicMode) -> Self {
        self.default_topic_mode = mode;
        self
    }

    pub fn topic<T: Topic + 'static>(self, name: &str, topic: T) -> TopicBuilder<T> {
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
            name: name.to_string(),
            topic,
        }
    }

    pub fn bind(mut self, addr: &str) -> Self {
        self.bind_addr = addr.to_string();
        self
    }

    pub fn build_config(self) -> BrokerConfig {
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
        }
    }

    pub async fn start(self) -> std::io::Result<Broker> {
        Broker::start(self.build_config()).await
    }
}

impl Default for BrokerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<BrokerBuilder> for BrokerConfig {
    fn from(b: BrokerBuilder) -> Self {
        b.build_config()
    }
}

impl<T: Topic + 'static> From<TopicBuilder<T>> for BrokerConfig {
    fn from(b: TopicBuilder<T>) -> Self {
        b.build_config()
    }
}

pub struct TopicBuilder<T> {
    topics: HashMap<String, Box<dyn Topic>>,
    bind_addr: String,
    default_topic_mode: TopicMode,
    name: String,
    topic: T,
}

impl<T: Topic + 'static> TopicBuilder<T> {
    pub fn chaos<C: ChaosBehavior + 'static>(
        self,
        api: Api,
        behavior: C,
    ) -> TopicBuilder<ChaosLayer<T, C>> {
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
            name: self.name,
            topic: ChaosLayer::new(self.topic, api, behavior),
        }
    }

    pub fn generator<G: Generator + 'static>(
        self,
        generator: G,
    ) -> TopicBuilder<GeneratorLayer<T, G>> {
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
            name: self.name,
            topic: GeneratorLayer::new(self.topic, generator),
        }
    }

    pub fn topic<T2: Topic + 'static>(mut self, name: &str, topic: T2) -> TopicBuilder<T2> {
        self.topics.insert(self.name, Box::new(self.topic));
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
            name: name.to_string(),
            topic,
        }
    }

    pub fn bind(mut self, addr: &str) -> BrokerBuilder {
        self.topics.insert(self.name, Box::new(self.topic));
        BrokerBuilder {
            topics: self.topics,
            bind_addr: addr.to_string(),
            default_topic_mode: self.default_topic_mode,
        }
    }

    pub fn build_config(mut self) -> BrokerConfig {
        self.topics.insert(self.name, Box::new(self.topic));
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
            default_topic_mode: self.default_topic_mode,
        }
    }

    pub async fn start(self) -> std::io::Result<Broker> {
        Broker::start(self.build_config()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Api, BlackHole, Buffer, Delay, ErrorResponse, RandomBytes};
    use gaslightmq_scenario::TopicMode;
    use kafka_protocol::error::ResponseError;
    use std::time::Duration;

    #[test]
    fn builder_collects_topics() {
        let config = BrokerBuilder::new()
            .topic("orders", Buffer::new(3))
                .chaos(Api::Fetch, Delay::new(Duration::from_secs(2)))
            .topic("events", BlackHole::new(1))
            .build_config();

        assert_eq!(config.topics.len(), 2);
        assert!(config.topics.contains_key("orders"));
        assert!(config.topics.contains_key("events"));
    }

    #[test]
    fn builder_stacks_multiple_chaos_layers() {
        let config = BrokerBuilder::new()
            .topic("orders", Buffer::new(3))
                .chaos(Api::Fetch, Delay::new(Duration::from_secs(2)))
                .chaos(Api::Produce, ErrorResponse::new(ResponseError::RequestTimedOut))
            .build_config();

        assert_eq!(config.topics.len(), 1);
    }

    #[test]
    fn builder_accepts_generator() {
        let config = BrokerBuilder::new()
            .topic("orders", BlackHole::new(1))
                .generator(RandomBytes::new(64))
            .build_config();

        assert_eq!(config.topics.len(), 1);
    }

    #[test]
    fn builder_sets_bind_address() {
        let config = BrokerBuilder::new()
            .topic("t", BlackHole::new(1))
            .bind("127.0.0.1:9999")
            .build_config();

        assert_eq!(config.bind_addr, "127.0.0.1:9999");
    }

    #[test]
    fn builder_default_topic_mode_defaults_to_buffer() {
        let config = BrokerBuilder::new()
            .topic("t", BlackHole::new(1))
            .build_config();
        assert_eq!(config.default_topic_mode, TopicMode::Buffer);
    }

    #[test]
    fn builder_sets_custom_default_topic_mode() {
        let config = BrokerBuilder::new()
            .default_topic_mode(TopicMode::BlackHole)
            .topic("t", BlackHole::new(1))
            .build_config();
        assert_eq!(config.default_topic_mode, TopicMode::BlackHole);
    }
}
