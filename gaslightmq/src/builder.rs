use std::collections::HashMap;
use crate::broker::Broker;
use crate::chaos::ChaosBehavior;
use crate::generator::Generator;
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::Api;
use crate::topic::Topic;

pub struct BrokerConfig {
    pub topics: HashMap<String, Box<dyn Topic>>,
    pub bind_addr: String,
}

pub struct BrokerBuilder {
    topics: HashMap<String, Box<dyn Topic>>,
    bind_addr: String,
}

impl BrokerBuilder {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            bind_addr: "127.0.0.1:9092".to_string(),
        }
    }

    pub fn topic<T: Topic + 'static>(self, name: &str, topic: T) -> TopicBuilder<T> {
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            name: name.to_string(),
            topic,
        }
    }

    pub fn bind(mut self, addr: &str) -> Self {
        self.bind_addr = addr.to_string();
        self
    }

    fn build_config(self) -> BrokerConfig {
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
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
            name: self.name,
            topic: GeneratorLayer::new(self.topic, generator),
        }
    }

    pub fn topic<T2: Topic + 'static>(mut self, name: &str, topic: T2) -> TopicBuilder<T2> {
        self.topics.insert(self.name, Box::new(self.topic));
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            name: name.to_string(),
            topic,
        }
    }

    pub fn bind(mut self, addr: &str) -> BrokerBuilder {
        self.topics.insert(self.name, Box::new(self.topic));
        BrokerBuilder {
            topics: self.topics,
            bind_addr: addr.to_string(),
        }
    }

    fn build_config(mut self) -> BrokerConfig {
        self.topics.insert(self.name, Box::new(self.topic));
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
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
}
