use std::collections::HashMap;
use std::time::Duration;

use kafka_protocol::error::ResponseError;
use serde::Deserialize;

use crate::builder::BrokerConfig;
use crate::chaos::{AfterSequence, ChaosAction, Sequence, Step};
use crate::generator::{JsonTemplate, RandomBytes};
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::Api;
use crate::topic::{BlackHole, Buffer, Topic};

// ── Serde models ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    pub mode: TopicMode,
    #[serde(default)]
    pub chaos: Vec<ChaosConfig>,
    #[serde(default)]
    pub generators: Vec<GeneratorConfig>,
}

fn default_partitions() -> i32 {
    1
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    Buffer,
    BlackHole,
}

#[derive(Debug, Deserialize)]
pub struct ChaosConfig {
    pub api: ApiRef,
    /// Single behavior (mutually exclusive with `sequence`).
    pub behavior: Option<BehaviorRef>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub error_code: Option<ResponseErrorRef>,
    #[serde(default = "default_repeat")]
    pub repeat: usize,
    /// Multi-step sequence (mutually exclusive with `behavior`).
    pub sequence: Option<Vec<SequenceStepConfig>>,
    #[serde(default)]
    pub after_sequence: Option<AfterSequenceRef>,
}

#[derive(Debug, Deserialize)]
pub struct SequenceStepConfig {
    pub behavior: BehaviorRef,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub error_code: Option<ResponseErrorRef>,
    #[serde(default = "default_repeat")]
    pub repeat: usize,
}

fn default_repeat() -> usize {
    1
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum GeneratorConfig {
    Json { template: String },
    RandomBytes { size: usize },
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ApiRef {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum BehaviorRef {
    Success,
    Delay,
    Timeout,
    Error,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AfterSequenceRef {
    Loop,
    Stop,
    RepeatLast,
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub enum ResponseErrorRef {
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
}

// ── Conversions ───────────────────────────────────────────────────────────────

impl From<ApiRef> for Api {
    fn from(r: ApiRef) -> Api {
        match r {
            ApiRef::Produce => Api::Produce,
            ApiRef::Fetch => Api::Fetch,
            ApiRef::ListOffsets => Api::ListOffsets,
            ApiRef::Metadata => Api::Metadata,
        }
    }
}

impl From<AfterSequenceRef> for AfterSequence {
    fn from(r: AfterSequenceRef) -> AfterSequence {
        match r {
            AfterSequenceRef::Loop => AfterSequence::Loop,
            AfterSequenceRef::Stop => AfterSequence::Stop,
            AfterSequenceRef::RepeatLast => AfterSequence::RepeatLast,
        }
    }
}

impl From<ResponseErrorRef> for ResponseError {
    fn from(r: ResponseErrorRef) -> ResponseError {
        match r {
            ResponseErrorRef::OffsetOutOfRange => ResponseError::OffsetOutOfRange,
            ResponseErrorRef::CorruptMessage => ResponseError::CorruptMessage,
            ResponseErrorRef::UnknownTopicOrPartition => ResponseError::UnknownTopicOrPartition,
            ResponseErrorRef::LeaderNotAvailable => ResponseError::LeaderNotAvailable,
            ResponseErrorRef::NotLeaderOrFollower => ResponseError::NotLeaderOrFollower,
            ResponseErrorRef::RequestTimedOut => ResponseError::RequestTimedOut,
        }
    }
}

// ── Helpers to build ChaosAction from config ──────────────────────────────────

fn chaos_action_from_behavior(
    behavior: BehaviorRef,
    delay_ms: Option<u64>,
    error_code: Option<ResponseErrorRef>,
) -> ChaosAction {
    match behavior {
        BehaviorRef::Success => ChaosAction::Proceed,
        BehaviorRef::Delay => {
            let ms = delay_ms.unwrap_or(0);
            ChaosAction::Delay(Duration::from_millis(ms))
        }
        BehaviorRef::Timeout => ChaosAction::Timeout,
        BehaviorRef::Error => {
            let code = error_code
                .map(ResponseError::from)
                .unwrap_or(ResponseError::UnknownServerError);
            ChaosAction::Error(code)
        }
    }
}

fn build_sequence_from_chaos(chaos: &ChaosConfig) -> Sequence {
    if let Some(steps) = &chaos.sequence {
        // Multi-step sequence
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        let steps: Vec<Step> = steps
            .iter()
            .map(|s| {
                let action =
                    chaos_action_from_behavior(s.behavior, s.delay_ms, s.error_code);
                Step::new(action, s.repeat)
            })
            .collect();
        Sequence::new(steps, after)
    } else {
        // Single behavior → one-step sequence
        let behavior = chaos.behavior.unwrap_or(BehaviorRef::Success);
        let action = chaos_action_from_behavior(behavior, chaos.delay_ms, chaos.error_code);
        let step = Step::new(action, chaos.repeat);
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        Sequence::new(vec![step], after)
    }
}

// ── Scenario::from_yaml / into_broker_config ──────────────────────────────────

impl Scenario {
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn into_broker_config(self, bind_addr: &str) -> BrokerConfig {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();

        for topic_cfg in self.topics {
            // 1. Base topic
            let base: Box<dyn Topic> = match topic_cfg.mode {
                TopicMode::Buffer => Box::new(Buffer::new(topic_cfg.partitions)),
                TopicMode::BlackHole => Box::new(BlackHole::new(topic_cfg.partitions)),
            };

            // 2. Wrap with chaos layers
            let mut topic: Box<dyn Topic> = base;
            for chaos in &topic_cfg.chaos {
                let api: Api = chaos.api.into();
                let sequence = build_sequence_from_chaos(chaos);
                topic = Box::new(ChaosLayer::new(topic, api, sequence));
            }

            // 3. Wrap with generator layers
            for gen_cfg in &topic_cfg.generators {
                topic = match gen_cfg {
                    GeneratorConfig::Json { template } => {
                        Box::new(GeneratorLayer::new(topic, JsonTemplate::new(template)))
                    }
                    GeneratorConfig::RandomBytes { size } => {
                        Box::new(GeneratorLayer::new(topic, RandomBytes::new(*size)))
                    }
                };
            }

            topics.insert(topic_cfg.name, topic);
        }

        BrokerConfig {
            topics,
            bind_addr: bind_addr.to_string(),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const BASIC_YAML: &str = r#"
topics:
  - name: orders
    partitions: 3
    mode: buffer
    chaos:
      - api: Fetch
        behavior: delay
        delay_ms: 2000
        repeat: 5
      - api: Produce
        behavior: error
        error_code: NotLeaderOrFollower
    generators:
      - format: json
        template: '{"id": "{{uuid}}"}'
  - name: events
    partitions: 1
    mode: blackhole
"#;

    const SEQUENCE_YAML: &str = r#"
topics:
  - name: events
    partitions: 1
    mode: blackhole
    chaos:
      - api: Fetch
        sequence:
          - behavior: success
            repeat: 3
          - behavior: timeout
          - behavior: error
            error_code: CorruptMessage
            repeat: 2
        after_sequence: loop
"#;

    #[test]
    fn deserialize_basic_scenario() {
        let scenario = Scenario::from_yaml(BASIC_YAML).expect("parse failed");

        assert_eq!(scenario.topics.len(), 2);

        let orders = &scenario.topics[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.partitions, 3);
        assert!(matches!(orders.mode, TopicMode::Buffer));
        assert_eq!(orders.chaos.len(), 2);
        assert_eq!(orders.generators.len(), 1);

        let fetch_chaos = &orders.chaos[0];
        assert_eq!(fetch_chaos.api, ApiRef::Fetch);
        assert!(matches!(fetch_chaos.behavior, Some(BehaviorRef::Delay)));
        assert_eq!(fetch_chaos.delay_ms, Some(2000));
        assert_eq!(fetch_chaos.repeat, 5);

        let produce_chaos = &orders.chaos[1];
        assert_eq!(produce_chaos.api, ApiRef::Produce);
        assert!(matches!(produce_chaos.behavior, Some(BehaviorRef::Error)));
        assert!(matches!(
            produce_chaos.error_code,
            Some(ResponseErrorRef::NotLeaderOrFollower)
        ));

        let events = &scenario.topics[1];
        assert_eq!(events.name, "events");
        assert_eq!(events.partitions, 1);
        assert!(matches!(events.mode, TopicMode::BlackHole));
        assert!(events.chaos.is_empty());
        assert!(events.generators.is_empty());
    }

    #[test]
    fn deserialize_sequence_chaos() {
        let scenario = Scenario::from_yaml(SEQUENCE_YAML).expect("parse failed");

        assert_eq!(scenario.topics.len(), 1);
        let topic = &scenario.topics[0];
        assert_eq!(topic.chaos.len(), 1);

        let chaos = &topic.chaos[0];
        assert_eq!(chaos.api, ApiRef::Fetch);
        assert!(chaos.behavior.is_none());

        let sequence = chaos.sequence.as_ref().expect("sequence missing");
        assert_eq!(sequence.len(), 3);

        assert!(matches!(sequence[0].behavior, BehaviorRef::Success));
        assert_eq!(sequence[0].repeat, 3);

        assert!(matches!(sequence[1].behavior, BehaviorRef::Timeout));
        assert_eq!(sequence[1].repeat, 1);

        assert!(matches!(sequence[2].behavior, BehaviorRef::Error));
        assert!(matches!(
            sequence[2].error_code,
            Some(ResponseErrorRef::CorruptMessage)
        ));
        assert_eq!(sequence[2].repeat, 2);

        assert!(matches!(chaos.after_sequence, Some(AfterSequenceRef::Loop)));
    }

    #[test]
    fn scenario_to_broker_config() {
        let scenario = Scenario::from_yaml(BASIC_YAML).expect("parse failed");
        let config = scenario.into_broker_config("127.0.0.1:9092");

        assert_eq!(config.topics.len(), 2);
        assert!(config.topics.contains_key("orders"));
        assert!(config.topics.contains_key("events"));
        assert_eq!(config.bind_addr, "127.0.0.1:9092");
    }
}
