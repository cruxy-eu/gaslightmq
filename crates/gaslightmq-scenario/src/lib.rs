use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Scenario {
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    Buffer,
    BlackHole,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub api: ApiRef,
    pub behavior: Option<BehaviorRef>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub error_code: Option<ErrorCode>,
    #[serde(default = "default_repeat")]
    pub repeat: usize,
    pub sequence: Option<Vec<SequenceStepConfig>>,
    #[serde(default)]
    pub after_sequence: Option<AfterSequenceRef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SequenceStepConfig {
    pub behavior: BehaviorRef,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub error_code: Option<ErrorCode>,
    #[serde(default = "default_repeat")]
    pub repeat: usize,
}

fn default_repeat() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ErrorCode {
    Code(i16),
    Name(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum GeneratorConfig {
    Json { template: String },
    RandomBytes { size: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApiRef {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BehaviorRef {
    Success,
    Delay,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AfterSequenceRef {
    Loop,
    Stop,
    RepeatLast,
}

impl Scenario {
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }
}

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
        let scenario = Scenario::from_yaml(BASIC_YAML).unwrap();
        assert_eq!(scenario.topics.len(), 2);

        let orders = &scenario.topics[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.partitions, 3);
        assert!(matches!(orders.mode, TopicMode::Buffer));
        assert_eq!(orders.chaos.len(), 2);
        assert_eq!(orders.generators.len(), 1);
    }

    #[test]
    fn deserialize_sequence_scenario() {
        let scenario = Scenario::from_yaml(SEQUENCE_YAML).unwrap();
        assert_eq!(scenario.topics.len(), 1);

        let chaos = &scenario.topics[0].chaos[0];
        assert!(chaos.behavior.is_none());
        let seq = chaos.sequence.as_ref().unwrap();
        assert_eq!(seq.len(), 3);
        assert!(matches!(chaos.after_sequence, Some(AfterSequenceRef::Loop)));
    }

    #[test]
    fn error_code_deserializes_from_string() {
        let scenario = Scenario::from_yaml(BASIC_YAML).unwrap();
        let error_code = scenario.topics[0].chaos[1].error_code.as_ref().unwrap();
        assert!(matches!(error_code, ErrorCode::Name(s) if s == "NotLeaderOrFollower"));
    }

    #[test]
    fn error_code_deserializes_from_integer() {
        let yaml = r#"
topics:
  - name: t
    mode: blackhole
    chaos:
      - api: Fetch
        behavior: error
        error_code: 6
"#;
        let scenario = Scenario::from_yaml(yaml).unwrap();
        let error_code = scenario.topics[0].chaos[0].error_code.as_ref().unwrap();
        assert!(matches!(error_code, ErrorCode::Code(6)));
    }

    #[test]
    fn default_partitions_is_one() {
        let yaml = r#"
topics:
  - name: t
    mode: buffer
"#;
        let scenario = Scenario::from_yaml(yaml).unwrap();
        assert_eq!(scenario.topics[0].partitions, 1);
    }

    #[test]
    fn serialize_then_deserialize_round_trip() {
        let scenario = Scenario::from_yaml(BASIC_YAML).unwrap();
        let yaml = scenario.to_yaml().unwrap();
        let round_tripped = Scenario::from_yaml(&yaml).unwrap();

        assert_eq!(round_tripped.topics.len(), 2);
        assert_eq!(round_tripped.topics[0].name, "orders");
        assert_eq!(round_tripped.topics[0].partitions, 3);
        assert_eq!(round_tripped.topics[1].name, "events");
    }
}
