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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
    Name(String),
    Code(i16),
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
