# GaslightMQ Testcontainers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `gaslightmq-scenario` and `gaslightmq-testcontainers` crates so external Rust projects can spin up a real GaslightMQ container in integration tests.

**Architecture:** Extract scenario schema types into a lightweight `gaslightmq-scenario` crate (serde only, no kafka-protocol). The broker re-uses these types and adds conversion logic. A separate `gaslightmq-testcontainers` crate implements the testcontainers `Image` trait, mounting scenario YAML into the container.

**Tech Stack:** Rust, serde/serde_yaml, testcontainers 0.27

---

## File Structure

### New files
- `gaslightmq-scenario/Cargo.toml` — scenario schema crate manifest
- `gaslightmq-scenario/src/lib.rs` — all schema types (Scenario, TopicConfig, ChaosConfig, ErrorCode, etc.)
- `gaslightmq-testcontainers/Cargo.toml` — testcontainers crate manifest
- `gaslightmq-testcontainers/src/lib.rs` — GaslightMq struct, Image impl, From/TryFrom impls

### Modified files
- `Cargo.toml` — add new crates to workspace members
- `gaslightmq/Cargo.toml` — add `gaslightmq-scenario` dependency
- `gaslightmq/src/scenario.rs` — remove schema types, import from gaslightmq-scenario, keep only `into_broker_config()` and `ErrorCode → ResponseError` conversion
- `gaslightmq/src/lib.rs` — update re-exports to include gaslightmq-scenario types

---

### Task 1: Create `gaslightmq-scenario` crate with schema types

**Files:**
- Create: `gaslightmq-scenario/Cargo.toml`
- Create: `gaslightmq-scenario/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create `gaslightmq-scenario/Cargo.toml`**

```toml
[package]
name = "gaslightmq-scenario"
version.workspace = true
edition.workspace = true

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
```

- [ ] **Step 2: Create `gaslightmq-scenario/src/lib.rs` with all schema types**

Extract from `gaslightmq/src/scenario.rs` lines 17–112, adding `Serialize` to all derives and replacing `ResponseError` with the new `ErrorCode` type:

```rust
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum GeneratorConfig {
    Json { template: String },
    RandomBytes { size: usize },
}
```

Wait — `GeneratorConfig::Json` contains a `String`, so it can't derive `Copy`. Fix:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum GeneratorConfig {
    Json { template: String },
    RandomBytes { size: usize },
}
```

Continue with the remaining enums:

```rust
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
```

- [ ] **Step 3: Add `gaslightmq-scenario` to workspace members**

In root `Cargo.toml`, change:
```
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-testing", "gaslightmq-kafka-client/*"]
```
to:
```
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-scenario", "gaslightmq-testing", "gaslightmq-kafka-client/*"]
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p gaslightmq-scenario`
Expected: compiles with no errors

- [ ] **Step 5: Commit**

```bash
git add gaslightmq-scenario/ Cargo.toml
git commit -m "feat: add gaslightmq-scenario crate with schema types"
```

---

### Task 2: Test `gaslightmq-scenario` serde round-trip

**Files:**
- Modify: `gaslightmq-scenario/src/lib.rs` (add tests module)

- [ ] **Step 1: Write deserialization tests**

Add to the bottom of `gaslightmq-scenario/src/lib.rs`:

```rust
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
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p gaslightmq-scenario`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add gaslightmq-scenario/src/lib.rs
git commit -m "test: add gaslightmq-scenario serde round-trip tests"
```

---

### Task 3: Update `gaslightmq` broker to use `gaslightmq-scenario`

**Files:**
- Modify: `gaslightmq/Cargo.toml`
- Modify: `gaslightmq/src/scenario.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Add `gaslightmq-scenario` dependency to broker**

In `gaslightmq/Cargo.toml`, add to `[dependencies]`:

```toml
gaslightmq-scenario = { path = "../gaslightmq-scenario" }
```

- [ ] **Step 2: Rewrite `gaslightmq/src/scenario.rs`**

Replace the entire file. Remove all serde model structs (they now live in `gaslightmq-scenario`). Keep only:
- `use` imports from `gaslightmq_scenario`
- `ErrorCode → ResponseError` conversion
- `From<ApiRef> for Api` and `From<AfterSequenceRef> for AfterSequence` conversions
- `chaos_action_from_behavior` and `build_sequence_from_chaos` helper functions
- `Scenario::into_broker_config()` — implemented as a free function or extension since `Scenario` is now an external type

Since `Scenario` is defined in `gaslightmq-scenario`, we cannot add inherent methods to it from the broker crate. Convert `into_broker_config` to a free function:

```rust
use std::collections::HashMap;
use std::time::Duration;

use gaslightmq_scenario::{
    AfterSequenceRef, ApiRef, BehaviorRef, ChaosConfig, ErrorCode, GeneratorConfig, Scenario,
    SequenceStepConfig, TopicMode,
};
use kafka_protocol::error::ResponseError;

use crate::builder::BrokerConfig;
use crate::chaos::{AfterSequence, ChaosAction, Sequence, Step};
use crate::generator::{JsonTemplate, RandomBytes};
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::Api;
use crate::topic::{BlackHole, Buffer, Topic};

pub fn into_broker_config(scenario: Scenario, bind_addr: &str) -> BrokerConfig {
    let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();

    for topic_cfg in scenario.topics {
        let base: Box<dyn Topic> = match topic_cfg.mode {
            TopicMode::Buffer => Box::new(Buffer::new(topic_cfg.partitions)),
            TopicMode::BlackHole => Box::new(BlackHole::new(topic_cfg.partitions)),
        };

        let mut topic: Box<dyn Topic> = base;
        for chaos in &topic_cfg.chaos {
            let api: Api = chaos.api.into();
            let sequence = build_sequence_from_chaos(chaos);
            topic = Box::new(ChaosLayer::new(topic, api, sequence));
        }

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

fn resolve_error_code(code: &ErrorCode) -> ResponseError {
    match code {
        ErrorCode::Code(n) => {
            ResponseError::try_from_code(*n).unwrap_or(ResponseError::UnknownServerError)
        }
        ErrorCode::Name(name) => {
            for code in -1..=200i16 {
                if let Some(err) = ResponseError::try_from_code(code)
                    && err.to_string() == *name
                {
                    return err;
                }
            }
            ResponseError::UnknownServerError
        }
    }
}

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

fn chaos_action_from_behavior(
    behavior: BehaviorRef,
    delay_ms: Option<u64>,
    error_code: Option<&ErrorCode>,
) -> ChaosAction {
    match behavior {
        BehaviorRef::Success => ChaosAction::Proceed,
        BehaviorRef::Delay => {
            let ms = delay_ms.unwrap_or(0);
            ChaosAction::Delay(Duration::from_millis(ms))
        }
        BehaviorRef::Timeout => ChaosAction::Timeout,
        BehaviorRef::Error => {
            let resolved = error_code
                .map(resolve_error_code)
                .unwrap_or(ResponseError::UnknownServerError);
            ChaosAction::Error(resolved)
        }
    }
}

fn build_sequence_from_chaos(chaos: &ChaosConfig) -> Sequence {
    if let Some(steps) = &chaos.sequence {
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        let steps: Vec<Step> = steps
            .iter()
            .map(|s| {
                let action =
                    chaos_action_from_behavior(s.behavior, s.delay_ms, s.error_code.as_ref());
                Step::new(action, s.repeat)
            })
            .collect();
        Sequence::new(steps, after)
    } else {
        let behavior = chaos.behavior.unwrap_or(BehaviorRef::Success);
        let action =
            chaos_action_from_behavior(behavior, chaos.delay_ms, chaos.error_code.as_ref());
        let step = Step::new(action, chaos.repeat);
        let after = chaos
            .after_sequence
            .map(AfterSequence::from)
            .unwrap_or(AfterSequence::Stop);
        Sequence::new(vec![step], after)
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

    #[test]
    fn scenario_to_broker_config() {
        let scenario = Scenario::from_yaml(BASIC_YAML).unwrap();
        let config = into_broker_config(scenario, "127.0.0.1:9092");

        assert_eq!(config.topics.len(), 2);
        assert!(config.topics.contains_key("orders"));
        assert!(config.topics.contains_key("events"));
        assert_eq!(config.bind_addr, "127.0.0.1:9092");
    }

    #[test]
    fn resolve_error_code_by_name() {
        let code = ErrorCode::Name("NotLeaderOrFollower".to_string());
        assert_eq!(resolve_error_code(&code), ResponseError::NotLeaderOrFollower);
    }

    #[test]
    fn resolve_error_code_by_number() {
        let code = ErrorCode::Code(6);
        assert_eq!(resolve_error_code(&code), ResponseError::NotLeaderOrFollower);
    }
}
```

- [ ] **Step 3: Update `gaslightmq/src/lib.rs` re-exports**

Change line 20 from:
```rust
pub use scenario::Scenario;
```
to:
```rust
pub use gaslightmq_scenario::Scenario;
pub use scenario::into_broker_config;
```

- [ ] **Step 4: Update `bins/gmq/src/main.rs`**

Change line 22–23 from:
```rust
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::Broker::start(scenario.into_broker_config(&cli.bind)).await?
```
to:
```rust
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::Broker::start(gaslightmq::into_broker_config(scenario, &cli.bind)).await?
```

- [ ] **Step 5: Verify everything compiles and tests pass**

Run: `cargo test --workspace`
Expected: all existing tests pass, no regressions

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/Cargo.toml gaslightmq/src/scenario.rs gaslightmq/src/lib.rs bins/gmq/src/main.rs
git commit -m "refactor: use gaslightmq-scenario crate in broker"
```

---

### Task 4: Create `gaslightmq-testcontainers` crate

**Files:**
- Create: `gaslightmq-testcontainers/Cargo.toml`
- Create: `gaslightmq-testcontainers/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create `gaslightmq-testcontainers/Cargo.toml`**

```toml
[package]
name = "gaslightmq-testcontainers"
version.workspace = true
edition.workspace = true

[dependencies]
gaslightmq-scenario = { path = "../gaslightmq-scenario" }
testcontainers = "0.27"
```

- [ ] **Step 2: Add to workspace members**

In root `Cargo.toml`, change:
```
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-scenario", "gaslightmq-testing", "gaslightmq-kafka-client/*"]
```
to:
```
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-scenario", "gaslightmq-testcontainers", "gaslightmq-testing", "gaslightmq-kafka-client/*"]
```

- [ ] **Step 3: Create `gaslightmq-testcontainers/src/lib.rs`**

```rust
use std::path::{Path, PathBuf};

use gaslightmq_scenario::Scenario;
use testcontainers::core::{ContainerPort, CopyToContainer, WaitFor};
use testcontainers::Image;

const GASLIGHTMQ_PORT: ContainerPort = ContainerPort::Tcp(9092);
const SCENARIO_PATH: &str = "/etc/gaslightmq/scenario.yaml";

#[derive(Debug, Clone)]
pub struct GaslightMq {
    tag: String,
    copy_to: Vec<CopyToContainer>,
    cmd: Vec<String>,
}

impl Default for GaslightMq {
    fn default() -> Self {
        Self {
            tag: "latest".to_string(),
            copy_to: Vec::new(),
            cmd: Vec::new(),
        }
    }
}

impl GaslightMq {
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = tag.into();
        self
    }

    fn with_scenario_yaml(mut self, yaml: String) -> Self {
        self.copy_to = vec![CopyToContainer::new(
            yaml.into_bytes(),
            SCENARIO_PATH,
        )];
        self.cmd = vec![
            "--scenario".to_string(),
            SCENARIO_PATH.to_string(),
        ];
        self
    }
}

impl Image for GaslightMq {
    fn name(&self) -> &str {
        "ghcr.io/cruxy-eu/gaslightmq"
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::healthcheck()]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[GASLIGHTMQ_PORT]
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        &self.copy_to
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>> > {
        &self.cmd
    }
}

impl From<Scenario> for GaslightMq {
    fn from(scenario: Scenario) -> Self {
        let yaml = scenario
            .to_yaml()
            .expect("Scenario serialization should not fail");
        Self::default().with_scenario_yaml(yaml)
    }
}

impl From<String> for GaslightMq {
    fn from(yaml: String) -> Self {
        Self::default().with_scenario_yaml(yaml)
    }
}

impl From<&str> for GaslightMq {
    fn from(yaml: &str) -> Self {
        Self::default().with_scenario_yaml(yaml.to_string())
    }
}

impl TryFrom<&Path> for GaslightMq {
    type Error = std::io::Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let yaml = std::fs::read_to_string(path)?;
        Ok(Self::default().with_scenario_yaml(yaml))
    }
}

impl TryFrom<PathBuf> for GaslightMq {
    type Error = std::io::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Self::try_from(path.as_path())
    }
}
```

Note on `ready_conditions`: The plan uses `WaitFor::healthcheck()`. If the image doesn't have a HEALTHCHECK directive, switch to a simple log message wait or a short duration. The exact readiness strategy may need adjustment when testing against the real image. An alternative is:
```rust
vec![WaitFor::message_on_stderr("listening on")]
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p gaslightmq-testcontainers`
Expected: compiles with no errors. If `CopyToContainer::new` signature differs, adjust the constructor call per the testcontainers 0.27 API.

- [ ] **Step 5: Commit**

```bash
git add gaslightmq-testcontainers/ Cargo.toml
git commit -m "feat: add gaslightmq-testcontainers crate with Image impl"
```

---

### Task 5: Test `gaslightmq-testcontainers`

**Files:**
- Modify: `gaslightmq-testcontainers/src/lib.rs` (add tests module)

- [ ] **Step 1: Write unit tests for construction and Image trait**

Add to the bottom of `gaslightmq-testcontainers/src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_has_no_scenario() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.tag, "latest");
        assert!(gmq.copy_to.is_empty());
        assert!(gmq.cmd.is_empty());
    }

    #[test]
    fn from_string_sets_scenario() {
        let yaml = "topics:\n  - name: t\n    mode: buffer".to_string();
        let gmq = GaslightMq::from(yaml);
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn from_str_sets_scenario() {
        let gmq = GaslightMq::from("topics:\n  - name: t\n    mode: buffer");
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn from_scenario_struct() {
        let scenario = Scenario::from_yaml("topics:\n  - name: t\n    mode: buffer").unwrap();
        let gmq = GaslightMq::from(scenario);
        assert_eq!(gmq.copy_to.len(), 1);
        assert_eq!(gmq.cmd, vec!["--scenario", SCENARIO_PATH]);
    }

    #[test]
    fn with_tag_overrides_default() {
        let gmq = GaslightMq::default().with_tag("0.2.0");
        assert_eq!(gmq.tag, "0.2.0");
    }

    #[test]
    fn try_from_path_reads_file() {
        let dir = std::env::temp_dir().join("gaslightmq-test-scenario.yaml");
        std::fs::write(&dir, "topics:\n  - name: t\n    mode: buffer").unwrap();

        let gmq = GaslightMq::try_from(dir.as_path()).unwrap();
        assert_eq!(gmq.copy_to.len(), 1);

        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn try_from_path_returns_error_for_missing_file() {
        let result = GaslightMq::try_from(Path::new("/nonexistent/scenario.yaml"));
        assert!(result.is_err());
    }

    #[test]
    fn image_name_and_tag() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.name(), "ghcr.io/cruxy-eu/gaslightmq");
        assert_eq!(gmq.tag(), "latest");
    }

    #[test]
    fn image_exposes_9092() {
        let gmq = GaslightMq::default();
        assert_eq!(gmq.expose_ports(), &[GASLIGHTMQ_PORT]);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p gaslightmq-testcontainers`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add gaslightmq-testcontainers/src/lib.rs
git commit -m "test: add gaslightmq-testcontainers unit tests"
```

---

### Task 6: Final verification

- [ ] **Step 1: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all tests pass across all crates

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace`
Expected: no warnings

- [ ] **Step 3: Verify clean build from scratch**

Run: `cargo clean && cargo build --workspace`
Expected: clean build, no errors
