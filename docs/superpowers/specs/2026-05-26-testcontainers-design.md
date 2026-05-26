# GaslightMQ Testcontainers Design

## Overview

Two new crates enabling external Rust projects to spin up a real GaslightMQ container in their integration tests via the testcontainers ecosystem.

## Crate 1: `gaslightmq-scenario`

### Purpose

Shared scenario schema types with serde support. No broker internals, no `kafka-protocol` dependency. Provides the data model that both the broker and testcontainers crate depend on.

### Dependencies

- `serde` (derive)
- `serde_yaml`

### Types

Extracted from `gaslightmq/src/scenario.rs`, with `Serialize` added alongside `Deserialize`:

- `Scenario { topics: Vec<TopicConfig> }`
- `TopicConfig { name: String, partitions: i32, mode: TopicMode, chaos: Vec<ChaosConfig>, generators: Vec<GeneratorConfig> }`
- `TopicMode` — `Buffer | BlackHole`
- `ChaosConfig { api: ApiRef, behavior: Option<BehaviorRef>, delay_ms: Option<u64>, timeout_ms: Option<u64>, error_code: Option<ErrorCode>, repeat: usize, sequence: Option<Vec<SequenceStepConfig>>, after_sequence: Option<AfterSequenceRef> }`
- `SequenceStepConfig { behavior: BehaviorRef, delay_ms: Option<u64>, timeout_ms: Option<u64>, error_code: Option<ErrorCode>, repeat: usize }`
- `GeneratorConfig` — `Json { template: String } | RandomBytes { size: usize }`
- `ApiRef` — `Produce | Fetch | ListOffsets | Metadata`
- `BehaviorRef` — `Success | Delay | Timeout | Error`
- `AfterSequenceRef` — `Loop | Stop | RepeatLast`
- `ErrorCode` — `Name(String) | Code(i16)` (new type, replaces `ResponseError` in the schema)

### API

- `Scenario::from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error>` — convenience parser
- All types derive `Debug, Serialize, Deserialize`
- Enums derive `Clone, Copy` where applicable (`ErrorCode` gets `Clone` only since it contains `String`)

### What stays in the broker

- `Scenario::into_broker_config()` — converts scenario types into internal broker config using chaos layers, generator layers, and topic implementations
- `ErrorCode -> ResponseError` conversion — the broker maps `ErrorCode::Name` and `ErrorCode::Code` to `kafka_protocol::error::ResponseError`
- The custom `ResponseError` serde visitor is replaced by the `ErrorCode` type

## Crate 2: `gaslightmq-testcontainers`

### Purpose

Testcontainers `Image` implementation for GaslightMQ, enabling external Rust projects to run GaslightMQ in integration tests.

### Dependencies

- `testcontainers`
- `gaslightmq-scenario`

### Struct

```rust
pub struct GaslightMq {
    tag: String,
    scenario_yaml: Option<String>,
}
```

### Image implementation

- `name()` — `"ghcr.io/cruxy-eu/gaslightmq"`
- `tag()` — `self.tag` (default: `"latest"`)
- `ready_conditions()` — TCP check on port 9092
- `expose_ports()` — `[9092]`
- `copy_to_sources()` — when scenario is set, copies YAML content to `/etc/gaslightmq/scenario.yaml`
- `cmd()` — when scenario is set, returns `["--scenario", "/etc/gaslightmq/scenario.yaml"]`

### Construction

- `Default` — no scenario, tag `"latest"`
- `From<Scenario>` — serializes `Scenario` to YAML via `serde_yaml`, stores as `scenario_yaml`
- `From<String>` — raw YAML string
- `From<&str>` — raw YAML string (cloned)
- `TryFrom<&Path>` — reads file contents, returns `io::Result<Self>`
- `TryFrom<PathBuf>` — delegates to `TryFrom<&Path>`
- `with_tag(self, tag: impl Into<String>) -> Self` — override image tag

### Usage

```rust
use gaslightmq_testcontainers::GaslightMq;
use gaslightmq_scenario::Scenario;
use testcontainers::runners::AsyncRunner;

// From a Scenario struct
let scenario = Scenario::from_yaml(r#"
    topics:
      - name: orders
        partitions: 3
        mode: buffer
"#).unwrap();
let container = GaslightMq::from(scenario).start().await.unwrap();
let port = container.get_host_port_ipv4(9092).await.unwrap();

// From raw YAML
let container = GaslightMq::from("topics:\n  - name: t\n    mode: buffer")
    .start().await.unwrap();

// Default (no scenario)
let container = GaslightMq::default().start().await.unwrap();

// With tag override
let container = GaslightMq::default()
    .with_tag("0.2.0")
    .start().await.unwrap();
```

## Workspace changes

- Add `gaslightmq-scenario` and `gaslightmq-testcontainers` to workspace members in root `Cargo.toml`
- `gaslightmq` (broker) depends on `gaslightmq-scenario`, removes serde type definitions from `scenario.rs`, keeps `into_broker_config()` and `ErrorCode -> ResponseError` conversion
- `bins/gmq` unchanged

## Out of scope

- Typed scenario builder API (future addition on `gaslightmq-scenario`)
- Kafka client re-export
- Custom helper methods beyond construction and tag
- Local Dockerfile build support (future addition — currently registry-only, local build fallback deferred)
