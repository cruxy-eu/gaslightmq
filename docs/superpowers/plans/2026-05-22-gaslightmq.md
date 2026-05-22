# Gaslight MQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fake Kafka broker that speaks the real Kafka wire protocol, for testing chaos scenarios (delays, timeouts, error injection) against real Kafka clients.

**Architecture:** Cargo workspace with two crates — `gaslightmq` (library, all core logic) and `gmq` (thin CLI binary). Tower-style generic layers compose `Topic`, `ChaosBehavior`, and `Generator` traits. The broker listens on TCP, handles the Kafka protocol via `kafka-protocol` crate, and dispatches to registered topic impls. YAML scenario files provide declarative config; the Rust builder API provides programmatic control.

**Tech Stack:** Rust (edition 2024, toolchain 1.94.1), tokio, kafka-protocol, serde + serde_yaml, clap, fake

---

## File Structure

```
gaslightmq/                        # workspace root
├── Cargo.toml                     # workspace manifest
├── rust-toolchain.toml            # pin toolchain 1.94.1
├── gaslightmq/                    # library crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                 # public re-exports
│       ├── record.rs              # Record type, ProduceResult, FetchResult, ErrorCode
│       ├── topic.rs               # Topic trait (async via Pin<Box<dyn Future>>), BlackHole, Buffer
│       ├── chaos.rs               # ChaosAction, ChaosBehavior trait, Delay, Timeout, ErrorResponse, Sequence
│       ├── generator.rs           # Generator trait, RandomBytes, JsonTemplate
│       ├── layer.rs               # ChaosLayer<T,C>, GeneratorLayer<T,G> — both impl Topic
│       ├── builder.rs             # BrokerBuilder, TopicBuilder (typestate for fluent API)
│       ├── protocol.rs            # Kafka frame codec, header version mapping, request handlers
│       ├── broker.rs              # Broker struct, TCP accept loop, connection handling, shutdown
│       └── scenario.rs            # Scenario serde model, Broker::from_scenario
└── gmq/                           # binary crate
    ├── Cargo.toml
    └── src/
        └── main.rs                # CLI: --scenario, --bind
```

---

### Task 1: Workspace Scaffolding

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `gaslightmq/Cargo.toml`
- Create: `gaslightmq/src/lib.rs`
- Create: `gmq/Cargo.toml`
- Create: `gmq/src/main.rs`

- [ ] **Step 1: Create workspace Cargo.toml**

```toml
[workspace]
members = ["gaslightmq", "gmq"]
resolver = "2"

[workspace.package]
edition = "2024"
version = "0.1.0"
```

- [ ] **Step 2: Create rust-toolchain.toml**

```toml
[toolchain]
channel = "1.94.1"
```

- [ ] **Step 3: Create gaslightmq/Cargo.toml**

```toml
[package]
name = "gaslightmq"
version.workspace = true
edition.workspace = true

[dependencies]
bytes = "1"
kafka-protocol = "0.17"
tokio = { version = "1", features = ["net", "io-util", "time", "rt-multi-thread", "macros", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
fake = { version = "3", features = ["derive"] }
rand = "0.9"

[dev-dependencies]
tokio = { version = "1", features = ["test-util"] }
```

- [ ] **Step 4: Create gaslightmq/src/lib.rs**

```rust
pub mod record;
```

- [ ] **Step 5: Create gmq/Cargo.toml**

```toml
[package]
name = "gmq"
version.workspace = true
edition.workspace = true

[dependencies]
gaslightmq = { path = "../gaslightmq" }
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

- [ ] **Step 6: Create gmq/src/main.rs**

```rust
fn main() {
    println!("gmq");
}
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo build`
Expected: compiles successfully

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml rust-toolchain.toml gaslightmq/ gmq/
git commit -m "feat: scaffold workspace with gaslightmq lib and gmq binary"
```

---

### Task 2: Core Types

**Files:**
- Create: `gaslightmq/src/record.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Define Record, ProduceResult, FetchResult, ErrorCode**

`gaslightmq/src/record.rs`:

```rust
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: Some(value.into()),
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    None,
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    LeaderNotAvailable,
    NotLeaderForPartition,
    RequestTimedOut,
}

impl ErrorCode {
    pub fn code(self) -> i16 {
        match self {
            Self::None => 0,
            Self::OffsetOutOfRange => 1,
            Self::CorruptMessage => 2,
            Self::UnknownTopicOrPartition => 3,
            Self::LeaderNotAvailable => 5,
            Self::NotLeaderForPartition => 6,
            Self::RequestTimedOut => 7,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProduceResult {
    Success,
    Error(ErrorCode),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FetchResult {
    Empty,
    Records(Vec<Record>),
    Error(ErrorCode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    FindCoordinator,
    JoinGroup,
    SyncGroup,
    Heartbeat,
    ApiVersions,
}

pub type TopicFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
```

- [ ] **Step 2: Export from lib.rs**

`gaslightmq/src/lib.rs`:

```rust
pub mod record;

pub use record::{Api, ErrorCode, FetchResult, ProduceResult, Record, TopicFuture};
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build`
Expected: compiles successfully

- [ ] **Step 4: Commit**

```bash
git add gaslightmq/src/record.rs gaslightmq/src/lib.rs
git commit -m "feat: add core types — Record, ErrorCode, ProduceResult, FetchResult, Api"
```

---

### Task 3: Topic Trait + BlackHole

**Files:**
- Create: `gaslightmq/src/topic.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for Topic default behavior and BlackHole**

`gaslightmq/src/topic.rs` — start with the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use bytes::Bytes;

    #[tokio::test]
    async fn blackhole_returns_success_on_produce() {
        let topic = BlackHole::new(3);
        let records = vec![Record::new("hello")];
        let result = topic.on_produce(0, &records).await;
        assert_eq!(result, ProduceResult::Success);
    }

    #[tokio::test]
    async fn blackhole_returns_empty_on_fetch() {
        let topic = BlackHole::new(3);
        let result = topic.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn blackhole_reports_partition_count() {
        let topic = BlackHole::new(5);
        assert_eq!(topic.partitions(), 5);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `Topic`, `BlackHole` not defined

- [ ] **Step 3: Implement Topic trait and BlackHole**

Add above the tests in `gaslightmq/src/topic.rs`:

```rust
use crate::record::{FetchResult, ProduceResult, Record, TopicFuture};

pub trait Topic: Send + Sync {
    fn partitions(&self) -> i32 {
        1
    }

    fn on_produce<'a>(
        &'a self,
        _partition: i32,
        _records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async { ProduceResult::Success })
    }

    fn on_fetch(&self, _partition: i32, _offset: i64) -> TopicFuture<'_, FetchResult> {
        Box::pin(async { FetchResult::Empty })
    }
}

pub struct BlackHole {
    num_partitions: i32,
}

impl BlackHole {
    pub fn new(partitions: i32) -> Self {
        Self {
            num_partitions: partitions,
        }
    }
}

impl Topic for BlackHole {
    fn partitions(&self) -> i32 {
        self.num_partitions
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add to `gaslightmq/src/lib.rs`:

```rust
pub mod topic;

pub use topic::{BlackHole, Topic};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/topic.rs gaslightmq/src/lib.rs
git commit -m "feat: add Topic trait with async defaults and BlackHole implementation"
```

---

### Task 4: Buffer Topic

**Files:**
- Modify: `gaslightmq/src/topic.rs`

- [ ] **Step 1: Write tests for Buffer**

Add to the `tests` module in `gaslightmq/src/topic.rs`:

```rust
    #[tokio::test]
    async fn buffer_stores_and_retrieves_records() {
        let topic = Buffer::new(1);
        let records = vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ];
        let result = topic.on_produce(0, &records).await;
        assert_eq!(result, ProduceResult::Success);

        let result = topic.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ]));
    }

    #[tokio::test]
    async fn buffer_returns_empty_when_no_records() {
        let topic = Buffer::new(1);
        let result = topic.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn buffer_respects_offset() {
        let topic = Buffer::new(1);
        let records = vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
            Record::new("msg-2"),
        ];
        topic.on_produce(0, &records).await;

        let result = topic.on_fetch(0, 2).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-2"),
        ]));
    }

    #[tokio::test]
    async fn buffer_isolates_partitions() {
        let topic = Buffer::new(2);
        topic.on_produce(0, &[Record::new("p0")]).await;
        topic.on_produce(1, &[Record::new("p1")]).await;

        let r0 = topic.on_fetch(0, 0).await;
        let r1 = topic.on_fetch(1, 0).await;
        assert_eq!(r0, FetchResult::Records(vec![Record::new("p0")]));
        assert_eq!(r1, FetchResult::Records(vec![Record::new("p1")]));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `Buffer` not defined

- [ ] **Step 3: Implement Buffer**

Add to `gaslightmq/src/topic.rs`, before the tests module:

```rust
use std::collections::HashMap;
use std::sync::Mutex;

pub struct Buffer {
    num_partitions: i32,
    partitions: Mutex<HashMap<i32, Vec<Record>>>,
}

impl Buffer {
    pub fn new(partitions: i32) -> Self {
        Self {
            num_partitions: partitions,
            partitions: Mutex::new(HashMap::new()),
        }
    }
}

impl Topic for Buffer {
    fn partitions(&self) -> i32 {
        self.num_partitions
    }

    fn on_produce<'a>(
        &'a self,
        partition: i32,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async move {
            let mut store = self.partitions.lock().unwrap();
            store.entry(partition).or_default().extend(records.iter().cloned());
            ProduceResult::Success
        })
    }

    fn on_fetch(&self, partition: i32, offset: i64) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let store = self.partitions.lock().unwrap();
            match store.get(&partition) {
                Some(records) if (offset as usize) < records.len() => {
                    FetchResult::Records(records[offset as usize..].to_vec())
                }
                _ => FetchResult::Empty,
            }
        })
    }
}
```

- [ ] **Step 4: Export Buffer from lib.rs**

Add `Buffer` to the `pub use topic::` line in `lib.rs`:

```rust
pub use topic::{BlackHole, Buffer, Topic};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: 7 tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/topic.rs gaslightmq/src/lib.rs
git commit -m "feat: add Buffer topic with in-memory per-partition storage"
```

---

### Task 5: ChaosBehavior + Built-in Behaviors

**Files:**
- Create: `gaslightmq/src/chaos.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for Delay, Timeout, ErrorResponse**

`gaslightmq/src/chaos.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::ErrorCode;

    #[test]
    fn delay_returns_delay_action() {
        let behavior = Delay::new(Duration::from_millis(500));
        assert_eq!(behavior.next_action(), ChaosAction::Delay(Duration::from_millis(500)));
    }

    #[test]
    fn timeout_returns_timeout_action() {
        let behavior = Timeout;
        assert_eq!(behavior.next_action(), ChaosAction::Timeout);
    }

    #[test]
    fn error_response_returns_error_action() {
        let behavior = ErrorResponse::new(ErrorCode::NotLeaderForPartition);
        assert_eq!(
            behavior.next_action(),
            ChaosAction::Error(ErrorCode::NotLeaderForPartition)
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement ChaosAction, ChaosBehavior, Delay, Timeout, ErrorResponse**

Add above the tests in `gaslightmq/src/chaos.rs`:

```rust
use std::time::Duration;
use crate::record::ErrorCode;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChaosAction {
    Proceed,
    Delay(Duration),
    Error(ErrorCode),
    Timeout,
}

pub trait ChaosBehavior: Send + Sync {
    fn next_action(&self) -> ChaosAction;
}

pub struct Delay {
    duration: Duration,
}

impl Delay {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl ChaosBehavior for Delay {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Delay(self.duration)
    }
}

pub struct Timeout;

impl ChaosBehavior for Timeout {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Timeout
    }
}

pub struct ErrorResponse {
    code: ErrorCode,
}

impl ErrorResponse {
    pub fn new(code: ErrorCode) -> Self {
        Self { code }
    }
}

impl ChaosBehavior for ErrorResponse {
    fn next_action(&self) -> ChaosAction {
        ChaosAction::Error(self.code)
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add to `gaslightmq/src/lib.rs`:

```rust
pub mod chaos;

pub use chaos::{ChaosBehavior, ChaosAction, Delay, ErrorResponse, Timeout};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/chaos.rs gaslightmq/src/lib.rs
git commit -m "feat: add ChaosBehavior trait with Delay, Timeout, ErrorResponse"
```

---

### Task 6: Sequence

**Files:**
- Modify: `gaslightmq/src/chaos.rs`

- [ ] **Step 1: Write tests for Sequence**

Add to the `tests` module in `gaslightmq/src/chaos.rs`:

```rust
    #[test]
    fn sequence_steps_through_actions() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 2),
                Step::new(ChaosAction::Error(ErrorCode::RequestTimedOut), 1),
            ],
            AfterSequence::Stop,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Error(ErrorCode::RequestTimedOut));
        // After sequence, stop = proceed
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
    }

    #[test]
    fn sequence_loops() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 1),
                Step::new(ChaosAction::Timeout, 1),
            ],
            AfterSequence::Loop,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Timeout);
        // Loops back
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Timeout);
    }

    #[test]
    fn sequence_repeats_last() {
        let seq = Sequence::new(
            vec![
                Step::new(ChaosAction::Proceed, 1),
                Step::new(ChaosAction::Delay(Duration::from_secs(5)), 1),
            ],
            AfterSequence::RepeatLast,
        );
        assert_eq!(seq.next_action(), ChaosAction::Proceed);
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
        assert_eq!(seq.next_action(), ChaosAction::Delay(Duration::from_secs(5)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `Sequence`, `Step`, `AfterSequence` not defined

- [ ] **Step 3: Implement Sequence, Step, AfterSequence**

Add to `gaslightmq/src/chaos.rs`, before the tests module:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AfterSequence {
    Loop,
    Stop,
    RepeatLast,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub action: ChaosAction,
    pub repeat: usize,
}

impl Step {
    pub fn new(action: ChaosAction, repeat: usize) -> Self {
        Self { action, repeat }
    }
}

pub struct Sequence {
    steps: Vec<Step>,
    after: AfterSequence,
    counter: AtomicUsize,
    total: usize,
}

impl Sequence {
    pub fn new(steps: Vec<Step>, after: AfterSequence) -> Self {
        let total = steps.iter().map(|s| s.repeat).sum();
        Self {
            steps,
            after,
            counter: AtomicUsize::new(0),
            total,
        }
    }

    fn action_at(&self, index: usize) -> ChaosAction {
        let mut remaining = index;
        for step in &self.steps {
            if remaining < step.repeat {
                return step.action;
            }
            remaining -= step.repeat;
        }
        unreachable!()
    }
}

impl ChaosBehavior for Sequence {
    fn next_action(&self) -> ChaosAction {
        let call = self.counter.fetch_add(1, Ordering::Relaxed);

        if call < self.total {
            self.action_at(call)
        } else {
            match self.after {
                AfterSequence::Stop => ChaosAction::Proceed,
                AfterSequence::RepeatLast => self.steps.last().unwrap().action,
                AfterSequence::Loop => self.action_at(call % self.total),
            }
        }
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add `AfterSequence`, `Sequence`, `Step` to the chaos exports in `lib.rs`:

```rust
pub use chaos::{AfterSequence, ChaosBehavior, ChaosAction, Delay, ErrorResponse, Sequence, Step, Timeout};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/chaos.rs gaslightmq/src/lib.rs
git commit -m "feat: add Sequence chaos behavior with repeat and after_sequence policies"
```

---

### Task 7: ChaosLayer

**Files:**
- Create: `gaslightmq/src/layer.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for ChaosLayer**

`gaslightmq/src/layer.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Api, BlackHole, Buffer, Delay, ErrorCode, ErrorResponse, FetchResult, ProduceResult, Record};
    use std::time::Duration;

    #[tokio::test]
    async fn chaos_layer_applies_error_on_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(0, &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            ErrorResponse::new(ErrorCode::NotLeaderForPartition),
        );

        let result = chaos.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Error(ErrorCode::NotLeaderForPartition));
    }

    #[tokio::test]
    async fn chaos_layer_passes_through_on_non_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(0, &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Produce,
            ErrorResponse::new(ErrorCode::NotLeaderForPartition),
        );

        // Chaos targets Produce, so Fetch should pass through
        let result = chaos.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("hello")]));
    }

    #[tokio::test]
    async fn chaos_layer_applies_error_on_produce() {
        let chaos = ChaosLayer::new(
            BlackHole::new(1),
            Api::Produce,
            ErrorResponse::new(ErrorCode::RequestTimedOut),
        );

        let result = chaos.on_produce(0, &[Record::new("hello")]).await;
        assert_eq!(result, ProduceResult::Error(ErrorCode::RequestTimedOut));
    }

    #[tokio::test]
    async fn chaos_layer_delay_still_returns_result() {
        let topic = Buffer::new(1);
        topic.on_produce(0, &[Record::new("data")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            Delay::new(Duration::from_millis(1)),
        );

        let result = chaos.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("data")]));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `ChaosLayer` not defined

- [ ] **Step 3: Implement ChaosLayer**

Add above the tests in `gaslightmq/src/layer.rs`:

```rust
use crate::chaos::{ChaosAction, ChaosBehavior};
use crate::generator::Generator;
use crate::record::{Api, FetchResult, ProduceResult, Record, TopicFuture, ErrorCode};
use crate::topic::Topic;

pub struct ChaosLayer<T, C> {
    inner: T,
    api: Api,
    behavior: C,
}

impl<T, C> ChaosLayer<T, C> {
    pub fn new(inner: T, api: Api, behavior: C) -> Self {
        Self {
            inner,
            api,
            behavior,
        }
    }
}

impl<T: Topic, C: ChaosBehavior> Topic for ChaosLayer<T, C> {
    fn partitions(&self) -> i32 {
        self.inner.partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: i32,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async move {
            if self.api == Api::Produce {
                match self.behavior.next_action() {
                    ChaosAction::Proceed => self.inner.on_produce(partition, records).await,
                    ChaosAction::Delay(d) => {
                        tokio::time::sleep(d).await;
                        self.inner.on_produce(partition, records).await
                    }
                    ChaosAction::Error(code) => ProduceResult::Error(code),
                    ChaosAction::Timeout => std::future::pending().await,
                }
            } else {
                self.inner.on_produce(partition, records).await
            }
        })
    }

    fn on_fetch(&self, partition: i32, offset: i64) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            if self.api == Api::Fetch {
                match self.behavior.next_action() {
                    ChaosAction::Proceed => self.inner.on_fetch(partition, offset).await,
                    ChaosAction::Delay(d) => {
                        tokio::time::sleep(d).await;
                        self.inner.on_fetch(partition, offset).await
                    }
                    ChaosAction::Error(code) => FetchResult::Error(code),
                    ChaosAction::Timeout => std::future::pending().await,
                }
            } else {
                self.inner.on_fetch(partition, offset).await
            }
        })
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add to `gaslightmq/src/lib.rs`:

```rust
pub mod layer;

pub use layer::ChaosLayer;
```

Note: also add `pub mod generator;` stub (empty file) since `layer.rs` imports it. Create `gaslightmq/src/generator.rs` with:

```rust
use crate::record::Record;

pub trait Generator: Send + Sync {
    fn generate(&self) -> Vec<Record>;
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/layer.rs gaslightmq/src/generator.rs gaslightmq/src/lib.rs
git commit -m "feat: add ChaosLayer — tower-style generic wrapper applying chaos per API"
```

---

### Task 8: Generator + GeneratorLayer

**Files:**
- Modify: `gaslightmq/src/generator.rs`
- Modify: `gaslightmq/src/layer.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for RandomBytes and JsonTemplate**

Add to `gaslightmq/src/generator.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_bytes_generates_correct_size() {
        let gen = RandomBytes::new(64);
        let records = gen.generate();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref().unwrap().len(), 64);
        assert!(records[0].key.is_none());
    }

    #[test]
    fn json_template_replaces_uuid_placeholder() {
        let gen = JsonTemplate::new(r#"{"id": "{{uuid}}"}"#);
        let records = gen.generate();
        assert_eq!(records.len(), 1);
        let value = std::str::from_utf8(records[0].value.as_ref().unwrap()).unwrap();
        assert!(value.starts_with(r#"{"id": ""#));
        assert!(value.ends_with(r#""}"#));
        assert_ne!(value, r#"{"id": "{{uuid}}"}"#);
    }

    #[test]
    fn json_template_replaces_multiple_placeholders() {
        let gen = JsonTemplate::new(r#"{"a": "{{uuid}}", "b": "{{uuid}}"}"#);
        let records = gen.generate();
        let value = std::str::from_utf8(records[0].value.as_ref().unwrap()).unwrap();
        assert!(!value.contains("{{uuid}}"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `RandomBytes`, `JsonTemplate` not defined

- [ ] **Step 3: Implement RandomBytes and JsonTemplate**

Replace `gaslightmq/src/generator.rs` contents (keep the trait, add impls):

```rust
use bytes::Bytes;
use crate::record::Record;
use fake::Fake;
use fake::faker::name::en::Name;
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::{Word, Sentence};
use rand::rng;

pub trait Generator: Send + Sync {
    fn generate(&self) -> Vec<Record>;
}

pub struct RandomBytes {
    size: usize,
}

impl RandomBytes {
    pub fn new(size: usize) -> Self {
        Self { size }
    }
}

impl Generator for RandomBytes {
    fn generate(&self) -> Vec<Record> {
        use rand::Rng;
        let mut rng = rng();
        let bytes: Vec<u8> = (0..self.size).map(|_| rng.random()).collect();
        vec![Record {
            key: None,
            value: Some(Bytes::from(bytes)),
        }]
    }
}

pub struct JsonTemplate {
    template: String,
}

impl JsonTemplate {
    pub fn new(template: &str) -> Self {
        Self {
            template: template.to_string(),
        }
    }

    fn render(&self) -> String {
        let mut result = self.template.clone();
        while let Some(start) = result.find("{{") {
            if let Some(end) = result[start..].find("}}") {
                let end = start + end + 2;
                let placeholder = &result[start + 2..end - 2];
                let replacement = self.generate_value(placeholder);
                result.replace_range(start..end, &replacement);
            } else {
                break;
            }
        }
        result
    }

    fn generate_value(&self, placeholder: &str) -> String {
        let mut rng = rng();
        match placeholder {
            "uuid" => uuid::Uuid::new_v4().to_string(),
            "name" => Name().fake_with_rng::<String, _>(&mut rng),
            "email" => SafeEmail().fake_with_rng::<String, _>(&mut rng),
            "word" => Word().fake_with_rng::<String, _>(&mut rng),
            "sentence" => Sentence(3..8).fake_with_rng::<String, _>(&mut rng),
            "i32" => rng.random::<i32>().to_string(),
            "i64" => rng.random::<i64>().to_string(),
            "bool" => rng.random::<bool>().to_string(),
            _ => format!("{{{{{placeholder}}}}}"),
        }
    }
}

impl Generator for JsonTemplate {
    fn generate(&self) -> Vec<Record> {
        vec![Record {
            key: None,
            value: Some(Bytes::from(self.render())),
        }]
    }
}
```

Note: add `uuid` to `gaslightmq/Cargo.toml` dependencies:

```toml
uuid = { version = "1", features = ["v4"] }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 5: Write tests for GeneratorLayer**

Add to the `tests` module in `gaslightmq/src/layer.rs`:

```rust
    use crate::generator::RandomBytes;

    #[tokio::test]
    async fn generator_layer_fills_empty_fetch() {
        let topic = BlackHole::new(1);
        let gen = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = gen.on_fetch(0, 0).await;
        match result {
            FetchResult::Records(records) => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].value.as_ref().unwrap().len(), 32);
            }
            other => panic!("expected Records, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generator_layer_does_not_override_real_data() {
        let topic = Buffer::new(1);
        topic.on_produce(0, &[Record::new("real-data")]).await;

        let gen = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = gen.on_fetch(0, 0).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("real-data")]));
    }
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `GeneratorLayer` not defined

- [ ] **Step 7: Implement GeneratorLayer**

Add to `gaslightmq/src/layer.rs`:

```rust
pub struct GeneratorLayer<T, G> {
    inner: T,
    generator: G,
}

impl<T, G> GeneratorLayer<T, G> {
    pub fn new(inner: T, generator: G) -> Self {
        Self { inner, generator }
    }
}

impl<T: Topic, G: Generator> Topic for GeneratorLayer<T, G> {
    fn partitions(&self) -> i32 {
        self.inner.partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: i32,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        self.inner.on_produce(partition, records)
    }

    fn on_fetch(&self, partition: i32, offset: i64) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let result = self.inner.on_fetch(partition, offset).await;
            if result == FetchResult::Empty {
                FetchResult::Records(self.generator.generate())
            } else {
                result
            }
        })
    }
}
```

- [ ] **Step 8: Export from lib.rs**

Update the generator and layer exports in `lib.rs`:

```rust
pub use generator::{Generator, JsonTemplate, RandomBytes};
pub use layer::{ChaosLayer, GeneratorLayer};
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 10: Commit**

```bash
git add gaslightmq/src/generator.rs gaslightmq/src/layer.rs gaslightmq/src/lib.rs gaslightmq/Cargo.toml
git commit -m "feat: add Generator trait, RandomBytes, JsonTemplate, and GeneratorLayer"
```

---

### Task 9: Builder API

**Files:**
- Create: `gaslightmq/src/builder.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for BrokerBuilder**

`gaslightmq/src/builder.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Api, BlackHole, Buffer, Delay, ErrorCode, ErrorResponse, RandomBytes};
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
                .chaos(Api::Produce, ErrorResponse::new(ErrorCode::RequestTimedOut))
            .build_config();

        assert_eq!(config.topics.len(), 1);
        // The topic is wrapped in two ChaosLayers — verified by calling through it
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `BrokerBuilder` not defined

- [ ] **Step 3: Implement BrokerBuilder and TopicBuilder**

Add above tests in `gaslightmq/src/builder.rs`:

```rust
use std::collections::HashMap;
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

    pub fn build_config(self) -> BrokerConfig {
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
        }
    }
}

impl Default for BrokerBuilder {
    fn default() -> Self {
        Self::new()
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
        gen: G,
    ) -> TopicBuilder<GeneratorLayer<T, G>> {
        TopicBuilder {
            topics: self.topics,
            bind_addr: self.bind_addr,
            name: self.name,
            topic: GeneratorLayer::new(self.topic, gen),
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

    pub fn build_config(mut self) -> BrokerConfig {
        self.topics.insert(self.name, Box::new(self.topic));
        BrokerConfig {
            topics: self.topics,
            bind_addr: self.bind_addr,
        }
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add to `gaslightmq/src/lib.rs`:

```rust
pub mod builder;

pub use builder::BrokerBuilder;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/builder.rs gaslightmq/src/lib.rs
git commit -m "feat: add BrokerBuilder with typestate TopicBuilder for fluent API"
```

---

### Task 10: Protocol Framing

**Files:**
- Create: `gaslightmq/src/protocol.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write tests for frame reading/writing and header version mapping**

`gaslightmq/src/protocol.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::{ApiKey, RequestHeader};
    use kafka_protocol::protocol::Encodable;

    #[test]
    fn request_header_version_api_versions_v3_uses_v2() {
        assert_eq!(request_header_version(ApiKey::ApiVersions as i16, 3), 2);
    }

    #[test]
    fn request_header_version_metadata_v0_uses_v1() {
        assert_eq!(request_header_version(ApiKey::MetadataKey as i16, 0), 1);
    }

    #[test]
    fn response_header_version_api_versions_v3_uses_v1() {
        assert_eq!(response_header_version(ApiKey::ApiVersions as i16, 3), 1);
    }

    #[test]
    fn response_header_version_metadata_v0_uses_v0() {
        assert_eq!(response_header_version(ApiKey::MetadataKey as i16, 0), 0);
    }

    #[tokio::test]
    async fn read_and_parse_request_frame() {
        // Build a frame: length-prefixed encoded request header
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::ApiVersions as i16;
        header.request_api_version = 0;
        header.correlation_id = 42;

        let header_version = request_header_version(header.request_api_key, header.request_api_version);

        let mut body = BytesMut::new();
        header.encode(&mut body, header_version).unwrap();
        // ApiVersionsRequest v0 is empty — no body bytes needed

        let mut frame = BytesMut::new();
        frame.put_i32(body.len() as i32);
        frame.extend_from_slice(&body);

        let mut cursor = std::io::Cursor::new(frame.freeze());
        let parsed = read_request(&mut cursor).await.unwrap();
        assert_eq!(parsed.header.correlation_id, 42);
        assert_eq!(parsed.api_key, ApiKey::ApiVersions as i16);
        assert_eq!(parsed.api_version, 0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — functions not defined

- [ ] **Step 3: Implement protocol framing**

Add above tests in `gaslightmq/src/protocol.rs`:

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub struct ParsedRequest {
    pub header: RequestHeader,
    pub api_key: i16,
    pub api_version: i16,
    pub body: Bytes,
}

pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == ApiKey::ApiVersions as i16 && api_version >= 3 {
        2
    } else {
        1
    }
}

pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    if api_key == ApiKey::ApiVersions as i16 && api_version >= 3 {
        1
    } else {
        0
    }
}

pub async fn read_request<R: AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<ParsedRequest> {
    let size = stream.read_i32().await? as usize;
    let mut buf = vec![0u8; size];
    stream.read_exact(&mut buf).await?;

    let mut bytes = Bytes::from(buf);

    let api_key = i16::from_be_bytes([bytes[0], bytes[1]]);
    let api_version = i16::from_be_bytes([bytes[2], bytes[3]]);

    let header_version = request_header_version(api_key, api_version);
    let header = RequestHeader::decode(&mut bytes, header_version)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    Ok(ParsedRequest {
        header,
        api_key,
        api_version,
        body: bytes,
    })
}

pub async fn write_response<W: AsyncWrite + Unpin>(
    stream: &mut W,
    correlation_id: i32,
    api_key: i16,
    api_version: i16,
    response: &(impl Encodable + ?Sized),
) -> std::io::Result<()> {
    let resp_header_version = response_header_version(api_key, api_version);

    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = correlation_id;

    let mut buf = BytesMut::new();
    resp_header
        .encode(&mut buf, resp_header_version)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    response
        .encode(&mut buf, api_version)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

    stream.write_i32(buf.len() as i32).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}
```

Note: the `read_request` test uses `std::io::Cursor` which implements `AsyncRead` via `tokio::io`. This should work — verify `Cursor` implements `tokio::io::AsyncRead`, or wrap with `tokio::io::BufReader`. Adjust if needed.

- [ ] **Step 4: Export from lib.rs**

```rust
pub mod protocol;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass. Note: verify `ApiKey::MetadataKey` is the correct variant name in kafka-protocol 0.17 — it may be `ApiKey::Metadata`. Fix the test if needed.

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/protocol.rs gaslightmq/src/lib.rs
git commit -m "feat: add Kafka protocol framing — read/write length-prefixed frames"
```

---

### Task 11: Protocol Handlers

**Files:**
- Modify: `gaslightmq/src/protocol.rs`

This task adds handler functions for each supported Kafka API. Each handler takes a decoded request + topic registry and returns an encoded response. Consult `kafka-protocol` crate docs for exact field names on request/response types — the code below is best-effort and may need field name adjustments.

- [ ] **Step 1: Write test for ApiVersions handler**

Add to the tests module in `protocol.rs`:

```rust
    use kafka_protocol::messages::ApiVersionsResponse;
    use kafka_protocol::protocol::Decodable;

    #[test]
    fn handle_api_versions_returns_supported_keys() {
        let response = handle_api_versions(0);
        assert_eq!(response.error_code, 0);
        assert!(!response.api_keys.is_empty());

        let has_metadata = response.api_keys.iter().any(|k| k.api_key == ApiKey::MetadataKey as i16);
        assert!(has_metadata, "should advertise Metadata API");
    }
```

- [ ] **Step 2: Implement handle_api_versions**

Add to `protocol.rs`:

```rust
use kafka_protocol::messages::{
    ApiVersionsResponse,
    api_versions_response::ApiVersion,
};
use std::collections::HashMap;
use crate::topic::Topic;

pub fn handle_api_versions(api_version: i16) -> ApiVersionsResponse {
    let supported = [
        (ApiKey::ApiVersions, 0, 3),
        (ApiKey::MetadataKey, 0, 12),
        (ApiKey::Produce, 0, 9),
        (ApiKey::Fetch, 0, 13),
        (ApiKey::ListOffsets, 0, 7),
        (ApiKey::FindCoordinator, 0, 4),
        (ApiKey::JoinGroup, 0, 9),
        (ApiKey::SyncGroup, 0, 5),
        (ApiKey::Heartbeat, 0, 4),
    ];

    let api_keys = supported
        .iter()
        .map(|(key, min, max)| {
            ApiVersion::default()
                .with_api_key(*key as i16)
                .with_min_version(*min)
                .with_max_version(*max)
        })
        .collect();

    ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(api_keys)
}
```

Note: verify exact `ApiKey` variant names and `ApiVersion` builder methods against `kafka-protocol` 0.17 docs. Names may differ (e.g., `ApiKey::MetadataKey` vs `ApiKey::Metadata`).

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test -p gaslightmq handle_api_versions`
Expected: PASS

- [ ] **Step 4: Write test for Metadata handler**

```rust
    use kafka_protocol::messages::MetadataRequest;

    #[test]
    fn handle_metadata_returns_broker_and_topics() {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();
        topics.insert("orders".to_string(), Box::new(crate::Buffer::new(3)));

        let request = MetadataRequest::default();
        let response = handle_metadata(&request, 0, &topics, "127.0.0.1", 9092);

        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.topics.len(), 1);

        let topic = &response.topics[0];
        assert_eq!(topic.partitions.len(), 3);
    }
```

- [ ] **Step 5: Implement handle_metadata**

```rust
use kafka_protocol::messages::{
    MetadataRequest, MetadataResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition},
};
use kafka_protocol::protocol::StrBytes;

pub fn handle_metadata(
    _request: &MetadataRequest,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
    host: &str,
    port: i32,
) -> MetadataResponse {
    let broker = MetadataResponseBroker::default()
        .with_node_id(0)
        .with_host(StrBytes::from_string(host.to_string()))
        .with_port(port);

    let topic_metadata: Vec<MetadataResponseTopic> = topics
        .iter()
        .map(|(name, topic)| {
            let partitions: Vec<MetadataResponsePartition> = (0..topic.partitions())
                .map(|i| {
                    MetadataResponsePartition::default()
                        .with_partition_index(i)
                        .with_leader_id(0)
                        .with_replica_nodes(vec![0])
                        .with_isr_nodes(vec![0])
                        .with_error_code(0)
                })
                .collect();

            MetadataResponseTopic::default()
                .with_name(Some(StrBytes::from_string(name.clone())))
                .with_partitions(partitions)
                .with_error_code(0)
        })
        .collect();

    MetadataResponse::default()
        .with_brokers(vec![broker])
        .with_topics(topic_metadata)
}
```

Note: verify `MetadataResponseBroker`, `MetadataResponseTopic`, `MetadataResponsePartition` import paths. These nested types live in `metadata_response` module in kafka-protocol.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p gaslightmq handle_metadata`
Expected: PASS

- [ ] **Step 7: Implement remaining handlers as stubs**

Add Produce, Fetch, ListOffsets, FindCoordinator, and consumer group stubs. These are wired to the topic registry. The exact implementation depends on kafka-protocol's record batch API — check `kafka_protocol::records` module.

```rust
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse,
    FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse,
    FindCoordinatorRequest, FindCoordinatorResponse,
    JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
    HeartbeatRequest, HeartbeatResponse,
};

pub async fn handle_produce(
    request: &ProduceRequest,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> ProduceResponse {
    // For each topic+partition in request:
    // 1. Look up topic in registry
    // 2. Decode record batch from request partition data
    // 3. Call topic.on_produce(partition, records).await
    // 4. Build partition response with error code from result
    //
    // Check kafka_protocol::records module for RecordBatchDecoder API.
    // Our Record type needs conversion from/to kafka-protocol's Record type.
    todo!("implement after verifying kafka-protocol record batch API")
}

pub async fn handle_fetch(
    request: &FetchRequest,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> FetchResponse {
    // For each topic+partition in request:
    // 1. Look up topic in registry
    // 2. Call topic.on_fetch(partition, offset).await
    // 3. Encode records into record batch for response
    // 4. Build partition response
    //
    // Check kafka_protocol::records module for RecordBatchEncoder API.
    todo!("implement after verifying kafka-protocol record batch API")
}

pub fn handle_list_offsets(
    request: &ListOffsetsRequest,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> ListOffsetsResponse {
    // Return offset 0 for earliest, current log end for latest
    todo!()
}

pub fn handle_find_coordinator(
    _request: &FindCoordinatorRequest,
    api_version: i16,
    host: &str,
    port: i32,
) -> FindCoordinatorResponse {
    FindCoordinatorResponse::default()
        .with_error_code(0)
        .with_node_id(0)
        .with_host(StrBytes::from_string(host.to_string()))
        .with_port(port)
}

pub fn handle_join_group(
    _request: &JoinGroupRequest,
    api_version: i16,
) -> JoinGroupResponse {
    // Return minimal response: this consumer is the leader
    JoinGroupResponse::default()
        .with_error_code(0)
        .with_leader(StrBytes::from_static_str("gaslightmq"))
        .with_member_id(StrBytes::from_static_str("gaslightmq-member-0"))
}

pub fn handle_sync_group(
    _request: &SyncGroupRequest,
    api_version: i16,
) -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_error_code(0)
}

pub fn handle_heartbeat(
    _request: &HeartbeatRequest,
    api_version: i16,
) -> HeartbeatResponse {
    HeartbeatResponse::default()
        .with_error_code(0)
}
```

Important: the `handle_produce` and `handle_fetch` implementations require working with kafka-protocol's record batch encoding/decoding. Check the `kafka_protocol::records` module for:
- How to decode records from `ProduceRequest` partition data (likely `records` field is `Option<Bytes>`)
- How to encode records into `FetchResponse` partition data
- The record batch encoder/decoder API

Convert between kafka-protocol's `Record` type and our `crate::Record`. Create helper conversion functions.

- [ ] **Step 8: Run all tests**

Run: `cargo test -p gaslightmq`
Expected: all tests pass (todo!() functions won't run in tests)

- [ ] **Step 9: Commit**

```bash
git add gaslightmq/src/protocol.rs
git commit -m "feat: add protocol handlers — ApiVersions, Metadata, stubs for Produce/Fetch"
```

- [ ] **Step 10: Implement handle_produce with record batch decoding**

After verifying the kafka-protocol records API, implement the full `handle_produce`. Convert between our `Record` type and kafka-protocol's record format. Test with a round-trip test:

```rust
    #[tokio::test]
    async fn handle_produce_stores_records_in_buffer() {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();
        topics.insert("test".to_string(), Box::new(crate::Buffer::new(1)));

        // Build a ProduceRequest with records...
        // This requires encoding a record batch — check kafka_protocol::records

        // Call handle_produce
        // Verify topic.on_fetch returns the stored records
    }
```

Fill in the actual record batch encoding/decoding based on kafka-protocol's API.

- [ ] **Step 11: Implement handle_fetch with record batch encoding**

Similar to produce — encode our `Record` types into kafka-protocol record batches for the response.

- [ ] **Step 12: Implement handle_list_offsets**

```rust
pub fn handle_list_offsets(
    request: &ListOffsetsRequest,
    api_version: i16,
    topics: &HashMap<String, Box<dyn Topic>>,
) -> ListOffsetsResponse {
    // For each requested topic+partition, return:
    // - If timestamp == -2 (earliest): offset 0
    // - If timestamp == -1 (latest): current end offset
    // Build the response using ListOffsetsResponse builder
    todo!("implement with correct kafka-protocol types")
}
```

- [ ] **Step 13: Run all tests**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 14: Commit**

```bash
git add gaslightmq/src/protocol.rs
git commit -m "feat: implement Produce, Fetch, ListOffsets protocol handlers"
```

---

### Task 12: Broker Server

**Files:**
- Create: `gaslightmq/src/broker.rs`
- Modify: `gaslightmq/src/builder.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write test for Broker start and shutdown**

`gaslightmq/src/broker.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlackHole, BrokerBuilder};

    #[tokio::test]
    async fn broker_starts_and_shuts_down() {
        let broker = BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0")
            .start()
            .await
            .unwrap();

        let addr = broker.addr();
        assert_ne!(addr.port(), 0);

        broker.shutdown().await;
    }

    #[tokio::test]
    async fn broker_accepts_tcp_connection() {
        let broker = BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0")
            .start()
            .await
            .unwrap();

        let addr = broker.addr();
        let conn = tokio::net::TcpStream::connect(addr).await;
        assert!(conn.is_ok());

        broker.shutdown().await;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — `Broker` not defined, `BrokerBuilder::start` not implemented

- [ ] **Step 3: Implement Broker struct**

`gaslightmq/src/broker.rs`:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use crate::builder::BrokerConfig;
use crate::protocol::{self, read_request, write_response};
use crate::topic::Topic;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::protocol::Decodable;

pub struct Broker {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl Broker {
    pub async fn start(config: BrokerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(&config.bind_addr).await?;
        let addr = listener.local_addr()?;

        let topics = Arc::new(config.topics);
        let host = addr.ip().to_string();
        let port = addr.port() as i32;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                let topics = topics.clone();
                                let host = host.clone();
                                let mut shutdown_rx = shutdown_rx.clone();
                                tokio::spawn(async move {
                                    Self::handle_connection(stream, topics, host, port, shutdown_rx).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = shutdown_rx.changed() => break,
                }
            }
        });

        Ok(Self {
            addr,
            shutdown_tx,
            handle,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.handle.await;
    }

    async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        topics: Arc<HashMap<String, Box<dyn Topic>>>,
        host: String,
        port: i32,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            let request = tokio::select! {
                result = read_request(&mut stream) => {
                    match result {
                        Ok(req) => req,
                        Err(_) => return,
                    }
                }
                _ = shutdown_rx.changed() => return,
            };

            let correlation_id = request.header.correlation_id;
            let api_key = request.api_key;
            let api_version = request.api_version;

            match ApiKey::try_from(api_key) {
                Ok(ApiKey::ApiVersions) => {
                    let response = protocol::handle_api_versions(api_version);
                    let _ = write_response(&mut stream, correlation_id, api_key, api_version, &response).await;
                }
                Ok(ApiKey::MetadataKey) => {
                    let mut body = request.body;
                    if let Ok(req) = kafka_protocol::messages::MetadataRequest::decode(&mut body, api_version) {
                        let response = protocol::handle_metadata(&req, api_version, &topics, &host, port);
                        let _ = write_response(&mut stream, correlation_id, api_key, api_version, &response).await;
                    }
                }
                // Add Produce, Fetch, ListOffsets, FindCoordinator, JoinGroup, SyncGroup, Heartbeat
                // following the same pattern once those handlers are implemented
                _ => {
                    // Unsupported API — send ApiVersions response with error
                    let mut response = protocol::handle_api_versions(0);
                    response.error_code = 35; // UNSUPPORTED_VERSION
                    let _ = write_response(&mut stream, correlation_id, api_key, api_version, &response).await;
                }
            }
        }
    }
}
```

- [ ] **Step 4: Wire BrokerBuilder to Broker::start**

Add to `gaslightmq/src/builder.rs`:

```rust
use crate::broker::Broker;

impl BrokerBuilder {
    pub async fn start(self) -> std::io::Result<Broker> {
        Broker::start(self.build_config()).await
    }
}

impl<T: Topic + 'static> TopicBuilder<T> {
    pub async fn start(self) -> std::io::Result<Broker> {
        let mut topics = self.topics;
        topics.insert(self.name, Box::new(self.topic));
        Broker::start(BrokerConfig {
            topics,
            bind_addr: self.bind_addr,
        }).await
    }
}
```

- [ ] **Step 5: Export from lib.rs**

```rust
pub mod broker;

pub use broker::Broker;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add gaslightmq/src/broker.rs gaslightmq/src/builder.rs gaslightmq/src/lib.rs
git commit -m "feat: add Broker with TCP accept loop, connection handling, and shutdown"
```

---

### Task 13: Scenario YAML

**Files:**
- Create: `gaslightmq/src/scenario.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Write test for scenario deserialization**

`gaslightmq/src/scenario.rs` — start with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_basic_scenario() {
        let yaml = r#"
topics:
  - name: orders
    partitions: 3
    mode: buffer
    chaos:
      - api: Fetch
        behavior: delay
        delay_ms: 2000
        repeat: 5
  - name: events
    partitions: 1
    mode: blackhole
"#;
        let scenario: Scenario = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(scenario.topics.len(), 2);

        assert_eq!(scenario.topics[0].name, "orders");
        assert_eq!(scenario.topics[0].partitions, 3);
        assert_eq!(scenario.topics[0].mode, TopicMode::Buffer);
        assert_eq!(scenario.topics[0].chaos.len(), 1);
        assert_eq!(scenario.topics[0].chaos[0].api, ApiRef::Fetch);

        assert_eq!(scenario.topics[1].name, "events");
        assert_eq!(scenario.topics[1].mode, TopicMode::BlackHole);
    }

    #[test]
    fn deserialize_sequence_chaos() {
        let yaml = r#"
topics:
  - name: test
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
        let scenario: Scenario = serde_yaml::from_str(yaml).unwrap();
        let chaos = &scenario.topics[0].chaos[0];
        assert!(chaos.sequence.is_some());
        let seq = chaos.sequence.as_ref().unwrap();
        assert_eq!(seq.len(), 3);
        assert_eq!(chaos.after_sequence, Some(AfterSequenceRef::Loop));
    }

    #[test]
    fn scenario_to_broker_config() {
        let yaml = r#"
topics:
  - name: orders
    partitions: 3
    mode: buffer
  - name: events
    partitions: 1
    mode: blackhole
"#;
        let scenario: Scenario = serde_yaml::from_str(yaml).unwrap();
        let config = scenario.into_broker_config("127.0.0.1:9092");
        assert_eq!(config.topics.len(), 2);
        assert!(config.topics.contains_key("orders"));
        assert!(config.topics.contains_key("events"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p gaslightmq`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement Scenario model**

Add above tests in `gaslightmq/src/scenario.rs`:

```rust
use serde::Deserialize;
use std::time::Duration;
use crate::builder::BrokerConfig;
use crate::chaos::{self, AfterSequence, ChaosAction, Sequence, Step};
use crate::generator::{JsonTemplate, RandomBytes};
use crate::layer::{ChaosLayer, GeneratorLayer};
use crate::record::{Api, ErrorCode};
use crate::topic::{BlackHole, Buffer, Topic};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub topics: Vec<TopicConfig>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    Buffer,
    #[serde(alias = "blackhole")]
    BlackHole,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ApiRef {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BehaviorRef {
    Success,
    Delay,
    Timeout,
    Error,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AfterSequenceRef {
    Loop,
    Stop,
    RepeatLast,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCodeRef {
    OffsetOutOfRange,
    CorruptMessage,
    UnknownTopicOrPartition,
    LeaderNotAvailable,
    NotLeaderForPartition,
    RequestTimedOut,
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

#[derive(Debug, Deserialize)]
pub struct ChaosConfig {
    pub api: ApiRef,
    pub behavior: Option<BehaviorRef>,
    pub delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub error_code: Option<ErrorCodeRef>,
    pub repeat: Option<usize>,
    pub sequence: Option<Vec<SequenceStepConfig>>,
    pub after_sequence: Option<AfterSequenceRef>,
}

#[derive(Debug, Deserialize)]
pub struct SequenceStepConfig {
    pub behavior: BehaviorRef,
    pub delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub error_code: Option<ErrorCodeRef>,
    #[serde(default = "default_repeat")]
    pub repeat: usize,
}

fn default_repeat() -> usize {
    1
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format")]
#[serde(rename_all = "lowercase")]
pub enum GeneratorConfig {
    Json { template: String },
    #[serde(alias = "random_bytes")]
    RandomBytes { size: usize },
}

impl Scenario {
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn into_broker_config(self, bind_addr: &str) -> BrokerConfig {
        let mut topics: HashMap<String, Box<dyn Topic>> = HashMap::new();

        for tc in self.topics {
            let base: Box<dyn Topic> = match tc.mode {
                TopicMode::Buffer => Box::new(Buffer::new(tc.partitions)),
                TopicMode::BlackHole => Box::new(BlackHole::new(tc.partitions)),
            };

            // Wrap with chaos layers if configured
            // Note: since we're using Box<dyn Topic>, we apply chaos at this level
            // For scenario files, we use Sequence to represent all chaos
            // This is a simplified version — full implementation would wrap generically
            // but Box<dyn> requires a different approach

            topics.insert(tc.name, base);
        }

        BrokerConfig {
            topics,
            bind_addr: bind_addr.to_string(),
        }
    }
}

impl ErrorCodeRef {
    fn to_error_code(&self) -> ErrorCode {
        match self {
            Self::OffsetOutOfRange => ErrorCode::OffsetOutOfRange,
            Self::CorruptMessage => ErrorCode::CorruptMessage,
            Self::UnknownTopicOrPartition => ErrorCode::UnknownTopicOrPartition,
            Self::LeaderNotAvailable => ErrorCode::LeaderNotAvailable,
            Self::NotLeaderForPartition => ErrorCode::NotLeaderForPartition,
            Self::RequestTimedOut => ErrorCode::RequestTimedOut,
        }
    }
}

impl ApiRef {
    fn to_api(&self) -> Api {
        match self {
            Self::Produce => Api::Produce,
            Self::Fetch => Api::Fetch,
            Self::ListOffsets => Api::ListOffsets,
            Self::Metadata => Api::Metadata,
        }
    }
}
```

Note: the `into_broker_config` method needs a way to wrap `Box<dyn Topic>` with chaos and generators. Since ChaosLayer is generic, wrapping a `Box<dyn Topic>` requires `ChaosLayer<Box<dyn Topic>, Sequence>`. This works because `Box<dyn Topic>` implements `Topic`. Extend `into_broker_config` to apply chaos and generator wrapping using this approach.

- [ ] **Step 4: Export from lib.rs**

```rust
pub mod scenario;

pub use scenario::Scenario;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p gaslightmq`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/scenario.rs gaslightmq/src/lib.rs
git commit -m "feat: add Scenario YAML deserialization and broker config conversion"
```

---

### Task 14: gmq Binary

**Files:**
- Modify: `gmq/src/main.rs`

- [ ] **Step 1: Implement CLI with clap**

`gmq/src/main.rs`:

```rust
use clap::Parser;
use gaslightmq::Scenario;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "gmq", about = "Gaslight MQ — a fake Kafka broker for chaos testing")]
struct Cli {
    #[arg(short, long, help = "Path to YAML scenario file")]
    scenario: Option<PathBuf>,

    #[arg(short, long, default_value = "127.0.0.1:9092", help = "Address to bind to")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let broker = if let Some(path) = cli.scenario {
        let yaml = std::fs::read_to_string(&path)?;
        let scenario = Scenario::from_yaml(&yaml)?;
        gaslightmq::Broker::start(scenario.into_broker_config(&cli.bind)).await?
    } else {
        gaslightmq::BrokerBuilder::new()
            .topic("default", gaslightmq::BlackHole::new(1))
            .bind(&cli.bind)
            .start()
            .await?
    };

    eprintln!("Gaslight MQ listening on {}", broker.addr());
    eprintln!("Press Ctrl+C to stop");

    tokio::signal::ctrl_c().await?;
    broker.shutdown().await;

    Ok(())
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p gmq`
Expected: compiles successfully

- [ ] **Step 3: Test CLI help output**

Run: `cargo run -p gmq -- --help`
Expected: shows help text with --scenario and --bind options

- [ ] **Step 4: Commit**

```bash
git add gmq/src/main.rs
git commit -m "feat: add gmq binary with --scenario and --bind CLI options"
```

---

### Task 15: Integration Test

**Files:**
- Create: `gaslightmq/tests/integration.rs`

- [ ] **Step 1: Write integration test — connect and send ApiVersions**

`gaslightmq/tests/integration.rs`:

```rust
use bytes::{BufMut, BytesMut};
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn send_request(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    body: &(impl Encodable + ?Sized),
) {
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id);

    let header_version = if api_key == ApiKey::ApiVersions && api_version >= 3 {
        2
    } else {
        1
    };

    let mut buf = BytesMut::new();
    header.encode(&mut buf, header_version).unwrap();
    body.encode(&mut buf, api_version).unwrap();

    stream.write_i32(buf.len() as i32).await.unwrap();
    stream.write_all(&buf).await.unwrap();
}

async fn read_response<T: Decodable>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
) -> T {
    let size = stream.read_i32().await.unwrap() as usize;
    let mut buf = vec![0u8; size];
    stream.read_exact(&mut buf).await.unwrap();
    let mut bytes = bytes::Bytes::from(buf);

    let resp_header_version = if api_key == ApiKey::ApiVersions && api_version >= 3 {
        1
    } else {
        0
    };
    let _header =
        kafka_protocol::messages::ResponseHeader::decode(&mut bytes, resp_header_version)
            .unwrap();

    T::decode(&mut bytes, api_version).unwrap()
}

#[tokio::test]
async fn api_versions_handshake() {
    let broker = BrokerBuilder::new()
        .topic("test", BlackHole::new(1))
        .bind("127.0.0.1:0")
        .start()
        .await
        .unwrap();

    let mut stream = TcpStream::connect(broker.addr()).await.unwrap();

    let request = ApiVersionsRequest::default();
    send_request(&mut stream, ApiKey::ApiVersions, 0, 1, &request).await;

    let response: ApiVersionsResponse =
        read_response(&mut stream, ApiKey::ApiVersions, 0).await;

    assert_eq!(response.error_code, 0);
    assert!(!response.api_keys.is_empty());

    broker.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_topics() {
    let broker = BrokerBuilder::new()
        .topic("orders", Buffer::new(3))
        .topic("events", BlackHole::new(1))
        .bind("127.0.0.1:0")
        .start()
        .await
        .unwrap();

    let mut stream = TcpStream::connect(broker.addr()).await.unwrap();

    // First: ApiVersions handshake
    let request = ApiVersionsRequest::default();
    send_request(&mut stream, ApiKey::ApiVersions, 0, 1, &request).await;
    let _: ApiVersionsResponse = read_response(&mut stream, ApiKey::ApiVersions, 0).await;

    // Then: Metadata request
    let request = kafka_protocol::messages::MetadataRequest::default();
    send_request(&mut stream, ApiKey::MetadataKey, 0, 2, &request).await;

    let response: kafka_protocol::messages::MetadataResponse =
        read_response(&mut stream, ApiKey::MetadataKey, 0).await;

    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.topics.len(), 2);

    broker.shutdown().await;
}
```

Note: verify `ApiKey::MetadataKey` is the correct variant name. It may be `ApiKey::Metadata` in kafka-protocol 0.17.

- [ ] **Step 2: Run integration tests**

Run: `cargo test -p gaslightmq --test integration`
Expected: 2 tests pass

- [ ] **Step 3: Commit**

```bash
git add gaslightmq/tests/integration.rs
git commit -m "test: add integration tests — ApiVersions handshake and Metadata query"
```

---

## Summary

| Task | Description | Key Files |
|------|-------------|-----------|
| 1 | Workspace scaffolding | `Cargo.toml`, `rust-toolchain.toml` |
| 2 | Core types | `record.rs` |
| 3 | Topic trait + BlackHole | `topic.rs` |
| 4 | Buffer topic | `topic.rs` |
| 5 | ChaosBehavior + impls | `chaos.rs` |
| 6 | Sequence | `chaos.rs` |
| 7 | ChaosLayer | `layer.rs` |
| 8 | Generator + GeneratorLayer | `generator.rs`, `layer.rs` |
| 9 | Builder API | `builder.rs` |
| 10 | Protocol framing | `protocol.rs` |
| 11 | Protocol handlers | `protocol.rs` |
| 12 | Broker server | `broker.rs` |
| 13 | Scenario YAML | `scenario.rs` |
| 14 | gmq binary | `gmq/src/main.rs` |
| 15 | Integration test | `tests/integration.rs` |
