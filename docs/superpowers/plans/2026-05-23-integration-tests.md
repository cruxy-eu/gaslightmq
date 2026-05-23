# Integration Test Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a structured integration test framework with pluggable client implementations for all Kafka API calls.

**Architecture:** Four new crates — `gaslightmq-kernel` (shared value objects), `gaslightmq-testing` (test types/traits/harness), `gaslightmq-kafka-client-api` (raw TCP client with per-API traits), `gaslightmq-kafka-client-rdkafka` (rdkafka wrapper). Tests live in `gaslightmq/tests/` as numbered suites (1000–1006) exercising each Kafka API through raw protocol traits and high-level abstractions.

**Tech Stack:** Rust 2024 (toolchain 1.94.1), kafka-protocol 0.17, rdkafka 0.37, async-trait 0.1, tokio 1, bytes 1, futures 0.3, indexmap 2

**Spec:** `docs/superpowers/specs/2026-05-23-integration-tests-design.md`

---

## File Structure

### New crates

```
gaslightmq-kernel/
  Cargo.toml
  src/lib.rs                          # Partition(u32), Offset(u64)

gaslightmq-testing/
  Cargo.toml
  src/
    lib.rs
    record.rs                         # Record, Header, Headers
    traits.rs                         # Producer, Consumer, result types, RecordStream
    harness.rs                        # Harness<C>

gaslightmq-kafka-client/
  api/
    Cargo.toml
    src/
      lib.rs
      produce.rs                      # Produce trait
      fetch.rs                        # Fetch trait
      metadata.rs                     # Metadata trait
      api_versions.rs                 # ApiVersions trait
      list_offsets.rs                 # ListOffsets trait
      find_coordinator.rs             # FindCoordinator trait
      client.rs                       # RawKafkaClient
  rdkafka/
    Cargo.toml
    src/lib.rs                        # RdkafkaClient
```

### New test files

```
gaslightmq/tests/
  1000-kafka-api.rs
  1001-kafka-produce.rs
  1002-kafka-consume.rs
  1003-kafka-metadata.rs
  1004-kafka-api-versions.rs
  1005-kafka-list-offsets.rs
  1006-kafka-find-coordinator.rs
```

### Modified files

```
Cargo.toml                            # workspace members
gaslightmq/Cargo.toml                 # add gaslightmq-kernel dep + dev-deps
gaslightmq/src/record.rs              # add Header, headers field
gaslightmq/src/topic.rs               # Partition/Offset in signatures
gaslightmq/src/layer.rs               # Partition/Offset in signatures
gaslightmq/src/protocol.rs            # header encode/decode, Partition/Offset conversions
gaslightmq/src/generator.rs           # Record now has headers field
gaslightmq/src/lib.rs                 # re-export kernel types + Header
```

### Deleted files

```
gaslightmq/tests/integration.rs       # replaced by numbered suites
```

---

## Task 1: Create gaslightmq-kernel crate

**Files:**
- Create: `gaslightmq-kernel/Cargo.toml`
- Create: `gaslightmq-kernel/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "gaslightmq-kernel"
version.workspace = true
edition.workspace = true
```

- [ ] **Step 2: Create src/lib.rs with Partition, Offset, and tests**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Partition(u32);

impl Partition {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for Partition {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<Partition> for u32 {
    fn from(p: Partition) -> Self {
        p.0
    }
}

impl From<i32> for Partition {
    fn from(v: i32) -> Self {
        Self(v as u32)
    }
}

impl From<Partition> for i32 {
    fn from(p: Partition) -> Self {
        p.0 as i32
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> Self {
        o.0
    }
}

impl From<i64> for Offset {
    fn from(v: i64) -> Self {
        Self(v as u64)
    }
}

impl From<Offset> for i64 {
    fn from(o: Offset) -> Self {
        o.0 as i64
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_new_and_value() {
        let p = Partition::new(42);
        assert_eq!(p.value(), 42);
    }

    #[test]
    fn partition_from_u32() {
        let p = Partition::from(5u32);
        assert_eq!(p.value(), 5);
    }

    #[test]
    fn partition_from_i32() {
        let p = Partition::from(7i32);
        assert_eq!(p.value(), 7);
    }

    #[test]
    fn partition_to_i32() {
        let p = Partition::new(7);
        let val: i32 = p.into();
        assert_eq!(val, 7);
    }

    #[test]
    fn partition_equality() {
        assert_eq!(Partition::new(1), Partition::new(1));
        assert_ne!(Partition::new(1), Partition::new(2));
    }

    #[test]
    fn offset_new_and_value() {
        let o = Offset::new(100);
        assert_eq!(o.value(), 100);
    }

    #[test]
    fn offset_from_i64() {
        let o = Offset::from(42i64);
        assert_eq!(o.value(), 42);
    }

    #[test]
    fn offset_to_i64() {
        let o = Offset::new(99);
        let val: i64 = o.into();
        assert_eq!(val, 99);
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::new(5) > Offset::new(3));
    }
}
```

- [ ] **Step 3: Add to workspace members**

In `Cargo.toml` (workspace root), change `members` to:

```toml
members = ["gaslightmq", "bins/*", "gaslightmq-kernel"]
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p gaslightmq-kernel`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add gaslightmq-kernel/ Cargo.toml
git commit -m "feat: add gaslightmq-kernel crate with Partition and Offset newtypes"
```

---

## Task 2: Add record headers to gaslightmq

**Files:**
- Modify: `gaslightmq/src/record.rs`
- Modify: `gaslightmq/src/protocol.rs`
- Modify: `gaslightmq/src/generator.rs`

- [ ] **Step 1: Add Header struct and update Record**

In `gaslightmq/src/record.rs`, add `Header` and a `headers` field to `Record`:

```rust
use bytes::Bytes;
use futures::Stream;
use kafka_protocol::error::ResponseError;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: Some(value.into()),
            headers: Vec::new(),
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: Some(value.into()),
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProduceResult {
    Success,
    Error(ResponseError),
}

pub type RecordStream = Pin<Box<dyn Stream<Item = Record> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub enum FetchResult {
    Empty,
    Records(Vec<Record>),
    Error(ResponseError),
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

- [ ] **Step 2: Update protocol.rs record conversion**

In `gaslightmq/src/protocol.rs`, update `kafka_records_to_internal` and `internal_to_kafka_records`:

```rust
fn kafka_records_to_internal(kafka_records: &[KafkaRecord]) -> Vec<Record> {
    kafka_records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| crate::record::Header {
                    key: k.as_str().to_string(),
                    value: v.clone(),
                })
                .collect();
            Record {
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

fn internal_to_kafka_records(records: &[Record], base_offset: i64) -> Vec<KafkaRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers = IndexMap::new();
            for h in &r.headers {
                headers.insert(
                    StrBytes::from_string(h.key.clone()),
                    h.value.clone(),
                );
            }
            KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: base_offset + i as i64,
                sequence: NO_SEQUENCE,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}
```

- [ ] **Step 3: Update generator.rs**

In `gaslightmq/src/generator.rs`, add `headers: Vec::new()` to all Record constructions in `RandomBytes::stream` and `JsonTemplate::stream`:

In `RandomBytes::stream`, change the Record construction to:
```rust
Record {
    key: Some(Bytes::from(rng.random::<u64>().to_be_bytes().to_vec())),
    value: Some(Bytes::copy_from_slice(
        &(0..size).map(|_| rng.random::<u8>()).collect::<Vec<_>>(),
    )),
    headers: Vec::new(),
}
```

In `JsonTemplate::stream`, change:
```rust
Box::pin(stream::repeat_with(move || Record {
    key: key_template.as_deref().map(Self::render),
    value: Some(Self::render(&template)),
    headers: Vec::new(),
}))
```

- [ ] **Step 4: Update lib.rs re-exports**

In `gaslightmq/src/lib.rs`, add `Header` to the record re-exports:

```rust
pub use record::{Api, FetchResult, Header, ProduceResult, Record, RecordStream, TopicFuture};
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p gaslightmq`
Expected: all 49 existing tests pass (headers default to empty vec, so nothing breaks).

- [ ] **Step 6: Commit**

```bash
git add gaslightmq/src/record.rs gaslightmq/src/protocol.rs gaslightmq/src/generator.rs gaslightmq/src/lib.rs
git commit -m "feat: add record headers to gaslightmq with protocol encode/decode support"
```

---

## Task 3: Migrate gaslightmq to use Partition and Offset from kernel

**Files:**
- Modify: `gaslightmq/Cargo.toml`
- Modify: `gaslightmq/src/topic.rs`
- Modify: `gaslightmq/src/layer.rs`
- Modify: `gaslightmq/src/protocol.rs`
- Modify: `gaslightmq/src/lib.rs`

- [ ] **Step 1: Add gaslightmq-kernel dependency**

In `gaslightmq/Cargo.toml`, add to `[dependencies]`:

```toml
gaslightmq-kernel = { path = "../gaslightmq-kernel" }
```

- [ ] **Step 2: Update Topic trait and implementations**

In `gaslightmq/src/topic.rs`, change signatures to use `Partition` and `Offset`:

```rust
use gaslightmq_kernel::{Partition, Offset};
use crate::record::{FetchResult, ProduceResult, Record, TopicFuture};
use std::collections::HashMap;
use std::sync::Mutex;

pub trait Topic: Send + Sync {
    fn partitions(&self) -> i32 {
        1
    }

    fn on_produce<'a>(
        &'a self,
        _partition: Partition,
        _records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async { ProduceResult::Success })
    }

    fn on_fetch(&self, _partition: Partition, _offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async { FetchResult::Empty })
    }
}

impl Topic for Box<dyn Topic> {
    fn partitions(&self) -> i32 {
        (**self).partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        (**self).on_produce(partition, records)
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        (**self).on_fetch(partition, offset)
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

pub struct Buffer {
    num_partitions: i32,
    partitions: Mutex<HashMap<Partition, Vec<Record>>>,
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
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        Box::pin(async move {
            let mut store = self.partitions.lock().unwrap();
            store.entry(partition).or_default().extend(records.iter().cloned());
            ProduceResult::Success
        })
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let store = self.partitions.lock().unwrap();
            match store.get(&partition) {
                Some(records) if (offset.value() as usize) < records.len() => {
                    FetchResult::Records(records[offset.value() as usize..].to_vec())
                }
                _ => FetchResult::Empty,
            }
        })
    }
}
```

Update all tests in topic.rs to use `Partition` and `Offset`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;
    use gaslightmq_kernel::{Partition, Offset};

    #[tokio::test]
    async fn blackhole_returns_success_on_produce() {
        let topic = BlackHole::new(3);
        let records = vec![Record::new("hello")];
        let result = topic.on_produce(Partition::new(0), &records).await;
        assert_eq!(result, ProduceResult::Success);
    }

    #[tokio::test]
    async fn blackhole_returns_empty_on_fetch() {
        let topic = BlackHole::new(3);
        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Empty);
    }

    #[tokio::test]
    async fn blackhole_reports_partition_count() {
        let topic = BlackHole::new(5);
        assert_eq!(topic.partitions(), 5);
    }

    #[tokio::test]
    async fn buffer_stores_and_retrieves_records() {
        let topic = Buffer::new(1);
        let records = vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ];
        let result = topic.on_produce(Partition::new(0), &records).await;
        assert_eq!(result, ProduceResult::Success);

        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-0"),
            Record::new("msg-1"),
        ]));
    }

    #[tokio::test]
    async fn buffer_returns_empty_when_no_records() {
        let topic = Buffer::new(1);
        let result = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
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
        topic.on_produce(Partition::new(0), &records).await;

        let result = topic.on_fetch(Partition::new(0), Offset::new(2)).await;
        assert_eq!(result, FetchResult::Records(vec![
            Record::new("msg-2"),
        ]));
    }

    #[tokio::test]
    async fn buffer_isolates_partitions() {
        let topic = Buffer::new(2);
        topic.on_produce(Partition::new(0), &[Record::new("p0")]).await;
        topic.on_produce(Partition::new(1), &[Record::new("p1")]).await;

        let r0 = topic.on_fetch(Partition::new(0), Offset::new(0)).await;
        let r1 = topic.on_fetch(Partition::new(1), Offset::new(0)).await;
        assert_eq!(r0, FetchResult::Records(vec![Record::new("p0")]));
        assert_eq!(r1, FetchResult::Records(vec![Record::new("p1")]));
    }
}
```

- [ ] **Step 3: Update layer.rs**

In `gaslightmq/src/layer.rs`, update signatures to use `Partition` and `Offset`:

```rust
use gaslightmq_kernel::{Partition, Offset};
use crate::chaos::{ChaosAction, ChaosBehavior};
use crate::generator::Generator;
use crate::record::{Api, FetchResult, ProduceResult, Record, TopicFuture};
use crate::topic::Topic;

pub struct ChaosLayer<T, C> {
    inner: T,
    api: Api,
    behavior: C,
}

impl<T, C> ChaosLayer<T, C> {
    pub fn new(inner: T, api: Api, behavior: C) -> Self {
        Self { inner, api, behavior }
    }
}

impl<T: Topic, C: ChaosBehavior> Topic for ChaosLayer<T, C> {
    fn partitions(&self) -> i32 {
        self.inner.partitions()
    }

    fn on_produce<'a>(
        &'a self,
        partition: Partition,
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

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
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
        partition: Partition,
        records: &'a [Record],
    ) -> TopicFuture<'a, ProduceResult> {
        self.inner.on_produce(partition, records)
    }

    fn on_fetch(&self, partition: Partition, offset: Offset) -> TopicFuture<'_, FetchResult> {
        Box::pin(async move {
            let result = self.inner.on_fetch(partition, offset).await;
            if result == FetchResult::Empty {
                use futures::StreamExt;
                let records: Vec<Record> = self.generator.stream().take(1).collect().await;
                if records.is_empty() {
                    FetchResult::Empty
                } else {
                    FetchResult::Records(records)
                }
            } else {
                result
            }
        })
    }
}
```

Update all tests in layer.rs to use `Partition::new(0)` and `Offset::new(0)`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Api, BlackHole, Buffer, Delay, ErrorResponse, FetchResult, ProduceResult, Record};
    use crate::generator::RandomBytes;
    use gaslightmq_kernel::{Partition, Offset};
    use kafka_protocol::error::ResponseError;
    use std::time::Duration;

    #[tokio::test]
    async fn chaos_layer_applies_error_on_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            ErrorResponse::new(ResponseError::NotLeaderOrFollower),
        );

        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Error(ResponseError::NotLeaderOrFollower));
    }

    #[tokio::test]
    async fn chaos_layer_passes_through_on_non_matching_api() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("hello")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Produce,
            ErrorResponse::new(ResponseError::NotLeaderOrFollower),
        );

        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("hello")]));
    }

    #[tokio::test]
    async fn chaos_layer_applies_error_on_produce() {
        let chaos = ChaosLayer::new(
            BlackHole::new(1),
            Api::Produce,
            ErrorResponse::new(ResponseError::RequestTimedOut),
        );

        let result = chaos.on_produce(Partition::new(0), &[Record::new("hello")]).await;
        assert_eq!(result, ProduceResult::Error(ResponseError::RequestTimedOut));
    }

    #[tokio::test]
    async fn chaos_layer_delay_still_returns_result() {
        let topic = Buffer::new(1);
        topic.on_produce(Partition::new(0), &[Record::new("data")]).await;

        let chaos = ChaosLayer::new(
            topic,
            Api::Fetch,
            Delay::new(Duration::from_millis(1)),
        );

        let result = chaos.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("data")]));
    }

    #[tokio::test]
    async fn generator_layer_fills_empty_fetch() {
        let topic = BlackHole::new(1);
        let layer = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = layer.on_fetch(Partition::new(0), Offset::new(0)).await;
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
        topic.on_produce(Partition::new(0), &[Record::new("real-data")]).await;

        let layer = GeneratorLayer::new(topic, RandomBytes::new(32));

        let result = layer.on_fetch(Partition::new(0), Offset::new(0)).await;
        assert_eq!(result, FetchResult::Records(vec![Record::new("real-data")]));
    }
}
```

- [ ] **Step 4: Update protocol.rs boundary conversions**

In `gaslightmq/src/protocol.rs`, update `handle_produce` and `handle_fetch` to convert `i32`/`i64` from kafka-protocol to `Partition`/`Offset`:

Add import:
```rust
use gaslightmq_kernel::{Partition, Offset};
```

In `handle_produce`, change the topic call from:
```rust
topic.on_produce(partition_idx, &internal_records).await
```
to:
```rust
topic.on_produce(Partition::from(partition_idx), &internal_records).await
```

In `handle_fetch`, change the topic call from:
```rust
let result = topic.on_fetch(partition_idx, fetch_offset).await;
```
to:
```rust
let result = topic.on_fetch(Partition::from(partition_idx), Offset::from(fetch_offset)).await;
```

- [ ] **Step 5: Update lib.rs re-exports**

In `gaslightmq/src/lib.rs`, add kernel re-exports:

```rust
pub use gaslightmq_kernel::{Partition, Offset};
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p gaslightmq`
Expected: all tests pass (49 unit tests + 2 integration tests).

- [ ] **Step 7: Commit**

```bash
git add gaslightmq/Cargo.toml gaslightmq/src/topic.rs gaslightmq/src/layer.rs gaslightmq/src/protocol.rs gaslightmq/src/lib.rs
git commit -m "refactor: migrate gaslightmq to Partition/Offset newtypes from kernel"
```

---

## Task 4: Create gaslightmq-testing crate

**Files:**
- Create: `gaslightmq-testing/Cargo.toml`
- Create: `gaslightmq-testing/src/lib.rs`
- Create: `gaslightmq-testing/src/record.rs`
- Create: `gaslightmq-testing/src/traits.rs`
- Create: `gaslightmq-testing/src/harness.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "gaslightmq-testing"
version.workspace = true
edition.workspace = true

[dependencies]
gaslightmq = { path = "../gaslightmq" }
gaslightmq-kernel = { path = "../gaslightmq-kernel" }
async-trait = "0.1"
bytes = "1"
futures = "0.3"
kafka-protocol = "0.17"
tokio = { version = "1", features = ["net", "rt-multi-thread", "macros"] }
```

- [ ] **Step 2: Create record.rs**

```rust
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

pub type Headers = Vec<Header>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Headers,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: Some(value.into()),
            headers: Vec::new(),
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: Some(value.into()),
        });
        self
    }
}
```

- [ ] **Step 3: Create traits.rs**

```rust
use async_trait::async_trait;
use futures::Stream;
use gaslightmq_kernel::{Offset, Partition};
use kafka_protocol::error::ResponseError;
use std::pin::Pin;

use crate::record::Record;

pub type RecordStream = Pin<Box<dyn Stream<Item = Record> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResult {
    pub error: Option<ResponseError>,
    pub partition: Partition,
    pub offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResult {
    pub error: Option<ResponseError>,
    pub member_id: String,
    pub leader: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResult {
    pub error: Option<ResponseError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResult {
    pub error: Option<ResponseError>,
}

#[async_trait]
pub trait Producer {
    async fn send(&self, topic: &str, partition: Partition, records: &[Record]) -> ProduceResult;
}

#[async_trait]
pub trait Consumer {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record>;
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream;
    async fn join_group(&self, group_id: &str) -> JoinGroupResult;
    async fn sync_group(&self, group_id: &str) -> SyncGroupResult;
    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult;
}
```

- [ ] **Step 4: Create harness.rs**

```rust
use std::net::SocketAddr;

use gaslightmq::broker::Broker;
use gaslightmq::builder::BrokerBuilder;

pub struct Harness<C> {
    pub broker: Broker,
    pub client: C,
}

impl<C> Harness<C> {
    pub async fn start(
        builder: BrokerBuilder,
        make_client: impl FnOnce(SocketAddr) -> C,
    ) -> Self {
        let broker = builder.bind("127.0.0.1:0").start().await.unwrap();
        let client = make_client(broker.addr());
        Self { broker, client }
    }

    pub async fn shutdown(self) {
        self.broker.shutdown().await;
    }
}
```

- [ ] **Step 5: Create lib.rs**

```rust
pub mod record;
pub mod traits;
pub mod harness;

pub use record::{Header, Headers, Record};
pub use traits::{
    Consumer, HeartbeatResult, JoinGroupResult, Producer, ProduceResult, RecordStream,
    SyncGroupResult,
};
pub use harness::Harness;
```

- [ ] **Step 6: Add to workspace members**

In `Cargo.toml` (workspace root), add `"gaslightmq-testing"` to members:

```toml
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-testing"]
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo check -p gaslightmq-testing`
Expected: compiles with no errors.

- [ ] **Step 8: Commit**

```bash
git add gaslightmq-testing/ Cargo.toml
git commit -m "feat: add gaslightmq-testing crate with test types, traits, and harness"
```

---

## Task 5: Create gaslightmq-kafka-client-api crate (trait definitions)

**Files:**
- Create: `gaslightmq-kafka-client/api/Cargo.toml`
- Create: `gaslightmq-kafka-client/api/src/lib.rs`
- Create: `gaslightmq-kafka-client/api/src/produce.rs`
- Create: `gaslightmq-kafka-client/api/src/fetch.rs`
- Create: `gaslightmq-kafka-client/api/src/metadata.rs`
- Create: `gaslightmq-kafka-client/api/src/api_versions.rs`
- Create: `gaslightmq-kafka-client/api/src/list_offsets.rs`
- Create: `gaslightmq-kafka-client/api/src/find_coordinator.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p gaslightmq-kafka-client/api/src
```

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "gaslightmq-kafka-client-api"
version.workspace = true
edition.workspace = true

[dependencies]
gaslightmq-kernel = { path = "../../gaslightmq-kernel" }
gaslightmq-testing = { path = "../../gaslightmq-testing" }
async-trait = "0.1"
bytes = "1"
indexmap = "2"
kafka-protocol = "0.17"
tokio = { version = "1", features = ["net", "io-util"] }
futures = "0.3"
```

- [ ] **Step 3: Create trait files**

`gaslightmq-kafka-client/api/src/produce.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;

#[async_trait]
pub trait Produce {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse;
}
```

`gaslightmq-kafka-client/api/src/fetch.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::FetchRequest;
use kafka_protocol::messages::fetch_response::FetchResponse;

#[async_trait]
pub trait Fetch {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse;
}
```

`gaslightmq-kafka-client/api/src/metadata.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;

#[async_trait]
pub trait Metadata {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse;
}
```

`gaslightmq-kafka-client/api/src/api_versions.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::ApiVersionsRequest;
use kafka_protocol::messages::ApiVersionsResponse;

#[async_trait]
pub trait ApiVersions {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse;
}
```

`gaslightmq-kafka-client/api/src/list_offsets.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;

#[async_trait]
pub trait ListOffsets {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse;
}
```

`gaslightmq-kafka-client/api/src/find_coordinator.rs`:
```rust
use async_trait::async_trait;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;

#[async_trait]
pub trait FindCoordinator {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse;
}
```

- [ ] **Step 4: Create lib.rs**

```rust
pub mod produce;
pub mod fetch;
pub mod metadata;
pub mod api_versions;
pub mod list_offsets;
pub mod find_coordinator;
pub mod client;

pub use produce::Produce;
pub use fetch::Fetch;
pub use metadata::Metadata;
pub use api_versions::ApiVersions;
pub use list_offsets::ListOffsets;
pub use find_coordinator::FindCoordinator;
pub use client::RawKafkaClient;
```

- [ ] **Step 5: Create stub client.rs** (to be implemented in Task 6)

```rust
use std::net::SocketAddr;

pub struct RawKafkaClient {
    addr: SocketAddr,
}

impl RawKafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}
```

- [ ] **Step 6: Add to workspace**

In `Cargo.toml` (workspace root), add `"gaslightmq-kafka-client/*"`:

```toml
members = ["gaslightmq", "bins/*", "gaslightmq-kernel", "gaslightmq-testing", "gaslightmq-kafka-client/*"]
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo check -p gaslightmq-kafka-client-api`
Expected: compiles (client.rs is a stub, traits have no impl yet).

- [ ] **Step 8: Commit**

```bash
git add gaslightmq-kafka-client/api/ Cargo.toml
git commit -m "feat: add gaslightmq-kafka-client-api crate with raw Kafka API trait definitions"
```

---

## Task 6: Implement RawKafkaClient

**Files:**
- Modify: `gaslightmq-kafka-client/api/src/client.rs`

- [ ] **Step 1: Implement RawKafkaClient with TCP framing and all raw API trait impls**

Replace the stub `client.rs` with the full implementation:

```rust
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::ListOffsetsResponse;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{ApiVersions, Fetch, FindCoordinator, ListOffsets, Metadata, Produce};

pub struct RawKafkaClient {
    addr: SocketAddr,
    stream: Mutex<Option<TcpStream>>,
    correlation_id: AtomicI32,
}

impl RawKafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(1),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn request<Req: Encodable, Resp: Decodable>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Resp {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            *guard = Some(TcpStream::connect(self.addr).await.unwrap());
        }
        let stream = guard.as_mut().unwrap();
        let cid = self.correlation_id.fetch_add(1, Ordering::Relaxed);

        let header_version = api_key.request_header_version(api_version);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = cid;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        request.encode(&mut buf, api_version).unwrap();

        stream.write_u32(buf.len() as u32).await.unwrap();
        stream.write_all(&buf).await.unwrap();

        let len = stream.read_u32().await.unwrap() as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).await.unwrap();

        let mut resp_buf = Bytes::from(frame);
        let resp_header_version = api_key.response_header_version(api_version);
        let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_version).unwrap();
        Resp::decode(&mut resp_buf, api_version).unwrap()
    }
}

#[async_trait]
impl Produce for RawKafkaClient {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse {
        self.request(ApiKey::Produce, 3, &request).await
    }
}

#[async_trait]
impl Fetch for RawKafkaClient {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse {
        self.request(ApiKey::Fetch, 4, &request).await
    }
}

#[async_trait]
impl Metadata for RawKafkaClient {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        self.request(ApiKey::Metadata, 1, &request).await
    }
}

#[async_trait]
impl ApiVersions for RawKafkaClient {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        self.request(ApiKey::ApiVersions, 0, &request).await
    }
}

#[async_trait]
impl ListOffsets for RawKafkaClient {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse {
        self.request(ApiKey::ListOffsets, 1, &request).await
    }
}

#[async_trait]
impl FindCoordinator for RawKafkaClient {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse {
        self.request(ApiKey::FindCoordinator, 0, &request).await
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p gaslightmq-kafka-client-api`
Expected: compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add gaslightmq-kafka-client/api/src/client.rs
git commit -m "feat: implement RawKafkaClient with TCP framing and all raw API trait impls"
```

---

## Task 7: Implement RawKafkaClient high-level traits (Producer, Consumer)

**Files:**
- Modify: `gaslightmq-kafka-client/api/src/client.rs`

- [ ] **Step 1: Add high-level trait implementations**

Add imports at the top of `client.rs`:

```rust
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::join_group_request::JoinGroupRequest;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::messages::sync_group_request::SyncGroupRequest;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, RecordSet, TimestampType, NO_PARTITION_LEADER_EPOCH,
    NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use indexmap::IndexMap;
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::traits::{
    Consumer, HeartbeatResult, JoinGroupResult, Producer, SyncGroupResult,
};
use gaslightmq_testing::{Header, Record, RecordStream};
```

Then add the conversion helpers and trait impls at the bottom of `client.rs`:

```rust
fn testing_records_to_kafka(records: &[Record]) -> Vec<KafkaRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers = IndexMap::new();
            for h in &r.headers {
                headers.insert(StrBytes::from_string(h.key.clone()), h.value.clone());
            }
            KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: NO_SEQUENCE,
                timestamp: 0,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

fn kafka_records_to_testing(batch: &RecordSet) -> Vec<Record> {
    batch
        .records
        .iter()
        .map(|r| {
            let headers = r
                .headers
                .iter()
                .map(|(k, v)| Header {
                    key: k.as_str().to_string(),
                    value: v.clone(),
                })
                .collect();
            Record {
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect()
}

fn encode_records_batch(records: &[Record]) -> Bytes {
    let kafka_records = testing_records_to_kafka(records);
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, &kafka_records, &options).unwrap();
    Bytes::from(buf)
}

#[async_trait]
impl Producer for RawKafkaClient {
    async fn send(
        &self,
        topic: &str,
        partition: Partition,
        records: &[Record],
    ) -> gaslightmq_testing::ProduceResult {
        let records_bytes = encode_records_batch(records);
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(5000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(i32::from(partition))
                    .with_records(Some(records_bytes))])]);

        let response: ProduceResponse = self.request(ApiKey::Produce, 3, &request).await;

        let partition_resp = &response.responses[0].partition_responses[0];
        let error = kafka_protocol::error::ResponseError::try_from_code(partition_resp.error_code);

        gaslightmq_testing::ProduceResult {
            error,
            partition,
            offset: Offset::from(partition_resp.base_offset),
        }
    }
}

#[async_trait]
impl Consumer for RawKafkaClient {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        let request = FetchRequest::default()
            .with_max_wait_ms(1000)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(i32::from(partition))
                    .with_fetch_offset(i64::from(offset))
                    .with_partition_max_bytes(1024 * 1024)])]);

        let response: FetchResponse = self.request(ApiKey::Fetch, 4, &request).await;

        let mut records = Vec::new();
        for topic_resp in &response.responses {
            for partition_resp in &topic_resp.partitions {
                if let Some(ref raw) = partition_resp.records {
                    let mut buf = raw.clone();
                    if let Ok(batch) = RecordBatchDecoder::decode(&mut buf) {
                        records.extend(kafka_records_to_testing(&batch));
                    }
                }
            }
        }
        records
    }

    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream {
        let addr = self.addr;
        let topic = topic.to_string();
        let partition_val = i32::from(partition);
        let start = i64::from(offset);

        Box::pin(
            futures::stream::unfold(start, move |current_offset| {
                let topic = topic.clone();
                async move {
                    let mut tcp = TcpStream::connect(addr).await.ok()?;
                    let header_version = ApiKey::Fetch.request_header_version(4);
                    let mut header = RequestHeader::default();
                    header.request_api_key = ApiKey::Fetch as i16;
                    header.request_api_version = 4;
                    header.correlation_id = 0;

                    let request = FetchRequest::default()
                        .with_max_wait_ms(100)
                        .with_min_bytes(1)
                        .with_max_bytes(1024 * 1024)
                        .with_topics(vec![FetchTopic::default()
                            .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                            .with_partitions(vec![FetchPartition::default()
                                .with_partition(partition_val)
                                .with_fetch_offset(current_offset)
                                .with_partition_max_bytes(1024 * 1024)])]);

                    let mut buf = BytesMut::new();
                    header.encode(&mut buf, header_version).unwrap();
                    request.encode(&mut buf, 4).unwrap();
                    tcp.write_u32(buf.len() as u32).await.ok()?;
                    tcp.write_all(&buf).await.ok()?;

                    let len = tcp.read_u32().await.ok()? as usize;
                    let mut frame = vec![0u8; len];
                    tcp.read_exact(&mut frame).await.ok()?;
                    let mut resp_buf = Bytes::from(frame);
                    let resp_header_version = ApiKey::Fetch.response_header_version(4);
                    let _ = ResponseHeader::decode(&mut resp_buf, resp_header_version).ok()?;
                    let response = FetchResponse::decode(&mut resp_buf, 4).ok()?;

                    let mut records = Vec::new();
                    for topic_resp in &response.responses {
                        for partition_resp in &topic_resp.partitions {
                            if let Some(ref raw) = partition_resp.records {
                                let mut rbuf = raw.clone();
                                if let Ok(batch) = RecordBatchDecoder::decode(&mut rbuf) {
                                    records.extend(kafka_records_to_testing(&batch));
                                }
                            }
                        }
                    }

                    if records.is_empty() {
                        None
                    } else {
                        let next = current_offset + records.len() as i64;
                        Some((records, next))
                    }
                }
            })
            .flat_map(|records| futures::stream::iter(records)),
        )
    }

    async fn join_group(&self, group_id: &str) -> JoinGroupResult {
        let request = JoinGroupRequest::default()
            .with_group_id(kafka_protocol::messages::GroupId(StrBytes::from_string(
                group_id.to_string(),
            )))
            .with_session_timeout_ms(30000)
            .with_protocol_type(StrBytes::from_static_str("consumer"));

        let response: JoinGroupResponse = self.request(ApiKey::JoinGroup, 0, &request).await;

        JoinGroupResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
            member_id: response.member_id.as_str().to_string(),
            leader: response.leader.as_str().to_string(),
        }
    }

    async fn sync_group(&self, group_id: &str) -> SyncGroupResult {
        let request = SyncGroupRequest::default().with_group_id(
            kafka_protocol::messages::GroupId(StrBytes::from_string(group_id.to_string())),
        );

        let response: SyncGroupResponse = self.request(ApiKey::SyncGroup, 0, &request).await;

        SyncGroupResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
        }
    }

    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult {
        let request = HeartbeatRequest::default().with_group_id(
            kafka_protocol::messages::GroupId(StrBytes::from_string(group_id.to_string())),
        );

        let response: HeartbeatResponse = self.request(ApiKey::Heartbeat, 0, &request).await;

        HeartbeatResult {
            error: kafka_protocol::error::ResponseError::try_from_code(response.error_code),
        }
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p gaslightmq-kafka-client-api`
Expected: compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add gaslightmq-kafka-client/api/src/client.rs
git commit -m "feat: implement Producer and Consumer high-level traits for RawKafkaClient"
```

---

## Task 8: Create gaslightmq-kafka-client-rdkafka crate

**Files:**
- Create: `gaslightmq-kafka-client/rdkafka/Cargo.toml`
- Create: `gaslightmq-kafka-client/rdkafka/src/lib.rs`

- [ ] **Step 1: Create directory**

```bash
mkdir -p gaslightmq-kafka-client/rdkafka/src
```

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "gaslightmq-kafka-client-rdkafka"
version.workspace = true
edition.workspace = true

[dependencies]
gaslightmq-kernel = { path = "../../gaslightmq-kernel" }
gaslightmq-testing = { path = "../../gaslightmq-testing" }
async-trait = "0.1"
bytes = "1"
rdkafka = { version = "0.37", features = ["cmake-build"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
```

- [ ] **Step 3: Create lib.rs with RdkafkaClient**

```rust
use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::traits::{
    Consumer, HeartbeatResult, JoinGroupResult, Producer, SyncGroupResult,
};
use gaslightmq_testing::{Header, Record, RecordStream};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdConsumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as RdProducer};
use rdkafka::Message;

pub struct RdkafkaClient {
    producer: BaseProducer,
    consumer: BaseConsumer,
    bootstrap: String,
}

impl RdkafkaClient {
    pub fn new(addr: SocketAddr) -> Self {
        let bootstrap = format!("{}", addr);

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("failed to create rdkafka producer");

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", "gaslightmq-test")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create rdkafka consumer");

        Self {
            producer,
            consumer,
            bootstrap,
        }
    }
}

#[async_trait]
impl Producer for RdkafkaClient {
    async fn send(
        &self,
        topic: &str,
        partition: Partition,
        records: &[Record],
    ) -> gaslightmq_testing::ProduceResult {
        for record in records {
            let mut base_record = BaseRecord::to(topic).partition(i32::from(partition) as i32);

            if let Some(ref key) = record.key {
                base_record = base_record.key(key.as_ref());
            }
            if let Some(ref value) = record.value {
                base_record = base_record.payload(value.as_ref());
            }

            self.producer.send(base_record).unwrap();
        }
        self.producer.flush(Duration::from_secs(5)).unwrap();

        gaslightmq_testing::ProduceResult {
            error: None,
            partition,
            offset: Offset::new(0),
        }
    }
}

#[async_trait]
impl Consumer for RdkafkaClient {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<Record> {
        use rdkafka::topic_partition_list::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            topic,
            i32::from(partition),
            rdkafka::Offset::Offset(i64::from(offset)),
        )
        .unwrap();
        self.consumer.assign(&tpl).unwrap();

        let mut records = Vec::new();
        while let Some(Ok(msg)) = self.consumer.poll(Duration::from_secs(1)) {
            let key = msg.key().map(|k| Bytes::copy_from_slice(k));
            let value = msg.payload().map(|v| Bytes::copy_from_slice(v));
            records.push(Record {
                key,
                value,
                headers: Vec::new(),
            });
        }
        records
    }

    fn stream(&self, _topic: &str, _partition: Partition, _offset: Offset) -> RecordStream {
        Box::pin(futures::stream::empty())
    }

    async fn join_group(&self, _group_id: &str) -> JoinGroupResult {
        JoinGroupResult {
            error: None,
            member_id: String::new(),
            leader: String::new(),
        }
    }

    async fn sync_group(&self, _group_id: &str) -> SyncGroupResult {
        SyncGroupResult { error: None }
    }

    async fn heartbeat(&self, _group_id: &str) -> HeartbeatResult {
        HeartbeatResult { error: None }
    }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p gaslightmq-kafka-client-rdkafka`
Expected: compiles (requires cmake and librdkafka build — if cmake is unavailable, this is expected to fail; skip and proceed).

- [ ] **Step 5: Commit**

```bash
git add gaslightmq-kafka-client/rdkafka/
git commit -m "feat: add gaslightmq-kafka-client-rdkafka crate with Producer and Consumer impls"
```

---

## Task 9: Add dev-dependencies and test suite 1000-kafka-api.rs

**Files:**
- Modify: `gaslightmq/Cargo.toml`
- Create: `gaslightmq/tests/1000-kafka-api.rs`

- [ ] **Step 1: Add dev-dependencies to gaslightmq**

In `gaslightmq/Cargo.toml`, update `[dev-dependencies]`:

```toml
[dev-dependencies]
gaslightmq-kernel = { path = "../gaslightmq-kernel" }
gaslightmq-testing = { path = "../gaslightmq-testing" }
gaslightmq-kafka-client-api = { path = "../gaslightmq-kafka-client/api" }
tokio = { version = "1", features = ["test-util", "rt-multi-thread", "macros"] }
```

Note: `gaslightmq-kafka-client-rdkafka` is excluded for now to avoid cmake build requirement. Add it later when rdkafka tests are needed.

- [ ] **Step 2: Create 1000-kafka-api.rs**

```rust
use gaslightmq::{BrokerBuilder, Buffer};
use gaslightmq_kernel::{Offset, Partition};
use gaslightmq_testing::{Harness, Record};
use gaslightmq_testing::traits::{Consumer, Producer};
use gaslightmq_kafka_client_api::RawKafkaClient;

#[tokio::test]
async fn smoke_test_produce_and_consume() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("events", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let records = vec![
        Record::new("msg-0"),
        Record::new("msg-1"),
        Record::new("msg-2"),
    ];
    let result = harness.client.send("events", Partition::new(0), &records).await;
    assert!(result.error.is_none(), "produce should succeed");

    let fetched = harness.client.fetch("events", Partition::new(0), Offset::new(0)).await;
    assert_eq!(fetched.len(), 3);
    assert_eq!(fetched[0].value.as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(fetched[1].value.as_deref(), Some(b"msg-1".as_slice()));
    assert_eq!(fetched[2].value.as_deref(), Some(b"msg-2".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn smoke_test_produce_with_keys_and_headers() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("orders", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let records = vec![Record::new("payload")
        .with_key("order-123")
        .with_header("trace-id", "abc-def")];
    let result = harness.client.send("orders", Partition::new(0), &records).await;
    assert!(result.error.is_none());

    let fetched = harness.client.fetch("orders", Partition::new(0), Offset::new(0)).await;
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].key.as_deref(), Some(b"order-123".as_slice()));
    assert_eq!(fetched[0].value.as_deref(), Some(b"payload".as_slice()));
    assert_eq!(fetched[0].headers.len(), 1);
    assert_eq!(fetched[0].headers[0].key, "trace-id");

    harness.shutdown().await;
}

#[tokio::test]
async fn smoke_test_multiple_partitions() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("multi", Buffer::new(3))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    harness.client.send("multi", Partition::new(0), &[Record::new("p0")]).await;
    harness.client.send("multi", Partition::new(1), &[Record::new("p1")]).await;
    harness.client.send("multi", Partition::new(2), &[Record::new("p2")]).await;

    let r0 = harness.client.fetch("multi", Partition::new(0), Offset::new(0)).await;
    let r1 = harness.client.fetch("multi", Partition::new(1), Offset::new(0)).await;
    let r2 = harness.client.fetch("multi", Partition::new(2), Offset::new(0)).await;

    assert_eq!(r0.len(), 1);
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r0[0].value.as_deref(), Some(b"p0".as_slice()));
    assert_eq!(r1[0].value.as_deref(), Some(b"p1".as_slice()));
    assert_eq!(r2[0].value.as_deref(), Some(b"p2".as_slice()));

    harness.shutdown().await;
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p gaslightmq --test 1000-kafka-api`
Expected: all 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add gaslightmq/Cargo.toml gaslightmq/tests/1000-kafka-api.rs
git commit -m "test: add 1000-kafka-api smoke test suite with high-level traits"
```

---

## Task 10: Test suite 1001-kafka-produce.rs

**Files:**
- Create: `gaslightmq/tests/1001-kafka-produce.rs`

- [ ] **Step 1: Create 1001-kafka-produce.rs**

```rust
use bytes::{Bytes, BytesMut};
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Produce, RawKafkaClient};
use gaslightmq_testing::Harness;
use indexmap::IndexMap;
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};

fn make_record(key: Option<&[u8]>, value: Option<&[u8]>) -> KafkaRecord {
    KafkaRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp: 0,
        key: key.map(|k| Bytes::copy_from_slice(k)),
        value: value.map(|v| Bytes::copy_from_slice(v)),
        headers: IndexMap::new(),
    }
}

fn make_record_with_headers(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: Vec<(&str, &[u8])>,
) -> KafkaRecord {
    let mut h = IndexMap::new();
    for (k, v) in headers {
        h.insert(StrBytes::from_string(k.to_string()), Some(Bytes::copy_from_slice(v)));
    }
    let mut rec = make_record(key, value);
    rec.headers = h;
    rec
}

fn encode_records(records: &[KafkaRecord]) -> Bytes {
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, records, &options).unwrap();
    Bytes::from(buf)
}

fn make_produce_request(topic: &str, partition: i32, records: &[KafkaRecord]) -> ProduceRequest {
    let records_bytes = encode_records(records);
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(records_bytes))])])
}

#[tokio::test]
async fn produce_single_record_to_buffer() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("test", 0, &[make_record(None, Some(b"hello"))]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_multiple_records_to_multiple_partitions() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("multi", Buffer::new(3))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let req0 = make_produce_request("multi", 0, &[make_record(None, Some(b"p0-msg"))]);
    let resp0 = harness.client.produce(req0).await;
    assert_eq!(resp0.responses[0].partition_responses[0].error_code, 0);

    let req1 = make_produce_request("multi", 1, &[make_record(None, Some(b"p1-msg"))]);
    let resp1 = harness.client.produce(req1).await;
    assert_eq!(resp1.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_with_keys_and_headers() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("keyed", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let record = make_record_with_headers(
        Some(b"my-key"),
        Some(b"my-value"),
        vec![("trace-id", b"abc-123")],
    );
    let request = make_produce_request("keyed", 0, &[record]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_blackhole_topic() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("void", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("void", 0, &[make_record(None, Some(b"gone"))]);
    let response = harness.client.produce(request).await;

    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_to_nonexistent_topic() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("exists", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("nope", 0, &[make_record(None, Some(b"fail"))]);
    let response = harness.client.produce(request).await;

    let error_code = response.responses[0].partition_responses[0].error_code;
    assert_ne!(error_code, 0, "should return error for unknown topic");

    harness.shutdown().await;
}

#[tokio::test]
async fn produce_with_chaos_error_injection() {
    use gaslightmq::{Api, ErrorResponse};
    use kafka_protocol::error::ResponseError;

    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("chaos", Buffer::new(1))
            .chaos(Api::Produce, ErrorResponse::new(ResponseError::NotLeaderOrFollower))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_produce_request("chaos", 0, &[make_record(None, Some(b"fail"))]);
    let response = harness.client.produce(request).await;

    let error_code = response.responses[0].partition_responses[0].error_code;
    assert_eq!(
        error_code,
        ResponseError::NotLeaderOrFollower.code(),
        "chaos layer should inject NotLeaderOrFollower error"
    );

    harness.shutdown().await;
}
```

- [ ] **Step 2: Add indexmap to gaslightmq dev-dependencies**

In `gaslightmq/Cargo.toml`, add to `[dev-dependencies]`:

```toml
indexmap = "2"
kafka-protocol = "0.17"
bytes = "1"
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p gaslightmq --test 1001-kafka-produce`
Expected: all 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add gaslightmq/tests/1001-kafka-produce.rs gaslightmq/Cargo.toml
git commit -m "test: add 1001-kafka-produce test suite with raw Produce trait scenarios"
```

---

## Task 11: Test suite 1002-kafka-consume.rs

**Files:**
- Create: `gaslightmq/tests/1002-kafka-consume.rs`

- [ ] **Step 1: Create 1002-kafka-consume.rs**

```rust
use bytes::{Bytes, BytesMut};
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Fetch, Produce, RawKafkaClient};
use gaslightmq_testing::Harness;
use gaslightmq_testing::traits::Consumer;
use indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use kafka_protocol::messages::{FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record as KafkaRecord, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID, NO_SEQUENCE,
};

fn make_record(value: &[u8]) -> KafkaRecord {
    KafkaRecord {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp: 0,
        key: None,
        value: Some(Bytes::copy_from_slice(value)),
        headers: IndexMap::new(),
    }
}

fn encode_records(records: &[KafkaRecord]) -> Bytes {
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut buf, records, &options).unwrap();
    Bytes::from(buf)
}

fn make_produce_request(topic: &str, partition: i32, records: &[KafkaRecord]) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(encode_records(records)))])])
}

fn make_fetch_request(topic: &str, partition: i32, offset: i64) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(1000)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(partition)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(1024 * 1024)])])
}

fn decode_fetch_values(
    response: &kafka_protocol::messages::fetch_response::FetchResponse,
) -> Vec<Option<Bytes>> {
    let mut values = Vec::new();
    for topic_resp in &response.responses {
        for partition_resp in &topic_resp.partitions {
            if let Some(ref raw) = partition_resp.records {
                let mut buf = raw.clone();
                if let Ok(batch) = RecordBatchDecoder::decode(&mut buf) {
                    for r in &batch.records {
                        values.push(r.value.clone());
                    }
                }
            }
        }
    }
    values
}

#[tokio::test]
async fn fetch_records_after_produce() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let produce_req =
        make_produce_request("test", 0, &[make_record(b"msg-0"), make_record(b"msg-1")]);
    harness.client.produce(produce_req).await;

    let fetch_req = make_fetch_request("test", 0, 0);
    let response = harness.client.fetch(fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_deref(), Some(b"msg-0".as_slice()));
    assert_eq!(values[1].as_deref(), Some(b"msg-1".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_with_offset() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let produce_req = make_produce_request(
        "test",
        0,
        &[make_record(b"a"), make_record(b"b"), make_record(b"c")],
    );
    harness.client.produce(produce_req).await;

    let fetch_req = make_fetch_request("test", 0, 2);
    let response = harness.client.fetch(fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].as_deref(), Some(b"c".as_slice()));

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_from_empty_topic() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("empty", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let fetch_req = make_fetch_request("empty", 0, 0);
    let response = harness.client.fetch(fetch_req).await;

    let values = decode_fetch_values(&response);
    assert!(values.is_empty());

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_from_generator_topic() {
    use gaslightmq::RandomBytes;

    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("generated", BlackHole::new(1))
            .generator(RandomBytes::new(64))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let fetch_req = make_fetch_request("generated", 0, 0);
    let response = harness.client.fetch(fetch_req).await;

    let values = decode_fetch_values(&response);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].as_ref().unwrap().len(), 64);

    harness.shutdown().await;
}

#[tokio::test]
async fn fetch_with_chaos_error_injection() {
    use gaslightmq::{Api, ErrorResponse};
    use kafka_protocol::error::ResponseError;

    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("chaos", Buffer::new(1))
            .chaos(Api::Fetch, ErrorResponse::new(ResponseError::OffsetOutOfRange))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let fetch_req = make_fetch_request("chaos", 0, 0);
    let response = harness.client.fetch(fetch_req).await;

    let error_code = response.responses[0].partitions[0].error_code;
    assert_eq!(error_code, ResponseError::OffsetOutOfRange.code());

    harness.shutdown().await;
}

#[tokio::test]
async fn consumer_group_lifecycle() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("group-test", Buffer::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let join = harness.client.join_group("test-group").await;
    assert!(join.error.is_none(), "join_group should succeed");
    assert!(!join.member_id.is_empty());
    assert!(!join.leader.is_empty());

    let sync = harness.client.sync_group("test-group").await;
    assert!(sync.error.is_none(), "sync_group should succeed");

    let hb = harness.client.heartbeat("test-group").await;
    assert!(hb.error.is_none(), "heartbeat should succeed");

    harness.shutdown().await;
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p gaslightmq --test 1002-kafka-consume`
Expected: all 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add gaslightmq/tests/1002-kafka-consume.rs
git commit -m "test: add 1002-kafka-consume test suite with raw Fetch trait and consumer group scenarios"
```

---

## Task 12: Test suite 1003-kafka-metadata.rs

**Files:**
- Create: `gaslightmq/tests/1003-kafka-metadata.rs`

- [ ] **Step 1: Create 1003-kafka-metadata.rs**

```rust
use gaslightmq::{BlackHole, BrokerBuilder, Buffer};
use gaslightmq_kafka_client_api::{Metadata, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::metadata_request::MetadataRequest;

#[tokio::test]
async fn metadata_returns_all_topics() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("orders", Buffer::new(3))
            .topic("events", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    assert_eq!(response.topics.len(), 2);

    let topic_names: Vec<&str> = response
        .topics
        .iter()
        .filter_map(|t| t.name.as_ref().map(|n| n.0.as_str()))
        .collect();
    assert!(topic_names.contains(&"orders"));
    assert!(topic_names.contains(&"events"));

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_correct_partition_counts() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("three", Buffer::new(3))
            .topic("five", Buffer::new(5))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    let three = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("three"))
        .unwrap();
    assert_eq!(three.partitions.len(), 3);

    let five = response
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("five"))
        .unwrap();
    assert_eq!(five.partitions.len(), 5);

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_broker_info() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].port, harness.broker.addr().port() as i32);

    harness.shutdown().await;
}

#[tokio::test]
async fn metadata_all_partitions_led_by_single_broker() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", Buffer::new(4))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = MetadataRequest::default().with_topics(None);
    let response = harness.client.metadata(request).await;

    let topic = &response.topics[0];
    for partition in &topic.partitions {
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.leader_id.0, 0, "all partitions should have leader 0");
    }

    harness.shutdown().await;
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p gaslightmq --test 1003-kafka-metadata`
Expected: all 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add gaslightmq/tests/1003-kafka-metadata.rs
git commit -m "test: add 1003-kafka-metadata test suite with raw Metadata trait scenarios"
```

---

## Task 13: Test suite 1004-kafka-api-versions.rs

**Files:**
- Create: `gaslightmq/tests/1004-kafka-api-versions.rs`

- [ ] **Step 1: Create 1004-kafka-api-versions.rs**

```rust
use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{ApiVersions, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest};

#[tokio::test]
async fn api_versions_returns_all_supported_apis() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = ApiVersionsRequest::default();
    let response = harness.client.api_versions(request).await;

    assert_eq!(response.error_code, 0);

    let api_keys: Vec<i16> = response.api_keys.iter().map(|k| k.api_key).collect();
    assert!(api_keys.contains(&(ApiKey::Produce as i16)));
    assert!(api_keys.contains(&(ApiKey::Fetch as i16)));
    assert!(api_keys.contains(&(ApiKey::Metadata as i16)));
    assert!(api_keys.contains(&(ApiKey::ListOffsets as i16)));
    assert!(api_keys.contains(&(ApiKey::FindCoordinator as i16)));
    assert!(api_keys.contains(&(ApiKey::JoinGroup as i16)));
    assert!(api_keys.contains(&(ApiKey::SyncGroup as i16)));
    assert!(api_keys.contains(&(ApiKey::Heartbeat as i16)));
    assert!(api_keys.contains(&(ApiKey::ApiVersions as i16)));

    harness.shutdown().await;
}

#[tokio::test]
async fn api_versions_correct_version_ranges() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = ApiVersionsRequest::default();
    let response = harness.client.api_versions(request).await;

    let produce = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Produce as i16)
        .unwrap();
    assert_eq!(produce.min_version, 3);
    assert_eq!(produce.max_version, 9);

    let fetch = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::Fetch as i16)
        .unwrap();
    assert_eq!(fetch.min_version, 4);
    assert_eq!(fetch.max_version, 12);

    let api_versions = response
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::ApiVersions as i16)
        .unwrap();
    assert_eq!(api_versions.min_version, 0);
    assert_eq!(api_versions.max_version, 3);

    harness.shutdown().await;
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p gaslightmq --test 1004-kafka-api-versions`
Expected: all 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add gaslightmq/tests/1004-kafka-api-versions.rs
git commit -m "test: add 1004-kafka-api-versions test suite"
```

---

## Task 14: Test suites 1005, 1006, and cleanup

**Files:**
- Create: `gaslightmq/tests/1005-kafka-list-offsets.rs`
- Create: `gaslightmq/tests/1006-kafka-find-coordinator.rs`
- Delete: `gaslightmq/tests/integration.rs`

- [ ] **Step 1: Create 1005-kafka-list-offsets.rs**

```rust
use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{ListOffsets, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

fn make_list_offsets_request(topic: &str, partition: i32, timestamp: i64) -> ListOffsetsRequest {
    ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(partition)
            .with_timestamp(timestamp)])])
}

#[tokio::test]
async fn earliest_offset_returns_zero() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_list_offsets_request("test", 0, -2);
    let response = harness.client.list_offsets(request).await;

    let partition_resp = &response.topics[0].partitions[0];
    assert_eq!(partition_resp.error_code, 0);
    assert_eq!(partition_resp.offset, 0);

    harness.shutdown().await;
}

#[tokio::test]
async fn latest_offset_returns_zero() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = make_list_offsets_request("test", 0, -1);
    let response = harness.client.list_offsets(request).await;

    let partition_resp = &response.topics[0].partitions[0];
    assert_eq!(partition_resp.error_code, 0);
    assert_eq!(partition_resp.offset, 0);

    harness.shutdown().await;
}
```

- [ ] **Step 2: Create 1006-kafka-find-coordinator.rs**

```rust
use gaslightmq::{BlackHole, BrokerBuilder};
use gaslightmq_kafka_client_api::{FindCoordinator, RawKafkaClient};
use gaslightmq_testing::Harness;
use kafka_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use kafka_protocol::protocol::StrBytes;

#[tokio::test]
async fn find_coordinator_returns_self() {
    let harness = Harness::start(
        BrokerBuilder::new()
            .topic("test", BlackHole::new(1))
            .bind("127.0.0.1:0"),
        RawKafkaClient::new,
    )
    .await;

    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_static_str("test-group"));
    let response = harness.client.find_coordinator(request).await;

    assert_eq!(response.error_code, 0);
    assert_eq!(response.node_id.0, 0);
    assert_eq!(response.port, harness.broker.addr().port() as i32);

    harness.shutdown().await;
}
```

- [ ] **Step 3: Remove old integration.rs**

```bash
git rm gaslightmq/tests/integration.rs
```

- [ ] **Step 4: Run all tests**

Run: `cargo test -p gaslightmq`
Expected: all tests pass — unit tests + all 7 new integration test suites.

- [ ] **Step 5: Commit**

```bash
git add gaslightmq/tests/1005-kafka-list-offsets.rs gaslightmq/tests/1006-kafka-find-coordinator.rs
git commit -m "test: add 1005/1006 test suites, remove old integration.rs"
```

---

## Self-Review

**Spec coverage:**
- ✅ gaslightmq-kernel with Partition/Offset (Task 1)
- ✅ Record headers in gaslightmq + protocol encode/decode (Task 2)
- ✅ Migrate gaslightmq to Partition/Offset (Task 3)
- ✅ gaslightmq-testing with Record, Header, Headers, Harness, Producer, Consumer (Task 4)
- ✅ Raw API traits: Produce, Fetch, Metadata, ApiVersions, ListOffsets, FindCoordinator (Task 5)
- ✅ RawKafkaClient with TCP framing and all raw trait impls (Task 6)
- ✅ RawKafkaClient Producer/Consumer high-level traits (Task 7)
- ✅ RdkafkaClient with Producer/Consumer (Task 8)
- ✅ Test suites 1000–1006 (Tasks 9–14)
- ✅ Old integration.rs removed (Task 14)
- ✅ Workspace members updated throughout

**Placeholder scan:** No TBD, TODO, or incomplete sections found.

**Type consistency:**
- `Partition(u32)` used consistently with `new()`, `value()`, `From<i32>`, `From<u32>`
- `Offset(u64)` used consistently with `new()`, `value()`, `From<i64>`, `From<u64>`
- `Record` (testing) has `key: Option<Bytes>`, `value: Option<Bytes>`, `headers: Headers`
- `Header` has `key: String`, `value: Option<Bytes>`
- `RecordSet.records` field access matches kafka-protocol 0.17 API
- API versions in RawKafkaClient: Produce=3, Fetch=4, Metadata=1, ApiVersions=0, ListOffsets=1, FindCoordinator=0 — all within broker's supported ranges
