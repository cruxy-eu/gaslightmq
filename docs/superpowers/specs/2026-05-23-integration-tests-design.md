# Integration Test Framework Design Spec

Structured integration test suites for all implemented Kafka API calls, with pluggable client implementations.

## Crate Structure

```
gaslightmq-kernel/                   # value objects: Partition, Offset, TopicName, etc.
  Cargo.toml                         # name = "gaslightmq-kernel"
  src/
    lib.rs
gaslightmq/                          # existing library crate (the broker), depends on gaslightmq-kernel
gaslightmq-testing/                  # test support library
  Cargo.toml                         # name = "gaslightmq-testing"
  src/
    lib.rs
    record.rs                        # TestRecord, TestHeader
    harness.rs                       # TestHarness
    traits.rs                        # high-level traits: Producer, Consumer
gaslightmq-kafka-client/             # directory grouping, not a crate
  api/                               # raw Kafka API traits + TCP client
    Cargo.toml                       # name = "gaslightmq-kafka-client-api"
    src/
      lib.rs
      produce.rs                     # Produce trait
      fetch.rs                       # Fetch trait
      metadata.rs                    # Metadata trait
      api_versions.rs                # ApiVersions trait
      list_offsets.rs                # ListOffsets trait
      find_coordinator.rs            # FindCoordinator trait
      client.rs                      # RawKafkaClient — TCP impl of all traits
  rdkafka/                           # rdkafka-based client
    Cargo.toml                       # name = "gaslightmq-kafka-client-rdkafka"
    src/
      lib.rs                         # RdkafkaClient — impl of Producer, Consumer
```

Workspace members addition: `"gaslightmq-kernel"`, `"gaslightmq-testing"`, `"gaslightmq-kafka-client/*"`.

## Kernel Value Objects (`gaslightmq-kernel`)

Shared newtype wrappers used across all crates. No dependencies beyond std.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Partition(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);
```

Private inner value — accessed via `Partition::new(u32)` / `Partition::value(&self) -> u32` (and same for `Offset` with `u64`). Implement `From<i32> for Partition`, `From<Partition> for i32` (and `i64`↔`Offset`) for ergonomic conversion at kafka-protocol boundaries. `gaslightmq`, `gaslightmq-testing`, and the client crates all depend on `gaslightmq-kernel`.

The existing `gaslightmq` crate is updated to use `Partition` and `Offset` in its public API (Topic trait, Record types, FetchResult, etc.).

## Two Trait Layers

### Raw API Traits (`gaslightmq-kafka-client-api`)

One trait per Kafka API. Take and return `kafka_protocol` types directly. Only the raw TCP client implements these.

```rust
use kafka_protocol::messages::*;

#[async_trait]
pub trait Produce {
    async fn produce(&self, request: ProduceRequest) -> ProduceResponse;
}

#[async_trait]
pub trait Fetch {
    async fn fetch(&self, request: FetchRequest) -> FetchResponse;
}

#[async_trait]
pub trait Metadata {
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse;
}

#[async_trait]
pub trait ApiVersions {
    async fn api_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse;
}

#[async_trait]
pub trait ListOffsets {
    async fn list_offsets(&self, request: ListOffsetsRequest) -> ListOffsetsResponse;
}

#[async_trait]
pub trait FindCoordinator {
    async fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse;
}
```

### High-Level Traits (`gaslightmq-testing`)

Client-like abstractions using `TestRecord`. Both raw and rdkafka clients can implement these.

```rust
#[async_trait]
pub trait Producer {
    async fn send(&self, topic: &str, partition: Partition, records: &[TestRecord]) -> ProduceResult;
}

#[async_trait]
pub trait Consumer {
    async fn fetch(&self, topic: &str, partition: Partition, offset: Offset) -> Vec<TestRecord>;
    fn stream(&self, topic: &str, partition: Partition, offset: Offset) -> RecordStream;
    async fn join_group(&self, group_id: &str) -> JoinGroupResult;
    async fn sync_group(&self, group_id: &str) -> SyncGroupResult;
    async fn heartbeat(&self, group_id: &str) -> HeartbeatResult;
}
```

`RecordStream = Pin<Box<dyn Stream<Item = TestRecord> + Send>>`.

Result types use `kafka_protocol::error::ResponseError` — `None` means success:

```rust
pub struct ProduceResult {
    pub error: Option<ResponseError>,
    pub partition: Partition,
    pub offset: Offset,
}
```

Consumer group result types:

```rust
pub struct JoinGroupResult {
    pub error: Option<ResponseError>,
    pub member_id: String,
    pub leader: String,
}

pub struct SyncGroupResult {
    pub error: Option<ResponseError>,
}

pub struct HeartbeatResult {
    pub error: Option<ResponseError>,
}
```

## Shared Types (`gaslightmq-testing`)

```rust
pub struct TestHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

pub struct TestRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<TestHeader>,
}
```

## TestHarness (`gaslightmq-testing`)

```rust
pub struct TestHarness<C> {
    pub broker: Broker,
    pub client: C,
}

impl<C> TestHarness<C> {
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

## Client Implementations

### RawKafkaClient (`gaslightmq-kafka-client-api`)

TCP client using `kafka_protocol` for encoding/decoding. Implements all raw API traits and both high-level traits (`Producer`, `Consumer`).

Holds a `TcpStream` (or reconnectable wrapper), manages correlation IDs, handles framing (4-byte length prefix + request header + body).

### RdkafkaClient (`gaslightmq-kafka-client-rdkafka`)

Wraps `rdkafka::producer::BaseProducer` and `rdkafka::consumer::BaseConsumer` (or `StreamConsumer` for the stream trait). Implements the high-level traits only (`Producer`, `Consumer`). Does NOT implement the raw API traits.

## Test Suites

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

The existing `gaslightmq/tests/integration.rs` is removed — these suites replace it.

### 1000-kafka-api.rs

Smoke test exercising the full happy path: connect → api versions → metadata → produce records → consume records. Uses high-level traits. Tests run against both raw and rdkafka clients.

### 1001-kafka-produce.rs

Detailed produce scenarios using the raw `Produce` trait:
- Produce single record to buffer topic
- Produce multiple records to multiple partitions
- Produce with keys and headers
- Produce to blackhole topic
- Produce to nonexistent topic (error response)
- Produce with chaos layer (error injection, delay)

### 1002-kafka-consume.rs

Detailed fetch/consume scenarios using the raw `Fetch` trait:
- Fetch records after produce
- Fetch with offset
- Fetch from empty topic
- Fetch from partition with generator
- Fetch with chaos layer (error injection, delay)
- Consumer group lifecycle: join → sync → heartbeat

### 1003-kafka-metadata.rs

Using the raw `Metadata` trait:
- Metadata for all topics
- Metadata returns correct partition counts
- Metadata returns broker info
- Single broker, all partitions local

### 1004-kafka-api-versions.rs

Using the raw `ApiVersions` trait:
- Returns all supported APIs
- Correct version ranges per API

### 1005-kafka-list-offsets.rs

Using the raw `ListOffsets` trait:
- Earliest offset returns 0
- Latest offset behavior

### 1006-kafka-find-coordinator.rs

Using the raw `FindCoordinator` trait:
- Returns self as coordinator

## Test Style

Scenario functions — well-named test functions that read like scenarios. No macro DSL, no Given/When/Then builder.

Each test file that uses raw API traits has helper functions to build common request patterns (e.g., `make_produce_request(topic, partition, records)`), keeping the test bodies focused on the scenario.

## Prerequisite: Record Headers

`gaslightmq::Record` currently only has `key: Option<Bytes>` and `value: Option<Bytes>`. Add:

```rust
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}
```

Update protocol handlers to encode/decode headers in record batches.

## Dependencies

### gaslightmq-kernel

```toml
[dependencies]
# no external dependencies — std only
```

### gaslightmq-testing

```toml
[dependencies]
gaslightmq = { path = "../gaslightmq" }
gaslightmq-kernel = { path = "../gaslightmq-kernel" }
async-trait = "0.1"
futures = "0.3"
tokio = { version = "1", features = ["net", "rt-multi-thread", "macros"] }
```

### gaslightmq-kafka-client-api

```toml
[dependencies]
gaslightmq-kernel = { path = "../../gaslightmq-kernel" }
gaslightmq-testing = { path = "../../gaslightmq-testing" }
async-trait = "0.1"
bytes = "1"
kafka-protocol = "0.17"
tokio = { version = "1", features = ["net", "io-util"] }
```

### gaslightmq-kafka-client-rdkafka

```toml
[dependencies]
gaslightmq-kernel = { path = "../../gaslightmq-kernel" }
gaslightmq-testing = { path = "../../gaslightmq-testing" }
async-trait = "0.1"
rdkafka = { version = "0.37", features = ["cmake-build"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### gaslightmq (dev-dependencies addition)

```toml
[dev-dependencies]
gaslightmq-testing = { path = "../gaslightmq-testing" }
gaslightmq-kafka-client-api = { path = "../gaslightmq-kafka-client/api" }
gaslightmq-kafka-client-rdkafka = { path = "../gaslightmq-kafka-client/rdkafka" }
tokio = { version = "1", features = ["test-util"] }
```

## Toolchain

- Rust edition 2024
- Toolchain 1.94.1
