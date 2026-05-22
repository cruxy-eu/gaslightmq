# Gaslight MQ Design Spec

A fake Kafka broker for testing chaos scenarios using real connections. It speaks the Kafka wire protocol, accepts real Kafka clients, and lets you control exactly what happens — delays, timeouts, error responses, fake data — per topic, per API call.

It's even lying about being a message queue.

## Workspace Structure

Cargo workspace with two crates:

```
gaslightmq/
├── gaslightmq/            # library crate
│   └── src/
│       ├── lib.rs
│       ├── broker.rs      # Broker struct, TCP accept loop
│       ├── protocol.rs    # Kafka protocol framing + request dispatch
│       ├── topic.rs       # Topic trait, Buffer, BlackHole
│       ├── chaos.rs       # ChaosBehavior trait, Delay, Timeout, ErrorResponse, Sequence
│       ├── generator.rs   # Generator trait, RandomBytes, JsonTemplate
│       ├── layer.rs       # ChaosLayer, GeneratorLayer (tower-style wrappers)
│       └── scenario.rs    # Scenario model, YAML deserialization
└── gmq/                   # binary crate
    └── src/
        └── main.rs        # CLI (clap), loads YAML scenario, starts broker
```

- `gaslightmq` — the library. All core logic lives here. No dependency on axum.
- `gmq` — thin binary. CLI parsing, YAML loading, starts the broker.

## Core Traits

### Topic

Pluggable topic behavior. Default implementations do the sensible thing — accept produces, return empty fetches — so users only override what they care about.

```rust
trait Topic: Send + Sync {
    fn partitions(&self) -> i32 {
        1
    }

    fn on_produce(&self, partition: i32, records: &[Record]) -> ProduceResult {
        ProduceResult::Success
    }

    fn on_fetch(&self, partition: i32, offset: i64) -> FetchResult {
        FetchResult::Empty
    }
}
```

`FetchResult::Records` contains a `Vec<Record>` — a concrete batch, matching Kafka's Fetch response semantics.

Built-in implementations:

- **`BlackHole`** — accepts produces, returns nothing on fetch. Uses the defaults as-is.
- **`Buffer`** — stores produced messages in memory, returns them on fetch. Configurable partition count.

Users implement `Topic` for custom behavior (e.g., a topic that drops every Nth message).

### ChaosBehavior

Controls what happens to a request before or instead of the inner topic handling it.

```rust
trait ChaosBehavior: Send + Sync {
    async fn apply<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send;
}
```

Note: this is an async trait — `Delay` and `Timeout` need to sleep/stall, which requires `.await`.

Built-in implementations:

- **`Delay`** — sleeps for a configured duration, then executes the inner call.
- **`Timeout`** — never responds. Holds the connection until the client gives up.
- **`ErrorResponse`** — skips the inner call, returns a configured Kafka error code.
- **`Sequence`** — wraps a list of chaos behaviors, advances through them per call.

### Generator

Produces a stream of records for consumers when a topic has no real data. Returns a `Stream` instead of a `Vec` — enabling lazy, potentially infinite data generation.

```rust
pub type RecordStream = Pin<Box<dyn Stream<Item = Record> + Send>>;

trait Generator: Send + Sync {
    fn stream(&self) -> RecordStream;
}
```

Built-in implementations:

- **`RandomBytes`** — yields an infinite stream of records with random byte values of a configurable size.
- **`JsonTemplate`** — yields an infinite stream of records from a template string with faker placeholders (e.g., `{{uuid}}`, `{{name}}`).

Users implement `Generator` for custom data shapes (protobuf, avro, etc.). The protocol handler collects records from the stream until it fills the response (up to `max_bytes` / `max_wait_ms`).

## Composition Model

Tower-style generic layers. Each layer wraps an inner `Topic` and is itself a `Topic`:

```rust
struct ChaosLayer<T, C> {
    inner: T,
    api: Api,
    behavior: C,
}

impl<T: Topic, C: ChaosBehavior> Topic for ChaosLayer<T, C> { ... }

struct GeneratorLayer<T, G> {
    inner: T,
    generator: G,
}

impl<T: Topic, G: Generator> Topic for GeneratorLayer<T, G> { ... }
```

Multiple chaos rules nest: `ChaosLayer<ChaosLayer<Buffer, Delay>, ErrorResponse>`.

All layers are monomorphic — no `Box<dyn>` in the middleware chain. The only type erasure is at the `Broker` level, which stores `Box<dyn Topic>` per topic name because it needs a heterogeneous collection.

## Builder API

```rust
let broker = gaslightmq::Broker::builder()
    .topic("orders", Buffer::new(3))
        .chaos(Api::Fetch, Delay::new(Duration::from_secs(2)))
        .chaos(Api::Produce, ErrorResponse::new(ResponseError::NotLeaderForPartition))
        .generator(JsonTemplate::new(r#"{"user_id": "{{uuid}}", "name": "{{name}}"}"#))
    .topic("events", BlackHole::new(1))
    .bind("127.0.0.1:0")
    .start()
    .await?;

let addr = broker.addr();
// ... run tests ...
broker.shutdown().await;
```

Each `.chaos()` / `.generator()` call wraps the current topic in another generic layer. The builder erases to `Box<dyn Topic>` when transitioning to the next `.topic()` call or `.start()`.

## Chaos Behaviors

### Repeat and Sequencing

Each behavior supports an optional repeat count (default 1). Sequences step through a list of behaviors, each with their own repeat count.

After all steps in a sequence complete, the `after_sequence` policy controls what happens:

- **`loop`** — restart from the beginning.
- **`stop`** — pass through to the inner topic (no more chaos).
- **`repeat_last`** — keep firing the final step forever.

### Chaos Enum

For sequences that mix different behavior types, a `Chaos` enum wraps the built-in behaviors:

```rust
enum Chaos {
    Delay(Delay),
    Timeout(Timeout),
    Error(ErrorResponse),
    Sequence(Sequence),
}
```

`Chaos` implements `ChaosBehavior`. `Sequence` holds a `Vec<Chaos>` with per-step repeat counts. Users implementing custom behaviors use the `ChaosBehavior` trait directly via generic layers.

## Protocol Handling

TCP listener using tokio, Kafka wire protocol via `kafka-protocol-rs`.

### Connection Flow

1. TCP accept loop — spawns a task per connection.
2. Read 4-byte length prefix, then the full request frame.
3. Decode `RequestHeader`, dispatch on `api_key`.
4. Encode response, write back with length prefix.

### Supported APIs

| API | Purpose |
|-----|---------|
| `ApiVersions` | Client handshake, returns supported API versions |
| `Metadata` | Returns topic metadata, single broker, all partitions local |
| `Produce` | Routes to topic's `on_produce` |
| `Fetch` | Routes to topic's `on_fetch` |
| `ListOffsets` | Returns current offsets for requested partitions |
| `FindCoordinator` | Returns self as coordinator |
| `JoinGroup` | Minimal consumer group stub |
| `SyncGroup` | Minimal consumer group stub |
| `Heartbeat` | Minimal consumer group stub |

Unsupported APIs return `UnsupportedVersion` error code.

The protocol module handles framing and dispatch. It does not know about chaos — it calls into whichever `Topic` impl is registered, and the chaos layers act transparently.

### Cluster Topology

Single-node cluster. One broker, all partitions live on it. The broker advertises itself in all metadata responses.

## Scenario Files

YAML files describe the broker's initial state. Loaded by the binary or usable from the library.

### Format

```yaml
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
        error_code: NotLeaderForPartition

    generators:
      - format: json
        template: '{"user_id": "{{uuid}}", "event": "{{word}}"}'

  - name: events
    partitions: 1
    mode: blackhole
    chaos:
      - api: Fetch
        sequence:
          - behavior: success
            repeat: 3
          - behavior: timeout
            timeout_ms: 30000
          - behavior: error
            error_code: CorruptMessage
            repeat: 2
        after_sequence: loop
```

### Usage

From the binary:

```
gmq --scenario chaos.yaml --bind 0.0.0.0:9092
```

From the library:

```rust
let scenario = Scenario::from_yaml(include_str!("chaos.yaml"))?;
let broker = Broker::from_scenario(scenario)
    .bind("127.0.0.1:9092")
    .start()
    .await?;
```

Scenario files only support built-in types (`Buffer`, `BlackHole`, built-in chaos behaviors, built-in generators). Custom `Topic` / `ChaosBehavior` / `Generator` implementations require the Rust API.

## Toolchain

- Rust edition 2024
- Toolchain 1.94.1

## Key Dependencies

- **`kafka-protocol`** — Kafka wire protocol encoding/decoding, message types, `ResponseError`
- **`tokio`** — async runtime, TCP listener, task spawning
- **`futures`** — `Stream` trait and combinators for `RecordStream` / `Generator`
- **`serde` + `serde_yaml`** — scenario file deserialization
- **`clap`** — CLI argument parsing (binary crate only)
- **`fake`** — faker data generation for `JsonTemplate`

## Future Work (Out of Scope for v1)

- HTTP control plane (axum) for runtime mutation
- Dioxus web console
- Data corruption chaos behavior
- Partial failure chaos behavior (some partitions succeed, others fail)
- Multi-broker simulation
- TOML scenario file support
