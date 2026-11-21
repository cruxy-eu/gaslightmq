# Crate layout

The workspace splits GaslightMQ into many small crates.
Each crate holds one district of the [town plan](../townplan/README.md),
or one component of the [engine](README.md#components).
This document describes the target layout.
Moving the current crates to it is a separate plan.

Every crate name starts with `gmq_` and uses underscores, never hyphens.
The name in `Cargo.toml`, the directory and the path in code are then one string,
so one search finds all three.
The tables leave the prefix out: `selector` is the crate `gmq_selector`.
Two crates carry no prefix: the facade `gaslightmq` and the binary `gmq`.

## Principles

- **Logic is sans-IO.**
  A sans-IO crate has no async code, no sockets, no file access, no system clock and no global randomness.
  It takes inputs and the current time as arguments, and returns what should happen.
  Tests drive it with plain function calls.
- **IO lives at the edge.**
  Only a few crates touch the network, the file system or a runtime.
  They are async and thin, and they call the sans-IO crates.
  No crate comes in a blocking and an async variant.
  See [the IO edge is async](#the-io-edge-is-async).
- **A crate boundary needs a reason.**
  A crate exists because something else reuses it,
  because it keeps a heavy dependency away from the rest,
  or because it must compile to WebAssembly for the UI.
  Code that always changes together stays in one crate.
- **Dependencies follow the town plan.**
  Vocabulary crates depend on nothing.
  Logic crates depend on vocabulary.
  The engine depends on logic.
  Nothing depends on the engine except the binaries and the test library.
- **Definitions live with their domain.**
  The crate that owns a concept owns its definition type and its `serde` mapping.
  The `workspace` crate only assembles and cross-checks them.

The workspace rules apply to every crate:
Rust edition 2024, native async traits and workspace dependency versions.

## Layers

```mermaid
flowchart BT
  V["Vocabulary<br/>field · clock · wire · lie · event · call"]
  T["Targeting and injection<br/>selector · trigger · rule · combine · wire_edit · lie_*"]
  P["Session and pipeline<br/>session · pipeline"]
  S["Scenarios<br/>scenario"]
  O["Observation<br/>journal · traffic · index · recording"]
  W["World and backends<br/>backend · virtual_log · world · sim_* · broker · overlay"]
  F["Workspace<br/>environment · workspace"]
  C["Control<br/>surface · quick_inject · control_api"]
  IO["IO edge<br/>listener · engine · host · backend_upstream · schema_registry<br/>workspace_fs · control_server · control_client"]
  A["Applications<br/>gmq · ui · test · testcontainers · kafka_client"]

  T --> V
  P --> T
  S --> T
  S --> O
  O --> V
  O --> T
  W --> V
  W --> O
  F --> T
  F --> S
  F --> W
  F --> C
  C --> T
  C --> S
  C --> W
  IO --> P
  IO --> S
  IO --> O
  IO --> W
  IO --> F
  IO --> C
  A --> IO
  A --> C
```

An arrow means "depends on".
Every crate outside the IO edge is sans-IO.
The tables list each IO crate next to the domain it serves, and mark it with "This crate does IO".

## Crates

### Vocabulary

Vocabulary crates hold types and almost no behaviour.
Every other crate speaks in them.

| Crate | Holds | Depends on |
|---|---|---|
| `field` | The [field vocabulary](../domains/targeting.md#fields): field names, values, and the `FieldSource` trait that a live call and a recorded call both implement. This is the shared kernel of the town plan. | — |
| `clock` | The [clock](README.md#clock): a `Clock` trait, the virtual clock, and instants that do not come from the system. | — |
| `wire` | The Kafka codec: frames, headers, lazily decoded bodies and record batches. | — |
| `lie` | The [lie](../domains/lies-and-events.md#lies) vocabulary: kind, stage, target, combination class, the `Lie` trait, and the effect a lie asks the pipeline for. | `field`, `wire` |
| `event` | The [event](../domains/lies-and-events.md#events) vocabulary, the expansion into consequences, and the capabilities a backend declares. | — |
| `call` | The [call](../domains/observation.md#call) record: identity, request summary, truth, delivered response, verdict, explanation and timing. | `field`, `lie`, `event` |

### Targeting and injection

| Crate | Holds | Depends on |
|---|---|---|
| `selector` | Conditions, selectors, includes and scope. Matching against a `FieldSource`, with the [target](../domains/lies-and-events.md#target) a match yields. An index of selectors by API. | `field` |
| `trigger` | [Triggers](../domains/rules.md#trigger), counters, dice, budgets and lifetimes. | `clock` |
| `rule` | The rule definition, its state, and the immutable snapshot of armed rules in priority order. | `selector`, `lie`, `event`, `trigger` |
| `combine` | [How lies combine](../domains/lies-and-events.md#how-lies-combine): which lies conflict, which win by priority, which are shadowed or pre-empted. | `lie` |
| `wire_edit` | Surgery on requests and responses: remove partitions, merge responses, synthesize an error response for any API, map topic IDs to names. | `wire` |
| `lie_refusal` | Refusals for every API, before forwarding and after the response. | `lie`, `wire_edit` |
| `lie_topology` | Rewrites of Metadata, `FindCoordinator` and `DescribeCluster`. | `lie`, `wire_edit` |
| `lie_data` | Altered, dropped, duplicated, reordered and corrupted records. | `lie`, `wire_edit`, `schema` |
| `lie_progress` | Rewrites of watermarks, log start offsets and committed offsets. | `lie`, `wire_edit` |
| `lie_config` | Rewrites of `DescribeConfigs` responses. | `lie`, `wire_edit` |

### Session and pipeline

| Crate | Holds | Depends on |
|---|---|---|
| `session` | The state of one [session](../domains/session.md), which its pipeline owns: lifecycle, identity, session key, per-session counters, in-flight calls, response ordering and the stalled state. Also the client context that sessions share. | `field`, `wire`, `trigger`, `clock` |
| `pipeline` | The [request pipeline](request-pipeline.md) as a state machine: all stages, both evaluation passes and partial refusals. | `wire`, `wire_edit`, `selector`, `rule`, `trigger`, `lie`, `combine`, `session`, `call`, `event`, `clock` |

### Scenarios

| Crate | Holds | Depends on |
|---|---|---|
| `scenario` | The [scenario](../domains/scenarios.md) definition, modes as priority orders, the step machine of a Sequence, the live set and capture. It consumes call records, journal entries and time, and it emits arm and disarm commands. | `rule`, `trigger`, `call`, `journal`, `event`, `clock` |

### Observation

| Crate | Holds | Depends on |
|---|---|---|
| `traffic` | The live window of recent calls, subscriptions by selector, and statistics per rule and per API. | `call`, `selector` |
| `index` | The [index](../domains/observation.md#index): facets, counts under the active filters, suggested patterns. | `call`, `selector` |
| `journal` | The [journal](../domains/observation.md#journal): its entries, and the window of recent entries the engine keeps in memory. | `event`, `clock` |
| `recording` | The [recording](storage.md#recordings) format: append-only chunks with summaries, calls and journal entries, metadata-only mode, blanked credentials, a reader and a writer over `std::io`. | `call`, `journal`, `wire` |

### World and backends

| Crate | Holds | Depends on |
|---|---|---|
| `backend` | The [backend contract](backends.md#contract): the request and answer messages, and the one async trait through which the engine reaches a backend. It holds no logic and needs no runtime. See [the backend seam](#the-backend-seam). | `wire`, `event`, `clock` |
| `schema` | Record [formats and schemas](../domains/world.md#schemas): encode and decode JSON, Avro and Protobuf, one feature per format. | — |
| `virtual_log` | [Virtual records](../domains/world.md#virtual-records): the record at a partition and offset as a pure function of the definition, templates, stable batches, and the mapping between offsets and time. | `wire`, `clock`, `schema` |
| `schema_registry` | The built-in [schema registry](backends.md#schema-registry) and a client for a real one. This crate does IO. | `schema` |
| `world` | The [World](../domains/world.md): its state, world definitions, how each event changes it, and where each topic's records come from. | `wire`, `event`, `clock`, `virtual_log` |
| `world_seed` | Seeding a world definition from a recording. | `world`, `index`, `recording` |
| `sim_log` | Produce, Fetch, offsets and idempotent producers against the World. | `world`, `wire` |
| `sim_group` | Group coordination against the World, for the classic and the consumer protocol. | `world`, `wire`, `clock` |
| `overlay` | The Upstream [overlay](backends.md#overlay): address translation, emulated events and sessionless Fetch. It rewrites requests and responses, and it opens no connection. | `wire`, `wire_edit`, `event` |
| `broker` | The broker instance as a [state machine](#the-broker-is-a-state-machine-too): it routes each request to a `sim_*` crate, applies events to the World and owns the virtual clock. It speaks the messages of the backend contract, so a proxy reaches it without a socket. | `backend`, `world`, `sim_log`, `sim_group`, `clock` |
| `backend_upstream` | The Upstream backend: connections to the real cluster, with the overlay around them. This crate does IO. | `backend`, `overlay` |

Transactions arrive later as `sim_txn`, next to `sim_log` and `sim_group`.

### Workspace

| Crate | Holds | Depends on |
|---|---|---|
| `environment` | [Environments](storage.md#environments), [secret references](storage.md#secrets), and the order in which flags, variables and files set a value. It never reads a secret. | — |
| `workspace` | The whole set of definitions as one model: cross-reference validation, renames that cascade, and the YAML layout of the [workspace](storage.md#workspace). It works on file contents, not on files. | `selector`, `lie`, `event`, `rule`, `scenario`, `surface`, `world`, `environment` |
| `workspace_fs` | Loading, watching and writing back, the scratch file, and reading the files that secret references name. This crate does IO. | `workspace` |

### Control

| Crate | Holds | Depends on |
|---|---|---|
| `surface` | [Pad layouts](../domains/control-surfaces.md#pad-layout), layers, pads as positions, the encoder mapping, and the intents a surface sends. | `rule` |
| `hardware_push2` | The Push 2 mapping: controller messages to surface intents, and rule state back to pad colours. | `surface` |
| `quick_inject` | The ⌘K sentence parser: text to a rule definition, with completions. | `selector`, `lie`, `rule` |
| `control_api` | The [control plane](control-plane.md) as types: resources, commands and stream messages. | `call`, `journal`, `rule`, `scenario`, `selector`, `surface`, `world`, `event`, `environment` |
| `control_client` | A client for the control plane, for native targets and for WebAssembly. This crate does IO. | `control_api` |
| `control_server` | The HTTP and WebSocket server in front of the host. This crate does IO. | `host`, `control_api` |

### Runtime and applications

| Crate | Holds | Depends on |
|---|---|---|
| `engine` | The runtime of one proxy: one task per session that owns the transport and drives the pipeline, the rule store, the scenario runner, the event bus, the journal, the recorder and the indexer. This crate does IO. | `pipeline`, `session`, `rule`, `scenario`, `journal`, `traffic`, `index`, `recording`, `backend`, `listener`, `workspace`, `environment`, `clock` |
| `ui` | The web UI, a Dioxus application. | `control_client`, `surface`, `hardware_push2`, `quick_inject`, `selector`, `call` |
| `listener` | Kafka on a socket: accept connections, frame bytes, and drive whatever handles a connection. A proxy's pipeline and a broker alone both sit behind it. This crate does IO. | `wire`, `clock` |
| `host` | One process with several [environments](storage.md#several-environments-in-one-process): it starts and stops proxies and brokers, runs the task that drives each broker, and shares the workspace and the control plane between them. This crate does IO. | `engine`, `broker`, `listener`, `backend_upstream`, `workspace_fs`, `environment` |
| `test` | The test library: create a proxy or a broker, activate a scenario, assert on the recording. | `host`, `control_client` |
| `testcontainers` | A container module that starts GaslightMQ next to a test. | `control_client` |
| `kafka_client` | A Kafka client for tests: it follows partition leaders, negotiates API versions, and reports every failure as an error. This crate does IO. | `wire` |
| `gaslightmq` | The facade crate: `Proxy`, `Broker` and `Host`, and re-exports of what library users need. | The public crates |
| `gmq` | The binary: `gmq proxy`, `gmq broker`, `gmq start` and `gmq run`. | `host`, `control_server` |

## The pipeline is sans-IO

`pipeline` is a state machine for one connection.
The engine feeds it what happened and performs what it returns.
The pipeline never reads a socket, never sleeps and never looks at a clock.

```rust
/// What happened. The engine reports it.
pub enum Input<'a> {
    ClientBytes(&'a [u8]),
    BackendAnswered { call: CallId, response: Response },
    TimerFired(TimerId),
    SnapshotPublished(Arc<Snapshot>),
    ClientClosed,
}

/// What should happen. The engine performs it.
pub enum Output {
    Forward { call: CallId, request: Request },
    Send(Bytes),
    SetTimer { id: TimerId, at: Instant },
    Close,
    Emit(Call),
    Fire(Event),
}

impl Pipeline {
    pub fn handle(&mut self, input: Input<'_>, now: Instant) -> Outputs;
}
```

The sketch shows the shape of the interface, not its final names.

- A delay is a `SetTimer`, and the delayed response leaves on `TimerFired`.
- A silence returns no `Send` and marks the session stalled.
- A partial refusal returns a `Forward` with fewer partitions,
  and merges the refusals when `BackendAnswered` arrives.
- Shared counters and budgets are handles to atomics that the engine passes in.
  The pipeline increments them, and it still does no IO.
- The [client context](../domains/session.md#client-context) is shared the same way.
  The engine owns the contexts and passes in a handle to them.
  At the Identify stage, the pipeline takes the context of its `client.id` and `host` from that handle.
  It reads learned fields from the context, and writes what it learns.

The same interface serves three drivers:

| Driver | Feeds the pipeline from |
|---|---|
| `engine` | Real sockets, real backends and the engine's clock. |
| Unit and property tests | Hand-written inputs and a virtual clock. |
| Replay | A recording: the recorded requests and truths go in, and the verdicts that come out are compared with the recorded ones. |

Replay makes every recording a regression test for the pipeline.

The other sans-IO crates follow the same pattern.
`scenario` consumes call records and time, and returns arm and disarm commands.
`overlay` and the `sim_*` crates consume a request and return a response.
`session` consumes headers and returns identity changes.

## A session is a transport and a pipeline

The engine runs every [session](../domains/session.md) as one task.
The task owns the session's two parts.

| Part | Comes from | Does |
|---|---|---|
| **Transport** | `listener`, part of the IO edge | Reads the client's bytes, and writes the bytes the client receives. |
| **Pipeline** | `pipeline`, sans-IO | Holds the session's state from the `session` crate, and decides what happens to each call. |

The task waits for the next thing to happen,
reports it to the pipeline as an `Input`,
and performs every `Output` the pipeline returns.

| What happens | Input | Comes from |
|---|---|---|
| The client sends bytes. | `ClientBytes` | The transport |
| The backend answers a call. | `BackendAnswered` | The [backend](#the-backend-seam) |
| A timer runs out. | `TimerFired` | The engine's clock |
| A rule changes. | `SnapshotPublished` | The rule store |
| The client closes the connection. | `ClientClosed` | The transport |

The pipeline keeps the snapshot it received last.
A call that is in flight finishes on the snapshot it started with,
and the next call uses the new one.

The session ends when its transport closes, whoever closed it.
The task ends, and the pipeline and its state go with it.
A client that reconnects gets a new task, a new transport and a new pipeline.
See [disconnects](../domains/session.md#disconnects).

## The broker is a state machine too

`broker` is a state machine for one simulated cluster.
It has the shape of the pipeline:
requests, events and fired timers go in, and answers and timers come out.

A broker cannot always answer at once.
A Fetch waits for records until its maximum wait ends,
and a `JoinGroup` waits for the other members of the group.
The broker therefore answers each request under its call ID,
in the same step or in a later one.

A proxy in front of a broker needs no socket between the two.
The engine takes the pipeline's `Forward` and hands the request to the broker in memory.
The broker's answer returns to the pipeline as `BackendAnswered`.
Both state machines run on one virtual clock, so a run is reproducible down to the order of events.

## The IO edge is async

The crates that do IO are async.
On a native target they run on Tokio.

| Crate | The IO it does |
|---|---|
| `listener` | Client sockets. |
| `backend_upstream` | Sockets to the real cluster. |
| `schema_registry` | The HTTP interface of the built-in registry, and requests to a real one. |
| `workspace_fs` | Files, and watching them. |
| `control_server` | HTTP and WebSocket. |
| `control_client` | HTTP and WebSocket, from a native target or from the browser. |
| `engine` | Tasks, timers and channels. It opens no socket itself. |
| `host` | One task per instance. |

- **The edge waits, and a sans-IO crate decides.**
  A task in the edge waits for a socket, a timer or a channel.
  Then it calls a state machine with what happened.
  That call never waits, so it runs inside the task.
- **Time comes from the clock.**
  The engine turns a `SetTimer` into a wait on its [clock](README.md#clock).
  With a virtual clock, the wait ends when the clock reaches the instant,
  however fast the clock runs.
- **The edge exists once.**
  A sans-IO crate has no blocking or async side, so it needs no second variant.
  The edge has no blocking variant either.
  A caller that is not async, such as a blocking test, runs the edge on a runtime of its own.

### The backend seam

The engine reaches its backend through one async trait in the `backend` crate.
The engine hands over a request under a call ID,
and the answer arrives later under the same ID.

The trait is a signature without logic.
`backend` needs no runtime, so it stays outside the IO edge.
It is the only crate there with an async signature.

| Backend | Behind the trait |
|---|---|
| Upstream | `backend_upstream`: connections to the real cluster, with the sans-IO `overlay` around them. |
| Simulated | A task in `host` that owns the `broker` state machine. The task feeds the broker the requests of every session, and waits for the timers the broker sets. |

The same task drives a broker that runs alone.
The listener then stands in front of the broker, without an engine.

## Lies as crates

A lie kind gets its own crate when it reads or rewrites payloads.
Those kinds are large, they know specific Kafka APIs,
and they change at their own pace.

| Lie kind | Crate | Reason |
|---|---|---|
| Refusal | `lie_refusal` | Knows the error shape of every API, and how to remove and merge partitions. |
| Topology | `lie_topology` | Knows Metadata, `FindCoordinator` and `DescribeCluster`. |
| Data | `lie_data` | Knows record batches, compression and checksums. |
| Progress | `lie_progress` | Knows offsets in Fetch, `ListOffsets` and `OffsetFetch`. |
| Configuration | `lie_config` | Knows `DescribeConfigs`. |
| Timing, silence, hang-up | `lie` | Each one is a single instruction to the pipeline: hold, drop or close. They need no payload code. |

Every kind implements the `Lie` trait from the `lie` crate.
The pipeline works with a set of lies that the engine registers,
so `pipeline` depends on `lie` and on none of the `lie_*` crates.
A build can leave a kind out, and a new kind does not touch the pipeline.

Events do not get a crate per event.
An event is a value in the `event` crate.
`world` applies it, and `overlay` emulates it.

## What compiles to WebAssembly

The UI runs in the browser.
It reuses the engine's own logic instead of a second implementation in the front end.

| Crate | The UI uses it to |
|---|---|
| `field`, `selector` | Highlight the recorded calls a draft selector matches, while someone types. |
| `call` | Render verdicts and explanations. |
| `lie`, `event`, `rule`, `trigger`, `scenario` | Edit definitions with the engine's own types and validation. |
| `quick_inject` | Parse and preview a ⌘K sentence without a round trip. |
| `surface`, `hardware_push2` | Turn pad presses and controller messages into intents. |
| `virtual_log`, `world` | Render the World view, and preview the record a template gives at any offset. |
| `control_api`, `control_client` | Talk to the engine. |

These crates stay free of threads, files and sockets.
`control_client` is the exception.
It is part of the IO edge, and in the browser it reaches the engine through the browser's own HTTP and WebSocket.
The same `surface` and `hardware_push2` crates later serve a hardware bridge in the CLI,
which sends the same intents through `control_client`.

## Features

- Every crate with definition types has a `serde` feature.
- `control_client` has one feature per transport: native and WebAssembly.
- `gmq` has one feature per backend and one per `lie_*` crate,
  so a build can ship the proxy without the fake broker.

## Build order

The crates arrive in the order the [first version](../README.md#first-version) needs them.

| Step | Crates | Result |
|---|---|---|
| 1 | `field`, `clock`, `wire`, `selector`, `lie`, `trigger`, `rule`, `combine` | Rules evaluate against hand-written calls in tests. |
| 2 | `wire_edit`, `lie_refusal`, `session`, `call`, `pipeline` | The pipeline runs in tests, with partial refusals and explanations. |
| 3 | `event`, `environment`, `backend`, `overlay`, `backend_upstream`, `listener`, `engine` | A proxy that lies. |
| 4 | `host`, `control_api`, `control_server`, `control_client`, `traffic`, `index`, `surface`, `quick_inject`, `ui` | The UI with live traffic, rules, pads and ⌘K. |
| 5 | `journal`, `scenario`, `workspace`, `workspace_fs`, `recording`, `test`, `gmq` | Scenarios in files, recordings, and tests in CI. |
| 6 | `schema`, `schema_registry`, `virtual_log`, `world`, `sim_log`, `sim_group`, `broker`, `world_seed` | The broker, alone and behind a proxy. |
| 7 | `lie_topology`, `lie_data`, `lie_progress`, `lie_config`, `hardware_push2`, `sim_txn` | The remaining lies, hardware and transactions. |
