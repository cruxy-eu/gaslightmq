# Crate layout

The workspace splits GaslightMQ into many small crates.
Each crate holds one district of the [town plan](../townplan/README.md),
or one component of the [engine](README.md#components).
This document describes the target layout.
Moving the current crates to it is a separate plan.

Every crate name starts with `gaslightmq_` and uses underscores, never hyphens.
The name in `Cargo.toml`, the directory and the path in code are then one string,
so one search finds all three.
The tables leave the prefix out: `selector` is the crate `gaslightmq_selector`.
Two crates stand apart: the facade is plain `gaslightmq`, and the binary is `gmq`.
The workspace's own checks are tooling, and not product code: they live in `xtask/`, outside these tables.

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
  or because its types travel to the UI, so it must compile to WebAssembly without the Kafka codec.
  Code that always changes together stays in one crate.
- **Dependencies follow the town plan.**
  Vocabulary crates depend only on other vocabulary crates.
  Logic crates depend on vocabulary.
  The engine depends on logic.
  Nothing depends on the engine except the binaries and the test library.
- **Definitions live with their domain.**
  The crate that owns a concept owns its definition type and its `serde` mapping.
  The `workspace` crate only assembles and cross-checks them.
- **The UI shares types, not logic.**
  The UI reaches the engine only through the control plane.
  It shares the engine's definition types.
  It runs none of the engine's behaviour and none of the Kafka codec.
  See [what compiles to WebAssembly](#what-compiles-to-webassembly).
- **A name says what a thing means.**
  A type, a variant, a method or a module carries a word of the [glossary](../domains/README.md#glossary),
  or it says what it holds or what went wrong.
  It never says which crate, module or type a thing came from, and it never says how the thing is built.
  A partition number that is not a number is an `InvalidPartition`, and not a `Kernel` error.
- **A value object owns its error.**
  A type that checks its value has an error type of its own, named after it:
  `TopicName` has `TopicNameError`, and `Offset` has `OffsetError`.
  Its constructor, its `TryFrom`, its `FromStr` and its `serde` form all return that type.
  The error holds only what can go wrong with that value, and each variant says what is wrong:
  a topic name is `Empty` or `TooLong`, and an offset is `Negative`.
  A type made of parts says which part failed, and it keeps the error of that part:
  `TopicPartitionError::InvalidPartition` holds a `PartitionNumberError`.
  A whole that checks its members does the same: `SelectorSet` has `SelectorSetError`.
  No crate has one error type for everything in it.
  Types may share their checks as private helpers.
- **An error says everything in one message.**
  A file reader, the control plane and the UI show one line, and not a chain of causes.
  A variant that holds another error names its own value first, and then writes that error, once.
  It names the text it read too, when the other error read only a part of it:
  ``lifetime `0 firings`: a count is at least one``.
  Its `source()` returns `None`, because a reporter that walks the chain would print the cause twice.
  The variant keeps the other error in a field, named `error`, for code that wants to look at it.
  The path of a value in a file comes from the [reader](storage.md#reading), and not from the error of the value:
  `trigger.every: a count is at least one`.
- **Nothing tunable is hardcoded.**
  A timeout, a wait, a size, a count or an interval that someone may want to change is a property of a configuration.
  The property has a name and a default, and the code that waits or counts reads the property.
  A client waits for a response for its `request_timeout`, which is 30 seconds until someone sets it,
  and not for a `Duration::from_secs(30)` in the code that sends the request.
  A value with rules is a value object: a number of attempts that is at least one is not a plain `u32`.
  A number that the Kafka protocol fixes is not tunable, and stays a constant.
- **A Kafka client has Kafka's own properties.**
  A property that Kafka's own clients have keeps its name, its meaning, its unit and its default in our client.
  The configuration of the Java client is the reference.
  The doc comment of the field names the property, so someone who knows Kafka finds it at once:
  `request_timeout` is `request.timeout.ms`.
  A property that Kafka does not have is added only when the client needs it, and its doc comment says that it is ours.
  No property exists for behaviour that the client does not have.

The workspace rules apply to every crate:
Rust edition 2024, native async traits and workspace dependency versions.

## Checks

`cargo xtask` checks the rules of this document that a machine can decide and the compiler cannot see.
`cargo xtask sans-io` runs one check alone.
`just checks` and CI run them all.

| Check | Fails when |
|---|---|
| `dependencies` | A crate depends on a workspace crate that its row does not list, or its row lists one that it does not depend on. |
| `crate-names` | A crate name does not start with `gaslightmq_` or holds a hyphen, or the crate's directory does not carry the name. |
| `wasm-boundary` | A crate that [compiles to WebAssembly](#what-compiles-to-webassembly) reaches `kafka-protocol`. |
| `workspace-rules` | A crate builds with another edition than 2024, depends on `async-trait`, or names a dependency without `workspace = true`. |
| `sans-io` | [Sans-IO code](#sans-io-code) is async, or it names a runtime, a socket, the file system, the system clock, the process or global randomness. |
| `wrapped-errors` | An error type holds another error in a field that is not named `error`, or its `source()` returns something other than `None`. |

A check reads the names in the code, and not what the code does.
The documentation of each check, in `xtask/src/`, says what it does not catch.
The other principles need a reviewer.

### Sans-IO code

A crate whose row does not say "This crate does IO" is sans-IO as a whole.
The `sans-io` check reads its dependencies and every file of its source.

A crate that does IO declares the modules it keeps sans-IO in its `Cargo.toml`:

```toml
[package.metadata.gaslightmq]
sans_io_modules = ["logic"]
```

The check then reads `src/logic.rs`, or `src/logic/mod.rs` and every file below `src/logic/`, as it reads a sans-IO crate.
A sans-IO module names nothing of its own crate outside the sans-IO modules.
A module that holds the types it works on, such as a configuration or an error, is therefore declared too.

One sans-IO crate has an async signature: [`backend`](#the-backend-seam).
The check lets its trait declare async functions, and no other async code.

## Layers

```mermaid
flowchart BT
  V["Vocabulary<br/>kernel · field · clock · wire · lie · incident · call"]
  T["Targeting and injection<br/>selector · trigger · rule · combine · wire_edit · lie_*"]
  P["Session and pipeline<br/>session · pipeline"]
  S["Scenarios<br/>scenario"]
  O["Observation<br/>journal · traffic · index · recording"]
  W["World and backends<br/>backend · virtual_log · world · sim_* · broker · overlay"]
  F["Workspace<br/>environment · workspace"]
  C["Control<br/>surface · quick_inject · control_api"]
  IO["IO edge<br/>listener · engine · host · backend_upstream · schema_registry<br/>workspace_fs · control_server · control_client"]
  A["Applications<br/>gmq · ui · test · testcontainers · kafka_client_*"]

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
| `kernel` | The value objects of Kafka: topic names and IDs, partition numbers, offsets, positions such as `orders.eu/3`, broker IDs, epochs, the IDs of clients, groups and producers, and API keys and error codes with the names Kafka gives them. Each type only holds valid values. | — |
| `field` | The [field vocabulary](../domains/targeting.md#fields): field names, their typed values, and the `FieldSource` trait that a live call and a recorded call both implement. Also the parts of a call, and the [target](../domains/lies-and-incidents.md#target) that a match yields. Every district of the town plan [shares this vocabulary](../townplan/README.md#targeting-is-a-shared-kernel). | `kernel`, `clock` |
| `clock` | The [clock](README.md#clock): a `Clock` trait, the virtual clock, and instants that do not come from the system. | — |
| `wire` | The Kafka codec: frames, headers, lazily decoded bodies and record batches. | — |
| `lie` | The [lie](../domains/lies-and-incidents.md#lies) vocabulary: kind, stage, combination class, aspect and priority, the definition of every lie kind as data, and a lie with the [target](../domains/lies-and-incidents.md#target) it applies to. It holds no behaviour. See [lies as crates](#lies-as-crates). | `field`, `kernel`, `clock` |
| `incident` | The [incident](../domains/lies-and-incidents.md#incidents) vocabulary, the expansion into consequences, and the support a backend declares for each kind of incident. | `kernel` |
| `call` | The [call](../domains/observation.md#call) record: identity, request summary, truth, delivered response, verdict, explanation and timing. | `field`, `lie`, `incident`, `kernel`, `clock` |

### Targeting and injection

| Crate | Holds | Depends on |
|---|---|---|
| `selector` | Conditions, selectors, includes and scope. The set of selectors, which resolves includes into effective selectors. Matching against a `FieldSource`, with the [target](../domains/lies-and-incidents.md#target) a match yields and the evaluation pass that decides it. An index of selectors by API. | `field`, `kernel` |
| `trigger` | [Triggers](../domains/rules.md#trigger), counters, dice, budgets and lifetimes. | `clock` |
| `rule` | The rule definition, the [preset](../domains/lies-and-incidents.md#presets), the rule's state, and the immutable snapshot of armed rules in priority order. The snapshot [evaluates a call](../domains/rules.md#evaluating-one-call): for each pass and each matching rule, the target, what the trigger rolled, and the lie or the incidents it fired. | `selector`, `lie`, `incident`, `trigger`, `field`, `kernel`, `clock` |
| `combine` | [How lies combine](../domains/lies-and-incidents.md#how-lies-combine): which lies conflict, which win by priority, which are shadowed or pre-empted, across [both evaluation passes](request-pipeline.md#two-evaluation-passes). Also what the lies that apply do to the call at each stage: the refusals and rewrites per target, the summed delay and the maximum throttle. | `lie`, `field`, `clock` |
| `wire_edit` | Surgery on requests and responses: remove partitions, merge responses, synthesize an error response for any API, map topic IDs to names. It also reads what a response says about each [part](../domains/lies-and-incidents.md#target) of its request. | `wire`, `field`, `kernel` |
| `lie_api` | The contract between the pipeline and a lie kind: the `Lie` trait, and the effect that one lie asks the pipeline for. Also the behaviour of timing, silence and hang-up. See [lies as crates](#lies-as-crates). | `lie`, `wire` |
| `lie_refusal` | Refusals for every API, before forwarding and after the response. | `lie`, `lie_api`, `wire_edit` |
| `lie_topology` | Rewrites of Metadata, `FindCoordinator` and `DescribeCluster`. | `lie`, `lie_api`, `wire_edit` |
| `lie_data` | Altered, dropped, duplicated, reordered and corrupted records. | `lie`, `lie_api`, `wire_edit`, `schema` |
| `lie_progress` | Rewrites of watermarks, log start offsets and committed offsets. | `lie`, `lie_api`, `wire_edit` |
| `lie_config` | Rewrites of `DescribeConfigs` responses. | `lie`, `lie_api`, `wire_edit` |

### Session and pipeline

| Crate | Holds | Depends on |
|---|---|---|
| `session` | The state of one [session](../domains/session.md), which its pipeline owns: lifecycle, identity, session key, per-session counters, in-flight calls, response ordering and the stalled state. Also the client context that sessions share. | `field`, `kernel`, `call`, `wire`, `trigger`, `clock` |
| `pipeline` | The [request pipeline](request-pipeline.md) as a state machine: all stages, both evaluation passes and partial refusals. | `wire`, `wire_edit`, `selector`, `rule`, `trigger`, `lie`, `lie_api`, `combine`, `session`, `call`, `incident`, `clock` |

### Scenarios

| Crate | Holds | Depends on |
|---|---|---|
| `scenario` | The [scenario](../domains/scenarios.md) definition, modes as priority orders, the step machine of a Sequence, the live set and capture. It consumes call records, journal entries and time, and it emits arm and disarm commands. | `rule`, `trigger`, `call`, `journal`, `incident`, `clock` |

### Observation

| Crate | Holds | Depends on |
|---|---|---|
| `traffic` | The live window of recent calls, subscriptions by selector, and statistics per rule and per API. | `call`, `selector` |
| `index` | The [index](../domains/observation.md#index): facets, counts under the active filters, suggested patterns. | `call`, `selector` |
| `journal` | The [journal](../domains/observation.md#journal): its entries, and the window of recent entries the engine keeps in memory. | `incident`, `clock` |
| `recording` | The [recording](storage.md#recordings) format: append-only chunks with summaries, calls and journal entries, metadata-only mode, blanked credentials, a reader and a writer over `std::io`. | `call`, `journal`, `wire` |

### World and backends

| Crate | Holds | Depends on |
|---|---|---|
| `backend` | The [backend contract](backends.md#contract): the request and answer messages, and the one async trait through which the engine reaches a backend. It holds no logic and needs no runtime. See [the backend seam](#the-backend-seam). | `wire`, `incident`, `clock` |
| `schema` | Record [formats and schemas](../domains/world.md#schemas): encode and decode JSON, Avro and Protobuf, one feature per format. | — |
| `virtual_log` | [Virtual records](../domains/world.md#virtual-records): the record at a partition and offset as a pure function of the definition, templates, stable batches, and the mapping between offsets and time. | `wire`, `clock`, `schema` |
| `schema_registry` | The built-in [schema registry](backends.md#schema-registry) and a client for a real one. This crate does IO. | `schema` |
| `world` | The [World](../domains/world.md): its state, world definitions, how each incident changes it, and where each topic's records come from. | `wire`, `incident`, `clock`, `virtual_log` |
| `world_seed` | Seeding a world definition from a recording. | `world`, `index`, `recording` |
| `sim_log` | Produce, Fetch, offsets and idempotent producers against the World. | `world`, `wire` |
| `sim_group` | Group coordination against the World, for the classic and the consumer protocol. | `world`, `wire`, `clock` |
| `overlay` | The Upstream [overlay](backends.md#overlay): address translation, emulated incidents and sessionless Fetch. It rewrites requests and responses, and it opens no connection. | `wire`, `wire_edit`, `incident` |
| `broker` | The broker instance as a [state machine](#the-broker-is-a-state-machine-too): it routes each request to a `sim_*` crate, applies incidents to the World and owns the virtual clock. It speaks the messages of the backend contract, so a proxy reaches it without a socket. | `backend`, `world`, `sim_log`, `sim_group`, `clock` |
| `backend_upstream` | The Upstream backend: connections to the real cluster, with the overlay around them. This crate does IO. | `backend`, `overlay` |

Transactions arrive later as `sim_txn`, next to `sim_log` and `sim_group`.

### Workspace

| Crate | Holds | Depends on |
|---|---|---|
| `environment` | [Environments](storage.md#environments), [secret references](storage.md#secrets), and the order in which flags, variables and files set a value. It never reads a secret. | — |
| `workspace` | The whole set of definitions as one model: cross-reference validation, renames that cascade, and the YAML layout of the [workspace](storage.md#workspace). A [reading error](storage.md#reading) names the path of the key that failed. It works on file contents, not on files. | `selector`, `lie`, `incident`, `rule`, `scenario`, `surface`, `world`, `environment` |
| `workspace_fs` | Loading, watching and writing back, the scratch file, and reading the files that secret references name. This crate does IO. | `workspace` |

### Control

| Crate | Holds | Depends on |
|---|---|---|
| `surface` | [Pad layouts](../domains/control-surfaces.md#pad-layout), layers, pads as positions, the encoder mapping, and the intents a surface sends. | `rule` |
| `hardware_push2` | The Push 2 mapping: controller messages to surface intents, and rule state back to pad colours. | `surface` |
| `quick_inject` | The ⌘K sentence parser: text to a rule definition, with completions. The host runs it behind the [control plane](control-plane.md#drafts). | `selector`, `lie`, `rule` |
| `control_api` | The [control plane](control-plane.md) as types: resources, commands and stream messages. It re-exports the definition types its messages carry, and it holds its own view of the World's state. | `call`, `journal`, `rule`, `scenario`, `selector`, `surface`, `incident`, `environment` |
| `control_client` | A client for the control plane, for native targets and for WebAssembly. This crate does IO. | `control_api` |
| `control_server` | The HTTP and WebSocket server in front of the host. This crate does IO. | `host`, `control_api` |

### Runtime and applications

| Crate | Holds | Depends on |
|---|---|---|
| `engine` | The runtime of one proxy: one task per session that owns the transport and drives the pipeline, the rule store, the scenario runner, the event bus, the journal, the recorder and the indexer. This crate does IO. | `pipeline`, `session`, `rule`, `scenario`, `journal`, `traffic`, `index`, `recording`, `backend`, `listener`, `workspace`, `environment`, `clock` |
| `ui` | The web UI, a Dioxus application. It reaches the engine only through the control plane. This crate does IO. | `control_api`, `control_client`, `surface`, `hardware_push2` |
| `listener` | Kafka on a socket: accept connections, frame bytes, and drive whatever handles a connection. A proxy's pipeline and a broker alone both sit behind it. This crate does IO. | `wire`, `clock` |
| `host` | One process with several [environments](storage.md#several-environments-in-one-process): it starts and stops proxies and brokers, runs the task that drives each broker, and shares the workspace and the control plane between them. This crate does IO. | `engine`, `broker`, `listener`, `backend_upstream`, `workspace_fs`, `environment`, `quick_inject` |
| `test` | The test library: it starts a proxy, a broker, or a proxy in front of a broker inside the test's own process, on free ports, and hands the test the addresses its clients connect to. The test then activates a scenario and asserts on the recording. This crate does IO. See [instances in a test](#instances-in-a-test). | `host`, `control_client` |
| `testcontainers` | A container module: it starts the GaslightMQ image next to a test, with the workspace the test hands over, waits until the instance is ready, and hands the test the addresses its clients connect to. This crate does IO. See [instances in a test](#instances-in-a-test). | `control_client` |
| `kafka_client_api` | What a Kafka client offers to a test, whichever client it is: the producer, consumer and admin traits, one trait per protocol API, and a raw client that sends one request at a fixed version. This crate does IO. | `wire` |
| `kafka_client_native` | Our own Kafka client behind those traits: it follows partition leaders, negotiates API versions, and reports every failure as an error. This crate does IO. | `kafka_client_api`, `wire` |
| `gaslightmq` | The facade crate: `Proxy`, `Broker` and `Host`, and re-exports of what library users need. This crate does IO. | The public crates |
| `gmq` | The binary: `gmq proxy`, `gmq broker`, `gmq start` and `gmq run`. This crate does IO. | `host`, `control_server` |

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
    Fire(Incident),
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
requests, incidents and fired timers go in, and answers and timers come out.

A broker cannot always answer at once.
A Fetch waits for records until its maximum wait ends,
and a `JoinGroup` waits for the other members of the group.
The broker therefore answers each request under its call ID,
in the same step or in a later one.

A proxy in front of a broker needs no socket between the two.
The engine takes the pipeline's `Forward` and hands the request to the broker in memory.
The broker's answer returns to the pipeline as `BackendAnswered`.
Both state machines run on one virtual clock, so a run is reproducible down to the order in which things happen.

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

A lie has a definition and a behaviour, and they live in different crates.

| Part | Crate | Holds | Reaches `wire` |
|---|---|---|---|
| Definition | `lie` | Every lie kind as data: its parameters, its stage and its `serde` mapping. | No |
| Contract | `lie_api` | The `Lie` trait, and the effect that one lie asks the pipeline for. | Yes |
| Behaviour | One crate per kind, listed below | What a kind does to a request or a response. | Yes |

Rules, call records, the workspace and the control plane carry lie definitions.
A definition needs no Kafka codec, so none of them reaches `wire` through `lie`.
The `Lie` trait reads requests and responses, so `lie_api` depends on `wire`.

The split rests on one rule:
every lie definition is data in `lie`, and the `lie_*` crates hold behaviour only.
A `lie_*` crate with a definition type of its own would put `wire` back under every crate that carries a rule,
and so under `control_api` and the UI.

A lie kind gets its own crate when it reads or rewrites payloads.
Those kinds are large, they know specific Kafka APIs,
and they change at their own pace.

| Lie kind | Behaviour in | Reason |
|---|---|---|
| Refusal | `lie_refusal` | Knows the error shape of every API, and how to remove and merge partitions. |
| Topology | `lie_topology` | Knows Metadata, `FindCoordinator` and `DescribeCluster`. |
| Data | `lie_data` | Knows record batches, compression and checksums. |
| Progress | `lie_progress` | Knows offsets in Fetch, `ListOffsets` and `OffsetFetch`. |
| Configuration | `lie_config` | Knows `DescribeConfigs`. |
| Timing, silence, hang-up | `lie_api` | Each one is a single instruction to the pipeline: hold, drop or close. They need no payload code. |

Every kind implements the `Lie` trait from `lie_api`.
The pipeline works with a set of lies that the engine registers,
so `pipeline` depends on `lie` and `lie_api`, and on none of the crates of a lie kind.
A new kind adds its definition to `lie` and its behaviour to a crate of its own, and it does not touch the pipeline.
A build can leave a kind out.
Such a build still reads that kind's definitions, because they are data in `lie`.

Incidents do not get a crate per incident.
An incident is a value in the `incident` crate.
`world` applies it, and `overlay` emulates it.

## What compiles to WebAssembly

The UI runs in the browser.
It reaches the engine only through the [control plane](control-plane.md).
It shares the engine's definition types, and it runs none of the engine's behaviour and none of the Kafka codec.

The definition types are a public contract already, because they are the workspace's YAML files.
The control plane carries the same types, so a rule has one form in a file, in a request and in the UI.
A definition type only holds valid values.
The UI therefore checks a value in a form with the engine's own types, and without a round trip.

| Crate | The UI uses it to |
|---|---|
| `kernel` | Check and show topic names, partitions and offsets with the engine's own types. |
| `field`, `selector` | Build and edit selector definitions. |
| `lie`, `incident`, `trigger`, `rule`, `scenario` | Edit definitions with the engine's own types. |
| `call`, `journal` | Render verdicts, explanations and the history of a rule. |
| `environment`, `clock` | Show environments, and the instants that calls and journal entries carry. |
| `surface`, `hardware_push2` | Turn pad presses and controller messages into intents. |
| `control_api`, `control_client` | Talk to the engine. |

The UI names the definition types through `control_api`, which re-exports them.

Whatever needs the engine's logic is an operation of the control plane.

| The UI wants to | It asks the control plane to |
|---|---|
| Highlight the recorded calls a draft selector matches, while someone types. | Query the recording with the draft. |
| Preview a ⌘K sentence. | Parse the sentence. |
| Show the records of a topic in the World view. | Preview the records at an offset. |
| Check a definition against the rest of the workspace. | Create or update it. The engine validates it with the `workspace` crate. |

See [drafts](control-plane.md#drafts).
The engine sees the whole recording and the whole workspace, and the browser only holds a window of both.
The CLI and tests get the same operations.

The World is the one district whose definition types the UI does not share.
Those types live in `world` and `virtual_log`, which reach `wire` and `schema`.
`control_api` holds its own view of the World's state instead.

`surface` and `hardware_push2` do run behaviour in the browser:
they turn pad presses and controller messages into intents.
That behaviour belongs to a control surface, not to the engine.
Web MIDI only exists in the browser, so the hardware mapping runs there.
The same two crates later serve a hardware bridge in the CLI,
which sends the same intents through `control_client`.

The crates in the table stay free of threads, files and sockets.
`control_client` is the exception.
It is part of the IO edge, and in the browser it reaches the engine through the browser's own HTTP and WebSocket.

The dependency closures of `control_api` and `ui` never reach `kafka-protocol`.
CI enforces the boundary for the crates in the table that exist.
It builds each one for WebAssembly, with all its features,
and it checks that the dependency closure of none of them reaches `kafka-protocol`.
Two missing edges keep it that way.
`lie` does not depend on `wire`, and `control_api` does not depend on `world`.

## Instances in a test

Two crates start GaslightMQ for a test.
`test` starts it inside the test's own process, and `testcontainers` starts it in a container.

| A test needs | `test` | `testcontainers` |
|---|---|---|
| An instance | A proxy, a broker, or a proxy in front of a broker, inside the test's own process. | A container from the GaslightMQ image, at the version the test names. It starts the [environments](storage.md#environments) the test names. |
| Definitions | The workspace the test hands over. | The workspace the test hands over. The crate puts it into the container before the process starts. |
| An address | Every listener takes a free port, so tests run side by side. The crate hands out the addresses the test's clients connect to. | The crate hands out the addresses under which the test reaches the Kafka endpoints and the control plane. |
| A moment to begin | The listeners accept connections from the moment the test holds the instance. | The crate waits until the process has started its environments and their listeners accept connections. |

A cluster of several brokers needs no instances of its own.
One broker simulates a whole cluster, and its [World](../domains/world.md) says how many broker nodes the cluster has.
A proxy in front of that broker gives the test such a cluster behind a proxy.

From there, a test works the same way with both crates:
it activates a scenario, runs its clients and [asserts on the recording](control-plane.md#tests).

## Features

- Every crate with definition types has a `serde` feature.
- `control_client` has one feature per transport: native and WebAssembly.
- `gmq` has one feature per backend and one per crate of a lie kind,
  so a build can ship the proxy without the fake broker.

## Build order

The crates arrive in the order the [first version](../README.md#first-version) needs them.

| Step | Crates | Result |
|---|---|---|
| 1 | `kernel`, `field`, `clock`, `wire`, `selector`, `lie`, `incident`, `trigger`, `rule`, `combine` | Rules evaluate against hand-written calls in tests. |
| 2 | `wire_edit`, `lie_api`, `lie_refusal`, `session`, `call`, `pipeline` | The pipeline runs in tests, with partial refusals and explanations. |
| 3 | `environment`, `backend`, `overlay`, `backend_upstream`, `listener`, `engine` | A proxy that lies. |
| 4 | `host`, `control_api`, `control_server`, `control_client`, `traffic`, `index`, `surface`, `quick_inject`, `ui` | The UI with live traffic, rules, pads and ⌘K. |
| 5 | `journal`, `scenario`, `workspace`, `workspace_fs`, `recording`, `test`, `testcontainers`, `gmq` | Scenarios in files, recordings, and tests in CI. |
| 6 | `schema`, `schema_registry`, `virtual_log`, `world`, `sim_log`, `sim_group`, `broker`, `world_seed` | The broker, alone and behind a proxy. |
| 7 | `lie_topology`, `lie_data`, `lie_progress`, `lie_config`, `hardware_push2`, `sim_txn` | The remaining lies, hardware and transactions. |

`test` and `testcontainers` arrive in step 5.
`testcontainers` needs the binary for its image, and both need the workspace files.
Until step 6, the instance they start is a proxy in front of a real cluster.
From step 6 on, they start a broker too.
