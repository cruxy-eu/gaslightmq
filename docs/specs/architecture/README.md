# Target architecture

GaslightMQ is built from two kinds of instances.

- A **proxy** stands in front of a cluster and decides what clients get to see.
  It hosts sessions, evaluates rules, runs scenarios and lies.
- A **broker** is a simulated Kafka cluster.
  It holds a [World](../domains/world.md) and answers calls truthfully from it.
  A broker never lies.

A proxy's backend is a real cluster, or a broker in the same process.
One process hosts any number of proxies and brokers,
and a single web UI controls all of them.

These documents call the runtime of one proxy the **engine**.

| Document | What it covers |
|---|---|
| [Request pipeline](request-pipeline.md) | What happens to one call, stage by stage. |
| [Backends](backends.md) | Upstream and Simulated, their capabilities, the overlay and incident emulation. |
| [Storage and configuration](storage.md) | The workspace, environments, secrets, live state and recordings. |
| [Identity](identity.md) | Names, titles and IDs: how GaslightMQ identifies every definition and every runtime thing. |
| [Control plane](control-plane.md) | The API the UI, the CLI and hardware use. |
| [Crate layout](crates.md) | The crates of the workspace, what each one holds, which ones are sans-IO, and how the async IO edge drives them. |

The [interactive diagram](diagram.html) shows one session, the pipeline stages and every crate with its dependencies.
Open the file in a browser.

## Components

```mermaid
flowchart LR
  client["Kafka client"] -->|TCP| LI

  subgraph engine["Engine"]
    LI["Listeners<br/>one per endpoint"] --> SS["Sessions<br/>one per connection"]
    SS --> PL["Pipeline<br/>decode · match · decide · lie"]
    PL --> BE{{"Backend"}}
    RS["Rule store<br/>snapshots · counters · budgets"] -->|publishes| SS
    SR["Scenario runner<br/>mode · steps · live set"] -->|arms and fires| RS
    CK["Clock"] --- SR
    PL -->|call events| BUS["Event bus"]
    BUS --> REC["Recorder"]
    BUS --> IDX["Indexer"]
    BUS --> SR
    WS["Workspace store"] <--> RS
    WS <--> SR
    CP["Control plane<br/>HTTP + WebSocket"] --> RS
    CP --> SR
    BUS --> CP
  end

  BE -->|Upstream| kafka["Real cluster"]
  BE -->|Simulated| world["Broker<br/>World"]
  CP <--> ui["Web UI"]
  ui <-->|Web MIDI| push["Push 2"]
  WS <--> files[("Workspace files")]
  REC --> recs[("Recordings")]
```

| Component | Responsibility |
|---|---|
| **Host** | Start and stop environments in one process. Share the workspace store and the control plane between them. |
| **Listeners** | Accept connections on the bootstrap endpoint and one endpoint per broker. Refuse connections when a broker is down. |
| **Sessions** | Run one connection each. A session is a transport and a pipeline: the transport reads and writes the client's bytes, and the pipeline decides. Share a client context with the other connections of the same client. |
| **Pipeline** | Hold the state of one session: identity, the rule snapshot, per-session counters and in-flight calls. Take each call through the stages in [request pipeline](request-pipeline.md). |
| **Backend** | Answer a call with the truth, from a real cluster or from the World. Expand incidents into their consequences, and apply or emulate them. |
| **Rule store** | Hold every rule and its state, workspace rules and live rules alike. Publish an immutable snapshot of armed rules in priority order on every change. Hold each armed rule's shared counter and budget as atomics. |
| **Scenario runner** | Run the active scenario and the live set: turn the mode into a priority order, advance steps, and hand incidents to the backend. |
| **Clock** | Tell the time for triggers, lifetimes, steps, arriving records and timeouts. |
| **Event bus** | Carry call events, rule changes and world changes to everyone who listens. Keep every event for the recorder, and the newest ones for a listener that only watches. |
| **Recorder** | Write call events and journal entries to a recording while recording is on. It misses no event, however slow it writes. |
| **Journal** | Keep what changed and who changed it: arms, disarms, parameter changes, steps and incidents. |
| **Indexer** | Maintain the facets of the current recording. |
| **Workspace store** | Load definitions from files, watch them for changes and write edits back. Keep live rules in the scratch file. |
| **Control plane** | Expose the engine to the UI, the CLI and tests. |

## Principles

- **The data plane never waits for the control plane.**
  Sessions read an immutable snapshot of armed rules.
  A rule change publishes a new snapshot,
  and sessions pick it up at the next call.
  The snapshot carries the priority order, so a session resolves conflicts on its own.
  A session does not wait for whoever listens on the event bus either.
  It hands an event over and goes on, so a slow listener never changes when a client gets its answer.
  What the bus does for a listener that falls behind depends on what the listener is for:

  | The listener | When it falls behind | The price |
  |---|---|---|
  | Shows what happens now: the traffic view, the UI. | It misses the oldest events, and learns how many. | Events. The bus keeps a number of them that the configuration sets, and no more memory than that. |
  | Keeps what happened: the recorder. | It misses nothing. The bus keeps every event until the recorder took it. | Memory, without a limit. A recorder that writes slower than the sessions record makes the engine hold what is not written yet, and one that stops writing ends with a process that has no memory left. |

  The recorder pays in memory and not in time, because time is what a lie is made of:
  a session that waited for a slow disk would delay answers that no rule delays.
- **Budgets are exact.**
  Shared counters and budgets are atomics next to the snapshot.
  A session takes from a budget at the moment a rule fires.
  A lifetime of three firings or a step that ends after three firings never yields a fourth,
  although the new snapshot reaches other sessions a moment later.
- **One snapshot per call.**
  A call is evaluated against one snapshot from start to finish,
  even if rules change while it is in flight.
- **Determinism is designed in, and its limits are stated.**
  Dice derive from the seed, the rule and the match number.
  See [dice](../domains/rules.md#dice).
  With the same seed, the same match numbers fire in every run.
  Per-session counters make each connection reproducible on its own.
  A shared counter hands out match numbers in arrival order,
  so across connections, which call gets the 3rd match depends on timing.
  The explanation records the match number of every call.
  On the Simulated backend, the virtual clock removes timing as a source of difference.
- **A restart is a clean slate.**
  The engine keeps its live state in memory and has no database.
  After a restart, nothing is armed.
  See [live state](storage.md#live-state).
- **Logic decides, and the edge waits.**
  The pipeline, the broker and the logic of the scenario runner are state machines.
  They do no IO, and they never wait.
  The engine is async: it owns the sockets, the timers and the tasks,
  and it calls the state machines with what happened.
  See [the IO edge is async](crates.md#the-io-edge-is-async).
- **The engine is headless.**
  The UI is one client of the control plane.
  The CLI and test libraries are others.
  Hardware connects through the UI with Web MIDI,
  so the engine needs no device drivers.

## Clock

The engine has one clock.
Time triggers, duration lifetimes, step endings, arriving records and the World's timeouts all read it.

| Backend | Clock |
|---|---|
| Simulated | Virtual. It is the broker's clock, and the proxy in front of the broker reads it. It can run faster than real time, or step on demand. |
| Upstream | The wall clock. |

## Instances and processes

Proxies and brokers combine into three shapes.

| Shape | Instances | Use |
|---|---|---|
| A proxy in front of a real cluster | One proxy | Lie about real traffic. |
| A proxy in front of a broker | One proxy, one broker | A fake cluster that lies, fully reproducible. |
| A broker alone | One broker | A truthful fake Kafka, for tests that need no lies. |

```mermaid
flowchart LR
  c1["Kafka clients"] --> p1
  c2["Kafka clients"] --> p2
  c3["Kafka clients"] --> b2

  subgraph process["One process"]
    subgraph e1["Environment: staging-proxy"]
      p1["Proxy"]
    end
    subgraph e2["Environment: orders-sim"]
      p2["Proxy"] -->|in memory| b1["Broker"]
    end
    subgraph e3["Environment: plain-kafka"]
      b2["Broker"]
    end
    cp["Control plane"] --- e1
    cp --- e2
    cp --- e3
    ws["Workspace store"] --- cp
  end

  p1 -->|TCP| kafka["Real cluster"]
  cp <--> ui["Web UI"]
```

- **A proxy reaches its broker without a socket.**
  The pipeline and the broker are both sans-IO state machines.
  The engine hands the request that the pipeline forwards to the broker in memory,
  and hands the broker's answer back to the pipeline.
  Both read the same virtual clock.
- **An [environment](storage.md#environments) names one shape.**
  A process starts one environment or several.
  Each environment has its own listeners and its own live state.
  Environments share the workspace and the control plane.
- **Lies happen in one place.**
  A proxy lies, and a broker does not.
  A fake cluster that lies is a proxy in front of a broker.
- **A process can run without a control plane.**
  It is then one proxy or one broker, set up by flags and files alone.

A library user creates the same instances in a test:

```rust
let broker = gmq::Broker::new(world);
let proxy = gmq::Proxy::builder()
    .backend(&broker)
    .workspace(workspace)
    .build();
```

The sketch shows the shape of the interface, not its final names.

## Deployment shapes

| Command | Starts | Use |
|---|---|---|
| `gmq proxy --upstream kafka:9092` | A proxy in front of a real cluster | Explore with the UI. Needs no workspace. |
| `gmq proxy --world orders-sim` | A proxy in front of a broker | A fake cluster that lies. Explore with the UI. |
| `gmq broker --world orders-sim` | A broker alone | A truthful fake Kafka. |
| `gmq start --env <environment> …` | One or more [environments](storage.md#environments) from the workspace | Several proxies and brokers in one process, with one UI. |
| `gmq run <scenario> --env <environment>` | One environment | Run a scenario without the UI, for example in CI. Exit when the last step ends, or when stopped. |
| Library and testcontainers | Instances in a test | Create a proxy or a broker from a test, drive it through the control plane, assert on explanations. |

Every command takes `--no-control` to run without a control plane.

`gmq init` creates a new workspace.
The flags of `gmq proxy` and `gmq broker` describe an environment without a file.

`gmq run` reports whether the scenario loaded and ran to its end.
It does not judge the client under test.
A scenario has no expectations yet,
so tests decide pass or fail by [asserting on the recording](control-plane.md#tests).
