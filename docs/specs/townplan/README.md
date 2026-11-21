# Town plan

The town plan splits GaslightMQ into districts.
Each district owns a set of concepts and a clear responsibility.
Districts talk to each other through narrow, named contracts.

## Map

```mermaid
flowchart TB
  subgraph surfaces["Control surfaces"]
    CS["Rules list · Clients board · ⌘K · Pad grid · Push · Capture"]
  end

  subgraph core["Core"]
    SC["Scenarios<br/>mode · steps · seed · live set"]
    IN["Injection<br/>rules · triggers · counters · dice"]
    LE["Lies and events<br/>stages · targets · presets"]
  end

  subgraph support["Supporting"]
    TG["Targeting<br/>selectors · fields · scope"]
    SE["Session<br/>identity · client context"]
    OB["Observation<br/>recordings · index · explanations"]
    WO["World<br/>simulated cluster state"]
  end

  subgraph generic["Generic"]
    BE["Backends<br/>Upstream · Simulated"]
    WI["Wire<br/>Kafka protocol codec"]
    ST["Workspace<br/>files · recordings"]
  end

  CS -->|commands| SC
  CS -->|commands| IN
  SC -->|arms and fires| IN
  IN -->|acts through| LE
  IN -->|uses| TG
  SE -->|evaluates| IN
  SE -->|uses| TG
  SE -->|forwards to| BE
  BE -->|Simulated drives| WO
  SE -->|call events| OB
  OB -->|uses| TG
  OB -.->|seeds| WO
  SE --- WI
  BE --- WI
  SC --- ST
  TG --- ST
  OB --- ST
```

## Districts

| District | Type | Owns | Responsibility |
|---|---|---|---|
| [Scenarios](../domains/scenarios.md) | Core | Scenario, Mode, Step, Live set, Capture | Tells failure stories over time and resolves conflicts between rules. |
| [Injection](../domains/rules.md) | Core | Rule, Action, Trigger, Counter, Dice, Lifetime, Budget | Decides what happens to a call or to the world. |
| [Lies and events](../domains/lies-and-events.md) | Core | Truth, Lie, Stage, Target, Event, Consequence, Preset | Defines the vocabulary of everything GaslightMQ can make a client believe. |
| [Targeting](../domains/targeting.md) | Supporting | Field, Learned field, Condition, Selector, Scope | Provides one predicate language for filtering, targeting and indexing. |
| [Session](../domains/session.md) | Supporting | Session, Identity, Client context | Runs every connection and holds the state rules need per connection and per client. |
| [Observation](../domains/observation.md) | Supporting | Call, Verdict, Explanation, Recording, Journal, Index | Shows what happened and why. |
| [World](../domains/world.md) | Supporting | Cluster, Broker node, Topic, Partition, Group, Producer, Record source, Template | Holds the state of a simulated cluster. |
| [Control surfaces](../domains/control-surfaces.md) | Supporting | View, Pad layout, Layer, Hardware mapping | Gives people fast ways to change rules and scenarios. |
| [Backends](../architecture/backends.md) | Generic | Backend, Capability, Overlay, Emulation | Answers calls, either from a real cluster or from a broker with its World. |
| Wire | Generic | Frame, Request, Response | Encodes and decodes the Kafka protocol. |
| [Workspace](../architecture/storage.md) | Generic | Workspace, Environment, Secret reference, Scratch file, Recording file | Persists definitions and recordings, and configures where the engine runs. |

The core districts are where GaslightMQ differs from every other Kafka tool.
The supporting districts exist to serve the core.
The generic districts solve problems that are not specific to GaslightMQ.

The engine owns the [Clock](../architecture/README.md#clock).
Scenarios, Injection and the World all read it, and none of them owns it.

## Relationships

### Targeting is a shared kernel

Targeting defines the field vocabulary: `api`, `topic`, `client.id`, `group`, and so on.
Observation, Injection, Session and Control surfaces all speak this vocabulary.
A change to a field affects all of them,
so the vocabulary changes deliberately and rarely.

### Scenarios drive Injection

Scenarios arm, disarm and fire rules.
Injection does not know about modes, steps or timelines.
It only knows which rules are armed, in which priority order,
and how to evaluate one call against them.
Scenarios turns the live set and the active scenario's mode into that priority order.

### Session evaluates Injection

Every session receives a snapshot of the armed rules.
The session evaluates that snapshot for each call.
Injection owns each rule's counter, dice and budget.
Sessions share them, and only keep the counters a rule asks for per session.
Injection publishes a new snapshot whenever a rule changes.

### Backends sit behind an anti-corruption layer

Injection speaks in lies and events.
A backend translates an event into something it can do.
The Simulated backend applies the event to the World.
The Upstream backend emulates the event inside its overlay on the real cluster.
Neither backend leaks its internals back into Injection:
what a backend answers is the truth, and rules cannot tell an emulated event from a real one.

### Observation is downstream of everything

Sessions emit a call event for every call,
including the explanation of the verdict.
Observation stores, indexes and shows those events.
Nothing upstream depends on Observation.

### Observation seeds the World

A recording from a real cluster knows its topics, partitions, groups and transactional IDs.
The World can be seeded from that knowledge.
This is how a team captures reality and then breaks it deterministically and offline.

### Control surfaces hold no domain state

Every surface sends commands to Scenarios or Injection.
A pad layout is the only state a surface owns,
and a layout only maps positions to names.

## Boundaries that matter

- **A pad is not a rule.**
  A pad is a position that points at zero or one rule.
- **Scope is not a separate concept.**
  Scope is a selector over connection fields that the UI shows as its own axis.
  It gives a rule no priority.
- **A layer is not the live set.**
  A layer is a page of the pad grid for one scope.
  The live set is the rules someone armed by hand.
- **Broker state is not a separate concept.**
  In the World, a broker going down is an event.
  On the proxy, the Upstream backend emulates it by refusing and dropping connections.
- **A lie is not always a fault.**
  A lie is anything the client is told that differs from the truth,
  including a different leader and altered data.
- **The overlay is not a lie.**
  The proxy's address translation and its emulated events are part of the truth.
  Rules and explanations treat them like the real cluster's answers.
