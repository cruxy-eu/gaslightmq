# Backends

A backend answers a call after the session's pipeline has decided what to do with it.
The engine has two backends.
Everything in front of the backend is the same for both.

| Backend | Truth comes from | Typical use |
|---|---|---|
| **Upstream** | A real Kafka cluster, seen through the overlay | Proxy in front of a real cluster. Real Kafka semantics, real data. |
| **Simulated** | The [World](../domains/world.md) | Fake broker. Full control, fully reproducible. |

## Contract

A backend:

- answers a decoded request with the truth,
- declares its **capabilities**: which events it can apply, and which it can only emulate,
- applies or emulates an event when the scenario runner fires one,
- reports the endpoints clients should connect to.

A backend never lies.
Lies happen in the pipeline, in front of and behind the backend.
Whatever a backend answers is the truth:
rules match on it, and explanations compare the delivered response against it.

## Capabilities

| Capability | Upstream | Simulated |
|---|---|---|
| Lies of every kind | ✅ | ✅ |
| Broker down, unresponsive, back | Emulated, without failover | ✅ |
| Rebalance | Emulated as a forced rejoin | ✅ |
| Records expired | Emulated | ✅ |
| Committed offset reset | Emulated | ✅ |
| Producer fenced | Emulated | Later, with transactions |
| Leader moves | ❌ in the first version | ✅ |
| Coordinator moves | ❌ in the first version | ✅ |
| Log truncated | ❌ | ✅ |
| Partition added | ❌ | ✅ |
| Topic deleted | ❌ | ✅ |
| Records appended, generators | ❌ | ✅ |
| Editable world state | ❌, read-only observation | ✅ |
| Virtual clock | ❌, wall clock | ✅ |
| Reproducible runs | Dice only. The network and the cluster are not reproducible. | ✅ |

A scenario declares the events it needs.
A backend rejects a scenario that needs events it cannot apply or emulate,
when the scenario loads, not halfway through a run.
The check includes each event's [consequences](../domains/lies-and-events.md#consequences):
"broker goes down" with `failover: true` needs "leader moves".

## Upstream

The Upstream backend forwards calls to a real cluster.
It runs one listener per real broker.

### Overlay

The overlay is what the Upstream backend lays over the real cluster's answers.
It holds two things:

- **Address translation.**
  The overlay rewrites broker addresses in Metadata and `FindCoordinator` responses,
  so clients connect to the proxy's listeners.
  It is always on.
  Without it, clients would bypass GaslightMQ after bootstrap.
- **Emulated events.**
  An event stays in the overlay for as long as it is in force.

The overlay is part of the backend, not a set of rules.
Its output is the truth.
It takes no part in conflicts between lies,
and rules cannot tell an emulated event from a real one.
A call keeps the real cluster's answer next to the truth when the overlay changed it,
so the UI can show both.

### Emulating events

| Event | Emulation |
|---|---|
| Broker down | Close the broker's listener. Drop its connections. Leave it out of Metadata. Report the partitions it led as leaderless, and the coordinators it hosted as unavailable. |
| Broker unresponsive | Accept connections on the broker's endpoint, never answer. |
| Broker back | Open the listener again. Stop changing Metadata. |
| Rebalance | Answer each member's next heartbeat with `REBALANCE_IN_PROGRESS`, or with `FENCED_MEMBER_EPOCH` for the consumer protocol, so the member rejoins. The real coordinator decides what happens next. |
| Records expired | Answer fetches below the new log start offset with `OFFSET_OUT_OF_RANGE`. Report the new offset in `ListOffsets` and in Fetch responses. The emulation ends when the real log start offset passes it. |
| Committed offset reset | Answer `OffsetFetch` for the partition with the new offset, until the group commits that partition again. |
| Producer fenced | Refuse the producer's transactional calls with `PRODUCER_FENCED`, until it calls `InitProducerId` again. |

An emulation only fools clients that go through GaslightMQ.
The UI marks emulated events as emulated.

### What the first version does not emulate

"Leader moves" and "coordinator moves" look like one more rewrite, and are not.

- **Split routing.**
  After a move, a client sends a partition's calls to the new leader's endpoint,
  together with partitions that broker really leads.
  The backend has to split one Produce or Fetch over two real brokers and merge the answers,
  including the long-poll behaviour of Fetch.
- **Leader epochs.**
  A believable move bumps the leader epoch.
  The real brokers do not know that epoch,
  so the backend has to translate epochs in every request and response that carries one.

The *Leader flap* preset covers the most common test without either:
a refusal with `NOT_LEADER_OR_FOLLOWER` and truthful Metadata.
The client refreshes its metadata, finds the same leader and retries,
as it does after an election that brought the leadership back.

"Log truncated" would have to hide real records and renumber new ones.
The Upstream backend does not emulate it.

### Fetch sessions

A client with an incremental fetch session only names the partitions that changed,
so most Fetch requests would not carry the fields selectors test.

The Upstream backend makes every Fetch sessionless before it forwards it.
The real broker never creates a fetch session,
and the client keeps sending full Fetch requests.
The Simulated backend never grants a fetch session.

The cost is bandwidth: a client that reads many partitions sends larger Fetch requests.

### What lies leave behind in the real cluster

A lie only changes what the client is told.
On the proxy, the real cluster still reacts to what it did or did not receive.

| Lie | Effect on the real cluster |
|---|---|
| Refusal before forwarding | None. The refused part never arrived. |
| Refusal after the response | The request took effect. Records are written, offsets are committed, the transaction is ended. |
| Silence, hang-up or delay before forwarding on `Heartbeat` or `JoinGroup` | The coordinator can expire the member and start a real rebalance. |
| Silence or hang-up in the middle of a transaction | The transaction stays open until it times out. Until then it holds back read-committed consumers of its partitions. |
| Producer fenced (emulated) | The same, until the producer calls `InitProducerId` again. |

The UI shows these effects next to the preset.

## Simulated

The Simulated backend answers calls from the World.
It implements the Kafka protocol against the World's state:
appending records, serving fetches, tracking idempotent producers
and running the group protocol, classic and consumer.

It applies events directly to the World,
so every call and every client sees one consistent state.

It runs on the engine's virtual [clock](README.md#clock).
Time triggers, timeouts and generators all advance with that clock,
which tests can speed up or step.

The first version leaves out transactions.
The backend does not advertise the transactional APIs,
and "producer fenced" is not available on it yet.

### Staying faithful

The Simulated backend is only useful when clients cannot tell it from a real broker.
A proxy in front of a fake broker checks that:
`gmq proxy --upstream` can point at `gmq broker`.

This is a test setup, not a feature.
A scenario that only uses lies must give the same verdicts
through the proxy and directly on the fake broker.
The UI offers no way to combine the two backends.

## One UI for both

The UI talks to the engine, not to the backend.
The environment switcher shows which backend runs.

- Views and actions that work on both backends look the same.
- Emulated events carry an *emulated* badge.
- Events a backend cannot do are disabled, with the reason.
- The World view appears only with the Simulated backend.

A recording made on the Upstream backend can [seed](../domains/world.md#seeding) a World for the Simulated backend.
