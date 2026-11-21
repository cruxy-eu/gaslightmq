# Backends

A backend answers a call after the session's pipeline has decided what to do with it.
The engine has two backends.
Everything in front of the backend is the same for both.

| Backend | Truth comes from | Typical use |
|---|---|---|
| **Upstream** | A real Kafka cluster | Proxy in front of a real cluster. Real Kafka semantics, real data. |
| **Simulated** | The [World](../domains/world.md) | Fake broker. Full control, fully reproducible. |

## Contract

A backend:

- answers a decoded request with the truth,
- declares its **capabilities**: which events it can apply, and which it can only emulate,
- applies or emulates an event when the scenario runner fires one,
- reports the endpoints clients should connect to.

A backend never decides about lies.
Lies happen in the pipeline, in front of and behind the backend.

## Capabilities

| Capability | Upstream | Simulated |
|---|---|---|
| Lies of every kind | ✅ | ✅ |
| Leader moves | Emulated | ✅ |
| Broker down, unresponsive, back | Emulated on the proxy's endpoints | ✅ |
| Coordinator moves | Emulated | ✅ |
| Rebalance | Emulated, partially | ✅ |
| Producer fenced | Emulated | ✅ |
| Partition added | ❌ | ✅ |
| Topic deleted | ❌ | ✅ |
| Records appended, generators | ❌ | ✅ |
| Editable world state | ❌, read-only observation | ✅ |
| Virtual clock | ❌, wall clock | ✅ |
| Reproducible runs | Dice only. The network and the cluster are not reproducible. | ✅ |

A scenario declares the events it needs.
A backend rejects a scenario that needs events it cannot apply or emulate,
when the scenario loads, not halfway through a run.

## Upstream

The Upstream backend forwards calls to a real cluster
and returns the real response as the truth.

It runs one listener per real broker,
and lies about the topology so clients connect to those listeners.
That topology lie is always on.
Without it, clients would bypass GaslightMQ after bootstrap.

### Emulating events

The Upstream backend emulates an event with a bundle of lies,
held as an **emulation** while the event is in force.

| Event | Emulation |
|---|---|
| Leader moves | Refuse calls for the partition on the old leader's endpoint with `NOT_LEADER_OR_FOLLOWER`. Rewrite Metadata so the partition points at the new leader's endpoint. Forward calls on the new endpoint to the real leader. |
| Broker down | Close the broker's listener. Drop its connections. Leave it out of Metadata. |
| Broker unresponsive | Accept connections on the broker's endpoint, never answer. |
| Coordinator moves | Refuse group calls with `NOT_COORDINATOR`. Rewrite `FindCoordinator` to point at another endpoint. Forward there to the real coordinator. |
| Producer fenced | Refuse the producer's transactional calls with `PRODUCER_FENCED`, until it calls `InitProducerId` again. |

An emulation only fools clients that go through GaslightMQ.
The UI marks emulated events as emulated.

## Simulated

The Simulated backend answers calls from the World.
It implements the Kafka protocol against the World's state:
appending records, serving fetches, running the group protocol and transactions.

It applies events directly to the World,
so every call and every client sees one consistent state.

It runs on the World's virtual clock.
Time triggers, timeouts and generators all advance with that clock,
which tests can speed up or step.

## One UI for both

The UI talks to the engine, not to the backend.
The environment switcher shows which backend runs.

- Views and actions that work on both backends look the same.
- Emulated events carry an *emulated* badge.
- Events a backend cannot do are disabled, with the reason.
- The World view appears only with the Simulated backend.

A recording made on the Upstream backend can [seed](../domains/world.md#seeding) a World for the Simulated backend.
