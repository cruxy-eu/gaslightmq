# Lies and events

GaslightMQ changes what clients experience in two ways.
It can **lie** about the truth,
or it can **change the truth** with an event.

## Truth

The truth is what the backend answers when GaslightMQ does not interfere.
On the proxy, the truth comes from the real cluster.
On the fake broker, the truth comes from the [World](world.md).

## Lies

A lie is anything the client is told that differs from the truth.
A lie affects what one client sees on one call.
Nothing changes in the cluster,
so other calls and other clients can contradict the lie.

A lie is not necessarily a fault.
Some lies make a client fail, some make it slow,
and some quietly change what it believes about data or the cluster.

### Kinds of lies

| Kind | The client is told | Examples |
|---|---|---|
| **Refusal** | The request failed. | An error code on the whole response, or on some topics or partitions. |
| **Timing** | The answer took longer, or the client must back off. | Delay with jitter, `throttle_time_ms`. |
| **Silence** | Nothing at all. | The request is read but never answered (blackhole). |
| **Hang-up** | The connection is gone. | Close the socket before forwarding, or after the response. |
| **Topology** | The cluster looks different. | Rewritten broker addresses, a different leader, a missing broker, another controller. |
| **Data** | The records are different. | Altered, dropped, duplicated or reordered records, corrupted batches. |
| **Progress** | Offsets and positions are different. | A stale high watermark, a different committed offset, a log start offset that jumped. |
| **Configuration** | Settings are different. | Rewritten topic or broker configs in `DescribeConfigs`. |

Topology lies are what make the proxy work at all.
The proxy rewrites broker addresses in Metadata responses,
so clients connect to the proxy instead of the real brokers.
That lie is always on.

### When a lie acts

A lie acts at one moment of a call:

```
receive ─► [before forwarding] ─► backend ─► [after the response] ─► send
            refusal, silence,                 timing, refusal per partition,
            hang-up, request rewrite          topology, data, progress,
                                              configuration, hang-up
```

- **Before forwarding**: the backend never sees the request,
  or sees a rewritten version of it.
- **After the response**: the backend answers truthfully,
  and the lie changes the answer before the client sees it.

A lie that acts after the response can depend on the truth.
For example, it can apply only when the truth was a success.

### How lies combine

When several lies apply to one call, their combination class decides what happens.

| Class | Lies | Combination |
|---|---|---|
| **Modifier** | Timing | Modifiers stack. Delays add up. Throttle takes the maximum. |
| **Rewrite** | Topology, data, progress, configuration, refusal per partition | Rewrites merge per target. Two rewrites of different partitions both apply. Two rewrites of the same partition conflict. |
| **Terminal** | Refusal of the whole response, silence, hang-up | Only one terminal lie can apply. The scenario's [mode](scenarios.md#modes) picks the winner. |

Kafka responses are per partition,
so many apparent conflicts are not conflicts at all.
A rule that refuses `orders.*` and a rule that refuses `payments.*`
both apply to one Produce request that touches both topics.

## Events

An event changes the truth.
After an event, every call and every client sees the same new world.

| Event | Effect |
|---|---|
| Leader moves | A partition gets a new leader. |
| Broker goes down | A broker refuses connections and drops existing ones. |
| Broker goes unresponsive | A broker accepts connections and never answers. |
| Broker comes back | A broker serves traffic again. |
| Rebalance | A consumer group starts a new generation. |
| Coordinator moves | A group or transaction coordinator moves to another broker. |
| Partition added | A topic gets more partitions. |
| Topic deleted | A topic disappears. |
| Records appended | Data arrives on a partition, for example from a generator. |
| Producer fenced | A transactional producer's epoch is bumped. |

Events describe the cluster, not one call.
A rule can still fire an event in reaction to a call:
"when the 3rd Produce arrives, move the leader".

### Events on each backend

The [Simulated backend](../architecture/backends.md) applies events to the World.
The event really happens, and everything stays consistent.

The [Upstream backend](../architecture/backends.md) cannot change a real cluster.
It **emulates** an event with a bundle of lies.
For "leader moves", the emulation:

- refuses calls to the old leader with `NOT_LEADER_OR_FOLLOWER`,
- rewrites Metadata so the partition points at the new leader,
- forwards calls on the new leader's endpoint to the real leader.

An emulation is only as consistent as the traffic that passes through GaslightMQ.
Clients that talk to the real cluster directly see the truth.
The UI marks emulated events, so nobody mistakes them for real ones.
A backend rejects an event it cannot emulate when the scenario loads.

## Presets

A preset is a named, reusable lie or event with its parameters.

```yaml
name: Delay 1 s
lie: timing
delay: 1s
jitter: 200ms
```

```yaml
name: Leader election
event: leader-moves
```

Pad grid columns, ⌘K suggestions and rule editors all offer presets.
Rules reference presets by name, or define their action inline.
