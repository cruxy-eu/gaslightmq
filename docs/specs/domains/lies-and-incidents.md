# Lies and incidents

GaslightMQ changes what clients experience in two ways.
It can **lie** about the truth,
or it can **change the truth** with an incident.

## Truth

The truth is what the backend answers when no lie applies.
In front of a real cluster, the truth comes from that cluster,
seen through the Upstream backend's [overlay](../architecture/backends.md#overlay).
In front of a broker, the truth comes from the broker's [World](world.md).

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
| **Refusal** | The request failed. | An error code on the partitions a rule targets, or on the whole response. |
| **Timing** | The answer took longer, or the client must back off. | Delay with jitter, `throttle_time_ms`. |
| **Silence** | Nothing at all. | The request is read but never answered (blackhole). |
| **Hang-up** | The connection is gone. | Close the socket before forwarding, or after the backend answered. |
| **Topology** | The cluster looks different. | A different leader, a missing broker, another controller, an unreachable address. |
| **Data** | The records are different. | Altered, dropped, duplicated or reordered records, corrupted batches. |
| **Progress** | Offsets and positions are different. | A stale high watermark, a different committed offset, a log start offset that jumped. |
| **Configuration** | Settings are different. | Rewritten topic or broker configs in `DescribeConfigs`. |

The proxy also rewrites broker addresses in Metadata responses,
so clients connect to the proxy instead of the real brokers.
That address translation is not a lie.
It belongs to the Upstream backend's [overlay](../architecture/backends.md#overlay),
and it is part of the truth that rules see.

### Stage

A lie acts at one of two **stages** of a call:

```
receive ─► [before forwarding] ─► backend ─► [after the response] ─► send
            refusal, timing,                  refusal, timing,
            silence, hang-up,                 topology, data, progress,
            request rewrite                   configuration, hang-up
```

- **Before forwarding**: the backend never sees the request,
  or sees it late, or sees a rewritten version of it.
- **After the response**: the backend answers truthfully,
  and the lie changes the answer before the client sees it.

Refusal, timing and hang-up name their stage,
because the stage changes what really happens in the cluster.

| Lie | Before forwarding | After the response |
|---|---|---|
| **Refusal** | The backend never sees the refused part. Nothing is written. This is the default. | The backend handles the request, and the client is told it failed. A refused Produce is written anyway, so a retry duplicates the records unless the producer is idempotent. |
| **Timing** | The backend sees the request late. A late Heartbeat can expire the member and cause a real rebalance. | The backend answers on time, and the client waits. This is the default. |
| **Hang-up** | The connection closes, and the request is dropped. This is the default. | The backend handles the request, and the connection closes before the client reads the answer. |

Silence always acts before forwarding.
Topology, data, progress and configuration lies always act after the response.

A lie that acts after the response can depend on the truth.
For example, it can apply only when the truth was a success.

### Target

A lie applies to a **target**: the part of the call it changes.

- Most Kafka requests carry several topics and partitions.
  On those requests, the target is the set of partitions the rule's selector matched.
  See [multi-valued fields](targeting.md#multi-valued-fields).
- On requests without partitions, such as `FindCoordinator` or `Heartbeat`,
  the target is the whole response.
- Timing, silence and hang-up always target the whole call.

A rule that refuses `orders.*` only refuses the `orders.*` partitions
of a Produce request that also carries `payments.settled`.
The rest of the request goes through.

When a refusal before forwarding targets only some partitions,
the engine removes those partitions from the request,
forwards the rest,
and merges the refusals into the response.
The outcome for a partition does not depend on how the client batched its request.

### How lies combine

When several lies apply to one call, their combination class decides what happens.

| Class | Lies | Combination |
|---|---|---|
| **Modifier** | Timing | Modifiers stack. Delays add up, with their jitters. Throttle takes the maximum. |
| **Rewrite** | Topology, data, progress, configuration | Rewrites merge per target and aspect. A stale high watermark and dropped records on the same partition both apply. Two rewrites of the same aspect of the same target conflict. |
| **Refusal** | Refusal | Refusals of different partitions all apply. Two refusals of the same partition conflict. A refusal replaces everything about its partition, so it pre-empts rewrites of that partition. |
| **Terminal** | Silence, hang-up | A terminal lie ends the whole call. Only one can apply. It conflicts with every refusal on the call, and it pre-empts modifiers and rewrites. |

Lies that do not conflict all apply.
A delay, a refusal of `orders.eu/3` and a stale watermark on `orders.eu/4` combine on one Fetch.

Lies that conflict go to the scenario's [mode](scenarios.md#modes),
which picks one winner per target.
The losing rule is **shadowed** for that target.

Within one [evaluation pass](../architecture/request-pipeline.md#two-evaluation-passes),
a rule that lost a target shadows no rule there.
A refusal above a blackhole shadows the blackhole,
and a refusal of another partition below the two then applies.
Across the two passes, a lie before forwarding keeps what it lost in the request pass,
even when its winner loses in the response pass.

A lie that fired but has nothing left to change is **pre-empted**.
A delay on a call that a silence ended is pre-empted,
and so is a data rewrite of a partition that another rule refused.

Pre-emption does not depend on priority, and it comes before shadowing.
A rewrite of a refused partition is pre-empted,
even when a rewrite above it changes the same aspect.

A terminal lie pre-empts the lies that act at its [stage](#stage) or later.
A delay before forwarding still holds the request back
when a hang-up after the response ends the call.

The combined delay of a stage is the sum of the delays that apply there.
It strays from that sum by at most the sum of their jitters.
The throttle is a field of the answer.
It is the maximum over every timing lie that applies, at either stage.
A call that a terminal lie ends has no answer, so no throttle reaches the client.

The whole call is one target.
A refusal of the whole call and a refusal of a partition conflict,
and the loser loses its whole target, as with a terminal lie.
A rewrite of the whole call keeps something to change until a terminal lie applies or the whole call is refused.

## Incidents

An incident changes the truth.
After an incident, every call and every client sees the same new world.

| Incident | Effect |
|---|---|
| Leader moves | A partition gets a new leader and a new leader epoch. |
| Broker goes down | A broker refuses connections and drops existing ones. |
| Broker goes unresponsive | A broker accepts connections and never answers. |
| Broker comes back | A broker serves traffic again. |
| Rebalance | A consumer group starts a new generation or epoch. |
| Coordinator moves | A group or transaction coordinator moves to another broker. |
| Partition added | A topic gets more partitions. |
| Topic deleted | A topic disappears. |
| Records appended | Data arrives on a partition. |
| Records expired | A partition's log start offset moves forward. |
| Log truncated | A partition's end offset moves back, as after an unclean leader election. |
| Committed offset reset | A group's committed offset on a partition changes. |
| Producer fenced | A transactional producer's epoch is bumped. |

Incidents describe the cluster, not one call.
A rule can still fire an incident in reaction to a call:
"when the 3rd Produce arrives, move the leader".
The incident takes effect after that call.

Incidents are not lies.
An incident never conflicts with a lie, and no rule shadows it.
Every incident that a firing rule causes takes effect.

### Consequences

Some incidents have consequences.
A backend expands an incident into its consequences before it applies or emulates it.
The expansion is one rule that both backends share, so both see the same list for the same cluster.

| Incident | Consequences |
|---|---|
| Broker goes down | Every partition the broker led has no leader. Every coordinator it hosted is unavailable. |
| Broker goes down, with `failover: true` | Every partition the broker led gets a "leader moves" incident. Every coordinator it hosted gets a "coordinator moves" incident. |
| Broker comes back | The broker is a replica again. Leaders and coordinators stay where they are. |
| Leader moves | None. |

A backend that cannot apply or emulate one of the consequences rejects the incident.

### Incidents on each backend

The [Simulated backend](../architecture/backends.md#simulated) applies incidents to the World.
The incident really happens, and everything stays consistent.

The [Upstream backend](../architecture/backends.md#upstream) cannot change a real cluster.
It **emulates** an incident inside its overlay.
For "broker goes down", the overlay:

- closes the broker's listener and drops its connections,
- leaves the broker out of Metadata,
- reports the partitions it led as leaderless.

Rules cannot tell an emulated incident from a real one.
What the overlay answers is the truth.

An emulation is only as consistent as the traffic that passes through GaslightMQ.
Clients that talk to the real cluster directly see the real cluster.
The UI marks emulated incidents, so nobody mistakes them for real ones.
A backend rejects an incident it cannot emulate when the scenario loads.

### A lie or an incident?

A lie is one call wide.
Clients cross-check what they are told,
so a lie about durable state soon contradicts itself.

A progress lie that answers a Fetch with `OFFSET_OUT_OF_RANGE` is an example.
The client asks `ListOffsets` for the earliest offset, gets the truth,
seeks to it and receives the same lie again, forever.

Use a lie for what a client sees once: an error, a delay, a corrupted batch.
Use an incident when the client must keep believing it across calls and APIs:
"records expired" moves the log start offset for Fetch and `ListOffsets` alike.

## Presets

A preset is a named, reusable lie or incident with its parameters.

```yaml
name: Delay 1 s
lie: timing
delay: 1s
jitter: 200ms
```

```yaml
name: Lost acknowledgement
lie: refusal
error: REQUEST_TIMED_OUT
stage: after
```

```yaml
name: Leader flap
lie: refusal
error: NOT_LEADER_OR_FOLLOWER
```

```yaml
name: Leader election
incident: leader-moves
```

Pad grid columns, ⌘K suggestions and rule editors all offer presets.
Rules reference presets by name, or define their action inline.

A preset name is unique among presets, and it follows the rule for [rule names](rules.md#rule).
A preset holds the keys of its lie or incident next to its name.
It holds either a `lie` key or an `incident` key, and not both.
