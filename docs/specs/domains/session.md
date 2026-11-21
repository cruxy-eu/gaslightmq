# Session

A session is the runtime of one client connection.
Every call travels through exactly one session.
The session holds everything rules need to decide about that call.

## Parts

A session has two parts.

| Part | Holds | Does |
|---|---|---|
| **Transport** | The client's connection. | Reads the bytes the client sends, and writes the bytes the client receives. |
| **Pipeline** | The session's [state](#state), including the snapshot of the armed rules. | Decides what happens to each call. See the [request pipeline](../architecture/request-pipeline.md). |

The pipeline never touches the connection.
The session tells the pipeline what happened, such as bytes that arrived or an answer from the backend,
and carries out what the pipeline decides.
A rule change reaches the session as a new snapshot, and the session hands it to its pipeline.

The [crate layout](../architecture/crates.md#a-session-is-a-transport-and-a-pipeline) describes how the engine runs the two parts.

## Lifecycle

```mermaid
stateDiagram-v2
  [*] --> Opened: client connects
  Opened --> Identified: first request carries client.id
  Identified --> Identified: calls
  Identified --> Closed: client, backend or lie hangs up
  Opened --> Closed: hang-up before the first request
  Closed --> [*]
```

- **Opened**: the connection exists, but the client is anonymous.
  Only connection fields such as the address and the endpoint are known.
- **Identified**: the first request carries `client.id`.
  An `ApiVersions` request also carries the client software name and version.
  From here on, every connection field is known.
- **Closed**: the session ends.
  The session records who closed it: the client, the backend or a lie.

Rules scoped to a client cannot apply before the session is identified.
Lies that act before identification, such as refusing the connection,
can only target connection fields that are known at that point.

## Identity

| Field | Known from | Example |
|---|---|---|
| `connection` | Opened | `#7` |
| `address` | Opened | `10.0.4.27:51459` |
| `host` | Opened | `10.0.4.27` |
| `endpoint` | Opened | `bootstrap`, `broker 2` |
| `client.id` | Identified | `order-ingest` |
| `client.software` | First `ApiVersions` | `apache-kafka-java 3.8.0` |

Identity fields are part of the [field vocabulary](targeting.md#fields).
Scope selectors test them.

A `connection` number only lives as long as its connection.
A client that reconnects gets a new session and a new number.
`client.id` with `host` identifies one client instance across reconnects.

## Client context

One Kafka client opens several connections,
and most of them never say which group or transaction the client belongs to.
A consumer names its group on the coordinator connection,
and fetches on other connections.

The engine therefore keeps a **client context** for every pair of `client.id` and `host`.
Sessions with the same pair share it.
The context remembers what any of those sessions learned:

| Learned field | Learned from |
|---|---|
| `group` | Group protocol requests, classic and consumer protocol alike. |
| `member.id` | The response to joining the group. |
| `group.instance.id` | Group protocol requests of static members. |
| `transactional.id` | `InitProducerId` and other transactional requests. |

Selectors use these as [learned fields](targeting.md#learned-fields),
so "delay Fetch for group `analytics-etl`" works
although a Fetch request never names a group.

The context lives in the engine, because it outlives single connections.
It ends when the last session of the pair has been closed for a while.

## State

The session's pipeline keeps this state in memory:

- **Rule snapshot**: the armed rules at this moment.
  The pipeline swaps in a new snapshot whenever a rule changes.
  A call always uses one consistent snapshot from start to finish.
- **Session key**: the host, the endpoint,
  and the ordinal of this connection among the connections from that host to that endpoint.
  The key is stable across runs, unlike the connection number.
- **Per-session counters**: the match numbers of rules that count `per: session`.
  See [counters](rules.md#counters).
- **In-flight calls**: requests waiting for a response,
  so lies after the response apply to the right call.
- **Stalled**: whether a [silence](../architecture/request-pipeline.md#ordering) stopped the connection.

None of this state is persisted.
A session dies with its connection.

## Disconnects

A session never outlives its connection.
A simulated disconnect closes the real connection to the client,
so it ends the session like any other close.

| What happens | The session |
|---|---|
| A [hang-up](lies-and-incidents.md#lies) fires. | Closes, and records that a lie closed it. |
| A broker goes down. | Closes, together with every other session on that broker's endpoint. The endpoint refuses new connections until the broker is back. |
| A silence fires, or the broker is unresponsive. | Stays open and never answers. It closes when the client gives up. |

A client that reconnects gets a new session,
with a new transport and a new pipeline.
The pipeline starts with the current snapshot, no in-flight calls, and per-session counters at zero.

Two things outlive the old session, because they live in the engine:

- the [client context](#client-context), so learned fields such as the group still apply,
- each rule's shared counter and its budget.

## Dice and counters

The session evaluates every trigger.
It rolls the [dice](rules.md#dice) of chance triggers
and reads or increments the [counters](rules.md#counters) of count triggers.

- A rule's default counter and its budget live in the engine.
  All sessions share them and update them atomically.
- A `per: session` counter lives in the session.
  Its dice use the session key, so two runs give each connection the same rolls.
