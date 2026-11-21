# Targeting

Targeting provides one predicate language for three jobs:

- **Filtering** recorded traffic in Observation.
- **Targeting** calls with rules in Injection.
- **Indexing** recordings into facets.

Because all three share the language,
a filter built while looking at traffic becomes a selector without translation.

## Fields

A field is a named property of a connection or a call.
Fields come in four groups.

### Connection fields

Connection fields describe *who* is calling.
They come from the [session identity](session.md#identity).

| Field | Example |
|---|---|
| `connection` | `#7` |
| `address` | `10.0.4.27:51459` |
| `host` | `10.0.4.27` |
| `endpoint` | `broker 2` |
| `client.id` | `order-ingest` |
| `client.software` | `librdkafka 2.6.1` |

### Request fields

Request fields describe *what* is being asked.
They come from decoding the request.

| Field | Example | Present on |
|---|---|---|
| `api` | `Produce` | Every request |
| `api.version` | `11` | Every request |
| `topic` | `orders.eu` | Requests that name topics |
| `partition` | `3` | Requests that name partitions |
| `acks` | `-1` | Produce |

The wire does not always carry what a selector needs.
The engine fills the gaps, so `topic` and `partition` mean the same on every request:

- **Topic IDs.**
  Newer request versions name topics by ID.
  The engine learns the IDs from Metadata responses and always exposes the topic name.
- **Fetch sessions.**
  A client with an incremental fetch session only names the partitions that changed since its last Fetch.
  Both backends decline fetch sessions,
  so every Fetch names every partition it reads.
  See [backends](../architecture/backends.md#fetch-sessions).

### Learned fields

Learned fields describe *which group or transaction* the caller belongs to.
A request that carries the value itself provides it.
On every other request, the value comes from the [client context](session.md#client-context).

| Field | Example | Carried by |
|---|---|---|
| `group` | `analytics-etl` | Group protocol requests, `OffsetCommit` and `OffsetFetch` |
| `member.id` | `consumer-1-4f2a…` | Group protocol requests |
| `group.instance.id` | `etl-0` | Group protocol requests of static members |
| `transactional.id` | `payments-tx-1` | Transactional requests |

`api is Fetch` with `group is analytics-etl` matches the fetches of that group's consumers,
once any connection of the same client named the group.

A client that runs several consumers under one `client.id` has several groups.
The field is then multi-valued.

### Response fields

Response fields describe *what the truth was*.
They come from the backend's response, before any lie after the response applies.

| Field | Example |
|---|---|
| `outcome` | `ok`, `error`, `timeout` |
| `error.code` | `NOT_LEADER_OR_FOLLOWER` |
| `latency` | `14 ms` |

Response fields are available for filtering in Observation.
Rules can only use them for lies that act after the response,
because before the response, the truth is not known yet.

### Multi-valued fields

A request can carry many values for one field.
A Produce request can name several topics and several partitions.

- A positive condition matches when **any** value matches.
- `is not` matches when **no** value equals.
  `topic is not orders.eu` does not match a Produce that carries `orders.eu` and `payments.settled`.
- Conditions on `topic` and `partition` apply to the same partition.
  `topic is orders.eu` with `partition is 3` matches `orders.eu/3`,
  not a request that carries `orders.eu/1` and `payments.settled/3`.

The partitions that satisfy the conditions are the rule's [target](lies-and-events.md#target).
A selector on `orders.*` matches a Produce for `orders.eu` and `payments.settled`,
and the rule's lie applies to the `orders.eu` partitions only.

## Conditions

A condition tests one field against one or more values.

| Operator | Meaning | Example |
|---|---|---|
| `is` | Equals one value | `api is Produce` |
| `is any of` | Equals one of several values | `api is any of JoinGroup, SyncGroup` |
| `matches` | Glob match | `topic matches orders.*` |
| `is not` | Does not equal | `client.id is not kafka-ui` |

Values within one condition combine with **or**.
Conditions within one selector combine with **and**.

## Selectors

A selector is a named set of conditions.

```yaml
name: Order producers
match:
  api: Produce
  topic: "orders.*"
```

A selector can include another selector by name.
The included conditions combine with **and**.

```yaml
name: Order producers from ingest
include: Order producers
match:
  client.id: order-ingest
```

Things reference selectors by name.
Renaming a selector updates every reference to it.

### Building selectors from traffic

A selector can be built from a recorded call.
The UI shows the decoded fields of the call with a checkbox each.
Ticking a field adds a condition with the call's value.
A value can be generalised into a glob, such as `orders.eu` into `orders.*`.
While someone edits the selector, the UI highlights every recorded call it matches.

## Scope

Scope is a selector that only uses connection fields.
It answers the question *who does this apply to?*

Scope is not a separate concept in the domain.
The UI shows it as its own axis because it is the most common question:

- The Clients board has one column per scope.
- The Rules list groups by scope.
- The pad grid has one layer per scope.

Common scopes are:

| Scope | Selector |
|---|---|
| All clients | *(no conditions)* |
| One client | `client.id is order-ingest` |
| One client instance | `client.id is order-ingest` and `host is 10.0.4.27` |
| One connection | `connection is #7` |
| One broker endpoint | `endpoint is broker 2` |

A scope is a set of conditions like any other.
It does not give a rule [priority](scenarios.md#modes) over rules with a wider scope.

A scope on one connection ends with that connection.
Use a client instance to follow a client across reconnects.
