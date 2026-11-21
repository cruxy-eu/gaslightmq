# Targeting

Targeting provides one predicate language for three jobs:

- **Filtering** recorded traffic in Observation.
- **Targeting** calls with rules in Injection.
- **Indexing** recordings into facets.

Because all three share the language,
a filter built while looking at traffic becomes a selector without translation.

## Fields

A field is a named property of a connection or a call.
Fields come in three groups.

### Connection fields

Connection fields describe *who* is calling.
They come from the [session identity](session.md#identity).

| Field | Example |
|---|---|
| `connection` | `#7` |
| `address` | `10.0.4.27:51459` |
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
| `group` | `analytics-etl` | Group protocol requests |
| `transactional.id` | `payments-tx-1` | Transactional requests |
| `acks` | `-1` | Produce |

A request can carry many values for one field.
A Produce request can name several topics and several partitions.
A condition on a multi-valued field matches when **any** value matches.

### Response fields

Response fields describe *what the truth was*.
They come from the backend's response, before any post-response lie applies.

| Field | Example |
|---|---|
| `outcome` | `ok`, `error`, `timeout` |
| `error.code` | `NOT_LEADER_OR_FOLLOWER` |
| `latency` | `14 ms` |

Response fields are available for filtering in Observation.
Rules can only use them for lies that act after the response,
because before the response, the truth is not known yet.

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
| One connection | `connection is #7` |
| One broker endpoint | `endpoint is broker 2` |
