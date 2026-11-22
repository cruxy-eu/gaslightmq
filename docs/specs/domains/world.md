# World

The World is the state of a simulated cluster.
A [broker](../architecture/README.md#instances-and-processes) holds one World and answers calls from it,
and incidents change it.
A proxy in front of a real cluster has no World: the real cluster holds the truth.

## Contents

| Concept | Holds |
|---|---|
| **Cluster** | Cluster ID, controller, the list of broker nodes. |
| **Broker node** | Node ID, endpoint, state: live, unresponsive or down. One node of the simulated cluster. |
| **Topic** | Name, topic ID, partitions, configs. |
| **Partition** | Leader, leader epoch, replicas, in-sync replicas, the log, log start offset, high watermark. |
| **Log** | Record batches with offsets, timestamps, keys, values and headers. Stored, or [virtual](#virtual-records). |
| **Consumer group** | Coordinator, members, assignment, committed offsets. A generation for the classic protocol, group and member epochs for the consumer protocol. |
| **Producer** | Producer ID, epoch and sequence numbers per partition, for idempotent producers. |
| **Transaction** | Transactional ID, coordinator, state, partitions in the transaction. Not in the [first version](../README.md#first-version). |

The World is consistent by construction.
Every call sees the same leaders, offsets and group state.

## Incidents change the World

[Incidents](lies-and-incidents.md#incidents) are the only way the World changes,
apart from the clients' own calls.
A Produce call appends to a log.
A JoinGroup call changes a group.
A "leader moves" incident changes a partition's leader.

Clients' own calls follow the Kafka protocol's rules.
Incidents can do things the protocol never would on its own,
which is the point.

The World does not react to an incident by itself.
A broker node that goes down leaves its partitions without a leader,
until a "leader moves" incident gives them one.
A backend adds such [consequences](lies-and-incidents.md#consequences) to the incident,
the same way for both backends.

## The World never lies

A broker answers truthfully from its World.
Lies come from the proxy in front of the broker, exactly as they do in front of a real cluster.
A rule can make that proxy refuse a Produce
even though the World would accept it.

A broker that runs alone is a truthful fake Kafka.
Incidents still change its World.

## Records

A broker returns records to consumers without anyone producing them first.
A topic in the world definition says where its records come from.

| Source | The topic contains | Stored |
|---|---|---|
| **Virtual, existing** | A number of records that are already there when the World starts. | No |
| **Virtual, arriving** | Records that keep arriving at a rate while the clock runs. | No |
| **A file** | Exactly the records in a file. | In memory |
| **A recording** | The records a [recording](observation.md#recording) captured. | In memory |
| **Produced** | Whatever clients produce. | In memory |

```yaml
topics:
  - name: orders.eu
    partitions: 6
    records:
      template: templates/order.json
      key: "order-{{seq}}"
      existing: 100000000     # already in the topic when the World starts
      arriving: 50/s          # keeps arriving while the clock runs

  - name: payments.settled
    partitions: 3
    records:
      file: data/payments.jsonl
```

`existing` simulates a data set that is already there:
a consumer reads a giant topic that nobody had to fill.
`arriving` simulates live traffic.
A topic can have one of them or both.
Both spread evenly over the topic's partitions.

### Virtual records

The broker makes up a virtual record at the moment a consumer asks for it.

- **A virtual record is a pure function.**
  It follows from the world definition, the partition and the offset, and from nothing else.
  The same definition gives the same record at the same partition and offset,
  in every run and on every machine.
- **Nothing is stored.**
  A topic with a hundred million records takes no memory and no time to start.
- **Every offset works straight away.**
  A consumer can seek to any offset between the log start and the end.
- **The end offset follows from the clock.**
  It starts at the number of existing records and grows at the arriving rate.
  A consumer at the end waits for the next record, as on a real topic.
- **Timestamps follow from offsets.**
  The world definition names a start time.
  Arriving records carry the time at which they arrive.
  Existing records carry earlier times, spaced at the arriving rate,
  or one second apart when the topic has no arriving records.
  A lookup of an offset by timestamp is a calculation.
- **Batches are stable.**
  Virtual records come in batches that start at fixed offsets,
  so two fetches of the same offset return the same batch.

### Templates

A template describes one record.
The broker fills its placeholders for each partition and offset.

```json
{
  "id": "order-{{seq}}",
  "customer": "{{pick customers}}",
  "amount": "{{number 5 500}}",
  "placed_at": "{{timestamp}}"
}
```

| Placeholder | Becomes |
|---|---|
| `{{offset}}`, `{{partition}}` | The offset and the partition of the record. |
| `{{seq}}` | A number that is unique in the topic. |
| `{{timestamp}}` | The timestamp of the record. |
| `{{uuid}}` | An identifier derived from the partition and the offset. |
| `{{pick <list>}}` | One value from a named list in the world definition. |
| `{{number <low> <high>}}` | A number in a range. |

Every random-looking value derives from the World's seed, the topic, the partition and the offset.
The World's seed is its own, so changing a scenario's seed does not change the data.

A template is valid JSON.
A value that is only a placeholder takes the placeholder's type:
`"{{number 5 500}}"` becomes a number, not a string.

Templates are files in the workspace's `templates/` directory.
The record key has a template of its own.

### Schemas

A topic can name a record format and a schema.

```yaml
records:
  format: avro
  schema: schemas/order.avsc
  template: templates/order.json
```

| Format | Schema |
|---|---|
| `json` | None, or a JSON Schema file. |
| `avro` | An `.avsc` file. |
| `protobuf` | A `.proto` file and a message name. |

- Schemas are files in the workspace's `schemas/` directory.
- The broker encodes every record with the schema,
  in the framing that schema registry clients expect.
- With a schema, the template is optional.
  Without a template, the broker makes up every field from its type.
  The record is still a pure function of the definition, the partition and the offset.
- The workspace validation checks templates and data files against their schema.

Consumers find the schema in a [schema registry](../architecture/backends.md#schema-registry).

### Producing to a topic with virtual records

A client can still produce to such a topic.
The produced record takes the next offset, and the broker stores it.
Virtual records fill every offset that nobody produced to.
The data stays the same at every offset that no client wrote.

## Clock

## Clock

The World does not own time.
It reads the [engine's clock](../architecture/README.md#clock),
which is virtual on the Simulated backend.
Arriving records, session timeouts and transaction timeouts all use it.
In tests, the clock can run faster than real time, or step on demand,
so a 30-second session timeout does not take 30 seconds.

## Seeding

A world definition describes the starting state.
Its seed and its start time make the [virtual records](#virtual-records) the same in every run.

```yaml
name: orders-sim
seed: 7
start: 2026-01-01T00:00:00Z
brokers: 3
topics:
  - name: orders.eu
    partitions: 6
    replication: 3
    records:
      template: templates/order.json
      existing: 100000000
      arriving: 50/s
  - name: payments.settled
    partitions: 3
groups:
  - name: analytics-etl
```

A world definition can be **seeded from a recording**.
The recording's index already knows the topics, partition counts, groups and transactional IDs it saw.
Seeding turns that knowledge into a world definition,
optionally with the recorded records as initial log contents.
A [metadata-only](observation.md#recording) recording seeds the structure without the records.

This is the bridge between the two backends:
record real traffic through the proxy,
then run the same clients against a broker and break them deterministically.
