# World

The World is the state of a simulated cluster.
The Simulated backend answers calls from it,
and events change it.
On the proxy, there is no World: the real cluster holds the truth.

## Contents

| Concept | Holds |
|---|---|
| **Cluster** | Cluster ID, controller, the list of brokers. |
| **Broker** | Node ID, endpoint, state: live, unresponsive or down. |
| **Topic** | Name, topic ID, partitions, configs. |
| **Partition** | Leader, leader epoch, replicas, in-sync replicas, the log, log start offset, high watermark. |
| **Log** | Record batches with offsets, timestamps, keys, values and headers. |
| **Consumer group** | Coordinator, members, assignment, committed offsets. A generation for the classic protocol, group and member epochs for the consumer protocol. |
| **Producer** | Producer ID, epoch and sequence numbers per partition, for idempotent producers. |
| **Transaction** | Transactional ID, coordinator, state, partitions in the transaction. Not in the [first version](../README.md#first-version). |

The World is consistent by construction.
Every call sees the same leaders, offsets and group state.

## Events change the World

[Events](lies-and-events.md#events) are the only way the World changes,
apart from the clients' own calls.
A Produce call appends to a log.
A JoinGroup call changes a group.
A "leader moves" event changes a partition's leader.

Clients' own calls follow the Kafka protocol's rules.
Events can do things the protocol never would on its own,
which is the point.

The World does not react to an event by itself.
A broker that goes down leaves its partitions without a leader,
until a "leader moves" event gives them one.
The engine adds such [consequences](lies-and-events.md#consequences) to the event,
the same way for both backends.

## Lies still apply

The World is the truth on the fake broker.
Lies still apply on top of it, exactly as on the proxy.
A rule can make the fake broker refuse a Produce
even though the World would accept it.

## Generators

A generator appends records to a topic over time,
so consumers have something to read without a producer.

```yaml
generators:
  - topic: orders.eu
    rate: 50/s
    key: "order-{seq}"
    value: { template: order.json }
```

Generators run on the engine's clock.

## Clock

The World does not own time.
It reads the [engine's clock](../architecture/README.md#clock),
which is virtual on the Simulated backend.
Generators, session timeouts and transaction timeouts all use it.
In tests, the clock can run faster than real time, or step on demand,
so a 30-second session timeout does not take 30 seconds.

## Seeding

A world definition describes the starting state.

```yaml
name: orders-sim
brokers: 3
topics:
  - name: orders.eu
    partitions: 6
    replication: 3
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
then run the same clients against the fake broker and break them deterministically.
