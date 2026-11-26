# Examples

Each example shows one capability of GaslightMQ, with as few files as possible.
The number in the name orders them from easy to complex.
Every example is a workspace of its own: change into its directory and run the command in its README.

These examples show the target design.
The commands and the file formats are not implemented yet.

| Example | Level | Shows |
|---|---|---|
| [`01-fixed-records`](01-fixed-records/README.md) | Easy | A topic that contains exactly the records in a file. |
| [`02-virtual-records`](02-virtual-records/README.md) | Easy | A giant topic that is already there, without anyone filling it. |
| [`03-live-traffic`](03-live-traffic/README.md) | Easy | A topic that keeps receiving new records. |
| [`04-errors-on-a-topic`](04-errors-on-a-topic/README.md) | Easy | A topic that sometimes returns an error. |
| [`05-delays-for-one-client`](05-delays-for-one-client/README.md) | Easy | A slow cluster for one client, and a normal one for everyone else. |
| [`06-avro-and-schema-registry`](06-avro-and-schema-registry/README.md) | Medium | Avro records with a schema, and a schema registry to read them. |
| [`07-scenario-step-by-step`](07-scenario-step-by-step/README.md) | Medium | An exact test case in steps: exactly three errors, then recovery. |
| [`08-broker-node-goes-down`](08-broker-node-goes-down/README.md) | Medium | An incident that changes the fake cluster: a node fails and leaders move. |
| [`09-proxy-in-front-of-real-kafka`](09-proxy-in-front-of-real-kafka/README.md) | Medium | The same lies in front of a real cluster, with secrets kept in files. |
| [`10-several-environments`](10-several-environments/README.md) | Complex | A fake cluster and a real one side by side in one process, with one UI. |
| [`11-complete-orders-setup`](11-complete-orders-setup/README.md) | Complex | Everything together, with Docker Compose, Kubernetes and CI files. |

## The two halves

Every example uses one or both of these:

- A **broker** is a fake Kafka cluster. It owns the data. Examples 01 to 03 need nothing else.
- A **proxy** stands in front of a broker or a real cluster. It owns the lies: errors, delays and the rest.

## What a definition file holds

A definition file holds what you defined, and nothing that GaslightMQ keeps while it runs.
A key that a definition does not have is an error, so a file never holds an ID, a revision or whether a rule is armed.
