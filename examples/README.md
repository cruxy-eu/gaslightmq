# Examples

Each example shows one capability of GaslightMQ, with as few files as possible.
The number in the name orders them from easy to complex.
Every example is a workspace of its own: change into its directory and run the command in its README.

Examples 01 to 11 show the target design: not all of their commands and file formats are implemented yet.
Examples 12 and 13 run today, and CI runs their tests.

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

## Testing your application

Each of these two is a Rust project as yours would be: an orders service, and the tests that check how it handles a
cluster that lies to it. Each test starts GaslightMQ in front of a real Kafka cluster, arms a rule through
the control plane, and asserts what the service did. The tests run side by side, each with GaslightMQ of its own.

| Example | Level | Shows |
|---|---|---|
| [`12-test-in-process`](12-test-in-process/README.md) | Medium | GaslightMQ inside the process of each test, with `gaslightmq_test`, and a test that asserts on a recording. |
| [`13-test-with-a-container`](13-test-with-a-container/README.md) | Complex | The image of GaslightMQ in a container next to each test, with `gaslightmq_testcontainers`, on a network of containers that the test names, with rules from files of the project. |

## The two halves

Every example uses one or both of these:

- A **broker** is a fake Kafka cluster. It owns the data. Examples 01 to 03 need nothing else.
- A **proxy** stands in front of a broker or a real cluster. It owns the lies: errors, delays and the rest.

## What a definition file holds

A definition file holds what you defined, and nothing that GaslightMQ keeps while it runs.
A key that a definition does not have is an error, so a file never holds an ID, a revision or whether a rule is armed.
