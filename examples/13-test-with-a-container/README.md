# Test with a container

The tests of your own application, with the image of GaslightMQ in a container next to each test,
in front of a real Kafka cluster on a network of containers.
The application is the orders service of [`12-test-in-process`](../12-test-in-process/README.md),
and the rules that its tests arm are files of the project.

Level: complex.

## Files

| File | Holds |
|---|---|
| `src/lib.rs` | The orders service: it writes each order to the topic `orders`, and says what a failed write means. |
| `tests/orders.rs` | Its tests: each puts a cluster and GaslightMQ on a network of its own, and arms a rule. |
| `gaslight/rules/orders.yaml` | A rule that loses the acknowledgement of every order, which a test arms by name. |
| `Cargo.toml` | The project, which depends on `gaslightmq_testcontainers` for its tests alone. |

## Run

The tests start a real Kafka cluster and the image of GaslightMQ in Docker, so they need Docker.

```sh
cargo test
```

The container runs the image of the release of `gaslightmq_testcontainers`.
To test an image that you built, name it for the whole run:

```sh
GASLIGHTMQ_IMAGE=ghcr.io/cruxy-eu/gaslightmq:latest cargo test
```

In this repository, `just test-image` builds that image and runs these tests against it.

## What happens

- The test makes a network of containers, and starts the cluster on it under an alias, such as `kafka-4711-1:19092`.
- `GaslightMqContainer::proxy_in_front_of(alias).with_network(network).start()` starts the image on the same
  network, in front of the cluster as the containers there reach it.
  It returns once the process said that it is ready, and its control plane says that the proxy runs.
- The container publishes the proxy and its control plane on free ports of the machine.
  The test hands the service `gaslight.bootstrap()`, and the proxy names its brokers to the service under the
  ports they are published on.
- `with_workspace("gaslight")` copies the files of the workspace into the container, so its rules are there,
  disarmed until a test arms one by its name. What the process writes stays in the container.
- A test arms a rule written as a rule file writes it, `gaslight.rules().arm(rule! { … })`, or names one of the
  workspace before the container starts: `.arming("order-acknowledgements-get-lost")`.

| Test | The rule | The service |
|---|---|---|
| `an_order_that_the_cluster_does_not_let_the_service_write_is_not_allowed` | Written in the test with `rule!`: every Produce is refused with `TOPIC_AUTHORIZATION_FAILED`. | Says `NotAllowed`. The cluster holds nothing. |
| `an_order_whose_acknowledgement_got_lost_is_unconfirmed_though_the_cluster_holds_it` | `order-acknowledgements-get-lost` of `gaslight/rules/orders.yaml`, armed by name as the container starts. | Says `Unconfirmed`, and is right to: the cluster holds the order. |
| `two_services_side_by_side_each_meet_the_lies_of_their_own_container_alone` | One container refuses, the other one lies about nothing. | One service is refused, the other one places its order. |

A test asserts on a recording, and plays a scenario, here as it does in
[`12-test-in-process`](../12-test-in-process/README.md): both crates speak the same words.

## Tests side by side

`cargo test` runs the tests of a binary at the same time, and they never meet:

- **Published on free ports.** Each container publishes its listeners and its control plane on ports that the
  container runtime picks, and the proxy advertises its brokers under them. No test names a port of the machine.
- **A network each.** `ContainerNetwork::unique` and `NetworkListener::unique` give each test a network and an alias
  that no other test has, so a container reaches the cluster of its own test alone.
  The cluster creates the network, and removes it when it ends.
- **A cluster each.** Each test starts its own cluster, so its topic `orders` belongs to it alone.
- **A rule is armed in one container.** The last test starts two containers in front of one cluster: the rule that
  one of them holds changes nothing for the clients of the other.

## In your project

This example depends on the crates of this repository by their paths.
Your project names a release of them instead, once one is published:

```toml
[dev-dependencies]
gaslightmq_control_api = "0.1"
gaslightmq_testcontainers = "0.1"
```

Your tests start Kafka on a network of containers the way they already do, and hand GaslightMQ the network and the
address that the cluster has there.
The service writes with the Kafka client of GaslightMQ, which this repository builds.
Your application keeps its own client: the proxy speaks Kafka to whatever connects to it.
