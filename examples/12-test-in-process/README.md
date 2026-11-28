# Test in process

The tests of your own application, with GaslightMQ inside the process of each test.
The application is an orders service, and its tests check how it handles a cluster that refuses an order,
loses the acknowledgement of one, or moves the leader of its partition.

Level: medium.

## Files

| File | Holds |
|---|---|
| `src/lib.rs` | The orders service: it writes each order to the topic `orders`, and says what a failed write means. |
| `tests/orders.rs` | Its tests: each starts a cluster and GaslightMQ in front of it, and arms a rule. |
| `Cargo.toml` | The project, which depends on `gaslightmq_test` for its tests alone. |

## Run

The tests start a real Kafka cluster in Docker, so they need Docker.

```sh
cargo test
```

## What happens

- `GaslightMq::proxy_in_front_of(cluster).start()` starts a proxy in front of the cluster, and a control plane in
  front of the proxy, inside the process of the test.
- The test hands the service `gaslight.bootstrap()` instead of the address of the cluster.
  The service knows nothing about GaslightMQ: it speaks Kafka to the proxy as it would to the cluster.
- The test arms a rule written as a rule file writes it, `gaslight.rules().arm(rule! { … })`, places an order, and
  asserts what the service returned and what the cluster holds. A bare word is YAML, and a Rust value stands in
  parentheses: `until: { firings: (TRIES_REFUSED) }`. Nothing needs a name: a rule has the name of what it says.

| Test | The rule | The service |
|---|---|---|
| `an_order_that_the_cluster_does_not_let_the_service_write_is_not_allowed` | Every Produce is refused with `TOPIC_AUTHORIZATION_FAILED`. | Says `NotAllowed`. The cluster holds nothing. |
| `an_order_whose_acknowledgement_got_lost_is_unconfirmed_though_the_cluster_holds_it` | The cluster writes the order, and the service is told `REQUEST_TIMED_OUT`. | Says `Unconfirmed`, and is right to: the cluster holds the order. |
| `an_order_placed_while_the_leader_moves_is_placed_on_its_second_try` | The first Produce alone is refused with `NOT_LEADER_OR_FOLLOWER`. | Places the order. The recording shows the refused try and the one after it. |
| `orders_placed_while_the_leader_churns_are_each_written_once` | A scenario: one Produce passes, the next two are refused with `NOT_LEADER_OR_FOLLOWER`, and the leader stays. | Places all three orders. The recording, which keeps the records too, shows the two refused tries. |
| `two_services_side_by_side_each_meet_the_lies_of_their_own_proxy_alone` | One proxy refuses, the other one lies about nothing. | One service is refused, the other one places its order. |

The third and the fourth test assert on a recording: the proxy records from the start, the test follows the Produce
calls and waits until the recording holds them, stops the recording, and queries the Produce calls.
Each recorded call says which rule applied to it, so the test sees the retry that the service hid.
The fourth one declares its scenario before the proxy starts, with `.activating(scenario! { … })`: its steps and the
rule of its second step need no names.

## Tests side by side

`cargo test` runs the tests of a binary at the same time, and they never meet:

- **Free ports.** Every listener of each proxy, and its control plane, takes a port that the system picks.
  No test names a port, so two tests cannot ask for the same one.
- **A cluster each.** Each test starts its own cluster, so its topic `orders` belongs to it alone.
- **A rule is armed on one proxy.** The last test starts two proxies in front of one cluster: the rule that one of them
  holds changes nothing for the clients of the other.

The rules of the tests are written in the tests. To keep them in files instead, hand
`GaslightMqConfiguration::with_workspace` the directory of a workspace, as
[`13-test-with-a-container`](../13-test-with-a-container/README.md) does, and arm a rule of it by its name.

## In your project

This example depends on the crates of this repository by their paths.
Your project names a release of them instead, once one is published:

```toml
[dev-dependencies]
gaslightmq_control_api = "0.1"
gaslightmq_test = "0.1"
```

The service writes with the Kafka client of GaslightMQ, which this repository builds.
Your application keeps its own client: the proxy speaks Kafka to whatever connects to it.
