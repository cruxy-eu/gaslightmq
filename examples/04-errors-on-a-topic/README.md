# Errors on a topic

A topic that sometimes returns an error.
One in five Produce calls on `orders` fails with `NOT_LEADER_OR_FOLLOWER`.

Level: easy.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml` | The fake cluster: the data side. |
| `environments/shop.yaml` | The fake cluster with a proxy in front of it. |
| `rules/orders.yaml` | The rule: which calls, which error, how often. |
| `scenarios/flaky-orders.yaml` | The scenario that arms the rule. |

## Run

```sh
gmq start --env shop --scenario "Flaky orders"
echo "hello" | kcat -b localhost:9092 -P -t orders
```

## What happens

- The broker stores the records. It never lies.
- The proxy in front of it refuses one in five Produce calls.
  A refused call never reaches the broker, so nothing is written for it.
- The UI at `http://localhost:8080` shows every call, with the rule that refused it.
- Nothing is armed unless the command names a scenario.
  Without `--scenario`, the cluster answers truthfully.

*This example shows the target design. It does not run yet.*
