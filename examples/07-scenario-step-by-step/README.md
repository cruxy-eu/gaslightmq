# Scenario step by step

An exact test case in steps.
The consumer reads normally, gets exactly three errors, and must recover.

Level: medium.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml`, `templates/order.json` | A fake cluster with a million orders. |
| `environments/shop.yaml` | The fake cluster with a proxy in front of it. |
| `selectors.yaml`, `rules/orders.yaml` | The rule that refuses order fetches. |
| `scenarios/consumer-survives-leader-churn.yaml` | The three steps, and what ends each one. |

## Run

```sh
gmq run "Consumer survives leader churn" --env shop &
kcat -b localhost:9092 -C -t orders
```

## What happens

1. **Warm up.** The first hundred fetches pass untouched.
2. **Leader churn.** The next three fetches fail with `NOT_LEADER_OR_FOLLOWER`.
   It is three, never four, also when several connections fetch at the same moment.
3. **Recovery.** A hundred more fetches pass, and `gmq run` exits.

The run is the same every time, so it works as a test in CI.
A test asserts on the recording: the rule applied three times, and the consumer kept reading.

*This example shows the target design. It does not run yet.*
