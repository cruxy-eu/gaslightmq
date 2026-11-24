# Delays for one client

A slow cluster for one client, and a normal one for everyone else.
The example also introduces named selectors and presets.

Level: easy.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml`, `templates/order.json` | A fake cluster with a million orders. |
| `environments/shop.yaml` | The fake cluster with a proxy in front of it. |
| `selectors.yaml` | `order-fetches`: which calls. |
| `presets.yaml` | `delay-1s`: which lie. |
| `rules/orders.yaml` | The rule that joins the two, for the client `analytics-etl` only. |
| `scenarios/slow-analytics.yaml` | The scenario that arms the rule. |

## Run

```sh
gmq start --env shop --scenario slow-analytics
kcat -b localhost:9092 -C -t orders -X client.id=analytics-etl
kcat -b localhost:9092 -C -t orders -X client.id=billing
```

## What happens

- The first consumer waits about a second for every Fetch.
- The second consumer reads at full speed.
- Both read the same orders. A delay changes when the answer arrives, not what it says.

*This example shows the target design. It does not run yet.*
