# Several environments

A fake cluster and a proxy in front of a real cluster, side by side in one process.
One UI shows both.

Level: complex.

## Files

| File | Holds |
|---|---|
| `gaslight.yaml` | The environments that start together. |
| `environments/shop.yaml` | A fake cluster with a proxy, on ports 29092 and up. |
| `environments/staging.yaml` | A proxy in front of a real cluster, on ports 9092 and up. |
| `worlds/shop.yaml`, `templates/order.json` | The data of the fake cluster. |
| `rules/orders.yaml`, `scenarios/flaky-orders.yaml` | One rule and one scenario, shared by both. |

## Run

```sh
gmq start
```

## What happens

- One process listens on both sets of ports.
  A client on `localhost:29092` talks to the fake cluster,
  and a client on `localhost:9092` talks to the real one.
- The UI at `http://localhost:7092` has an environment switcher.
  Switching changes what you look at. Nothing restarts.
- The rule and the scenario exist once, and both environments can use them.
- Each environment has its own state.
  Activate `flaky-orders` in `shop`, and `staging` still answers truthfully.
- The World view only appears for `shop`, because only `shop` has a fake cluster.

*This example shows the target design. It does not run yet.*
