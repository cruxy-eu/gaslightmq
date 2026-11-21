# Live traffic

A topic that keeps receiving new records, as if a producer were running.

Level: easy.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml` | A fake cluster with an `orders` topic that grows by five orders per second. |
| `templates/order.json` | What one order looks like. |

## Run

```sh
gmq broker --world shop
kcat -b localhost:9092 -C -t orders -o end
```

## What happens

- The consumer starts at the end of the topic and prints about five new orders per second.
- The orders are still the same in every run: the order at an offset never changes.
- The broker runs on its own clock.
  A test can speed the clock up, so an hour of traffic takes a second.

*This example shows the target design. It does not run yet.*
