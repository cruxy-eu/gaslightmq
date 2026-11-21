# Virtual records

A giant topic that is already there, without anyone filling it.
The broker makes up each record at the moment a consumer asks for it.

Level: easy.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml` | A fake cluster with an `orders` topic of a hundred million records. |
| `templates/order.json` | What one order looks like. |

## Run

```sh
gmq broker --world shop
kcat -b localhost:9092 -C -t orders -p 0 -o 30000000 -c 3
```

## What happens

- The broker starts at once. It stores nothing.
- The consumer jumps to offset thirty million of partition 0 and prints three orders.
- Every run prints the same three orders.
  A record follows from the world file, the partition and the offset, and from nothing else.
- Change `seed` in `worlds/shop.yaml` and the orders change.

*This example shows the target design. It does not run yet.*
