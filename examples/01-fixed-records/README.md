# Fixed records

A topic that contains exactly the records in a file.
No producer runs anywhere.

Level: easy.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml` | A fake cluster with one topic. |
| `data/payments.jsonl` | The records of that topic, one per line. |

## Run

```sh
gmq broker --world shop
kcat -b localhost:9092 -C -t payments.settled -e
```

## What happens

- The consumer prints the three payments from the file and stops at the end of the topic.
- Every run prints the same records at the same offsets.

*This example shows the target design. It does not run yet.*
