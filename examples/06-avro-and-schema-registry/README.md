# Avro and schema registry

Avro records with a schema, and a schema registry that consumers read the schema from.

Level: medium.

## Files

| File | Holds |
|---|---|
| `schemas/order.avsc` | The Avro schema of an order. |
| `worlds/shop.yaml` | A fake cluster whose `orders` topic uses that schema. |
| `environments/shop.yaml` | The fake cluster, and a built-in schema registry next to it. |

## Run

```sh
gmq start --env shop
curl http://localhost:8081/subjects
kcat -b localhost:9092 -C -t orders -c 3 -s value=avro -r http://localhost:8081
```

## What happens

- The registry lists the subject `orders-value`.
- The consumer fetches Avro records, looks the schema up in the registry and prints three orders.
- The world file names no template, so the broker makes up every field from its type in the schema.
  Add a template to control the values, as in `02-virtual-records`.
- The schema gets the same ID in every run, so the bytes of a record never change.

*This example shows the target design. It does not run yet.*
