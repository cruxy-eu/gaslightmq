# A broker node goes down

An event that changes the fake cluster.
On the fiftieth fetch, node 2 fails, and its partitions get new leaders.

Level: medium.

## Files

| File | Holds |
|---|---|
| `worlds/shop.yaml`, `templates/order.json` | A fake cluster with three nodes and six partitions. |
| `environments/shop.yaml` | The fake cluster with a proxy in front of it. |
| `rules/nodes.yaml` | The rule that causes the event. |
| `scenarios/node-failure.yaml` | The scenario that arms the rule. |

## Run

```sh
gmq start --env shop --scenario "Node failure"
kcat -b localhost:9092 -C -t orders
kcat -b localhost:9092 -L
```

## What happens

- The first forty-nine fetches pass. The fiftieth causes the event, and passes too.
- From then on, node 2 refuses connections, and Metadata shows new leaders for its partitions.
- This is not a lie.
  The fake cluster really changed, so every client sees the same new state.
- The World view in the UI shows the event and each leader that moved.
- Set `failover: false` and the partitions of node 2 have no leader at all.

*This example shows the target design. It does not run yet.*
