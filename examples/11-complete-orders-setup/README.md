# Complete orders setup

Everything from the other examples together, as one setup you keep in git.
It adds the files that run it: Docker Compose, Kubernetes and a CI job.

Level: complex.
Start with the [smaller examples](../README.md) if a file here is unclear.

*This example shows the target design. It does not run yet.*

## What you get

- A **fake cluster** with order topics that already hold data.
  No producer runs anywhere.
- A **schema registry** that serves the Avro schema of an order.
- A **proxy** in front of the fake cluster that makes order calls fail now and then.
- A second environment that puts the same rules in front of a **real cluster**.

The broker owns the data, and the proxy owns the lies.

## The files

```
11-complete-orders-setup/
├── gaslight/                    # the workspace: everything GaslightMQ knows
│   ├── gaslight.yaml            # workspace settings
│   ├── environments/
│   │   ├── orders-sim.yaml      # fake cluster + schema registry + proxy
│   │   └── staging-proxy.yaml   # proxy in front of a real cluster
│   ├── worlds/
│   │   └── orders-sim.yaml      # the fake cluster: topics and their records
│   ├── schemas/
│   │   └── order.avsc           # what an order looks like
│   ├── templates/
│   │   └── order.json           # how the broker fills an order
│   ├── data/
│   │   └── payments.jsonl       # a fixed set of records
│   ├── selectors.yaml           # which calls
│   ├── presets.yaml             # which lies
│   ├── rules/
│   │   └── orders.yaml          # selector + lie + trigger
│   └── scenarios/
│       ├── flaky-orders.yaml
│       └── consumer-survives-leader-churn.yaml
├── docker-compose.yaml          # run it next to your application
├── kubernetes/                  # run it in a cluster
└── ci/                          # run a scenario in a pipeline
```

## Where the data comes from

Open `gaslight/worlds/orders-sim.yaml`.
Each topic says where its records come from.

| Topic | Records |
|---|---|
| `orders.eu` | 100 million existing orders, and 50 new ones per second. The broker makes each one up from `templates/order.json` when a consumer asks for it. |
| `orders.us` | 5 million existing orders, made up from the schema alone. |
| `payments.settled` | Exactly the records in `data/payments.jsonl`. |
| `audit.log` | Empty. It holds what your application produces. |

The same partition and offset always give the same order.
Nothing is stored, so the fake cluster starts at once.

## Where the errors come from

Open `gaslight/rules/orders.yaml` and `gaslight/scenarios/flaky-orders.yaml`.
The rule `orders-sometimes-fail` refuses one in five Produce calls on the order topics.
The scenario `flaky-orders` arms that rule and two others.

Nothing is armed until someone says so.
The commands below name the scenario to activate at start.

## Run it with Docker Compose

```sh
docker compose up
```

Your application connects to `gaslightmq:9092` for Kafka
and to `http://gaslightmq:8081` for the schema registry.
The UI is at `http://localhost:7092`.

## Run it on Kubernetes

```sh
kubectl apply -k kubernetes/
```

The pod clones the workspace from git and mounts it read-only.
The upstream password comes from a Kubernetes secret, mounted as a file.
See `gaslight/environments/staging-proxy.yaml` for the reference to it.

## Run a scenario in CI

See `ci/github-actions.yaml`.
The job starts the fake cluster, activates a scenario and runs your tests against it.
`--advertise localhost` overrides the environment file,
because the tests run on the runner and not inside the Compose network.
