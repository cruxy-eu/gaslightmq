# Proxy in front of real Kafka

The same kind of lies, in front of a real cluster.
The password of the cluster stays in a file outside git.

Level: medium.

## Files

| File | Holds |
|---|---|
| `environments/staging.yaml` | Where the real cluster is, how to log in, and where the proxy listens. |
| `rules/orders.yaml` | A rule that loses one in ten acknowledgements. |
| `scenarios/lost-acknowledgements.yaml` | The scenario that arms the rule. |
| `.gitignore` | Keeps `secrets/` and recordings out of git. |

## Run

Put the password and the CA certificate in place first:

```sh
mkdir secrets
echo -n "the-password" > secrets/kafka-password
cp /path/to/ca.pem secrets/kafka-ca.pem
```

```sh
gmq start --env staging --scenario "Lost acknowledgements"
```

Point your application at `localhost:9092` instead of the real cluster.

## What happens

- The proxy finds the brokers of the real cluster and opens one port for each.
- One in ten Produce calls on the order topics reaches the real cluster,
  is written there, and is reported to the client as a timeout.
  A producer that retries now writes duplicates, unless it is idempotent.
- The environment file is safe to commit. It holds the path of the password, not the password.

To try a proxy without any files:

```sh
gmq proxy --upstream kafka-1.staging.internal:9092
```

*This example shows the target design. It does not run yet.*
