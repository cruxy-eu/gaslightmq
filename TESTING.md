# Testing

## Kafka Clients

Integration tests support two Kafka client backends.

By default, tests use a lightweight hand-rolled protocol client.
Run them with:

```sh
cargo test -p gaslightmq
```

To run tests with librdkafka instead,
enable the `rdkafka` feature:

```sh
cargo test -p gaslightmq --features rdkafka
```

librdkafka negotiates the highest protocol version the broker supports,
which is useful for catching protocol encoding bugs
that the default client does not exercise.
