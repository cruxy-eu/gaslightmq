# GaslightMQ

## Rules

- Rust edition 2024.
- Use native async traits, not `async-trait`.
- Use workspace dependency versions (`dep.workspace = true`), not inline versions.
- Crate names start with `gaslightmq_` and use underscores, never hyphens. The directory carries the crate's name: `crates/gaslightmq_wire`. A family of crates shares a directory: `crates/gaslightmq_kafka_client/api` is the crate `gaslightmq_kafka_client_api`.
