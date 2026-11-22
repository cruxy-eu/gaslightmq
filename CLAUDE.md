# GaslightMQ

## Rules

- Rust edition 2024.
- Use native async traits, not `async-trait`.
- Use workspace dependency versions (`dep.workspace = true`), not inline versions.
- Crate names start with `gaslightmq_` and use underscores, never hyphens. The directory carries the crate's name: `crates/gaslightmq_wire`. A family of crates shares a directory: `crates/gaslightmq_kafka_client/api` is the crate `gaslightmq_kafka_client_api`.
- Tooling that is not product code, such as the workspace checks, lives in `xtask/` and has no row in `crates.md`. `cargo xtask` runs the checks.
- A name says what a thing means, never which crate, module or type it came from, or how it is built:
  `InvalidPartition`, not `Kernel`.
- A value object owns its error, named after it: `TopicName` has `TopicNameError`.
  No crate has one error type for everything in it.
- An error says everything in one message.
  A variant that wraps another error keeps it in a field `error`, writes it once, and returns `None` from `source()`.
- Nothing tunable is hardcoded.
  A timeout, a size, a count or an interval that someone may want to change is a named property of a configuration, with a default.
- A Kafka client has Kafka's own properties: the name, meaning, unit and default of the Java client's property.
  The doc comment names the property (`request.timeout.ms`), and no property exists for behaviour the code does not have.

The full text of the last five rules is in [crates.md → Principles](docs/specs/architecture/crates.md#principles).
