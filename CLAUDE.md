# GaslightMQ

## Rules

- Rust edition 2024.
- Use native async traits, not `async-trait`.
- Use workspace dependency versions (`dep.workspace = true`), not inline versions.
- Crate names start with `gaslightmq_` and use underscores, never hyphens. The directory carries the crate's name: `crates/gaslightmq_wire`. A family of crates shares a directory: `crates/gaslightmq_kafka_client/api` is the crate `gaslightmq_kafka_client_api`.
- Tooling that is not product code, such as the workspace checks, lives in `xtask/` and has no row in `crates.md`. `cargo xtask` runs the checks.
- A name says what a thing means, never which crate, module or type it came from, or how it is built:
  `InvalidPartition`, not `Kernel`.
- A value object that checks its value owns its error, named after it: `TopicName` has `TopicNameError`.
  A type that checks nothing has no error type.
- A crate has its own error for the rest of its public API, named after its subject: `WireError`, `LieError`.
  No handle, step or noun has an error of its own. A crate of value objects alone has no API error.
- A crate turns the errors of what it calls into its own error, where it calls them.
  No variant, field or type names the tool, library or mechanism behind a failure: `ConnectionLost`, not `IoError`.
  What the other side said is text, in a field `reason`.
  Only the error of a value object is kept as a type, in a field `error`.
- No `dyn Error`: not in code, not in a doc example and not in a test. A doc example returns one error type, by its name.
- An error says everything in one message.
  A variant writes the `error` or the `reason` it holds once, and no error type writes `source()`.
- Nothing tunable is hardcoded.
  A timeout, a size, a count or an interval that someone may want to change is a named property of a configuration, with a default.
- A Kafka client has Kafka's own properties: the name, meaning, unit and default of the Java client's property.
  The doc comment names the property (`request.timeout.ms`), and no property exists for behaviour the code does not have.

The full text of the last eight rules is in [crates.md → Principles](docs/specs/architecture/crates.md#principles).
