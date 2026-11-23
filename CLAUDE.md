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
- A variant is something its caller decides on, and the rest is text.
  Operations that fail in different ways have an error each: `ReceiveError` and `SendError`.
  Operations that fail in the same ways share one: every call of a client returns `ClientError`.
  No caller writes an arm for a variant that cannot come from what it called.
- A crate that holds a contract holds its errors, one for each thing the caller asks.
  An implementation returns only those, and they hold no error of what an implementation uses.
- A crate turns the errors of what it calls into its own, where it calls them.
  No variant, field or type names the tool, library or mechanism behind a failure: `ConnectionLost`, not `IoError`.
  What a library or the system said is text, in a field `reason`.
  A variant keeps the error of a workspace crate, in a field `error`, when it is the error of a part or the caller reads it: `Refused { error: ResponseError }`.
  No variant copies the variant below it, and no error is made up to fill a field.
- No error type is `#[non_exhaustive]`: a new variant breaks every `match` that decides on it.
- No `dyn Error`: not in code, not in a doc example and not in a test. A doc example returns one error type, by its name.
- An error says everything in one message.
  A variant writes the `error` or the `reason` it holds once, and no error type writes `source()`.
- Nothing tunable is hardcoded.
  A timeout, a size, a count or an interval that someone may want to change is a named property of a configuration, with a default.
- A Kafka client has Kafka's own properties: the name, meaning, unit and default of the Java client's property.
  The doc comment names the property (`request.timeout.ms`), and no property exists for behaviour the code does not have.

The full text of the last ten rules is in [crates.md → Principles](docs/specs/architecture/crates.md#principles).
