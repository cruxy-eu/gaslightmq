# GaslightMQ

## Rules

- Rust edition 2024.
- Use native async traits, not `async-trait`.
- Use workspace dependency versions (`dep.workspace = true`), not inline versions.
- Crate names start with `gmq_` and use underscores, never hyphens. The directory carries the crate's name: `crates/gmq_wire`.
