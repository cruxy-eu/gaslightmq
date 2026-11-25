<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo/gaslightmq-dark-theme.svg">
  <img alt="GaslightMQ" src="docs/assets/logo/gaslightmq-light-theme.svg" width="240" height="240">
</picture>

<h3>A Kafka proxy that lies to your clients on purpose.</h3>

<p>Find out how your producers and consumers handle errors, delays and outages<br>before production finds out for you.</p>

[![CI](https://img.shields.io/github/actions/workflow/status/cruxy-eu/gaslightmq/ci.yml?branch=main&label=CI&style=flat-square&labelColor=16171a)](https://github.com/cruxy-eu/gaslightmq/actions/workflows/ci.yml)
[![Licence: MIT OR Apache-2.0](https://img.shields.io/badge/licence-MIT%20OR%20Apache--2.0-7170ff?style=flat-square&labelColor=16171a)](#licence)
[![Rust 1.100.0](https://img.shields.io/badge/rust-1.100.0-7170ff?style=flat-square&logo=rust&logoColor=white&labelColor=16171a)](rust-toolchain.toml)
[![Edition 2024](https://img.shields.io/badge/edition-2024-7170ff?style=flat-square&labelColor=16171a)](Cargo.toml)
[![Status: in development](https://img.shields.io/badge/status-in%20development-f2c94c?style=flat-square&labelColor=16171a)](#status)

</div>

---

GaslightMQ stands between your Kafka clients and the cluster,
and decides per call what each client gets to see.
It refuses one Produce in five with `NOT_LEADER_OR_FOLLOWER`,
makes one consumer's Fetches slow,
or hangs up on a client,
while every other client sees the real cluster.

- **Selectors** say which calls: an API, a topic, a client, a group.
- **Lies** say what the client hears instead of the truth: an error, a delay, silence or a hang-up.
- **Incidents** change the world for everyone: a broker node goes down, and leaders move.
- **Rules** bind a selector to a lie or an incident, and a trigger decides when one fires: by chance, by count, by time or by hand.
- **Pads** put rules on a grid you play live, and **⌘K** turns a typed sentence into a rule.
- **Scenarios** tell a failure story in steps, with a seed, so the dice roll the same in every run.
- **Recordings** keep every call, with which rules matched, fired or were shadowed.

The proxy stands in front of a real cluster today.
It will also stand in front of GaslightMQ's own simulated broker, with the same rules and scenarios.

## In ten seconds

A selector names the calls, a preset names the lie, and a rule joins the two:

```yaml
name: order-producers
match:
  api: Produce
  topic: "orders.*"
```

```yaml
name: delay-1s
lie: timing
delay: 1s
jitter: 200ms
```

```yaml
name: slow-order-producers
title: Slow down order producers
selector: order-producers
scope:
  client.id: order-ingest
action: delay-1s
trigger:
  chance: 40%
```

Four in ten Produce calls of `order-ingest` to the `orders.*` topics wait about a second.
Every other call reaches the cluster untouched.

Or type a sentence in the ⌘K palette of the UI, and get a live rule:

```text
delay produce orders.* 750ms 40% order-ingest
```

It delays 40% of the Produce calls of `order-ingest` to the `orders.*` topics by 750 ms.

## Status

GaslightMQ is in development, and the crates arrive in [build order](docs/specs/architecture/crates.md#build-order).

| Step | Gives | State |
|---|---|---|
| 1–3 | Rules, the pipeline and a proxy that lies in front of a real cluster: refusals, delays, throttles, silence and hang-ups. | Done |
| 4 | The control plane over HTTP and WebSocket, and the web UI with live traffic, rules, pads and ⌘K. | Control plane done, UI in progress |
| 5 | Definitions and scenarios in files, recordings, the `gmq` command and tests in CI. | Planned |
| 6 | The simulated broker, alone and behind a proxy. | Planned |
| 7 | Lies about leaders, records, offsets and configs, and transactions. | Planned |

The [examples](examples/README.md) and the [specs](docs/specs/README.md) describe the target design, so most of them do not run yet.
The [UI mockups](mockups/proxy-ui-v2.html) show where the UI is heading: download the file and open it in a browser.

## Contributing

You need [Rust](https://rustup.rs) and [just](https://github.com/casey/just).
`rustup` installs the toolchain that [`rust-toolchain.toml`](rust-toolchain.toml) pins.

| Command | Does |
|---|---|
| `just pre-commit` | Formatting, the workspace checks and clippy. Run it before every commit. |
| `just test` | Every test. |
| `just test-kafka` | Only the tests against real Kafka brokers. They need Docker. |
| `just ui` | Builds the web UI and starts a local instance on `127.0.0.1:8080`. |
| `just ci` | Everything CI runs. |

[`CLAUDE.md`](CLAUDE.md) holds the rules of the codebase,
and [crates.md § Principles](docs/specs/architecture/crates.md#principles) explains them.

## Read more

| Spec | Covers |
|---|---|
| [Design specs](docs/specs/README.md) | The idea in one paragraph, and the design principles. |
| [Town plan](docs/specs/townplan/README.md) | The districts of the system and how they depend on each other. |
| [Domains](docs/specs/domains/README.md) | Sessions, selectors, lies, incidents, rules, scenarios, the world and recordings, with a glossary. |
| [Architecture](docs/specs/architecture/README.md) | The engine, the request pipeline, backends, storage, the control plane and the crates. |
| [Examples](examples/README.md) | Eleven workspaces, from a topic with fixed records to a complete setup. |

## Licence

GaslightMQ is licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT licence](LICENSE-MIT), at your option.
