# GaslightMQ design specs

GaslightMQ makes Kafka clients believe things that are not true.
It sits between a client and a cluster, or plays the cluster itself,
and decides per call what the client gets to see.

These specs describe the target design.
They are reference material: read them in any order and jump between them.
They describe where the system is heading, not how the current code works.

## Contents

| Directory | What it covers |
|---|---|
| [`townplan/`](townplan/README.md) | The districts of the system, what each one owns and how they depend on each other. |
| [`domains/`](domains/README.md) | The concepts in each district: sessions, selectors, lies, events, rules, triggers, scenarios, the world, recordings and control surfaces. |
| [`architecture/`](architecture/README.md) | The target runtime: the engine, the request pipeline, backends, storage and the control plane. |
| [`open-questions.md`](open-questions.md) | Decisions that are still open. |

## The idea in one paragraph

A **Selector** describes *which calls* a rule targets.
A **Lie** describes *what the client is told* instead of the truth.
An **Event** describes *how the world changes*.
A **Rule** binds a selector to a lie or an event, and a **Trigger** decides *when* it fires.
A **Scenario** groups rules and picks a **Mode** that resolves conflicts between them.
Every connection runs in its own **Session**, which holds identity, dice and counters.
Behind the session sits a **Backend**: a real cluster (proxy) or a simulated one (fake broker).
The same UI, the same rules and the same scenarios work on both.

## Design principles

- **One language for seeing and breaking.**
  The selector that filters recorded traffic is the same selector that targets a rule.
- **Names are the interface.**
  Users name selectors, rules and scenarios.
  Things reference each other by name.
  There are no ticket-style identifiers.
- **Files are the source of truth.**
  Definitions live in a plain-text workspace that works in git and in CI.
  Live state stays in memory until someone saves it.
- **Explain every verdict.**
  Every call records which rules matched, which fired and which were shadowed.
- **Reproducible by default.**
  Every source of randomness derives from a seed.
- **Backend-portable scenarios.**
  A scenario written in events runs on the proxy and on the fake broker.

## Related material

- [`mockups/proxy-ui.html`](../../mockups/proxy-ui.html): interactive UI mockups.
  Open the file in a browser.
