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
| [`architecture/`](architecture/README.md) | The target runtime: the engine, the request pipeline, backends, storage, the control plane and the crate layout. |
| [`open-questions.md`](open-questions.md) | Decisions that are still open, and the ones that were settled. |

## The idea in one paragraph

A **Selector** describes *which calls* a rule targets.
A **Lie** describes *what the client is told* instead of the truth.
An **Event** describes *how the world changes*.
A **Rule** binds a selector to a lie or an event, and a **Trigger** decides *when* it fires.
Lies that do not conflict all apply.
A **Scenario** groups rules and picks a **Mode** that resolves the conflicts between them.
Every connection runs in its own **Session**, which holds identity and counters and rolls seeded dice.
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
- **Honest about side effects.**
  A lie names its stage, so it is clear what really happened in the cluster.
- **Backend-portable scenarios.**
  A scenario written in events runs on the proxy and on the fake broker.

## First version

The specs describe the target.
The first version leaves these parts out:

| Left out | Instead |
|---|---|
| Triggers that react to other rules or events | A [Sequence](domains/scenarios.md#sequence) step ends on calls or firings, and the next step arms the reaction. |
| Counters per client | Counters per rule or per session. See [counters](domains/rules.md#counters). |
| Emulating "leader moves" and "coordinator moves" on the proxy | The *Leader flap* lie preset. See [capabilities](architecture/backends.md#capabilities). |
| Capturing parameter changes on an armed rule | Capture keeps the parameters from the moment of arming. See [capture](domains/scenarios.md#capture). |
| A pass or fail result from `gmq run` | Tests assert on recordings. See [tests](architecture/control-plane.md#tests). |
| Transactions on the fake broker | Idempotent producers without transactions. See [Simulated](architecture/backends.md#simulated). |
| A hardware bridge outside the browser | Web MIDI in the UI. See [hardware mapping](domains/control-surfaces.md#hardware-mapping). |

## Related material

- [`mockups/proxy-ui-v2.html`](../../mockups/proxy-ui-v2.html): interactive UI mockups.
  Open the file in a browser.
