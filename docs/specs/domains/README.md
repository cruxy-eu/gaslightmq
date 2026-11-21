# Domains and concepts

Each document in this directory describes one district from the [town plan](../townplan/README.md)
and the concepts it owns.

| Document | Concepts |
|---|---|
| [Session](session.md) | Session, Identity, Counter, Dice |
| [Targeting](targeting.md) | Field, Condition, Selector, Scope |
| [Lies and events](lies-and-events.md) | Truth, Lie, Event, Preset, Emulation |
| [Rules and triggers](rules.md) | Rule, Action, Trigger, Lifetime, Arming |
| [Scenarios](scenarios.md) | Scenario, Mode, Step, Live layer, Capture, Composition |
| [Observation](observation.md) | Call, Verdict, Explanation, Recording, Index, Facet |
| [World](world.md) | Cluster, Broker, Topic, Partition, Group, Transaction, Generator, Seed |
| [Control surfaces](control-surfaces.md) | View, Pad layout, Layer, Hardware mapping |

## Glossary

| Term | Meaning |
|---|---|
| **Action** | What a rule does when it fires: tell a lie or cause an event. |
| **Backend** | What answers a call after the session: a real cluster (Upstream) or the World (Simulated). |
| **Call** | One request and its response on one session. |
| **Capture** | Turning what someone played live into a Sequence scenario. |
| **Condition** | One clause of a selector: a field, an operator and one or more values. |
| **Counter** | A number a trigger counts with, kept per session, per client or globally. |
| **Dice** | A seeded random stream that decides chance triggers. |
| **Emulation** | The bundle of lies the Upstream backend uses to fake an event. |
| **Event** | A change to the state of the world. |
| **Explanation** | The record of which rules matched, rolled, fired or were shadowed for a call. |
| **Field** | A named property of a connection or a call that selectors can test. |
| **Index** | Facets built automatically from recorded calls. |
| **Layer** | A scope on the pad grid. The same row and column can hold different rules on different layers. |
| **Lie** | Anything the client is told that differs from the truth. |
| **Lifetime** | How long a rule stays armed. |
| **Live layer** | Rules armed by hand on top of the active scenario. Live rules always win. |
| **Mode** | How a scenario resolves conflicts: Live, Chain or Sequence. |
| **Pad layout** | A grid that maps rows to selectors, columns to presets and layers to scopes. |
| **Preset** | A named, reusable lie or event with its parameters, such as "Delay 1 s". |
| **Recording** | A captured stretch of calls across sessions. |
| **Rule** | A named selector, action and trigger, with a lifetime. |
| **Scenario** | A named set of rules with a mode and a seed. |
| **Scope** | A selector over connection fields, shown in the UI as *who* a rule applies to. |
| **Selector** | A named predicate over fields that describes a set of calls. |
| **Session** | The runtime of one client connection. |
| **Step** | One stage of a Sequence scenario. |
| **Trigger** | What decides that a rule fires: chance, count, time, signal or manual. |
| **Truth** | What the backend would have answered without GaslightMQ interfering. |
| **Verdict** | The outcome of evaluating one call: pass, or the lies that apply. |
| **World** | The state of a simulated cluster. |
