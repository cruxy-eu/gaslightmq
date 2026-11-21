# Domains and concepts

Each document in this directory describes one district from the [town plan](../townplan/README.md)
and the concepts it owns.

| Document | Concepts |
|---|---|
| [Session](session.md) | Session, Identity, Client context |
| [Targeting](targeting.md) | Field, Learned field, Condition, Selector, Scope |
| [Lies and events](lies-and-events.md) | Truth, Lie, Stage, Target, Event, Consequence, Preset |
| [Rules and triggers](rules.md) | Rule, Action, Trigger, Counter, Dice, Lifetime, Budget, Arming |
| [Scenarios](scenarios.md) | Scenario, Mode, Step, Live set, Capture |
| [Observation](observation.md) | Call, Verdict, Explanation, Recording, Index, Facet |
| [World](world.md) | Cluster, Broker, Topic, Partition, Group, Producer, Transaction, Generator, Seeding |
| [Control surfaces](control-surfaces.md) | View, Pad layout, Layer, Hardware mapping |

Backend, Capability, Overlay and Emulation belong to [backends](../architecture/backends.md).
The Clock belongs to the [engine](../architecture/README.md#clock).

## Glossary

| Term | Meaning |
|---|---|
| **Action** | What a rule does when it fires: tell a lie or cause an event. |
| **Backend** | What answers a call after the session: a real cluster (Upstream) or the World (Simulated). |
| **Budget** | A number of matches or firings that all sessions share and take from atomically. |
| **Call** | One request and its response on one session. |
| **Capture** | Turning what someone played live into a Sequence scenario. |
| **Client context** | What the engine learned about one client instance across its connections, such as its group. |
| **Clock** | The engine's time: virtual on the Simulated backend, the wall clock on the Upstream backend. |
| **Condition** | One clause of a selector: a field, an operator and one or more values. |
| **Consequence** | What the engine adds to an event, such as leaderless partitions when a broker goes down. |
| **Counter** | The number of calls a rule matched, kept per rule or per session. |
| **Dice** | Seeded rolls for chance triggers, derived from the seed, the rule and the match number. |
| **Emulation** | How the Upstream backend fakes an event inside its overlay. |
| **Event** | A change to the state of the world. |
| **Explanation** | The record of which rules matched, rolled, fired, applied, or were shadowed or pre-empted for a call. |
| **Field** | A named property of a connection or a call that selectors can test. |
| **Index** | Facets built automatically from recorded calls. |
| **Layer** | A scope on the pad grid. The same row and column can hold different rules on different layers. |
| **Learned field** | A field whose value comes from the client context when the request does not carry it. |
| **Lie** | Anything the client is told that differs from the truth. |
| **Lifetime** | How long a rule stays armed. |
| **Live rule** | A rule that only exists in the engine's memory, until someone keeps it. |
| **Live set** | Rules armed by hand on top of the active scenario. Live rules win conflicts with scenario rules. |
| **Mode** | How a scenario resolves conflicts: Live (most recently armed), Chain (order) or Sequence (order, with steps). |
| **Overlay** | The Upstream backend's layer over the real cluster: address translation and emulated events. Part of the truth. |
| **Pad layout** | A grid that maps rows to selectors, columns to presets and layers to scopes. |
| **Pre-empted** | A lie that fired but had nothing left to change, because another lie ended the call or replaced its target. |
| **Preset** | A named, reusable lie or event with its parameters, such as "Delay 1 s". |
| **Recording** | A captured stretch of calls across sessions. |
| **Rule** | A named selector, action and trigger, with a lifetime. |
| **Shadowed** | A lie that fired but lost a conflict to a rule with a higher priority. |
| **Stage** | When a lie acts: before forwarding, or after the response. |
| **Scenario** | A named set of rules with a mode and a seed. |
| **Scope** | A selector over connection fields, shown in the UI as *who* a rule applies to. |
| **Selector** | A named predicate over fields that describes a set of calls. |
| **Session** | The runtime of one client connection. |
| **Step** | One part of a Sequence scenario. It arms rules and ends on a condition. |
| **Target** | The part of a call a lie changes: the partitions the selector matched, or the whole call. |
| **Trigger** | What decides that a rule fires: chance, count, time or manual. |
| **Truth** | What the backend answers when no lie applies. On the proxy, that includes the overlay. |
| **Verdict** | The outcome of evaluating one call: pass, or the lies that apply. |
| **World** | The state of a simulated cluster. |
