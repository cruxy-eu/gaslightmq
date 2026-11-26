# Domains and concepts

Each document in this directory describes one district from the [town plan](../townplan/README.md)
and the concepts it owns.

| Document | Concepts |
|---|---|
| [Session](session.md) | Session, Identity, Client context |
| [Targeting](targeting.md) | Field, Learned field, Condition, Selector, Scope |
| [Lies and incidents](lies-and-incidents.md) | Truth, Lie, Stage, Target, Incident, Consequence, Preset |
| [Rules and triggers](rules.md) | Rule, Action, Trigger, Counter, Dice, Lifetime, Budget, Arming |
| [Scenarios](scenarios.md) | Scenario, Mode, Step, Live set, Capture |
| [Observation](observation.md) | Call, Verdict, Explanation, Recording, Journal, Index, Facet |
| [World](world.md) | Cluster, Broker node, Topic, Partition, Group, Producer, Transaction, Record source, Virtual record, Template, Seeding |
| [Control surfaces](control-surfaces.md) | View, Pad layout, Layer |

Backend, Capability, Overlay and Emulation belong to [backends](../architecture/backends.md).
Proxy, Broker and Host belong to the [architecture](../architecture/README.md#instances-and-processes).
The Clock belongs to the [engine](../architecture/README.md#clock).
Workspace, Environment, Secret and Scratch file belong to [storage](../architecture/storage.md).

## Glossary

| Term | Meaning |
|---|---|
| **Action** | What a rule does when it fires: tell a lie or cause an incident. |
| **Backend** | What answers a call after a proxy's session: a real cluster (Upstream) or a broker in the same process (Simulated). |
| **Broker** | A simulated Kafka cluster. It holds a World, answers truthfully and never lies. One broker simulates several broker nodes. |
| **Broker node** | One Kafka broker inside a cluster, real or simulated. Incidents such as "broker goes down" are about a node. |
| **Budget** | A number of matches or firings that all sessions share and take from atomically. |
| **Call** | One request and its response on one session. |
| **Capture** | Turning what someone played live into a Sequence scenario. |
| **Client context** | What the engine learned about one client instance across its connections, such as its group. |
| **Clock** | The engine's time: virtual on the Simulated backend, the wall clock on the Upstream backend. |
| **Condition** | One clause of a selector: a field, an operator and one or more values. |
| **Consequence** | What a backend adds to an incident, such as leaderless partitions when a broker goes down. |
| **Counter** | The number of calls a rule matched, kept per rule or per session. |
| **Dice** | Seeded rolls for chance triggers, derived from the seed, the rule and the match number. |
| **Effect** | What lies do to a call: end it, refuse it, rewrite it or make it wait. One lie asks for an effect, and the lies that apply at one stage have a stage effect together. |
| **Emulation** | How the Upstream backend fakes an incident inside its overlay. |
| **Environment** | A named definition of instances that run together: a proxy, a broker, or a proxy in front of a broker. A process runs one or several. |
| **Explanation** | The record of which rules matched, rolled, fired, applied, or were shadowed or pre-empted for a call. |
| **Field** | A named property of a connection or a call that selectors can test. |
| **Incident** | A change to the state of the world. |
| **Index** | Facets built automatically from recorded calls. |
| **Journal** | The record of what changed in an environment: arms, disarms, parameter changes, definitions, steps and incidents, each with the surface it came from. |
| **Layer** | A scope on the pad grid. The same row and column can hold different rules on different layers. |
| **Learned field** | A field whose value comes from the client context when the request does not carry it. |
| **Lie** | Anything the client is told that differs from the truth. |
| **Lifetime** | How long a rule stays armed. |
| **Live rule** | A rule that only exists in the engine's memory, until someone keeps it. |
| **Live set** | Rules armed by hand on top of the active scenario. Live rules win conflicts with scenario rules. |
| **Mode** | How a scenario resolves conflicts: Live (most recently armed), Chain (order) or Sequence (order, with steps). |
| **Name** | The identity of a definition: a slug, unique within its kind. Every reference uses it, and it never changes. |
| **Overlay** | The Upstream backend's layer over the real cluster: address translation and emulated incidents. Part of the truth. |
| **Pad layout** | A grid that maps rows to selectors, columns to presets and layers to scopes. |
| **Pre-empted** | A lie that fired but had nothing left to change, because another lie ended the call or replaced its target. |
| **Proxy** | The instance that stands in front of a cluster or a broker. It runs sessions, rules and scenarios, and it is the only place where lies happen. |
| **Preset** | A named, reusable lie or incident with its parameters, such as `delay-1s`. |
| **Priority** | Where a rule stands among the armed rules. When two lies conflict, the lie of the rule with the higher priority wins. |
| **Recording** | A captured stretch of calls across sessions. |
| **Revision** | A number that says which state of a definition, of a rule in an environment or of a scenario that runs someone saw. It rises with every change, and a change can name the one it expects. |
| **Rule** | A named selector, action and trigger, with a lifetime. |
| **Shadowed** | A lie that fired but lost a conflict to a rule with a higher priority. |
| **Stage** | When a lie acts: before forwarding, or after the response. |
| **Scenario** | A named set of rules with a mode and a seed. |
| **Schema registry** | The service where clients look up record schemas. Built in next to a broker, or the real one of a real cluster. |
| **Scratch file** | A file outside git that keeps live rules across a crash. They load disarmed. |
| **Secret reference** | The path of a file, or the name of an environment variable, that holds a secret. Definitions hold references, never values. |
| **Scope** | A selector over connection fields, shown in the UI as *who* a rule applies to. |
| **Selector** | A named predicate over fields that describes a set of calls. |
| **Session** | The runtime of one client connection: its transport and its pipeline. |
| **Step** | One part of a Sequence scenario. It arms rules and ends on a condition. |
| **Surface** | Where a change came from: a pad, ⌘K, the UI, `gmq`, a test, a step, a lifetime or a file. The journal records it. |
| **Template** | A file that describes one record of a topic, with placeholders the broker fills per partition and offset. |
| **Title** | What a definition is for, in free text. Optional, and nothing references it. |
| **Target** | The part of a call a lie changes: the partitions the selector matched, or the whole call. |
| **Trigger** | What decides that a rule fires: chance, count, time or manual. |
| **Truth** | What the backend answers when no lie applies. On the proxy, that includes the overlay. |
| **Undo** | Taking back a change that someone made in the same browser tab. It reverses what someone set, and never what happened. |
| **UUID** | What tells one definition from every other in one run of a process. Live state: no file holds it. |
| **Virtual record** | A record the broker makes up when a consumer asks. A pure function of the world definition, the partition and the offset. |
| **Verdict** | The outcome of evaluating one call: pass, or the lies that apply. |
| **World** | The state of a simulated cluster. |
