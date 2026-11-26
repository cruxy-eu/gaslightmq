# Observation

Observation shows what happened and why.
It is downstream of everything:
sessions emit events, and Observation stores, indexes and shows them.
Nothing depends on Observation.

## Call

A call is one request and its response on one session.

A call has a [call ID](../architecture/identity.md#call-id): one number.
An environment counts its calls from 1, across all its connections, so a higher ID is a later call.
The ID is written as the number alone, such as `42`.
The form `#7` is a connection number, and a call ID never looks like one.

| Part | Meaning |
|---|---|
| **Session** | The session the call ran on, with its identity. The number of its connection is no part of the call ID. |
| **Request** | The decoded request, with every [field](targeting.md#fields). |
| **Upstream** | What the real cluster answered, when the Upstream backend's [overlay](../architecture/backends.md#overlay) changed it. |
| **Truth** | What the backend answered, if the request reached it. |
| **Delivered** | What the client actually received. |
| **Verdict** | Pass, or the lies that applied. |
| **Explanation** | Why the verdict came out this way. |
| **Timing** | When the call arrived, how long the backend took, how long the client waited. |

Truth and delivered only differ when a lie applied.
The UI can show the two side by side.
Some lies leave them equal: a delay changes when the answer arrives, not what it says.

The overlay is not a lie.
Address translation and emulated incidents are part of the truth,
so a Metadata call that no rule touched has the verdict Pass.

## Verdict

The verdict is the outcome of evaluating the call:

- **Pass**: no rule fired, or no rule that fired changed anything.
- **Lied**: one or more lies applied.
  The verdict lists them, such as `+750 ms` and `NOT_LEADER_OR_FOLLOWER on orders.eu/3`.

Incidents a call fired are part of the explanation, not the verdict,
because they change the world and not this call.

## Explanation

An explanation records every rule the call touched:

| Status | Meaning |
|---|---|
| **Matched** | The selector matched, with the target it matched. |
| **Rolled** | The trigger was evaluated, with its result: the match number, the die roll, the time window, the budget. |
| **Fired** | The trigger said yes. |
| **Applied** | The action took effect. |
| **Shadowed** | The rule fired, but a rule with a higher priority won the conflict. The explanation names the winner and the target. |
| **Pre-empted** | The rule had nothing left to change: a terminal lie ended the call, or a refusal replaced the rule's partition. The explanation names the lie that got there first. |

A rule that waits for the truth is also pre-empted
when a lie ends the call before forwarding.

A lie can fire and still not be told, because GaslightMQ itself failed:
the build has no behaviour for its kind, or the response has no place for its error code.
The explanation then says that the rule fired, that its lie was not told, and why.
Such a lie is not part of the verdict, and it does not count as a firing.

Without explanations, any combination rule feels like magic.
With them, the UI answers *why did this call fail?*
and *why did my rule not fire?*

Shadowed and pre-empted rules do not count as firings for their [lifetime](rules.md#lifetime).
A rule that is shadowed on one partition and applies on another counts as one firing.

## Recording

A recording is a captured stretch of calls across all sessions.
Someone starts and stops it.

A recording keeps:

- every session with its identity, open and close times,
- every call with its request, truth, delivered response, verdict and explanation,
- the raw frames, unless the recording is metadata-only,
- the [journal](#journal) entries between the calls.

A **metadata-only** recording drops record keys, values and headers,
for traffic that must not leave the machine.
It is the default on the Upstream backend.
See [privacy](../architecture/storage.md#privacy).

No recording keeps a client's credentials.
The engine blanks the payload of `SaslAuthenticate` in every recording.

## Journal

The journal records what changed in an environment, next to what happened on the wire.
Each environment has a journal of its own.

| Entry | Records |
|---|---|
| Rule armed or disarmed | The rule. |
| Parameter changed | The live rule, the parameter, the old and the new value. A turn of an encoder changes one parameter. |
| Rule fired by hand | The rule. |
| Live rule made, changed, kept or deleted | The rule. *Changed* is an update that replaces the definition as a whole. A rule that is made armed has two entries with one revision: made, and armed. An armed rule that is deleted has two as well: disarmed, and deleted. |
| Definition created, changed or deleted | The definition and its kind. |
| Scenario activated or deactivated, step advanced | The scenario, and the step by its name. |
| Incident applied or emulated | The incident and its consequences. |
| Recording stopped by itself | Why: the recorder could not keep up, or it could not write. |

A definition belongs to the workspace and not to one environment.
A change of a definition therefore enters the journal of every environment that runs, and no journal when none runs.

Every entry carries:

- its [ID](../architecture/identity.md#journal-entry-id), a number that the environment counts from 1,
- the time, on the clock of the environment, which is the clock of its [calls](#call),
- the [revision](../architecture/identity.md#revision) that the change left, when the change has one,
- the surface the change came from,
- the cause, when the change takes back or repeats an earlier one: `undo` or `redo`.

A firing and an incident are what happens, and no change of what someone set, so their entries have no revision.

An entry names a definition by its name.
A name can go and come back, so the journal records every creation and every deletion of a definition.
The entries between a creation and the next deletion are the history of [one definition](../architecture/identity.md#two-definitions-of-one-name), and the history of one rule never runs into the history of another.
The revision orders the entries of every environment of one run, because one counter gives all of them.

- **A parameter is one part of a rule that a turn of an encoder changes.**
  It is the chance or the `every` count of the trigger, the delay, the jitter or the throttle of a timing lie,
  the error of a refusal, or the lifetime.
  The entry holds the old and the new value as a definition writes them, such as `1s` and `1100ms`.
- **A live rule that someone replaces as a whole has changed.**
  An update of a live rule can change everything but its name, so its entry names the rule, and no parameter.
  The undo of a turn is such an update.
- **One change can be two entries, with one revision.**
  A live rule that is made armed is made and armed.
  An armed live rule that is deleted is disarmed and deleted.
  Every arming in a journal therefore ends with a disarm, and [Capture](scenarios.md#capture) finds both.
- **A live rule that someone keeps goes on.**
  The journal of its environment records that it was kept, and its history goes on under the same name.
  For every other environment that runs, the workspace has a new rule: their journals record that it was created.
- **A lifetime that ended has its entry a moment later.**
  A rule whose lifetime ended or whose budget is spent disarms inside its proxy.
  Its entry comes when the engine [counts the change](../architecture/control-plane.md#revisions), with the revision and the time of that moment.
- **The start and the stop of an environment are no entries.**
  A journal begins and ends with its environment.
- **The entries of scenarios and recordings arrive with them.**
  Until a scenario runs and a recording records, no journal has an entry of a scenario, a step or a recording.
  No backend applies or emulates an incident yet either, so no journal has such an entry so far.

### Surfaces

A surface is where a change came from.
The journal records the surface, and not a person: the control plane has no users.

| Surface | The change came from |
|---|---|
| `pad` | A press or a turn at the pad grid. |
| `quick-inject` | A ⌘K sentence. |
| `ui` | Any other view of the UI. |
| `cli` | `gmq`. |
| `test` | A test library. |
| `api` | Any other client of the control plane. |
| `step` | A step of the active scenario. |
| `lifetime` | A lifetime that ended, or a firing that spent it. |
| `file` | A file of the workspace that someone edited, taken in by a [reload](../architecture/storage.md#reloading). |

A [request](../architecture/control-plane.md#where-a-request-comes-from) names one of the first six.
The engine names the last three itself, and no request can claim them.

A [presence](../architecture/control-plane.md#presence) that ends takes its holds with it, and a rule that disarms with its last hold has the surface `pad`:
the engine takes the hold away as the release would have, which the surface never sent.

No change has the surface `step` until a scenario runs, and none has `file` until the workspace reloads.

### The journal only grows

Nothing takes an entry out of the journal.
An [undo](control-surfaces.md#undo-and-redo) is a change like any other: it adds an entry, with the cause `undo`.
The journal then shows both: that someone armed a rule, and that they took it back.

The engine always keeps the recent journal in memory.
It keeps as many entries as `recent_journal` of its configuration says, 1000 until someone sets it,
and lets go of the oldest entry when a new one would be one too many.
An entry that left memory keeps its number, and no later entry gets it.
It feeds the history of a rule in the UI, and [Capture](scenarios.md#capture) reads it.
The control plane [answers with it](../architecture/control-plane.md#the-journal), and sends every new entry to the live stream.
While a recording runs, the journal entries go into the recording between the calls.
A recording therefore explains itself:
it shows which rules were armed when a call arrived.

## Index

The index is built automatically from a recording.
It groups calls into facets, each with counts.

| Facet | Values |
|---|---|
| Topics | `orders.eu`, `orders.us`, … and suggested patterns such as `orders.*` |
| APIs | `Produce`, `Fetch`, `Heartbeat`, … |
| Consumer groups | `payments`, `analytics-etl` |
| Transactional IDs | `payments-tx-1` |
| Clients | `order-ingest`, `payments-svc`, … |
| Brokers | `broker 1`, `broker 2`, `broker 3` |
| Outcomes | `ok`, `error`, delayed, throttled, … |

Facet counts respect the other active filters,
so the index always answers *what is left if I narrow down further?*

Clicking a facet value adds a condition to the current filter.
The current filter can be saved as a [selector](targeting.md#selectors),
or turned into a rule straight away.

## From observation to action

Observation feeds the rest of the system in three ways:

- **Filter → selector**: a filter built on the index becomes a named selector.
- **Call → selector**: a single call becomes a selector by ticking its fields.
  See [building selectors from traffic](targeting.md#building-selectors-from-traffic).
- **Recording → world**: a recording seeds a simulated cluster.
  See [seeding](world.md#seeding).
