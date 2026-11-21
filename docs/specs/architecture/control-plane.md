# Control plane

The control plane exposes a process to the UI, the CLI and tests.
It is an HTTP API with a WebSocket for live updates.
Every control surface uses it,
and nothing reaches a proxy or a broker any other way.

A process has one control plane for all its [environments](storage.md#several-environments-in-one-process).
Definitions belong to the workspace, so every environment shares them.
Everything live belongs to one environment:
armed rules, the active scenario, sessions, recordings, the journal and the World.
A command on something live names its environment.

A process can run without a control plane.
It is then set up by flags and files alone.

## Resources

| Resource | Operations |
|---|---|
| `selectors` | List, get, create, update, rename, delete. Test against the current recording. |
| `presets` | List, get, create, update, rename, delete. |
| `rules` | List, get, create, update, rename, delete. Arm, disarm, fire. Keep a live rule. |
| `scenarios` | List, get, create, update, rename, delete. Activate, deactivate, step. |
| `layouts` | List, get, create, update, rename, delete. |
| `sessions` | List the open sessions with their identity. Close one. |
| `recordings` | Start, stop, list, open. Query calls with a selector, saved or [draft](#drafts). Get the index. |
| `world` | Only for an environment with a broker. Get state, apply an event, save as a world definition, set the clock. [Preview the records](#drafts) at an offset. |
| `backend` | Get the backend kind, its endpoints and its capabilities. |
| `environments` | List and get the environments of the workspace. Start and stop one. |
| `journal` | Query what changed: arms, disarms, parameter changes, steps and events. |
| `capture` | Start, stop and save as a scenario. |
| `quick-inject` | [Parse a ⌘K sentence](#drafts) into a rule definition, with completions. |

Create and update operations on definitions write to the workspace files.
Live rules are the exception.
They stay in memory until *keep* saves them as workspace rules.

*Fire* skips a rule's trigger once.
An event fires immediately.
A lie applies to the next call the rule matches.
See [manual firing](../domains/rules.md#manual-firing).

## Drafts

A control surface holds none of the engine's logic.
It shares the engine's definition types,
and it asks the control plane for whatever needs the engine's behaviour.
See [what compiles to WebAssembly](crates.md#what-compiles-to-webassembly).

Three operations answer while someone is still typing.
They change nothing: no file, no rule and no live state.

| Operation | Takes | Returns |
|---|---|---|
| **Query with a draft selector** | A recording, and a selector definition that nobody saved yet. | The calls the draft matches, and their count over the whole recording. |
| **Parse a ⌘K sentence** | The text typed so far. | The rule definition the sentence gives, the completions for the next word, and what the parser could not read. |
| **Preview records** | A topic and a partition of the World, an offset and a count. | The records from that offset on, decoded with the topic's [schema](../domains/world.md#schemas). |

The engine answers from the whole recording and the whole workspace.
A browser only holds a window of both, so it cannot give these answers itself.

A parsed sentence is only a rule definition.
The surface creates the live rule with the `rules` resource.
The CLI and tests use the same three operations.

## Live stream

The WebSocket streams:

| Message | Carries |
|---|---|
| **Snapshot** | The full state, sent when a client connects. |
| **Call** | One call event: request summary, verdict and explanation. |
| **Stats** | Matches, firings and remaining budget per rule, and calls per API, at a fixed interval. |
| **Rule changed** | A rule changed state or definition. |
| **Scenario changed** | The active scenario changed, or advanced a step. |
| **World changed** | An event changed the World, or the Upstream backend's overlay. |
| **Session opened, identified, closed** | Session lifecycle. |
| **Journal entry** | Something changed, with the time and the surface that changed it. |
| **Environment started, stopped** | An environment came up or went down. |
| **Workspace error** | A file failed validation. |

Clients can subscribe to call events with a selector,
so a UI that only shows `orders.*` does not receive every call.

## Secrets

The control plane never returns a secret.
An environment shows its [secret references](storage.md#secrets), not the values behind them.
The engine blanks credentials in every call it streams.
See [credentials on the wire](storage.md#credentials-on-the-wire).

## Commands from surfaces

Surfaces send intent, not state.
A pad press sends *arm the rule at row 1, column 4 on layer order-ingest*,
and the engine creates a live rule if the pad has none yet.
An encoder turn sends *change probability by +3*, not *set probability to 43*,
so two surfaces turning the same encoder do not fight.

## Tests

Test libraries drive the same API:

1. Start an engine with a backend and a workspace.
2. Activate a scenario.
3. Run the client under test.
4. Query the recording with selectors,
   and assert on verdicts and explanations.

Explanations make assertions precise:
a test can check that *Refuse with NOT_LEADER* applied three times,
not merely that the client saw some errors.
[Budgets](../domains/rules.md#lifetime) make that count exact.

The test owns the verdict on the client.
A scenario has no expectations of its own yet.
