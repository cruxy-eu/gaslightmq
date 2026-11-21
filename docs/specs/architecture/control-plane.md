# Control plane

The control plane exposes the engine to the UI, the CLI and tests.
It is an HTTP API with a WebSocket for live updates.
Every control surface uses it,
and nothing reaches the engine any other way.

## Resources

| Resource | Operations |
|---|---|
| `selectors` | List, get, create, update, rename, delete. Test against the current recording. |
| `presets` | List, get, create, update, rename, delete. |
| `rules` | List, get, create, update, rename, delete. Arm, disarm, fire. |
| `scenarios` | List, get, create, update, rename, delete. Activate, deactivate, step. |
| `layouts` | List, get, create, update, rename, delete. |
| `sessions` | List the open sessions with their identity. Close one. |
| `recordings` | Start, stop, list, open. Query calls with a selector. Get the index. |
| `world` | Only with the Simulated backend. Get state, apply an event, save as a world definition, set the clock. |
| `backend` | Get the backend kind, its endpoints and its capabilities. |
| `capture` | Start, stop and save as a scenario. |

Create and update operations on definitions write to the workspace files.

## Live stream

The WebSocket streams:

| Message | Carries |
|---|---|
| **Snapshot** | The full state, sent when a client connects. |
| **Call** | One call event: request summary, verdict and explanation. |
| **Stats** | Counters per rule and per API, at a fixed interval. |
| **Rule changed** | A rule changed state or definition. |
| **Scenario changed** | The active scenario changed, or advanced a step. |
| **World changed** | An event changed the World. |
| **Session opened, identified, closed** | Session lifecycle. |
| **Workspace error** | A file failed validation. |

Clients can subscribe to call events with a selector,
so a UI that only shows `orders.*` does not receive every call.

## Commands from surfaces

Surfaces send intent, not state.
A pad press sends *arm the rule at row 1, column 4 on layer order-ingest*,
and the engine creates the rule if it does not exist yet.
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
a test can check that *Refuse with NOT_LEADER* fired three times,
not merely that the client saw some errors.
