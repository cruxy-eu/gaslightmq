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
| `selectors` | List, get, create, update, delete. Test against the current recording. |
| `presets` | List, get, create, update, delete. |
| `rules` | List, get, create, update, delete. List the rules of an environment with their state. Create, update and delete a live rule. Arm, disarm, fire. Keep a live rule. |
| `scenarios` | List, get, create, update, delete. Activate, deactivate, step. |
| `layouts` | List, get, create, update, delete. |
| `sessions` | List the open sessions with their identity. Close one. |
| `recordings` | Start, stop, list, open. Query calls with a selector, saved or [draft](#drafts). Get the index. |
| `world` | Only for an environment with a broker. Get state, apply an incident, save as a world definition, set the clock. [Preview the records](#drafts) at an offset. |
| `backend` | Get the backend kind, its endpoints and its capabilities. |
| `environments` | List and get the environments of the workspace. Start and stop one. |
| `journal` | Query what changed: arms, disarms, parameter changes, steps and incidents. |
| `capture` | Start, stop and save as a scenario. |
| `quick-inject` | [Parse a ⌘K sentence](#drafts) into a rule definition, with completions. |

Every operation on a definition names it by its [name](../domains/rules.md#names).
No operation names a definition by its [UUID](identity.md#uuid), which stays inside the workspace.
No operation renames a definition.
An update changes everything but the name, the [title](../domains/rules.md#titles) included.
Whoever wants another name creates a new definition and deletes the old one.

Create and update operations on definitions write to the workspace files.
Live rules are the exception.
They stay in memory until *keep* saves them as workspace rules.

A live rule belongs to one environment, and its name is a name that no rule of the workspace has.
*Keep* saves it in the workspace under the same name and UUID.
It stays in the state it has in its environment, and every other environment holds it disarmed.

*Fire* skips a rule's trigger once.
An incident fires immediately.
A lie applies to the next call the rule matches.
See [manual firing](../domains/rules.md#manual-firing).

## Operations

Every operation is one request, and it gets one answer or one error.
The crate `control_api` holds each operation as a type, with the type of its answer and of its error,
and a client and a server share them.
[Over the network](#over-the-network) says how they travel between a client and the server.

- **An error holds what can go wrong with its operation, and nothing else.**
  A client decides on each variant: it starts an environment that does not run, or picks another name for a rule.
  The rest is text.
  Operations that fail in the same ways share an error.
- **A command on something live names an environment that runs.**
  Every such command fails the same way when the environment does not run,
  whether it stopped or never existed.
- **A definition travels without its [UUID](identity.md#uuid).**
  An answer leaves it out.
  The workspace gives a definition its UUID when it creates it, and keeps it on every update, whatever UUID a request holds.

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
| **Snapshot** | The full state, sent when a client connects: every definition of the workspace, and every environment that runs with its backend, its rules and its sessions. |
| **Call** | One call event: request summary, verdict and explanation. |
| **Stats** | Matches, firings and remaining budget per rule, and calls per API, at a fixed interval. |
| **Rule changed, removed** | A rule of an environment changed state, or a live rule was made or changed its definition. A live rule left its environment: someone deleted or kept it. |
| **Definition changed, deleted** | Someone created, updated or deleted a definition of the workspace. |
| **Scenario changed** | The active scenario changed, or advanced a step. |
| **World changed** | An incident changed the World, or the Upstream backend's overlay. |
| **Session opened, identified, software named, closed** | Session lifecycle. The client software comes with the identification, or later with the first `ApiVersions` request that names it. |
| **Journal entry** | Something changed, with the time and the surface that changed it. |
| **Environment started, stopped** | An environment came up, with its backend, its rules and its sessions, or went down. |
| **Workspace invalid** | The definitions do not fit each other, or a file failed validation. The engine keeps the last valid version. |
| **Subscription refused** | The last subscription of this client cannot be followed, and why. The client follows no calls. |

A message about something live names its environment.
A client that applies every message to the snapshot holds what the process holds.

Clients can subscribe to call events with a selector,
so a UI that only shows `orders.*` does not receive every call.
A client follows the calls of one environment at a time, and no calls until it subscribes.
A new subscription replaces the one before it.
A selector without conditions follows every call.
Every other message reaches every client, except a refusal, which reaches the client whose subscription it refuses.

## Over the network

`control_server` serves the control plane, and `control_client` speaks to it.
The server listens on one address: `127.0.0.1:8080`, until its configuration names another.
Every body and every frame is JSON, and it is the `serde` form of a type of `control_api`.
No other form exists, so a client in the browser and one on a native target send the same bytes.

| What | Where |
|---|---|
| An operation | `POST /api/operations` |
| The live stream | `GET /api/stream`, which upgrades to a WebSocket |

The other paths stay free for the UI.

### Operations

A request is one `POST /api/operations` with an `AnyOperation` as its body:
`{ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }`.
The status of the response says what its body is.

| Status | The body is |
|---|---|
| `200 OK` | The answer of the operation, in the form of its `Answer`. An answer of `()` is `null`. |
| `422 Unprocessable Content` | The error of the operation, in the form of its `Error`. |
| `501 Not Implemented` | A `RequestError::NotServed`: the process does not serve the operation yet. |
| `400 Bad Request` | A `RequestError::Unreadable`: the body is larger than the server takes, or it is no operation that the server knows. |

A client knows which operation it sent, so it reads `200` with the type of the answer and `422` with the type of the error.
A `RequestError` is the same for every operation, and its `error` key names the variant.
The server answers every request to `/api/operations` with one of these four.
An answer takes as long as its operation: a start waits until the environment accepts connections.

### The live stream

A client opens the stream with `GET /api/stream`, which upgrades to a WebSocket.
Every frame is a text frame.

| Direction | Each frame is |
|---|---|
| Server to client | One `StreamMessage`. |
| Client to server | One `Subscription`. |

- **The snapshot comes first.**
  The first message is a `snapshot`, and every message after it is a change, a call or a refusal.
- **A message is a state, not a difference.**
  A change says what a rule, a definition or an environment is now.
  A change can repeat what the snapshot holds already, and applying it again changes nothing.
- **A client that falls behind gets a new snapshot.**
  The server keeps a backlog of changes for every client, as large as its configuration says.
  A client that falls further behind would miss changes, so the server sends it a new `snapshot` and goes on from there.
  Calls do not count: a client that falls behind the calls of its environment misses the oldest of them.
- **A subscription that cannot be followed is refused.**
  A frame that is no subscription, an environment that does not run and a selector that the workspace does not have
  each get a `subscription-refused` that says what was wrong.
  The client then follows no calls, as after a subscription to nothing.
- **A subscription ends with its environment.**
  When the environment stops, the client follows no calls, and the `environment-stopped` says so.
- **The server ends the stream with a close frame.**
  It closes with `1001 Going Away` when it stops,
  and with `1011 Internal Error` when the workspace cannot be read for a snapshot, with what the store said as the reason.

### What the server does not serve yet

The host has no live rules, no sessions and no pads yet.
Until it has, the server answers these operations with `501` and a `RequestError::NotServed`:
`create-live-rule`, `update-live-rule`, `delete-live-rule`, `keep-rule`, `send-intent`, `list-sessions` and `close-session`.
The `sessions` of an environment in a snapshot are empty.

The stream carries the changes that go through the control plane.
A rule that its lifetime disarms, an environment that `gmq` starts beside the server, and a file that someone edits
send no message yet. The next snapshot holds them.
`stats`, `rule-removed`, the messages of sessions and `workspace-invalid` arrive with what makes them in the host.

No request needs a token yet, and the server allows no request from a page of another origin.
The `token` of the [process configuration](storage.md#process-configuration) arrives with the binary.

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
a test can check that `refuse-with-not-leader` applied three times,
not merely that the client saw some errors.
[Budgets](../domains/rules.md#lifetime) make that count exact.

The test owns the verdict on the client.
A scenario has no expectations of its own yet.
