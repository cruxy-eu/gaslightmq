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
| `rules` | List, get, create, update, delete. List the rules of an environment with their state. Create a live rule, armed when the request says so. Update and delete a live rule. Arm, disarm, fire. Keep a live rule. |
| `scenarios` | List, get, create, update, delete. Activate, deactivate, step. |
| `layouts` | List, get, create, update, delete. |
| `sessions` | List the open sessions with their identity. Close one. |
| `recordings` | Start, stop, list, open. Query calls with a selector, saved or [draft](#drafts). Get the index. |
| `world` | Only for an environment with a broker. Get state, apply an incident, save as a world definition, set the clock. [Preview the records](#drafts) at an offset. |
| `backend` | Get the backend kind, its endpoints and its capabilities. |
| `environments` | List and get the environments of the workspace. Start and stop one. Say whether every environment that the process was started with runs. |
| `journal` | Query what changed in an environment: arms, disarms, parameter changes, definitions, steps and incidents. |
| `capture` | Start, stop and save as a scenario. |
| `quick-inject` | [Parse a ⌘K sentence](#drafts) into a rule definition, with completions. |

Every operation on a definition names it by its [name](../domains/rules.md#names).
No operation names a definition by its [UUID](identity.md#uuid).
A UUID is live state of the process, and it reaches a client in [journal entries](../domains/observation.md#journal) alone.
No operation renames a definition.
An update changes everything but the name, the [title](../domains/rules.md#titles) included.
Whoever wants another name creates a new definition and deletes the old one.

Create and update operations on definitions write to the workspace files.
See [writing back](storage.md#writing-back).
Live rules are the exception.
They stay in memory until *keep* saves them as workspace rules.

A live rule belongs to one environment, and its name is a name that no rule of the workspace has.
*Keep* saves it in the workspace under the same name and UUID.
It stays in the state it has in its environment, and every other environment holds it disarmed.

Activating a scenario asks the backend of the environment whether it supports the incidents that the scenario uses.
The activation fails with the list of those it does not.
See [portability](../domains/scenarios.md#portability).

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
- **A definition travels as its file holds it.**
  It holds no [UUID](identity.md#uuid) and no revision, in a request as in a file.
  An answer, the snapshot and a message of a change hold two things next to the definition:
  its [revision](#revisions), and whether [a write would lose a comment](storage.md#writing-back) of its file.
- **One gesture is one operation.**
  Whatever a surface does in one gesture is one request, so it applies as a whole or not at all.
  A ⌘K sentence creates its live rule armed, with one operation.

## Where a request comes from

Every request says which surface sends it.

```json
{ "from": { "surface": "pad" }, "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }
```

| Part of `from` | Says |
|---|---|
| `surface` | Where the request comes from: `pad`, `quick-inject`, `ui`, `cli`, `test` or `api`. See [surfaces](../domains/observation.md#surfaces). |
| `cause` | `undo` or `redo`, when the request takes back or repeats an earlier change of its sender. A request leaves it out otherwise. |

The [journal](../domains/observation.md#journal) records both with every change that the request makes.
The control plane has no users, so `from` is what the sender says about itself.

A change that no request made has a surface too: `step`, `lifetime` or `file`.
The engine names those itself, and a request that claims one is unreadable.

## Revisions

Several clients change one process: two tabs of the UI, `gmq`, a scenario, a lifetime, and whoever edits a file.
A revision keeps one of them from overwriting what it has not seen.

Whatever a request changes has a [revision](identity.md#revision):
a definition, a rule in an environment, and the scenario of an environment.

- **Whoever reads something gets its revision.**
  An answer, the snapshot and every message of a change hold it.
- **A change answers with the new revision.**
  A client that changes one thing twice in a row waits for no message of the stream in between.
- **A change can say which revision it expects.**
  An update, a delete, an arm, a disarm, an activation and a deactivation take `expects`.
  When the thing has another revision, the operation changes nothing,
  and its error is `changed`, with the revision that the thing has now.
- **Without `expects`, the last change wins.**
  `gmq` and a test that has its process to itself leave it out.
  The UI sends it with every change.
- **A create expects nothing.**
  It refuses a name that is taken, so it never overwrites.

```json
{ "from": { "surface": "ui" }, "operation": "update-rule", "expects": 7, "definition": { "name": "slow-order-producers", "selector": "order-producers", "action": "delay-1s" } }
```

A pad intent expects nothing either.
It says what someone did, and it acts on the rule as the rule is.
Its answer says what it did: the rule it acted on, whether it made that rule, and the definition, the state and the revision that the rule has now.
A surface never works out what a press or a turn did.
The name of a rule that a pad makes and the bounds of an encoder belong to the engine.

## Undo

The control plane has no operation that undoes, and it keeps no history for it.
A surface keeps the history of its own changes, as the [UI](../domains/control-surfaces.md#undo-and-redo) does for each tab.
An undo is an ordinary operation: the inverse of the change, with the revision that the change left as `expects`, and with the cause `undo`.

| A change | Its inverse |
|---|---|
| Create a definition | Delete it. |
| Update a definition | Update it to the definition before. |
| Delete a definition | Create it again. |
| Create a live rule | Delete it. |
| Update a live rule | Update it to the definition before. |
| Delete a live rule | Create it again, armed when it was armed. |
| Arm, disarm | Disarm, arm. |
| Activate a scenario, deactivate it | Deactivate it, activate it. |
| A pad press that armed a rule | Disarm it, or delete the live rule when the answer says that the press made it. |
| A pad press that disarmed a rule | Arm it. |
| Turns of an encoder | Update the live rule to the definition before the first turn. |

A redo is the change again, with the revision that the undo left and the cause `redo`.

- **The inverse restores a state, and never replays a gesture.**
  The undo of a turn of +3 is no turn of −3: an encoder stops at its bounds, and the turn may have changed less.
- **A surface reads the inverse from answers.**
  The definition before an update is the one whose revision the update expected, so a success says that the surface held the right one.
- **A definition that comes back is a new definition.**
  It has the name and the content of the one that went, and a new [UUID](identity.md#uuid).
- **A scenario that comes back starts again.**
  Activating it starts it at its first step, whichever step it was at.
- **A state that moved on is no state to undo.**
  When someone else changed the thing since, the inverse fails with `changed` and changes nothing.
  The surface drops the step.
- **Some operations have no inverse.**
  Fire, a step, keep, the start and the stop of an environment, of a recording and of a capture, and closing a session.
  What they did has happened.

The [journal](../domains/observation.md#the-journal-only-grows) records an undo as a new change with its cause, next to the change it took back.

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
| **Parse a ⌘K sentence** | An environment that runs, and the text typed so far. | The rule definition the sentence gives, the completions for the next word, and what the parser could not read. The sentence knows the names of the workspace and those that the traffic of the environment has seen. |
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
| **Rule changed, removed** | A rule of an environment changed state, or a live rule was made or changed its definition, with its revision. A live rule left its environment: someone deleted or kept it. |
| **Definition changed, deleted** | Someone created, updated or deleted a definition of the workspace, through the control plane or in its file. A change holds the revision of the definition. |
| **Scenario changed** | The active scenario changed, or advanced a step, with its revision. |
| **Recording started, stopped** | A recording of an environment began or ended, and why it ended when nobody stopped it. |
| **World changed** | An incident changed the World, or the Upstream backend's overlay. |
| **Session opened, identified, software named, closed** | Session lifecycle. The client software comes with the identification, or later with the first `ApiVersions` request that names it. |
| **Journal entry** | Something changed in an environment, with the time, the surface that changed it and the cause. |
| **Environment started, stopped** | An environment came up, with its backend, its rules and its sessions, or went down. |
| **Workspace invalid** | The definitions do not fit each other, or a file failed validation. The engine keeps the last valid version. |
| **Following** | The last subscription of this client is in force: the server sends every call it names from here on. |
| **Subscription refused** | The last subscription of this client cannot be followed, and why. The client follows no calls. |

A message about something live names its environment.
A client that applies every message to the snapshot holds what the process holds.

Clients can subscribe to call events with a selector,
so a UI that only shows `orders.*` does not receive every call.
A client follows the calls of one environment at a time, and no calls until it subscribes.
A new subscription replaces the one before it.
A selector without conditions follows every call.
Every other message reaches every client, except the answer to a subscription, which reaches the client that sent it.

## Over the network

`control_server` serves the control plane, and `control_client` speaks to it.
The server listens on one address: `127.0.0.1:7092`, until its configuration names another.
Every body and every frame is JSON, and it is the `serde` form of a type of `control_api`.
No other form exists, so a client in the browser and one on a native target send the same bytes.

| What | Where |
|---|---|
| An operation | `POST /api/operations` |
| The live stream | `GET /api/stream`, which upgrades to a WebSocket |

The server serves the [UI](crates.md#what-compiles-to-webassembly) on the other paths, so the page and the control plane share one origin.

| A `GET` of | Gets |
|---|---|
| A path that names a file of the UI | The file, with a tag of its content, so a browser that holds it gets `304 Not Modified`. |
| Any other path | The page of the UI, which opens the view of that path, such as `/rules`. |
| A path below `/api` that the control plane does not serve | `404 Not Found`. |

Every other method on those paths is `405 Method Not Allowed`.
A release build of the server holds the files of the UI, so the UI ships with the binary.
A build without them serves a page that says how to build them.

### Operations

A request is one `POST /api/operations`.
Its body is one object: an `AnyOperation`, with [where the request comes from](#where-a-request-comes-from) next to its keys.
`{ "from": { "surface": "cli" }, "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }`.
The status of the response says what its body is.

| Status | The body is |
|---|---|
| `200 OK` | The answer of the operation, in the form of its `Answer`. An answer of `()` is `null`. |
| `422 Unprocessable Content` | The error of the operation, in the form of its `Error`. |
| `501 Not Implemented` | A `RequestError::NotServed`: the process does not serve the operation yet. |
| `400 Bad Request` | A `RequestError::Unreadable`: the body is larger than the server takes, it is no operation that the server knows, or it does not say where it comes from. |

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
  The first message is a `snapshot`, and every message after it is a change, a call or the answer to a subscription.
- **A message is a state, not a difference.**
  A change says what a rule, a definition or an environment is now.
  A change can repeat what the snapshot holds already, and applying it again changes nothing.
- **A client that falls behind gets a new snapshot.**
  The server keeps a backlog of changes for every client, as large as its configuration says.
  A client that falls further behind would miss changes, so the server sends it a new `snapshot` and goes on from there.
  Calls do not count: a client that falls behind the calls of its environment misses the oldest of them.
- **Every subscription gets an answer.**
  A subscription that the server follows gets a `following` with the subscription,
  and the client gets every call it names from that message on, and none before it.
  A subscription to nothing gets one too: the client follows no calls from there.
- **A subscription that cannot be followed is refused.**
  A frame that is no subscription, an environment that does not run and a selector that the workspace does not have
  each get a `subscription-refused` that says what was wrong.
  The client then follows no calls, as after a subscription to nothing.
- **A subscription ends with its environment.**
  When the environment stops, the client follows no calls, and the `environment-stopped` says so.
- **The server ends the stream with a close frame.**
  It closes with `1001 Going Away` when it stops,
  and with `1011 Internal Error` when the workspace cannot be read for a snapshot, with what the store said as the reason.

### Where the changes come from

The server follows the changes of the host, whoever makes them: an operation of the control plane, a pad,
a rule that a firing spent or whose lifetime ended, a client that connects or leaves, or `gmq` itself.
Each change names what changed, and the server sends the state that thing has when it sends the message.
A change whose subject is gone again by then sends nothing, because a later message says that it went.

| The host tells | The server sends |
|---|---|
| An environment started or stopped. | `environment-started` with what runs in it, or `environment-stopped` once its sessions closed. |
| A definition changed through the host, or went. | `definition-changed` with the definition, or `definition-deleted`. |
| A rule armed or disarmed in an environment, or a live rule was made or changed. | `rule-changed` with the rule and its state. |
| A live rule left its environment. | `rule-removed`. Keeping a live rule sends `rule-removed`, `definition-changed` and `rule-changed`, in that order. |
| A session opened, was identified, named its software, or closed. | The message of the session. |

A rule of the workspace that someone creates sends `definition-changed` alone: every environment holds it, disarmed.
`stats` comes for every environment that runs, at the interval of the server's configuration.
A server that falls behind the changes of the host sends every client a new `snapshot`.

### What the server does not send yet

Every operation of `control_api` is served, so no answer is `501` today.
`RequestError::NotServed` stays for a process that knows fewer operations than its client:
an older process, or one built without a part.

`workspace-invalid` waits for the files of a workspace, which can change behind the host. Stores in memory cannot.

A request does not say where it comes from yet, and nothing has a revision yet.
Both arrive with the journal, which records the surface of every change.

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
The answer to an intent says [what it did](#revisions), so the surface knows without working it out.
A turn changes a live rule, and never a rule of the workspace, which changes in the workspace.
A pad whose rule someone kept therefore takes presses, and refuses turns.

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
