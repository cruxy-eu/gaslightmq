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
| `workspace` | [Reload](#reloading) the definitions from the files of the workspace. [Save](#saving) the edits that wait for a save. |
| `sessions` | List the open sessions with their identity. Close one. |
| `recordings` | Start, stop, list, open. Query calls with a selector, saved or [draft](#drafts). Get the index. |
| `world` | Only for an environment with a broker. Get state, apply an incident, save as a world definition, set the clock. [Preview the records](#drafts) at an offset. |
| `backend` | Get the backend kind, its endpoints and its capabilities. |
| `environments` | List and get the environments of the workspace. Start and stop one. Say whether every environment that the process was started with runs: [`check-health`](#health). |
| `journal` | [Query what changed](#the-journal) in an environment: arms, disarms, parameter changes, definitions, steps and incidents. |
| `capture` | Start, stop and save as a scenario. |
| `quick-inject` | [Parse a ⌘K sentence](#drafts) into a rule definition, with completions. |

Every operation on a definition names it by its [name](../domains/rules.md#names).
A definition has no other identity, so no operation names it by anything else.
No operation renames a definition.
An update changes everything but the name, the [title](../domains/rules.md#titles) included.
Whoever wants another name creates a new definition and deletes the old one.

Create, update and delete operations on definitions write to the workspace files:
before they answer, or with the next [save](#saving) when the workspace writes on save.
See [writing back](storage.md#writing-back).
Live rules are the exception.
They stay in memory until *keep* saves them as workspace rules.

An armed rule follows a change of the workspace, as it follows a [reload](storage.md#reloading):

- **An update replaces every armed rule that it changes.**
  A rule of the workspace whose definition changed, and a rule that uses a selector or a preset that changed, stays armed in every environment that armed it, with its counters and its budget.
  An armed rule of the workspace that the update changed is a `rule-changed` too.
- **A delete of a rule of the workspace disarms it** wherever it is armed.
  The journal records that it disarmed, and then that it went, both with the revision of the delete.
- **A change that an armed live rule could not follow changes nothing.**
  An update that leaves a selector or a preset that an armed live rule cannot fire with, and a delete of one that it uses, fail with `live-rule-does-not-fit`.
  Whoever wants the change disarms or deletes the live rule first.

A live rule belongs to one environment, and its name is a name that no rule of the workspace has.
Two environments can each have a live rule of one name, because each holds its own.
*Keep* saves a live rule in the workspace under the same name.
It stays in the state it has in its environment, and every other environment holds it disarmed.

A rule of the workspace is in every environment, so it never stands next to a live rule of its name.
Rule names are also unique among all rules, so no rule has the name of a rule that a [scenario owns](../domains/scenarios.md#scenario):

- **Creating a live rule refuses a name that a rule of the workspace or of a scenario has.**
- **Creating a rule of the workspace refuses a name that a live rule has**, in any environment that runs,
  **or a rule of a scenario.**
- **Keep refuses a name that a live rule of another environment, or a rule of a scenario, has.**

Each error is `name-taken`, and each names what holds the other rule in the same way:
`{ "holder": { "environment": "canary-proxy" } }` for a live rule of that environment,
and `{ "holder": { "scenario": "leader-election" } }` for a rule of that scenario.
A live rule that is not created names its own environment as the holder when a live rule of the environment has the name,
or a rule of that name is armed there.
The same error without a holder says that the workspace has a rule of that name.
Whoever wants the rule in the workspace deletes the other live rule first.
The check and the write are one step, so a live rule never gets the name in between.

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
  It holds [what a person writes](identity.md#what-a-definition-holds), in a request as in a file.
- **Live state travels beside a definition, never inside it.**
  An answer, the snapshot and a message of a change hold two things next to the definition:
  its [revision](#revisions), and whether [a write would lose a comment](storage.md#writing-back) of its file.
  The definition is one key of the answer, and each of the two is a key next to it: `{ "definition": { … }, "revision": 7, … }`.
- **One gesture is one operation.**
  Whatever a surface does in one gesture is one request, so it applies as a whole or not at all.
  A ⌘K sentence creates its live rule armed, with one operation.
  A rule that does not arm is no live rule either: creating it fails with `name-taken` when a rule of its name is armed in the environment already.

## Where a request comes from

Every request says which surface sends it.

```json
{ "from": { "surface": "pad" }, "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }
```

| Part of `from` | Says |
|---|---|
| `surface` | Where the request comes from: `pad`, `quick-inject`, `ui`, `cli`, `test` or `api`. See [surfaces](../domains/observation.md#surfaces). |
| `cause` | `undo` or `redo`, when the request takes back or repeats an earlier change of its sender. A request leaves it out otherwise. |
| `presence` | The ID of the [presence](#presence) of its sender, when the sender has one. A request leaves it out otherwise. |

The [journal](../domains/observation.md#journal) records the surface and the cause with every change that the request makes.
It records no presence: an entry says where a change came from, and not who was there.
The control plane has no users, so `from` is what the sender says about itself.

A client says once which surface it is, and every request of it says so.
One request can name another source: the UI is one client, and its pads and ⌘K name themselves with each request, as an undo names its cause.
A client that says nothing about itself is `api`.

The engine names three surfaces itself, and a request that claims one is unreadable.
A change that no request made has the surface `step` or `lifetime`.
A change that a [reload](storage.md#reloading) takes in has the surface `file`, whoever asked for the reload.

## Presence

A client of the control plane that is there is a presence.
A tab of the UI is one, from the moment it opens its [live stream](#live-stream) until it is gone.
The word *session* belongs to a Kafka client: a [session](../domains/session.md) is the runtime of one of its connections.
A client of the control plane has no session, and a presence is not one.

A presence stands above the connection of its client.
It begins with the first live stream of the client, outlives a stream that ends while the client opens a new one,
and ends when the client stays away.

```json
{ "from": { "surface": "pad", "presence": "9f86d081884c7d659a2feaa0c55ad015" }, "operation": "send-intent", "environment": "orders-sim", "intent": { "intent": "hold", "pad": { "layout": "order-desk", "layer": "order-ingest", "row": 0, "column": 3 } } }
```

- **Every live stream belongs to a presence.**
  A stream that asks for none opens a new one.
  The first message of the stream names the presence of its client.
- **A presence has an ID.**
  It is 128 random bits, written as 32 hexadecimal digits in lower case, as the ID of a [run](identity.md#run) is.
  The server makes it from the randomness of the system, for each client that is new.
  It is no number that the process counts: a tab that comes back after a restart never takes the presence of another tab for its own.
- **A request says which presence it comes from.**
  `from` holds the ID.
  A [hold](../domains/control-surfaces.md#modes-of-play) that the request adds belongs to that presence.
- **A presence outlives a stream that ends.**
  When the last stream of a presence ends, the server keeps the presence for a grace period.
  That is `presence_grace` of the server's configuration, and 10 seconds until someone sets it.
  The server cannot tell a tab that closed from one whose connection dropped, so every stream that ends gets the grace period.
- **A client that comes back names its presence.**
  It opens its new stream as the presence that it was.
  When the server still holds that presence, the client goes on as it, and everything of the presence stays.
  A client can come back before the server noticed that its earlier stream is gone: the presence then has two streams, and is there until the last of them ends.
- **A client that comes back too late is new.**
  The server no longer holds its presence: the grace period is over, or the process is another one.
  The stream opens a new presence, and its first message names another ID than the client asked for.
  The client reads from that that the presence it was has ended, with everything that belonged to it.
- **The holds of a presence end with it.**
  The engine takes away every hold of a presence that ended, in every environment, as the release of each would.
  A rule that disarms with its last hold takes one number, as with a release, and its `rule-changed` reaches every client.
  A hold that names a presence which has ended is refused, because nothing would end it.
- **A reload is a new presence.**
  A page keeps the ID of its presence in memory, so a page that loads again asks for none.
  The presence of the page before it ends with its grace period.
- **A client that names no presence has none.**
  `gmq` and a test send requests without one.
  A hold of such a request ends by its release or by a disarm, and by nothing else.
- **Every presence ends when the server stops.**
  No client comes back to a server that stopped, and the host may live on with the pads that its clients held.
  The server ends every presence at once, without a grace period.

A stream ends when its connection closes, and also when its client goes silent.
A connection can go silent without closing: the network of a client on another machine drops, or its laptop sleeps.

- **The server pings every client of the stream.**
  It does so at an interval: `ping_interval` of the server's configuration, and 15 seconds until someone sets it.
  A browser answers a ping itself, and so does a client that waits for the next message of its stream.
- **A client that sends nothing between one ping and the next has gone silent.**
  Whatever the client sends says that it is there: the answer to a ping, or a subscription.
- **A client that takes no message for a whole interval has gone silent too.**
  A client that reads nothing would hold its stream up for as long as its connection stays.
- **The stream of a silent client ends, and the grace period begins.**
  The server lets go of the connection without a word.
  A client that went away without closing anything is therefore gone within two intervals, and its presence ends a grace period later.
  The grace period counts from the instant at which the silence was a whole interval long.

## Revisions

Several clients change one process: two tabs of the UI, `gmq`, a scenario, a lifetime, and a reload after someone edited a file.
A revision keeps one of them from overwriting what it has not seen.

Whatever a request changes has a [revision](identity.md#revision):
a definition, a rule in an environment, and the scenario of an environment.
One counter of the process gives all of them, so a revision never comes back within a [run](identity.md#run).

- **Whoever reads something gets its revision.**
  An answer, the snapshot and every message of a change hold it, beside what it belongs to.
- **The snapshot names the run.**
  A revision means something in the run that gave it, and in no other.
- **A change answers with the new revision.**
  The answer of every change is `{ "revision": 8 }`.
  A client that changes one thing twice in a row waits for no message of the stream in between.
- **A delete answers with the revision of the delete.**
  The delete is a change, so it takes a number, and nothing has that revision afterwards.
- **A change can say what it expects.**
  `expects` holds two things: the run that the client read from, and the revision that it saw.
- **A change that expects another revision changes nothing.**
  Its error is `changed`, with the revision that the thing has now.
  It names the thing too: a definition by its kind and its name, and a rule of an environment by both names.
- **A change that expects another run changes nothing.**
  Its error is `other-run`, with the run ID of the process.
  The process started again since the client read, and nothing that the client holds is certain any more: it reads the snapshot again.
  A change learns of the other run before anything else that is wrong with it, such as an environment that does not run any more.
- **Without `expects`, the last change wins.**
  `gmq` and a test that has its process to itself leave it out.
  The UI sends it with every change.
- **A create expects nothing.**
  It refuses a name that is taken, so it never overwrites.
  A live rule that is created armed is one change, and takes one number.
- **A firing expects nothing, and leaves no revision.**
  It is what happens, and no change of what someone set.

```json
{ "from": { "surface": "ui" }, "operation": "update-rule", "expects": { "run": "4be0643f1d98573b97cdca98a65347dd", "revision": 7 }, "definition": { "name": "slow-order-producers", "selector": "order-producers", "action": "delay-1s" } }
```

| An operation | Expects the revision of |
|---|---|
| Update or delete a definition | The definition. |
| Arm or disarm a rule | The rule in its environment. |
| Update, delete or keep a live rule | The rule in its environment. |
| Activate or deactivate a scenario, or step | The scenario of the environment. |

- **A rule in an environment has one revision, for its definition and its state together.**
  Someone who arms a rule of the workspace after another client changed its definition gets `changed`, and arms nothing they have not seen.
- **Keep expects the live rule as its sender saw it.**
  A turn of an encoder by someone else in between is a change, so the workspace never gets a rule that nobody looked at.
- **A step expects the scenario as its sender saw it.**
  Two clients that both step from the same step advance the scenario once, and the second one gets `changed`.
- **A revision does not cover what a definition refers to.**
  A rule whose selector someone changed has the revision it had.
- **A change that nobody asked for takes a number too.**
  A rule whose lifetime ended or whose budget is spent has a higher revision than it had while it was armed.
  Whoever still expects the armed rule changes nothing.
  The rule disarms inside its proxy, and takes its number when the process next looks at the armed rules:
  a moment later, or when someone reads or changes a rule of the environment before that.
  Nobody reads the rule disarmed at the revision it had while it was armed.

A write to a file can also fail because [someone edited the file](storage.md#the-check-before-a-write) while the process ran.
Its error is `changed-outside`, and no revision is involved: the engine does not hold what the file holds now, until someone asks for a [reload](#reloading).
Create, update and delete of a definition and keep each fail that way, with the name of what they changed and a `reason` that says which file changed, and how.

```json
{ "error": "changed-outside", "name": { "selector": "order-producers" }, "reason": "selectors.yaml changed since the store read it" }
```

A [read-only workspace](storage.md#read-only-workspaces) refuses each of them with `store-failed`, and the snapshot says beforehand that it is read-only.

A pad intent expects nothing.
It acts on the rule as the rule is, and it says the state that it wants.

- **A press never says *the other state*.**
  It says *arm the rule of this position* or *disarm it*.
  Two people who press the same pad to arm it both find it armed, and the second press changes nothing.
- **The answer says what the intent did.**
  It holds the rule it acted on, whether it made that rule, whether it changed the state of the rule,
  the definition, the state and the revision that the rule has now,
  and the revision that the intent found the rule at, before it changed it: `found`, which a press that made the rule leaves out.
  A surface compares `found` with the revision it showed, and so knows that the rule it held is the one that the intent changed.
- **An intent that changes a rule takes one number.**
  A press that makes the live rule of its pad and arms it is one change.
  An intent that finds the state it wants takes none, and answers with the revision that the rule had.
  A disarm or a release of a pad that points at no rule finds no rule, and changes nothing.
- **A press in hold answers with its hold.**
  The engine numbers every hold, and no number comes back within a run.
  The release names the hold of its press, so it never takes away the hold of someone else.
- **A surface never works out what a press or a turn did.**
  The name of a rule that a pad makes and the bounds of an encoder belong to the engine.

## Reloading

`reload-workspace` reads the files of the workspace again, and takes in what changed as one change.
[Reloading](storage.md#reloading) says what it compares, and what it does to the armed rules.
It names nothing and expects nothing: what it changes comes from the files.

```json
{ "from": { "surface": "cli" }, "operation": "reload-workspace" }
```

It answers with its revision, and with what it changed, by the kind and the name of each definition:

```json
{ "revision": 12, "changes": [{ "updated": { "selector": "order-producers" } }, { "deleted": { "preset": "blackhole" } }] }
```

- **A reload that changed nothing answers with its revision alone.**
  It is a change all the same, and takes a number.
- **Every client hears of each change.**
  A definition that the reload created or updated is a `definition-changed`, and one that went is a `definition-deleted`.
  An armed rule of the workspace whose definition changed is a `rule-changed` too.
- **The journal of every environment that runs records each change with the surface `file`**, whoever asked for the reload, and with the revision of the reload.
  A rule of the workspace that disarms because the reload took it away is a `rule-disarmed` before its `definition-deleted`.
- **A change that still expects a definition as it was before the reload changes nothing.**
  The reload gave the definition a new revision, so the change fails with `changed`.

| Error | When | The other keys |
|---|---|---|
| `name-taken` | A rule of the files has the name of a live rule of an environment that runs. | `rule`, and the `holder`: `{ "environment": "canary-proxy" }`. |
| `live-rule-does-not-fit` | A live rule that is armed would not resolve against the files. | `environment`, `rule`, and the `reason`. |
| `does-not-fit` | The definitions of the files do not fit each other. | The `reason`, with every error. |
| `store-failed` | A file does not read. | The `reason`, with every file and the path of its key that failed. |
| `unsaved` | The workspace [writes on save](storage.md#saving), and holds edits that nobody saved. | `definitions`: each with its kind, as `{ "selector": "order-producers" }`. |

A reload that fails changes nothing.
Whoever deletes the live rule, or fixes the files, reloads again.
Whoever saves the edits that wait, or lets go of them, reloads again: a reload with `"discard-unsaved": true` takes what the files hold,
and each edit that it lets go of comes back as one of its changes.

## Saving

A workspace that [writes on save](storage.md#saving) holds every edit in memory until someone saves.
`save-workspace` writes every edit that waits, in one go.

```json
{ "from": { "surface": "ui" }, "operation": "save-workspace" }
```

It answers with the definitions whose edits it wrote, each with its kind: `{ "saved": [{ "selector": "order-producers" }] }`.
A save that had nothing to write answers `{}`.

- **The snapshot says what waits.**
  Its `workspace` holds when the workspace writes back, whether it is read-only, and every definition whose edit waits:
  `{ "write-back": "on-save", "unsaved": [{ "selector": "order-producers" }] }`.
- **A `workspace-changed` says what waits after a change.**
  It comes after every change of a definition of a workspace that writes on save, after a save, and after a reload,
  and it holds the `workspace` as the snapshot does.
- **A save expects nothing, and leaves no revision.**
  It changes no definition, so every one keeps its revision, and no journal records it.

| Error | When | The other keys |
|---|---|---|
| `changed-outside` | Someone edited a file that the save would write, since the process read it. The save wrote nothing. | The `reason`, with every such file and how it changed. |
| `store-failed` | A temporary file could not be written, so the save changed no file. Every edit still waits. | The `reason`, with the file and what the system said. |
| `partly-saved` | A rename failed after earlier ones went through. Every edit that is not in `saved` still waits. | `saved`, each with its kind, and the `reason`, with the file and what the system said. |

A workspace that writes each change has nothing to save, and a save there answers `{}`.

## Health

`check-health` says whether every environment that the process was started with runs.
Whoever starts the process names those environments: the one of the flags of `gmq proxy`, or the one of a test.
An environment that someone starts later counts for nothing, and one of the start that someone stops makes the process unhealthy until someone starts it again.
A process that started with no environment is healthy.

```json
{ "from": { "surface": "cli" }, "operation": "check-health" }
```

It answers with every environment of the start, in the order of their start, each with whether it runs.
When one of them does not run, it fails with `unhealthy`, which holds the same list:

```json
{ "environments": [{ "environment": "staging-proxy", "running": true }, { "environment": "orders-sim", "running": false }] }
```

It reads what the process holds in memory, and fails in no other way.

A probe of Kubernetes or of a load balancer sends no operation: it asks `GET /health`.
The server answers from the same state, with `200 OK` when every environment of the start runs and `503 Service Unavailable` when one does not, and with the list as the body either way.
`gmq health` asks `check-health`, and exits with a failure when an environment does not run or nothing answers.
The image of GaslightMQ checks its health with it.

## Undo

The control plane has no operation that undoes, and it keeps no history for it.
A surface keeps the history of its own changes, as the [UI](../domains/control-surfaces.md#undo-and-redo) does for each tab.
An undo is an ordinary operation: the inverse of the change, with the cause `undo`.
Its `expects` names the run of the change and the revision that the change left,
or the one that a later undo or redo of the same thing by the same surface left.

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
| A pad press that changed nothing | Nothing. The answer says so, and the surface keeps no step. |
| A pad press in hold that made a rule, and its release that disarmed it | Delete the live rule. |
| Turns of an encoder | Update the live rule to the definition before the first turn. |

A redo is the change again, with the revision that the undo left and the cause `redo`.
The redo of a pad press or a turn is no intent, because an intent expects nothing:
it arms, disarms, creates or updates the rule as the press or the turn left it.

- **The inverse restores a state, and never replays a gesture.**
  The undo of a turn of +3 is no turn of −3: an encoder stops at its bounds, and the turn may have changed less.
- **A surface reads the inverse from answers.**
  The definition before an update is the one whose revision the update expected, so a success says that the surface held the right one.
- **A definition that comes back is a new definition.**
  It has the name and the content of the one that went, and a higher revision than that one ever had.
  A change that still expects the one that went changes nothing.
- **A scenario that comes back starts again.**
  Activating it starts it at its first step, whichever step it was at.
- **A state that moved on is no state to undo.**
  When someone else changed the thing since, the inverse fails with `changed` and changes nothing.
  The surface drops the step.
- **A history ends with its run.**
  An inverse that names another run fails with `other-run`, and the surface drops its whole history.
  A process that started again armed nothing, and no undo of an earlier run arms a fault in it.
- **Some operations have no inverse.**
  Fire, a step, keep, a reload, a save, the start and the stop of an environment, of a recording and of a capture, and closing a session.
  What they did has happened.

The [journal](../domains/observation.md#the-journal-only-grows) records an undo as a new change with its cause, next to the change it took back.

## The journal

Each environment that runs has a [journal](../domains/observation.md#journal) of its own.
The engine keeps its recent entries in memory, and the control plane hands them out as they are.

```json
{ "from": { "surface": "cli" }, "operation": "query-journal", "environment": "orders-sim", "after": 41 }
```

```json
[
  { "id": 42, "at": 4200000000, "revision": 12, "from": { "surface": "pad" }, "change": "rule-armed", "rule": "slow-order-producers" },
  { "id": 43, "at": 9700000000, "revision": 13, "from": { "surface": "ui", "cause": "undo" }, "change": "rule-disarmed", "rule": "slow-order-producers" }
]
```

| Part of an entry | Says |
|---|---|
| `id` | The [ID](identity.md#journal-entry-id) of the entry: a number that the environment counts from 1. |
| `at` | The time, in nanoseconds on the clock of the environment, as the timing of a call says it. |
| `revision` | The [revision](#revisions) that the change left. An entry of what left none, such as a firing, leaves the key out. |
| `from` | The surface that the change came from, and its cause when it has one, in the form of the `from` of a [request](#where-a-request-comes-from). The surface can be one that no request names: `step`, `lifetime` or `file`. |
| `change` | Which change it is, such as `rule-armed`. The other keys of the entry belong to the change. |

| `change` | Other keys |
|---|---|
| `rule-armed`, `rule-disarmed`, `rule-fired-by-hand` | `rule`: the name of the rule. |
| `parameter-changed` | `rule`, `parameter`, `old` and `new`. The parameter is `chance`, `every`, `delay`, `jitter`, `throttle`, `error` or `lifetime`, and both values are text. |
| `live-rule-made`, `live-rule-changed`, `live-rule-kept`, `live-rule-deleted` | `rule`. |
| `definition-created`, `definition-changed`, `definition-deleted` | `definition`: the kind and the name, such as `{ "rule": "slow-order-producers" }`. |
| `incident-applied`, `incident-emulated` | `incident`: the incident, with its `consequences`. |
| `recording-stopped` | `why`: `fell-behind` or `could-not-write`. The recording of the environment stopped, and nobody stopped it. It has the surface of the start of the recording, and no revision. |

- **A query answers with the entries that the engine keeps, the oldest first.**
  With `after`, it answers with the entries that came after the entry of that ID.
  An environment that does not run has no journal, and the query fails as every command on something live does.
- **Every new entry is a `journal-entry` of the [live stream](#live-stream).**
  It reaches every client, and the entries of one environment come in the order of their IDs.
  A change of a definition comes once for every environment that runs.
- **The snapshot holds no journal.**
  A client that shows a journal reads it with a query, and takes every later entry from the stream.
  An entry can come twice that way, and the client knows it by its ID.
  A client that got a new snapshot asks for the entries after the last one it holds.
- **The IDs have no gap.**
  A client that gets an entry whose ID is not the next one knows that the engine let go of the entries in between.
- **The revision ties an entry to an answer.**
  A change answers with the revision it left, and the entry of that change holds the same one.

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
| **Present** | The ID of the [presence](#presence) of this client. It is the first message, and comes once. |
| **Snapshot** | The full state, sent when a client connects: the [run ID](identity.md#run), every definition of the workspace, the workspace as a whole (when it [writes back](storage.md#saving), whether it is [read-only](storage.md#read-only-workspaces), and what waits for a save), and every environment that runs with its backend, its rules, its sessions and the recording that it writes. Whatever has a revision comes with it. It holds no [journal](#the-journal). |
| **Call** | One call event: request summary, verdict and explanation. |
| **Stats** | Matches, firings and remaining budget per rule, and calls per API, at a fixed interval. |
| **Rule changed, removed** | A rule of an environment changed state, or a live rule was made or changed its definition, with its revision. A live rule left its environment: someone deleted or kept it. |
| **Definition changed, deleted** | Someone created, updated or deleted a definition of the workspace, through the control plane or with a [reload](storage.md#reloading). A change holds the revision of the definition. |
| **Workspace changed** | What waits for a [save](#saving) changed: an edit of a workspace that writes on save, a save, or a reload. It holds the workspace as the snapshot does. |
| **Scenario changed** | The active scenario changed, or advanced a step, with its revision. |
| **Recording started, stopped** | A recording of an environment began or ended, and why it ended when nobody stopped it. |
| **World changed** | An incident changed the World, or the Upstream backend's overlay. |
| **Session opened, identified, software named, closed** | Session lifecycle. The client software comes with the identification, or later with the first `ApiVersions` request that names it. |
| **Journal entry** | Something changed in an environment: the [entry](#the-journal) of its journal, with its ID, the time, the revision that the change left, and the surface and the cause that it came from. |
| **Environment started, stopped** | An environment came up, with its backend, its rules and its sessions, or went down. |
| **Following** | The last subscription of this client is in force: the server sends every call it names from here on. |
| **Subscription refused** | The last subscription of this client cannot be followed, and why. The client follows no calls. |

A message about something live names its environment.
A client that applies every message to the snapshot holds what the process holds.

Clients can subscribe to call events with a selector,
so a UI that only shows `orders.*` does not receive every call.
A client follows the calls of one environment at a time, and no calls until it subscribes.
A new subscription replaces the one before it.
A selector without conditions follows every call.
Every other message reaches every client, except two: the presence of a client and the answer to a subscription reach the client that they are about.

## Over the network

`control_server` serves the control plane, and `control_client` speaks to it.
The server listens on one address: `127.0.0.1:7092`, until its configuration names another.
Every body and every frame is JSON, and it is the `serde` form of a type of `control_api`.
No other form exists, so a client in the browser and one on a native target send the same bytes.

| What | Where |
|---|---|
| An operation | `POST /api/operations` |
| The live stream | `GET /api/stream`, which upgrades to a WebSocket |
| The live stream of a client that comes back | `GET /api/stream?presence=9f86d081884c7d659a2feaa0c55ad015`, with the ID of the [presence](#presence) that it was |
| The [health](#health) of the process, for a probe | `GET /health` |

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
Its body is one object, an `OperationRequest`: an `AnyOperation`, with [where the request comes from](#where-a-request-comes-from) next to its keys.
`{ "from": { "surface": "cli" }, "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }`.
The status of the response says what its body is.

| Status | The body is |
|---|---|
| `200 OK` | The answer of the operation, in the form of its `Answer`. An answer of `()` is `null`. |
| `422 Unprocessable Content` | The error of the operation, in the form of its `Error`. |
| `501 Not Implemented` | A `RequestError::NotServed`: the process does not serve the operation yet. |
| `400 Bad Request` | A `RequestError::Unreadable`: the body is larger than the server takes, it is no operation that the server knows, it does not say where it comes from, or it claims a surface that the engine names itself. |

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

- **The presence comes first, and the snapshot next.**
  The first message is a `present`, with the ID of the [presence](#presence) of the client.
  It is the presence that the query of the stream names, when the server still holds that one, and a new one otherwise.
  A query that names no presence, or what is no ID of one, belongs to a client that is new.
  The second message is a `snapshot`, and every message after it is a change, a call or the answer to a subscription.
- **A message is a state, not a difference.**
  A change says what a rule, a definition or an environment is now.
  A change can repeat what the snapshot holds already, and applying it again changes nothing.
  A call, what happens to a session and a `journal-entry` are no states: each says what happened, once.
- **The later revision is the newer state.**
  A client that already holds a later revision of a thing, from the answer to its own change, drops a message that brings an earlier one.
- **A snapshot that names another run is another process.**
  The client drops what it held, its [undo history](../domains/control-surfaces.md#the-history-of-a-tab) included.
- **A client that falls behind gets a new snapshot.**
  The server keeps a backlog of changes for every client, as large as its configuration says.
  A client that falls further behind would miss changes, so the server sends it a new `snapshot` and goes on from there.
  Calls do not count: a client that falls behind the calls of its environment misses the oldest of them.
- **A rule of the workspace follows its definition.**
  A `definition-changed` of a rule gives the rule that definition in every environment that runs, with the state it has there.
  Its revision in an environment is the later of the two: the one of the message, and the one that the rule had there.
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
  It closes with `1011` too when the system gives no randomness for the ID of a presence.
- **The server pings the client, and the client answers.**
  A ping and its answer are the ping and the pong of the WebSocket, and no message of the stream.
  The server ends the stream of a client that [went silent](#presence), without a close frame.
  A client that stops reading its stream answers no ping, so the server takes it for gone.
- **A client can end the stream with a close frame.**
  The server notes that the client left, and only then lets go of the connection.
  A client that waits for the connection to close therefore knows that the grace period of its presence has begun.
  A connection that breaks without a close frame ends the stream the same way.

### Where the changes come from

The server follows the changes of the host, whoever makes them: an operation of the control plane, a pad,
a rule that a firing spent or whose lifetime ended, a client that connects or leaves, or `gmq` itself.
Each change names what changed, and the server sends the state that thing has when it sends the message.
A change whose subject is gone again by then sends nothing, because a later message says that it went.

| The host tells | The server sends |
|---|---|
| An environment started or stopped. | `environment-started` with what runs in it, or `environment-stopped` once its sessions closed. |
| A definition changed through the host, or went. | `definition-changed` with the definition, or `definition-deleted`. |
| A reload changed definitions. | `definition-changed` or `definition-deleted` for each of them, and `rule-changed` for every armed rule that follows its definition. |
| A rule armed or disarmed in an environment, or a live rule was made or changed. | `rule-changed` with the rule and its state. |
| A live rule left its environment. | `rule-removed`. Keeping a live rule sends `rule-removed`, `definition-changed` and `rule-changed`, in that order. |
| A session opened, was identified, named its software, or closed. | The message of the session. |
| The journal of an environment got an entry. | `journal-entry` with the entry, as the journal holds it. It comes beside the messages of the change that it records, before or after them. |
| A recording of an environment started, or ended. | `recording-started` with the ID of the recording, or `recording-stopped` with its ID and, when it stopped by itself, `why`. The end of a recording that the stop of its environment ends comes before `environment-stopped`. |

A rule of the workspace that someone creates sends `definition-changed` alone: every environment holds it, disarmed.
`stats` comes for every environment that runs, at the interval of the server's configuration.
A server that falls behind the changes of the host sends every client a new `snapshot`.

### What the server does not send yet

Every operation of `control_api` is served, so no answer is `501` today.
`RequestError::NotServed` stays for a process that knows fewer operations than its client:
an older process, or one built without a part.

No operation queries the calls of a recording with a selector, saved or draft, or gets its index yet: those arrive
with the index. A recording starts, stops, is listed and opens: `start-recording` and `stop-recording` name an
environment, `list-recordings` lists every recording of the process, and `open-recording` names one by its ID and
answers with what it is, how many entries it holds and how it ended.

No operation lists, gets, creates, updates or deletes a scenario yet.
So nothing refuses yet a scenario that owns a rule with the name of a live rule: that check arrives with those operations.
The snapshot holds every scenario of the workspace, and a change of one is a `definition-changed` or a `definition-deleted` message, as for every kind.

The journal has no entry of the activation of a scenario or of a step yet: those arrive with their operations.
Of a recording, the journal records the stop that nobody asked for, and neither a start nor a stop by request:
what was recorded has happened, and the live stream tells both.
No backend applies or emulates an incident yet, so no journal has an entry of one.
No change has the surface `step` yet.

The scenario of an environment has no revision yet: no operation activates a scenario or steps one.
Its revision, and the `expects` of those operations, arrive with them.

The UI sends no `expects` yet.
It keeps the run and every revision that the stream tells, and sends them with its changes once it keeps an undo history.

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
A press in latch sends *arm* or *disarm*, and never *toggle*, so two surfaces pressing the same pad do not undo each other.
A press in hold sends *hold*, and its release sends *release*: the engine [counts the holds](../domains/control-surfaces.md#modes-of-play) of a rule, and disarms it when the last one goes.
A hold belongs to the [presence](#presence) that its request names, and ends with it, so a surface that goes away without a release leaves no rule armed.
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
