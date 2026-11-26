# Open questions

The first part lists the decisions that are still open.
The second part lists the decisions that were settled, with the reason and the spec that now holds them.

## Still open

### Protocol details to verify

- **Does a sessionless Fetch rewrite work for every client?**
  The Upstream backend declines [fetch sessions](architecture/backends.md#fetch-sessions) by rewriting each Fetch.
  The rewrite needs a check against KIP-227 and against the Java client and librdkafka,
  including the extra bandwidth for clients with many partitions.
- **Does the forced rejoin behave the same for both group protocols?**
  [Emulating a rebalance](architecture/backends.md#emulating-incidents) relies on how clients react to
  `REBALANCE_IN_PROGRESS` and `FENCED_MEMBER_EPOCH`.
  Both need a test against real clients.

### Identity

- **Is `client.id` with `host` enough to identify a client instance?**
  The [client context](domains/session.md#client-context) uses that pair.
  Clients behind one NAT address or one sidecar share a host,
  and replicas often share a `client.id`.
  A selector cannot tell such clients apart, and their learned fields mix.

### Scenarios

- **Should a scenario carry expectations?**
  `gmq run` does not judge the client under test.
  An `expect` section, such as "a Produce on `orders.eu/3` succeeds within 30 s",
  would give CI a result without a test library.
- **Should Capture anchor steps on calls instead of time?**
  A [captured](domains/scenarios.md#capture) scenario replays the pauses between changes.
  The client rarely is at the same point after the same pause,
  so a step that ends after a number of calls may reproduce better.
- **When does a rule need to react to another rule?**
  Sequence steps cover "when this happened, arm that".
  A trigger that reacts to rules or incidents would also work in Live and Chain mode,
  but it needs an answer to *same call or next call?* and *for how long?*

### World data

- **Can the schema registry lie?**
  The built-in [schema registry](architecture/backends.md#schema-registry) answers truthfully, and rules do not apply to it.
  A registry that is slow, down or serves another schema is a failure worth testing.
  It speaks HTTP, so it needs its own selectors and lies.
- **Does a virtual record's key match its partition?**
  A real producer picks the partition from the key.
  A consumer that relies on that, such as a join over co-partitioned topics,
  needs keys that hash to the partition they appear in.

### Storage

- **Where do a World's initial records live?**
  A [seeded](domains/world.md#seeding) World can start with recorded records.
  A world definition is in git, and recordings are not.
  The records need a home that CI can reach.
- **Does write-back keep the comments inside the definition it writes?**
  A [write](architecture/storage.md#writing-back) keeps every byte outside the definition that changed, and the UI asks before it loses a comment inside it.
  Keeping those comments too needs an edit of the text in place, which changes only the values that differ.
- **Does a definition get an ID of its own, next to its name?**
  A definition has a name and no ID.
  Within a run, its [revision](architecture/identity.md#revision) and the journal tell it from a definition that [had its name before](architecture/identity.md#two-definitions-of-one-name).
  A rename, and recognising a definition in a later run or in another copy of the workspace, would each need an ID.
  The second one also needs a committed file next to the definitions that maps names to IDs.
  Nothing needs either yet. See [not decided yet](architecture/identity.md#not-decided-yet).
- **Does GaslightMQ watch the files of the workspace?**
  It loads them once, and reads them again when someone asks for a [reload](architecture/storage.md#reloading).
  A watcher would ask for the reload itself.
  It then has to cope with a file that an editor saved half-way and with a branch that someone checks out file by file.
- **Can several people share one engine?**
  The [journal](domains/observation.md#journal) records which surface changed something, not which person.
  A shared engine in a staging cluster needs identities and permissions on the control plane.

### Backends

- **How should "leader moves" reach the proxy?**
  The first version [does not emulate it](architecture/backends.md#what-the-first-version-does-not-emulate).
  One way is split routing with leader epoch translation.
  Another is to really move the leader through the admin API,
  which gives up the rule that the proxy never changes the real cluster.
- **When do transactions come to the fake broker?**
  They bring "producer fenced" to the Simulated backend,
  and incidents for a transaction that hangs or times out.

## Settled

### Combining rules

| Question | Decision | Reason |
|---|---|---|
| Does scope specificity come before the mode? | No. Scope gives no priority. The live set resolves conflicts by recency, a scenario by its mode. See [modes](domains/scenarios.md#modes). | Specificity has no definition for arbitrary selectors, and it contradicts "follows what the person just did". |
| What does a chance trigger mean in Chain mode? | Each rule rolls its own die. See [Chain](domains/scenarios.md#chain). | Slices of one roll couple the rules. Only rules that really conflict lose probability. |
| Can rewrites of the same target merge? | No. One wins and the other is shadowed. Different aspects of one partition do not conflict. See [how lies combine](domains/lies-and-incidents.md#how-lies-combine). | Chaining needs an order that Live mode does not have. |
| Do delays add up or take the maximum? | They add up. | Every rule that fired changes something, and the explanation sums up. |
| Does priority hold across the two passes? | No. A lie that ends the call before forwarding pre-empts a rule that waits for the truth. See [two evaluation passes](architecture/request-pipeline.md#two-evaluation-passes). | The truth does not exist yet. Forwarding first would turn "refused, nothing written" into "written, then refused". |

### Triggers and counters

| Question | Decision | Reason |
|---|---|---|
| What is the default counting scope? | One counter per rule, shared by all sessions. `per: session` splits it. See [counters](domains/rules.md#counters). | The selector already narrows whose calls count, `nth: 3` fires once per arming, and lifetimes count the same way. |
| Do count triggers count firings of other rules? | No. A rule counts its own matches. | Rules never see each other. |

### Scenarios

| Question | Decision | Reason |
|---|---|---|
| Can several scenarios be active at once? | No. One scenario plus the [live set](domains/scenarios.md#the-live-set). Scenarios share rules by name. | Two scenarios need a conflict rule between them. |
| What does Capture do with encoder turns? | It keeps the parameters from the moment of arming. Later, a change becomes one step once the value settles. | A step per encoder tick drowns the story. |
| When does GaslightMQ check that a backend supports the incidents of a scenario? | When someone activates the scenario in an environment. The workspace checks the references of a scenario alone. See [portability](domains/scenarios.md#portability). | A workspace serves every environment, and each has a backend of its own. |
| What identifies a step? | Its name, unique within its scenario. See [identity](architecture/identity.md#scenarios). | A step keeps its name when someone puts a step before it, and its place would not. |

### Backends

| Question | Decision | Reason |
|---|---|---|
| How far does incident emulation go on Upstream? | As far as the overlay can hold all the state: broker down, unresponsive and back, rebalance as a forced rejoin, records expired, committed offset reset, producer fenced. See [capabilities](architecture/backends.md#capabilities). | Anything more needs split routing or epoch translation. |
| Does a session get its own connection to the real cluster? | Yes. The Upstream backend opens one for every connection of a client. See [connections to the cluster](architecture/backends.md#connections-to-the-cluster). | A broker handles the requests of one connection one at a time, so on a shared connection one Fetch that waits holds back every other client. |
| Does the Upstream backend give up on a broker that does not answer? | No. The call stays in flight until the client gives up. The backend only limits how long it waits for a connection to open. | A Fetch and a `JoinGroup` wait for as long as the client asked, and the client has a timeout of its own. |
| Can a proxy front a fake broker? | Yes. That is how a fake cluster lies: a proxy in front of a [broker](architecture/backends.md#a-broker-never-lies), in one process, without a socket. | A broker never lies, so lies still happen in one place. |

### Runtime

| Question | Decision | Reason |
|---|---|---|
| Do crates come in a blocking and an async variant? | No. Logic is sans-IO, and the IO edge is async only. See [the IO edge is async](architecture/crates.md#the-io-edge-is-async). | A state machine has no blocking or async side to choose. A second edge would be a second runtime. |
| What is a session made of? | A transport and a pipeline. The pipeline holds the session's state, including the rule snapshot. See [parts](domains/session.md#parts). | The transport is the only part that does IO, so everything a rule decides runs in a test without a socket. |
| Does a session survive a simulated disconnect? | No. A hang-up or a broker that goes down closes the real connection, and the session ends with it. The client context and the shared counters stay. See [disconnects](domains/session.md#disconnects). | The client must see a real close. A reconnect is a new connection, as it is with a real broker. |
| Is a call to a broker a plain function call? | No. The broker is a state machine that answers under the call ID, at once or later. See [the broker is a state machine too](architecture/crates.md#the-broker-is-a-state-machine-too). | A Fetch that waits for records and a `JoinGroup` that waits for members cannot return at once. |
| What does the recorder do when it cannot keep up? | It stops the recording and says so. It writes on a thread of its own, behind a queue whose size the configuration sets. See [recordings](architecture/storage.md#recordings). | A session never waits for a disk, and a queue without a limit ends with a process that has no memory left. |

### Storage and naming

| Question | Decision | Reason |
|---|---|---|
| Are names unique across kinds? | No, per kind. The UI shows the kind where kinds mix. See [names](architecture/storage.md#names). | Every reference already says which kind it means. |
| What is a name? | A slug, like the name of a resource in Kubernetes. It is the identity of its definition, and every reference uses it. A definition has no other ID. See [names](domains/rules.md#names). | One string names a definition in a file, a command, an explanation and the statistics. |
| Where does free text go? | In an optional title on the definition itself. See [titles](domains/rules.md#titles). | Nothing references a title, so it changes without breaking anything. |
| Can someone rename a definition? | No. Another name means a new definition, and the old one is deleted. | A rename had to rewrite every reference, and its history hung on a name that no longer existed. |
| What is the default for recordings? | Metadata-only on Upstream, full on Simulated. See [privacy](architecture/storage.md#privacy). | The real cluster can carry sensitive data. The World's data is synthetic. |
| Does GaslightMQ need a database? | No. Definitions are files, live state is memory, recordings are append-only segments in files. See [storage](architecture/storage.md). | A recording is a write-once stream that travels as one file, and selectors already are the query language. |
| What survives a restart? | Definitions and recordings. Nothing is armed. See [a restart is a clean slate](architecture/storage.md#a-restart-is-a-clean-slate). | A fault injector that resumes on its own is a hazard. |
| What happens to live rules in a crash? | A [scratch file](architecture/storage.md#scratch) keeps their definitions. They load disarmed. | Work is not lost, and git stays clean. |
| Where does the history of changes live? | In the [journal](domains/observation.md#journal): in memory, and inside recordings. | A recording then explains itself, and Capture has one source. |
| How many environments does a process run? | One or several, each with its own live state. The UI switcher only changes which one it shows. See [several environments in one process](architecture/storage.md#several-environments-in-one-process). | One command and one UI address cover a proxy and a fake cluster side by side. |
| Where does a fake cluster's data come from? | From the topic's [record source](domains/world.md#records): virtual records from a template or a schema, a file, a recording, or what clients produce. | A consumer test needs data without a producer, and a virtual record is the same at the same partition and offset. |
| Do records have schemas? | Yes. A topic names a format and a schema, and an environment names a [schema registry](architecture/backends.md#schema-registry). | Avro and Protobuf consumers cannot read a record without one. |
| Where do secrets live? | In files outside git, referenced by path. An environment variable works as a fallback. See [secrets](architecture/storage.md#secrets). | Platforms mount secrets as files, files rotate, and a reference cannot leak through git. |
| Does a definition hold anything that GaslightMQ keeps? | No. A definition holds what a person writes: its name, its title and its content. Its revision, its armed state and its counters sit beside it, in memory, and never in a file. See [what a definition holds](architecture/identity.md#what-a-definition-holds). | A file is what someone defined. A file that someone wrote by hand and one that the UI saved look the same, and a copy of a file is just another definition. |
| Does a definition have a UUID? | No. Its name is its identity, and the revision tells it from a definition that had its name before. See [two definitions of one name](architecture/identity.md#two-definitions-of-one-name). | Nothing read the UUID. What it guarded, a definition that someone deleted and made again, the revision guards too, and it also guards the state of a rule and of a scenario, which had no UUID. |
| Does GaslightMQ need a store for what it keeps, as Kubernetes has? | No. Its memory is that store, and a restart is a clean slate. | Nothing has to recognise a definition across runs yet, and a second store has to be kept in step with files that people edit. |
| When does GaslightMQ read the files of the workspace? | Once, at the start, and again when someone asks for a reload. It does not watch them. See [loading](architecture/storage.md#loading). | A file that someone edits, or a branch that someone checks out, never changes an armed fault behind someone's back. |
| What keeps a write from the UI from overwriting an edit by hand? | The store checks the file before it writes: it compares a hash of the bytes with the one it read, writes nothing when they differ, and replaces the file in one step. See [the check before a write](architecture/storage.md#the-check-before-a-write). | The engine does not see an edit until a reload, so the file has to say that it changed. |
| What does a write from the UI do to a file that someone wrote by hand? | It rewrites the one definition that changed, in one fixed form, and leaves every other byte. The UI asks before a comment is lost. See [writing back](architecture/storage.md#writing-back). | People version the files with git, so a change of one definition has to be a diff of one definition. |
| How are two clients kept from overwriting each other? | Whatever a request changes has a revision, and a change can name the one it expects. See [revisions](architecture/control-plane.md#revisions). | Two tabs, `gmq`, a scenario and a reload change the same process. |
| Is a revision counted per definition? | No. One counter of the process gives every revision, so none comes back within a run. See [revision](architecture/identity.md#revision). | A counter per definition starts again when someone deletes a definition and makes it again, and a change that expects the old one would overwrite the new one. |
| What tells the revisions of two runs apart? | The run ID: random, made at the start, and in the snapshot. A change that expects a revision names its run, and another run refuses it. See [run](architecture/identity.md#run). | Revisions start again with every run, and an undo from before a restart could arm a fault in a process where nothing is armed. |
| Does arming a rule say which definition someone saw? | Yes. A rule in an environment has one revision, the later of its definition and its state. | Someone who arms a rule after another client changed it would arm a fault they have not seen. |
| How is a recording identified, and a journal entry? | A recording by its environment and the instant it started, which is the name of its directory. A journal entry by a number that its environment counts. See [identity](architecture/identity.md#recording-id). | A file name that says what it holds, and an order that needs no clock. |

### Control surfaces

| Question | Decision | Reason |
|---|---|---|
| Where does the undo history live? | In the browser tab, in memory. The engine keeps none. See [undo and redo](domains/control-surfaces.md#undo-and-redo). | A tab takes back its own changes alone, and the engine stays a clean slate. |
| How does an undo reach the engine? | As an ordinary operation: the inverse of the change, with the run and the revision that the change left and the cause `undo`. See [undo](architecture/control-plane.md#undo). | The journal records it as a new change, and the control plane needs no second way to change something. |
| What happens to an undo when the state has moved on? | Nothing changes, and the UI drops the step and says so. | An undo can arm a fault against real clients, so the last writer does not win. |
| What can an undo not take back? | What happened: a lie or an incident that fired, a step, and what clients did about them. | A client that got an error has already reacted. |
| Who says which surface a change came from? | The request, for a pad, ⌘K, the UI, `gmq`, a test or another client. The engine, for a step, a lifetime and a file. See [surfaces](domains/observation.md#surfaces). | The journal says where every change came from, also one that nobody asked for. |
| Does a press in latch toggle the pad? | No. It says the state it wants, *arm* or *disarm*. See [modes of play](domains/control-surfaces.md#modes-of-play). | Two people who press one pad to arm it would disarm it again. |
| What happens when two people hold one pad? | The engine counts the holds, and disarms the rule when the last one goes. | The first release would disarm a rule that someone still holds. |
| How does a release know which hold it ends? | The engine numbers every hold, the press answers with its number, and the release names it. A hold belongs to the arming of its rule, so a disarm ends it. See [modes of play](domains/control-surfaces.md#modes-of-play). | A count alone lets a release that comes after a disarm end the hold of someone who held the rule again since. |
| Does the release of a hold disarm a rule that something else armed? | No. Only a rule that a press in hold armed disarms when its last hold goes. A hold of a rule that a press in latch, a command, a scenario step or a create armed answers with a hold, and its release changes nothing. See [modes of play](domains/control-surfaces.md#modes-of-play). | A hold would otherwise take back what someone else armed on purpose. |
| What ends a hold whose holder went away? | The end of its presence. A client of the control plane that is there is a [presence](architecture/control-plane.md#presence): it opens with the live stream of the client, and every request of the client names it. The engine ends the holds of a presence that ended. A hold that names no presence, as one of `gmq` or of a test, ends by its release or by a disarm alone. | A tab that closes, crashes or loses its network never sends its release, and the fault would stay armed against real clients until someone notices. |
| Does a connection that drops end a presence at once? | No. The control plane keeps the presence for a grace period, 10 seconds by default, and a client that comes back within it keeps its holds. A reload is a new page and a new presence, so the holds of the page before end with the grace period. See [presence](architecture/control-plane.md#presence). | A network that drops for a moment must not let go of a pad that someone still holds, and the server cannot tell a tab that closed from one that lost its connection. |
| How does the control plane notice a client that went away without closing its connection? | It pings every client of the live stream at an interval, 15 seconds by default. A client that sends nothing between one ping and the next, or that takes no message for as long, has gone silent, and its stream ends as if its connection had closed. See [presence](architecture/control-plane.md#presence). | A client on another machine whose network drops closes nothing, and the system gives up on its connection only after minutes. |
| How does a client that comes back prove which presence it is? | By the ID of the presence: 128 random bits that the server made, which the client names when it opens its stream again. It is no number that the process counts. | A tab that comes back after a restart, or after the grace period, would otherwise take the number of another tab for its own, and keep the holds of that tab alive. |
