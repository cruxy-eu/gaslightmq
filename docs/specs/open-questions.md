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
  [Emulating a rebalance](architecture/backends.md#emulating-events) relies on how clients react to
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
  A trigger that reacts to rules or events would also work in Live and Chain mode,
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
- **Does write-back keep comments and formatting?**
  The UI writes edits back to [workspace files](architecture/storage.md#workspace) that people also edit by hand.
  Either the engine keeps hand-written comments and layout,
  or it owns the formatting and says so.
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
  and events for a transaction that hangs or times out.

## Settled

### Combining rules

| Question | Decision | Reason |
|---|---|---|
| Does scope specificity come before the mode? | No. Scope gives no priority. The live set resolves conflicts by recency, a scenario by its mode. See [modes](domains/scenarios.md#modes). | Specificity has no definition for arbitrary selectors, and it contradicts "follows what the person just did". |
| What does a chance trigger mean in Chain mode? | Each rule rolls its own die. See [Chain](domains/scenarios.md#chain). | Slices of one roll couple the rules. Only rules that really conflict lose probability. |
| Can rewrites of the same target merge? | No. One wins and the other is shadowed. Different aspects of one partition do not conflict. See [how lies combine](domains/lies-and-events.md#how-lies-combine). | Chaining needs an order that Live mode does not have. |
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

### Backends

| Question | Decision | Reason |
|---|---|---|
| How far does event emulation go on Upstream? | As far as the overlay can hold all the state: broker down, unresponsive and back, rebalance as a forced rejoin, records expired, committed offset reset, producer fenced. See [capabilities](architecture/backends.md#capabilities). | Anything more needs split routing or epoch translation. |
| Can a proxy front a fake broker? | Yes. That is how a fake cluster lies: a proxy in front of a [broker](architecture/backends.md#a-broker-never-lies), in one process, without a socket. | A broker never lies, so lies still happen in one place. |

### Storage and naming

| Question | Decision | Reason |
|---|---|---|
| Are names unique across kinds? | No, per kind. The UI shows the kind where kinds mix. See [names](architecture/storage.md#names). | Every reference already says which kind it means. |
| What is the default for recordings? | Metadata-only on Upstream, full on Simulated. See [privacy](architecture/storage.md#privacy). | The real cluster can carry sensitive data. The World's data is synthetic. |
| Does GaslightMQ need a database? | No. Definitions are files, live state is memory, recordings are append-only files. See [storage](architecture/storage.md). | A recording is a write-once stream that travels as one file, and selectors already are the query language. |
| What survives a restart? | Definitions and recordings. Nothing is armed. See [a restart is a clean slate](architecture/storage.md#a-restart-is-a-clean-slate). | A fault injector that resumes on its own is a hazard. |
| What happens to live rules in a crash? | A [scratch file](architecture/storage.md#scratch) keeps their definitions. They load disarmed. | Work is not lost, and git stays clean. |
| Where does the history of changes live? | In the [journal](domains/observation.md#journal): in memory, and inside recordings. | A recording then explains itself, and Capture has one source. |
| How many environments does a process run? | One or several, each with its own live state. The UI switcher only changes which one it shows. See [several environments in one process](architecture/storage.md#several-environments-in-one-process). | One command and one UI address cover a proxy and a fake cluster side by side. |
| Where does a fake cluster's data come from? | From the topic's [record source](domains/world.md#records): virtual records from a template or a schema, a file, a recording, or what clients produce. | A consumer test needs data without a producer, and a virtual record is the same at the same partition and offset. |
| Do records have schemas? | Yes. A topic names a format and a schema, and an environment names a [schema registry](architecture/backends.md#schema-registry). | Avro and Protobuf consumers cannot read a record without one. |
| Where do secrets live? | In files outside git, referenced by path. An environment variable works as a fallback. See [secrets](architecture/storage.md#secrets). | Platforms mount secrets as files, files rotate, and a reference cannot leak through git. |

### Control surfaces

| Question | Decision | Reason |
|---|---|---|
| Where does the hardware bridge live? | In the browser, over Web MIDI. A CLI bridge can follow as one more control-plane client. See [hardware mapping](domains/control-surfaces.md#hardware-mapping). | The engine stays headless and needs no device drivers. |
