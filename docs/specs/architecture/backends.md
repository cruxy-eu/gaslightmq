# Backends

A backend answers a call after the session's pipeline has decided what to do with it.
A proxy has one of two backends.
Everything in front of the backend is the same for both.

| Backend | Truth comes from | Typical use |
|---|---|---|
| **Upstream** | A real Kafka cluster, seen through the overlay | Proxy in front of a real cluster. Real Kafka semantics, real data. |
| **Simulated** | A [broker](README.md#instances-and-processes) in the same process, with its [World](../domains/world.md) | Fake cluster. Full control, fully reproducible. |

## Contract

A backend:

- answers a decoded request with the truth,
- declares its **capabilities**: which incidents it can apply, and which it can only emulate,
- applies or emulates an incident when the scenario runner fires one,
- reports the endpoints clients should connect to,
- sends clients to the listener that the engine opens for each endpoint,
- lets go of a connection when the engine says that it ended.

A backend never lies.
Lies happen in the pipeline, in front of and behind the backend.
Whatever a backend answers is the truth:
rules match on it, and explanations compare the delivered response against it.

A backend takes each request under its call ID, and answers under the same ID.
The answer does not have to come at once:
a Fetch waits for records, and a `JoinGroup` waits for the other members of the group.
Until the answer arrives, the call stays [in flight](../domains/session.md#state) in its session.

A request tells the backend which connection it arrived on, and which endpoint that connection is on.
A backend takes the requests of one connection in the order in which the engine hands them over.
It answers them in any order.

### Answers

A call ends at the backend in one of five ways.

| The backend | The engine |
|---|---|
| Answers with the truth. | Hands the truth to the pipeline. |
| Has the request, and the request has no response: a Produce with `acks=0`. | Hands nothing to the pipeline, which expects no answer. |
| Got an answer from the cluster that is no response. | Tells the pipeline. The proxy closes the connection when that answer would be the next to leave, and the session records that the proxy closed it. |
| Has no answer, and none will come: it cannot reach the cluster, or it lost its connection to it. | Closes the session at once, and records that the backend closed it. The record of every call in flight says why. |
| Never answers: the broker is unresponsive. | Keeps the call in flight until the session ends. |

The proxy closes a session over an answer that is no response, whoever finds that out:
the backend, which cannot read a response in the bytes of the cluster,
or the pipeline, which gets a response that does not answer the request.
The backend did not go away, and the proxy [cannot keep the answers of the connection in order](request-pipeline.md#failure-inside-the-pipeline).

A backend says "no response" only for a request that has none.
The backend and the pipeline both read that from the request.

A backend never turns a failure into a response.
Kafka has no error for a proxy that lost its cluster,
and a failure of GaslightMQ must never look like an answer of the cluster.
A client whose broker goes away loses its connection, so that is what a failure looks like.

The engine drops a call that nobody waits for any more, such as a call of a session that closed.
The backend then owes no answer.
The cluster may still have taken the request.

### Endpoints

A backend reports the endpoints that accept connections, and every change of them.
The endpoint of a broker that is down is closed.
The endpoint of a broker that is unresponsive is open.

The engine never misses a change, however late it asks.
An endpoint that closed and opened again counts as closed, because every connection to it ended in between.
A broker that goes down and comes back at once still ends the sessions on its endpoint.

For an endpoint that closed, the engine closes the listener and [every session on that endpoint](../domains/session.md#disconnects),
and records that the backend closed them.
Then it opens a listener for every endpoint that opened.
The [environment](storage.md#environments) says where each endpoint listens.

A backend names endpoints, and no address.
A listener can take any free port, and nobody knows that port before the listener is open.
The engine therefore tells the backend the address that each listener got.
From then on, the backend sends clients to that listener:
to its port, on the host under which clients see the proxy.

| The endpoint of a broker | What the backend tells clients |
|---|---|
| Has no listener yet. | Nothing about the broker. A client that asks in between asks again, as it does while a broker starts. |
| Has a listener. | The address of the listener, wherever an answer names the broker. |
| Closed. | Nothing about the broker. The address of its listener is gone with the endpoint. |

A backend rejects a listener that it cannot send clients to,
such as a listener that clients cannot tell from the one of another broker.
A rejected listener changes nothing, and the engine closes it.
A proxy does not start with a listener that its backend rejects.

An endpoint that accepts connections and has no listener gets another try:
its address was taken, or the backend rejected its listener.
The engine tries again after a time that its configuration sets, and whenever the backend reports its endpoints.

An engine that stops lets its listeners finish the change they follow, closes every endpoint,
and waits until every session has ended, so every address is free afterwards.
A stop takes no longer than a time that the configuration sets.
A backend that never says whether it takes a listener, or never lets go of a connection, does not keep a proxy from stopping:
the engine lets go of what is left when that time is over.

### Incidents

The scenario runner hands an incident to the backend.
The backend adds the [consequences](../domains/lies-and-incidents.md#consequences) from the topology of its cluster,
and applies or emulates the incident and its consequences in one step.
It returns the incident with its consequences, and the journal records both.

A backend rejects an incident that it can neither apply nor emulate,
and an incident that cannot happen in the cluster as it is, such as one about a broker the cluster does not have.
A rejected incident changes nothing.

An incident happens as a whole or not at all.
An engine that stops waiting for an incident that it handed over does not take it back.

### Reaching a backend

| Backend | Does IO | How the engine reaches it |
|---|---|---|
| Upstream | Yes, async | Over connections to the real cluster. |
| Simulated | No | In memory. The broker is a state machine. |

See [the backend seam](crates.md#the-backend-seam).

## Capabilities

| Capability | Upstream | Simulated |
|---|---|---|
| Lies of every kind | ✅ | ✅ |
| Broker down, unresponsive, back | Emulated, without failover | ✅ |
| Rebalance | Emulated as a forced rejoin | ✅ |
| Records expired | Emulated | ✅ |
| Committed offset reset | Emulated | ✅ |
| Producer fenced | Emulated | Later, with transactions |
| Leader moves | ❌ in the first version | ✅ |
| Coordinator moves | ❌ in the first version | ✅ |
| Log truncated | ❌ | ✅ |
| Partition added | ❌ | ✅ |
| Topic deleted | ❌ | ✅ |
| Records appended, virtual records | ❌ | ✅ |
| Editable world state | ❌, read-only observation | ✅ |
| Virtual clock | ❌, wall clock | ✅ |
| Reproducible runs | Dice only. The network and the cluster are not reproducible. | ✅ |

A scenario declares the incidents it needs.
A backend rejects a scenario that needs incidents it cannot apply or emulate,
when the scenario loads, not halfway through a run.
The check includes each incident's [consequences](../domains/lies-and-incidents.md#consequences):
"broker goes down" with `failover: true` needs "leader moves".

## Upstream

The Upstream backend forwards calls to a real cluster.
It does IO, and it is async like the rest of the [IO edge](crates.md#the-io-edge-is-async).
It discovers the brokers from the bootstrap address,
and names one endpoint per real broker.
The proxy runs a listener for each endpoint.
The [environment](storage.md#environments) says which ports the listeners use
and which host clients see.

### Connections to the cluster

The backend opens one connection to the cluster for every connection of a client.
That connection goes to the broker of the endpoint that the client connected to.
A broker handles the requests of one connection one at a time,
so a client that waits for records never holds up another client.
The cluster sees each client come and go:
the backend closes its connection when the connection of the client ends.

A connection to the cluster that ends is gone for its client too.
The backend opens no new one in its place.
Every call of that client's connection fails from then on, and the session ends.

The backend never gives up on a broker that takes its time.
A Fetch waits for records, and the client has a timeout of its own.
The backend only limits how long it waits for a connection to open.

### Discovering the brokers

The backend asks the cluster for its brokers before the proxy starts.
A proxy does not start when its cluster does not name its brokers.

The backend asks again from then on:
after a time that its configuration sets, when a call loses its connection to a broker,
and when an answer names a broker that has no endpoint.

| The cluster | The backend |
|---|---|
| Names a broker it did not name before. | Opens the endpoint of the broker. |
| No longer names a broker. | Closes the endpoint of the broker. |

A question that the cluster does not answer changes nothing.
The endpoints stay as the last answer left them.

The backend knows no port.
The engine opens a listener for each endpoint, and gives each broker its port as the
[listeners of the environment](storage.md#brokers) say.
A proxy does not start when those listeners do not hold the brokers of its cluster.
When a broker gets another port, the engine moves its listener, and tells the backend the new address.
The sessions on a listener that moves end, and the engine records that the backend closed them:
the cluster changed, and the clients find the broker at its new address.

### Overlay

The overlay is what the Upstream backend lays over the real cluster's answers.
It holds two things:

- **Address translation.**
  The overlay rewrites the broker addresses in every response that tells one,
  so clients connect to the proxy's listeners.
  It is always on.
  Without it, clients would bypass GaslightMQ after bootstrap.
  See [address translation](#address-translation).
- **Emulated incidents.**
  An incident stays in the overlay for as long as it is in force.

The overlay is part of the backend, not a set of rules.
Its output is the truth.
It takes no part in conflicts between lies,
and rules cannot tell an emulated incident from a real one.
A call keeps the real cluster's answer next to the truth when the overlay changed it,
so the UI can show both.

### Address translation

A client learns the address of a broker from the cluster's answers.
The overlay writes the address of the broker's listener wherever an answer tells the address of a broker.
That address is the host under which clients see the proxy, and the port that the listener got.
The host comes from the environment, and the port from the [engine](#endpoints), once the listener is open.

| Response | Tells the address of |
|---|---|
| Metadata, `DescribeCluster` | Every broker. |
| `FindCoordinator` | The coordinator of each key. |
| Produce, Fetch, `ShareFetch`, `ShareAcknowledge` | The new leader of a partition that moved. |

The overlay changes the host and the port.
Broker IDs, racks, leaders and replicas stay as the cluster tells them.
A `DescribeCluster` answer that lists controllers passes unchanged, because a controller is no broker and has no listener.

The translation works both ways.
A request that arrives on the listener of a broker goes to that broker.
A request that arrives on the bootstrap listener goes to any broker:
the bootstrap server of the environment, or another broker when that server takes no connection.
The overlay learns the real address of each broker from the same answers, before it rewrites them.

A cluster can name a broker that has no listener yet, such as a broker that just joined.
A client never learns the real address of that broker.
Metadata and `DescribeCluster` leave the broker out, and a coordinator on it reads as `COORDINATOR_NOT_AVAILABLE`.
The backend opens the endpoint of the broker, the engine opens a listener for it,
and the next answer names the broker at that listener.

### Emulating incidents

| Incident | Emulation |
|---|---|
| Broker down | Close the broker's listener. Drop its connections. Leave it out of Metadata. Report the partitions it led as leaderless, and the coordinators it hosted as unavailable. |
| Broker unresponsive | Accept connections on the broker's endpoint, never answer. |
| Broker back | Open the listener again. Stop changing Metadata. |
| Rebalance | Answer each member's next heartbeat with `REBALANCE_IN_PROGRESS`, or with `FENCED_MEMBER_EPOCH` for the consumer protocol, so the member rejoins. The real coordinator decides what happens next. |
| Records expired | Answer fetches below the new log start offset with `OFFSET_OUT_OF_RANGE`. Report the new offset in `ListOffsets` and in Fetch responses. The emulation ends when the real log start offset passes it. |
| Committed offset reset | Answer `OffsetFetch` for the partition with the new offset, until the group commits that partition again. |
| Producer fenced | Refuse the producer's transactional calls with `PRODUCER_FENCED`, until it calls `InitProducerId` again. |

An emulation only fools clients that go through GaslightMQ.
The UI marks emulated incidents as emulated.

### What the first version does not emulate

"Leader moves" and "coordinator moves" look like one more rewrite, and are not.

- **Split routing.**
  After a move, a client sends a partition's calls to the new leader's endpoint,
  together with partitions that broker really leads.
  The backend has to split one Produce or Fetch over two real brokers and merge the answers,
  including the long-poll behaviour of Fetch.
- **Leader epochs.**
  A believable move bumps the leader epoch.
  The real brokers do not know that epoch,
  so the backend has to translate epochs in every request and response that carries one.

The *Leader flap* preset covers the most common test without either:
a refusal with `NOT_LEADER_OR_FOLLOWER` and truthful Metadata.
The client refreshes its metadata, finds the same leader and retries,
as it does after an election that brought the leadership back.

"Log truncated" would have to hide real records and renumber new ones.
The Upstream backend does not emulate it.

### Fetch sessions

A client with an incremental fetch session only names the partitions that changed,
so most Fetch requests would not carry the fields selectors test.

The Upstream backend makes every Fetch sessionless before it forwards it.
The real broker never creates a fetch session,
and the client keeps sending full Fetch requests.
A Fetch response always says that it is part of no session.
The Simulated backend never grants a fetch session.

A Fetch that continues a session only names what changed in it, so the backend cannot forward it.
The backend answers it with `FETCH_SESSION_ID_NOT_FOUND`, as a broker does that dropped the session.
The client then sends a full Fetch.

The cost is bandwidth: a client that reads many partitions sends larger Fetch requests.

### What lies leave behind in the real cluster

A lie only changes what the client is told.
On the proxy, the real cluster still reacts to what it did or did not receive.

| Lie | Effect on the real cluster |
|---|---|
| Refusal before forwarding | None. The refused part never arrived. |
| Refusal after the response | The request took effect. Records are written, offsets are committed, the transaction is ended. |
| Silence, hang-up or delay before forwarding on `Heartbeat` or `JoinGroup` | The coordinator can expire the member and start a real rebalance. |
| Silence or hang-up in the middle of a transaction | The transaction stays open until it times out. Until then it holds back read-committed consumers of its partitions. |
| Producer fenced (emulated) | The same, until the producer calls `InitProducerId` again. |

The UI shows these effects next to the preset.

## Simulated

The Simulated backend is a broker in the same process.
The engine hands each request to the broker in memory.
No socket sits between them.
The broker is a sans-IO state machine.
It answers at once, or later when a wait ends.

The broker answers from its World.
It implements the Kafka protocol against the World's state:
appending records, serving fetches from stored and [virtual records](../domains/world.md#records), tracking idempotent producers
and running the group protocol, classic and consumer.

The broker applies incidents directly to the World,
so every call and every client sees one consistent state.

The broker owns the virtual [clock](README.md#clock), and the proxy in front of it reads the same clock.
Time triggers, timeouts and arriving records all advance together,
and tests can speed the clock up or step it.

The first version leaves out transactions.
The broker does not advertise the transactional APIs,
and "producer fenced" is not available on it yet.

### A broker never lies

A broker answers truthfully from its World.
Every lie about a simulated cluster comes from the proxy in front of the broker.
Lies therefore happen in one place, for both backends.

A broker also runs alone, with its own listeners and no proxy.
It is then a truthful fake Kafka.
Incidents still change its World, and no rule applies.

### Staying faithful

A broker is only useful when clients cannot tell it from a real cluster.
Two checks keep it honest:

- A scenario that only uses lies gives the same verdicts
  in front of a real cluster and in front of a broker.
- A proxy reaches a broker over TCP as well.
  `gmq proxy --upstream` can point at the listeners of `gmq broker`.
  The verdicts match the ones from the function-call path.

## Schema registry

Clients that use Avro or Protobuf look up schemas in a schema registry.
An [environment](storage.md#environments) says which registry belongs to it.

| Environment has | Registry | Used for |
|---|---|---|
| A broker | Built in. It serves the schemas of the workspace over the registry's HTTP interface. | Consumers decode the broker's [records](../domains/world.md#schemas). |
| A proxy in front of a real cluster | The real one, by URL. | The proxy decodes records for data lies, and the UI shows records decoded. |

```yaml
schema_registry:
  listen: 0.0.0.0:8081        # built in, next to a broker
```

```yaml
schema_registry:
  url: https://schema-registry.staging.internal:8081   # the real one
```

The URL of a real registry holds no user name and no password, because [no file holds a secret](storage.md#secrets).
The error for such a URL does not repeat it.

The built-in registry gives every schema an ID that follows from the workspace alone,
so the bytes of a record are the same in every run.
A subject is named after its topic, such as `orders.eu-value`, unless the topic says otherwise.

The built-in registry answers truthfully.
Rules do not apply to it.

## One UI for both

The UI talks to the control plane of the process.
The environment switcher changes which environment the UI shows.
Nothing restarts, and no client loses its connection.

- Views and actions that work on both backends look the same.
- Emulated incidents carry an *emulated* badge.
- Incidents a backend cannot do are disabled, with the reason.
- The World view appears only when the environment has a broker.
- An environment with a broker alone shows the World view and the traffic, and no rules.

A recording made on the Upstream backend can [seed](../domains/world.md#seeding) a World for the Simulated backend.
