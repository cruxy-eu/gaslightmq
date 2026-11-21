# Request pipeline

The pipeline takes one call from the moment its request arrives
to the moment the client receives an answer, or nothing.
It runs inside the call's [session](../domains/session.md).

## Stages

```mermaid
flowchart TB
  A["1 · Receive<br/>read the frame"] --> B["2 · Decode<br/>header and request fields"]
  B --> C["3 · Identify<br/>update session identity and client context"]
  C --> D["4 · Match<br/>armed rules whose selector matches, with targets"]
  D --> E["5 · Trigger<br/>count, roll dice, check time, take from budgets"]
  E --> F["6 · Resolve<br/>fire events, resolve conflicts by priority"]
  F --> G{"7 · Before forwarding<br/>does a lie end the call?"}
  G -->|silence| S["Never answer, stall the connection"]
  G -->|hang up| H["Close the connection"]
  G -->|every partition refused| R["Synthesize the error response"]
  G -->|no| I["8 · Backend<br/>forward what is left, or answer from the World"]
  I --> J["9 · After the response<br/>response pass, rewrites, refusals, modifiers"]
  R --> J
  J --> K["10 · Send"]
  S --> L["11 · Emit call event"]
  H --> L
  K --> L
```

| Stage | Does |
|---|---|
| **1 · Receive** | Reads one frame from the client. |
| **2 · Decode** | Decodes the request header and the fields selectors need. Decoding is lazy: the pipeline only decodes the body when an armed rule tests a body field, when the backend's overlay needs it, or when recording is on. |
| **3 · Identify** | Moves the session from Opened to Identified on its first request, and records the client software from `ApiVersions`. Updates the [client context](../domains/session.md#client-context) with a group or transactional ID the request names. |
| **4 · Match** | Finds the armed rules whose effective selector matches, and each rule's [target](../domains/lies-and-events.md#target). The rule snapshot keeps an index by API, so most calls test only a few rules. |
| **5 · Trigger** | Evaluates each matching rule's trigger: increments the rule's counter, rolls its die, checks its time window. A rule that fires takes from its budget, and does not fire when the budget is empty. |
| **6 · Resolve** | Hands the events of firing rules to the scenario runner. Lets lies that do not conflict through. Resolves conflicts with the snapshot's priority order: the live set first, then the scenario's mode. Marks the losers as shadowed or pre-empted. |
| **7 · Before forwarding** | Applies lies of the before-forwarding [stage](../domains/lies-and-events.md#stage). A silence or a hang-up ends the call. A delay holds the request back. A refusal removes its partitions from the request. A request rewrite changes the request. |
| **8 · Backend** | Forwards what is left of the request to the real cluster, or hands it to the broker in memory. The response is the truth. The backend does not see a request whose partitions were all refused. |
| **9 · After the response** | Merges the refusals of stage 7 into the response. Runs the response pass. Applies rewrites and after-response refusals per target. Then applies modifiers: the summed delay and the maximum throttle. A hang-up at this stage closes the connection instead of sending. |
| **10 · Send** | Writes the delivered response to the client, in the order the client sent its requests. |
| **11 · Emit** | Publishes the call event with request, truth, delivered response, verdict and explanation. |

## Who does the IO

The pipeline is a state machine, and it does no IO.
It decides, and the [session](../domains/session.md#parts) around it acts.
The stages describe the journey of one call.
At these points the session acts for the pipeline:

| Stage | The pipeline | The session |
|---|---|---|
| 1 · Receive | Takes the bytes it is given. | Reads them from the transport. |
| 6 · Resolve | Hands out the events of firing rules. | Passes them to the scenario runner. |
| 7 and 9 · Delay | Asks for a timer, and continues when the timer fires. | Waits on the engine's clock. |
| 7 and 9 · Hang-up | Asks to close the connection. | Closes the transport, which ends the session. |
| 8 · Backend | Hands out the request, and continues when the answer arrives. | Passes the request to the backend, and waits for the answer. |
| 10 · Send | Hands out the bytes of the response. | Writes them to the transport. |
| 11 · Emit | Hands out the call event. | Publishes it on the event bus. |

The pipeline never waits.
While one call waits for a timer or for the backend,
the pipeline takes the next request of the same connection, within the rules of [ordering](#ordering).

The [crate layout](crates.md#the-pipeline-is-sans-io) shows the interface between the two.

## Partial refusals

One Produce or Fetch request can carry partitions that different rules treat differently.
A refusal before forwarding must keep the refused partitions away from the backend,
and still let the others through.

1. Stage 7 removes the refused partitions from the request.
2. Stage 8 forwards the remaining partitions.
3. Stage 9 adds the refused partitions to the response, each with its error code.

The client receives one response that covers every partition it asked for.
Records for a refused partition never reach the backend.

## Two evaluation passes

Some rules test [response fields](../domains/targeting.md#response-fields),
such as "only when the truth was a success".
Those rules cannot match before the backend answers.

The pipeline therefore runs stages 4 to 6 twice:

- The **request pass** runs before forwarding.
  It evaluates every rule whose selector uses only connection, request and learned fields.
- The **response pass** runs after the backend answers.
  It evaluates the rules that also test response fields.

Priority orders the rules within one pass.
Between the passes, the stage decides:

- A silence or a hang-up that won the request pass ends the call before the truth exists.
  The response pass does not run.
  A rule that waited for the truth is pre-empted, even when it has a higher priority,
  and the explanation says so.
- A partition that the request pass refused before forwarding has no truth.
  The response pass skips that partition.
- Everything else from the request pass meets the response pass in stage 9,
  where conflicts resolve by priority as usual.

This is the only place where a rule with a lower priority can beat a higher one.
A live rule that waits for the truth does not stop a scenario rule from ending the call first.

## Events

Events that rules fire go to the scenario runner during resolve.
The runner adds their [consequences](../domains/lies-and-events.md#consequences)
and hands them to the backend.
They take effect on the World or the overlay after this call,
not during it.

## Ordering

Kafka clients expect responses in the order they sent requests on one connection.
A delayed or silenced call must not let later calls overtake it.

- A **delay after the response** holds back its response and every response behind it on the same connection.
  The backend still receives later requests on time.
- A **delay before forwarding** holds back its request and every request behind it on the same connection.
- A **silence** never answers, and it stalls the connection.
  The session stops forwarding:
  later requests on the same connection never reach the backend and never get an answer.
  The client sees a stalled connection, as it would with a real hung broker.

## Failure inside the pipeline

When a stage fails, for example because a frame cannot be decoded,
the pipeline forwards the call untouched and records the failure in the explanation.
A bug in GaslightMQ must never look like a lie.
