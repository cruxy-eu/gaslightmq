# Scenarios

A scenario tells a failure story.
It groups rules, decides how they combine and can play them out over time.

## Scenario

A scenario has:

| Part | Meaning |
|---|---|
| **Name** | Unique among scenarios. |
| **Mode** | How rules combine: Live, Chain or Sequence. |
| **Seed** | The root of every die in the run. |
| **Rules** | The rules the scenario owns, or references by name. |
| **Steps** | Only in Sequence mode: the stages of the story. |
| **Requirements** | What the backend must support, such as the events the scenario uses. |

```yaml
name: Leader election
mode: chain
seed: 42
rules:
  - name: Leader moves away
    selector: Order producers
    action: { event: leader-moves, partition: orders.eu/3 }
    trigger: { nth: 1, counting: global }
  - name: Stale metadata
    selector: { api: Metadata }
    action: { lie: topology, leader: old }
    trigger: { first: 2, counting: session }
```

## Modes

The mode decides what happens when more than one rule fires on the same call
with lies that conflict.
Modifier and rewrite lies [combine on their own](lies-and-events.md#how-lies-combine).
The mode only decides between **terminal** lies,
and between rewrites of the same target.

### Live

Live mode is for exploring.
Rules are armed and disarmed by hand, usually from the pad grid.

- The rule that was armed most recently wins.
- The order of the rules list does not matter.

Live mode follows what the person at the controls just did.
It is not meant to be reproducible.

### Chain

Chain mode is for steady background behaviour with priorities.
The scenario orders its rules.

- Rules are evaluated in order.
- The first rule that fires wins.
- Later rules are shadowed for this call.

A rule further down the chain fires less often than its probability suggests,
because it only gets a chance when every rule above it did not fire.
The UI shows each rule's **effective** probability next to its configured one.

### Sequence

Sequence mode is for exact, reproducible test cases.
The scenario is a list of steps.
Each step arms a set of rules and ends on a condition.

```yaml
name: Producer recovers from leader churn
mode: sequence
seed: 7
steps:
  - name: Warm up
    until: { calls: 100, selector: Order producers }
  - name: Leader churn
    arm: [Refuse with NOT_LEADER]
    until: { firings: 3, rule: Refuse with NOT_LEADER }
  - name: Slow recovery
    arm: [Delay 1 s]
    until: { after: 20s }
  - name: Back to normal
```

A step ends when its `until` condition holds.
Then the next step starts, and the rules of the previous step disarm.
Within a step, rules combine as in Chain mode, in the order the step lists them.

## The live layer

At most one scenario is active at a time.
On top of the active scenario sits the **live layer**:
rules that someone arms by hand while the scenario runs.

- Live rules always win over scenario rules.
- Disarming a live rule hands control back to the scenario.
- The live layer never changes the scenario's own rules or steps.

This lets someone poke at a running Sequence without breaking it.

## Capture

Capture turns what someone played live into a Sequence scenario.
While capture runs, the engine records every arm and disarm on the live layer with a timestamp.
Stopping capture produces a scenario with one step per change,
ending on the time between changes.

Someone explores with the pads,
captures the moment the client misbehaves,
and keeps it as a reproducible test.

## Scope and layers

Rules in a scenario can have different scopes.
When two conflicting rules have different scopes,
the more specific scope wins before the mode is consulted:

1. One connection
2. One client
3. All clients

This matches the layers of the pad grid:
a client layer sits on top of the all-clients layer.
See [open questions](../open-questions.md):
whether specificity should come before the mode in every mode.

## Portability

A scenario that uses only lies runs on every backend.
A scenario that uses events runs where the backend can apply or emulate those events.
The backend checks a scenario's requirements when the scenario loads,
and rejects it with the list of unsupported events.
