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
| **Rules** | The rules the scenario owns, or references by name. They stay armed for the whole run. |
| **Steps** | Only in Sequence mode: the stages of the story. |
| **Requirements** | What the backend must support, such as the incidents the scenario uses. |

```yaml
name: Leader election
mode: chain
seed: 42
rules:
  - name: Leader moves away
    selector: Order producers
    action: { incident: leader-moves, partition: orders.eu/3 }
    trigger: { nth: 1 }
  - name: Stale metadata
    selector: { api: Metadata }
    action: { lie: topology, leader: previous }
    trigger: { first: 2, per: session }
```

Rule names are unique among all rules,
whether a scenario owns the rule or the workspace does.

## Modes

Lies that do not conflict [combine on their own](lies-and-incidents.md#how-lies-combine), in every mode.
The mode only decides between lies that **conflict**:
two terminal lies, a terminal lie and a refusal,
two refusals of the same partition,
or two rewrites of the same aspect of the same target.

A mode is a conflict strategy, with or without steps.

| Mode | A conflict goes to | Steps |
|---|---|---|
| **Live** | The rule that was armed most recently. | No |
| **Chain** | The rule that comes first in the scenario's order. | No |
| **Sequence** | The rule that comes first in the step's order. | Yes |

Scope plays no part in priority.
A rule for one client and a rule for all clients conflict like any two rules,
and the mode decides between them.

### Live

Live mode is for exploring.
Rules are armed and disarmed by hand, usually from the pad grid.

- The rule that was armed most recently wins a conflict.
- The order of the rules list does not matter.

A Live scenario is a saved set of rules to play with.
Activating it arms its rules in the order the scenario lists them,
so a rule further down counts as armed more recently.

Live mode follows what the person at the controls just did.
It is not meant to be reproducible.

### Chain

Chain mode is for steady background behaviour with priorities.
The scenario orders its rules.

- Every rule that fires applies, unless it conflicts with a rule above it.
- In a conflict, the higher rule wins.
- The lower rule is shadowed for the target they share.

A chain of *Delay 1 s* above *Refuse the 3rd Produce* delays every Produce and refuses the 3rd:
a delay and a refusal do not conflict.
A chain of *Blackhole at 50%* above *Refuse at 50%* refuses a quarter of the calls:
both are ways to fail the call, so the refusal only applies when the blackhole did not fire.

Each rule rolls its own die.
A rule that conflicts with rules above it applies less often than its probability suggests.
The UI shows that rule's **effective** probability next to its configured one.

### Sequence

Sequence mode is for exact, reproducible test cases.
The scenario is a list of steps.
Each step arms a set of rules and ends on a condition.

```yaml
name: Producer recovers from leader churn
mode: sequence
seed: 7
rules:
  - Background jitter
steps:
  - name: Warm up
    until: { calls: 100, selector: Order producers }
  - name: Leader churn
    arm: [Refuse with NOT_LEADER]
    until: { firings: 3, rule: Refuse with NOT_LEADER }
  - name: Slow recovery
    arm: [Slow down order producers]
    until: { after: 20s }
  - name: Back to normal
```

A step ends when its `until` condition holds.
Then the next step starts, and the rules the previous step armed disarm.

| `until` | The step ends |
|---|---|
| `calls`, with a selector | After that many calls matched the selector. |
| `firings`, with a rule | After that rule's action applied that many times. |
| `after` | After that much time on the [engine's clock](../architecture/README.md#clock), measured from the start of the step. |
| *(none)* | When someone advances or deactivates the scenario. |

A counted `until` is a [budget](rules.md#lifetime).
`firings: 3` lets the rule fire exactly three times,
even when several connections match it at the same moment.

A call belongs to the step that was current when the call arrived.

The scenario's own `rules` stay armed across all steps and keep their counters.
Within a step, conflicts resolve as in Chain mode:
first the step's rules in the order the step lists them,
then the scenario's rules in their order.

## The live set

At most one scenario is active at a time.
On top of the active scenario sits the **live set**:
rules that someone arms by hand while the scenario runs.

- Live rules win conflicts with scenario rules.
- Among live rules, the one armed most recently wins.
- Disarming a live rule hands control back to the scenario.
- The live set never changes the scenario's own rules or steps.

This lets someone poke at a running Sequence without breaking it.

When no scenario is active, the engine runs the live set alone.

Rules that pads and ⌘K create are [live rules](rules.md#where-rules-live).
They stay in memory until someone keeps them or saves the live set as a scenario.

Priority only orders lies at the same [stage](lies-and-incidents.md#stage).
A lie that ends a call before forwarding takes effect before the truth exists,
so it pre-empts a rule that waits for the truth, whatever their priority.
See [two evaluation passes](../architecture/request-pipeline.md#two-evaluation-passes).

## Capture

Capture turns what someone played live into a Sequence scenario.
While capture runs, the engine marks every arm and disarm in the live set.
It reads them from the [journal](observation.md#journal), with their timestamps.
Stopping capture produces a scenario with one step per change,
ending on the time between changes.

- Each step lists its rules with the most recently armed first,
  so the Sequence resolves conflicts the way the live set did.
- A captured rule keeps the parameters it had when it was armed.
  Capture ignores parameter changes on an armed rule.
- Saving the scenario also keeps the live rules it uses.

Someone explores with the pads,
captures the moment the client misbehaves,
and keeps it as a reproducible test.

## Portability

A scenario that uses only lies runs on every backend.
A scenario that uses incidents runs where the backend can apply or emulate those incidents
and their [consequences](lies-and-incidents.md#consequences).
The backend checks a scenario's requirements when the scenario loads,
and rejects it with the list of unsupported incidents.

The *Leader election* scenario above needs "leader moves",
so it runs on the Simulated backend only.
See [capabilities](../architecture/backends.md#capabilities).
