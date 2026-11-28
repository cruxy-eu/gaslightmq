# Scenarios

A scenario tells a failure story.
It groups rules, decides how they combine and can play them out over time.

## Scenario

A scenario has:

| Part | Meaning |
|---|---|
| **Name** | The identity of the scenario: a slug, unique among scenarios. |
| **Title** | What the scenario shows, in free text. Optional. |
| **Mode** | How rules combine: Live, Chain or Sequence. |
| **Seed** | The root of every die in the run. |
| **Rules** | The rules the scenario owns, or references by name, in its order. They stay armed for the whole run. Optional. |
| **Steps** | Only in Sequence mode: the stages of the story. A Sequence has at least one. |
| **Requirements** | What the backend must support: the incidents that the scenario's rules fire. Nobody writes them: they follow from the rules. |

A scenario holds [what a person writes](../architecture/identity.md#what-a-definition-holds), so its requirements are no part of the definition.
The backend of an environment reads them from the rules when someone activates the scenario there.
See [portability](#portability).

```yaml
name: leader-election
title: Leader election
mode: chain
seed: 42
rules:
  - name: leader-moves-away
    selector: order-producers
    action: { incident: leader-moves, partition: orders.eu/3 }
    trigger: { nth: 1 }
  - name: stale-metadata
    selector: { api: Metadata }
    action: { lie: topology, leader: previous }
    trigger: { first: 2, per: session }
```

A rule that the scenario owns is a map, as a rule of the workspace is.
A rule that it references is its name.
A scenario holds a rule of one name once.

Rule names are unique among all rules,
whether a scenario owns the rule or the workspace does.
The workspace checks that, together with every reference of a scenario:
the rules it names exist, and so do the selectors and presets that the rules it owns name.

The name and the title of a scenario follow the rules for the [name](rules.md#names) and the [title](rules.md#titles) of a rule.

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

- Every rule that fires applies, unless it conflicts with a rule above it that applies.
- In a conflict, the higher rule wins.
- The lower rule is shadowed for the target they share.

A chain of `delay-1s` above `refuse-the-3rd-produce` delays every Produce and refuses the 3rd:
a delay and a refusal do not conflict.
A chain of `blackhole-half` above `refuse-half`, each at 50%, refuses a quarter of the calls:
both are ways to fail the call, so the refusal only applies when the blackhole did not fire.

Each rule rolls its own die.
A rule that conflicts with rules above it applies less often than its probability suggests.
The UI shows that rule's **effective** probability next to its configured one.

### Sequence

Sequence mode is for exact, reproducible test cases.
The scenario is a list of steps.
Each step arms a set of rules and ends on a condition.

```yaml
name: producer-recovers-from-leader-churn
title: Producer recovers from leader churn
mode: sequence
seed: 7
rules:
  - background-jitter
steps:
  - name: warm-up
    until: { calls: 100, selector: order-producers }
  - name: leader-churn
    arm: [refuse-with-not-leader]
    until: { firings: 3, rule: refuse-with-not-leader }
  - name: slow-recovery
    title: Slow recovery of the producer
    arm: [slow-order-producers]
    until: { after: 20s }
  - name: back-to-normal
```

A step has a name, which is a slug and unique among the steps of its scenario.
The journal and the UI name a step by it.
A step can have a title too.

A step arms rules of the workspace, by name, each once.
It arms none of the scenario's own `rules`: those are armed for the whole run already,
and the end of the step would disarm them.
A scenario in Live or Chain mode has no steps.

A step ends when its `until` condition holds.
Then the next step starts, and the rules the previous step armed disarm.
After the last step no step runs: the scenario stays active, and its own `rules` stay armed, until someone deactivates it.

| `until` | The step ends |
|---|---|
| `calls`, with a selector | After that many calls matched the selector: a selector of the workspace by name, or conditions of the step's own. |
| `firings`, with a rule | After that rule's action applied that many times. The rule is armed during the step: the step arms it, or it is one of the scenario's `rules`. |
| `after` | After that much time on the [engine's clock](../architecture/README.md#clock), measured from the start of the step. |
| *(none)* | When someone advances or deactivates the scenario. |

A counted `until` is a [budget](rules.md#budgets).
`firings: 3` lets the rule fire exactly three times,
even when several connections match it at the same moment.

A call belongs to the step that was current when the call arrived.
The step ends on the call that makes its condition hold, so the next call of that connection belongs to the next step.

The scenario's own `rules` stay armed across all steps and keep their counters.
Within a step, conflicts resolve as in Chain mode:
first the step's rules in the order the step lists them,
then the scenario's rules in their order.

## The live set

At most one scenario is active at a time.
Whoever wants another deactivates the active one first.
On top of the active scenario sits the **live set**:
rules that someone arms by hand while the scenario runs.

- Live rules win conflicts with scenario rules.
- Among live rules, the one armed most recently wins.
- Disarming a live rule hands control back to the scenario.
- The live set never changes the scenario's own rules or steps.
- A rule is armed once. A rule that someone armed by hand stays in the live set when the scenario names it:
  the scenario does not arm it again, and deactivating the scenario leaves it armed.
- Disarming a rule of the scenario by hand takes it away until the scenario arms it again.

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
The backend of an environment checks a scenario's requirements when someone activates the scenario there,
and rejects the activation with the list of unsupported incidents.
The workspace does not check them: it serves every environment, and each has a backend of its own.
So a workspace can hold a scenario that only one of its environments runs.

The `leader-election` scenario above needs "leader moves",
so it runs on the Simulated backend only.
See [capabilities](../architecture/backends.md#capabilities).
