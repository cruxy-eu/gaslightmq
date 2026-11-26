# Control surfaces

Control surfaces give people fast ways to change rules and scenarios.
They hold no domain state of their own.
Every surface sends commands to Scenarios or Injection,
and shows what those districts publish.

## Views

| View | Purpose |
|---|---|
| **Record** | Live traffic per session, with the index alongside. Click to filter, save as selector, or inject. |
| **Selector from call** | Tick the fields of one recorded call to build a selector, and see what it matches. |
| **Rules** | Every rule grouped by scope, with an inspector for selector, action, trigger and lifetime. |
| **Clients** | One column per scope. Drag a rule to another column to change its scope. |
| **Pads** | A grid for playing rules live, by mouse or keyboard. |
| **Scenarios** | The scenarios in the workspace, their mode and steps, and the active one. |
| **World** | Only for an environment with a broker: topics, partitions, groups, broker nodes and where each topic's records come from. |
| **Quick inject (⌘K)** | Type a sentence such as `delay produce orders.* 750ms 40% order-ingest` and get a live rule. |

The environment switcher lists the [environments](../architecture/storage.md#environments) that run in the process:
a proxy in front of a real cluster, a proxy in front of a broker, or a broker alone.
Switching only changes which environment the UI shows.
Nothing restarts, and no client loses its connection.
The views stay the same.
Views that a backend cannot support are hidden,
and actions that need emulation are marked.

Names are unique per kind, so a selector and a rule can share a name.
Wherever the UI lists several kinds together, such as in ⌘K,
it shows the kind next to the name.

A surface shows the [title](rules.md#titles) of a definition where it has one, and its name where it has none.
A command always carries the name.

The [mockups](../../../mockups/proxy-ui-v2.html) show most of these views.

A definition in a file can hold comments that someone wrote by hand.
Before the UI saves an edit that loses such a comment, it says so and asks.
See [writing back](../architecture/storage.md#writing-back).

## Quick inject

⌘K, or Ctrl-K, opens a palette over every view.
The control plane reads the sentence while someone types,
once they paused for the interval of the UI's configuration, and only its answer to the latest sentence counts.
The palette shows the rule the sentence gives, part by part, and the words it could not read with the reason.

A sentence knows the names of the environment shown: the presets and the selectors of the workspace,
and the topics, consumer groups and client IDs that its traffic has seen.
It says those without the word for their kind, and completes them: `order-ingest` reads as a client once
`order-ingest` has called through the environment. Any other name follows the word for its kind: `client billing`.

| Key | Does |
|---|---|
| Up, down | Move through the rule, the completions of the word at the end, and the views. |
| Tab | Put the highlighted completion, or the first one, in place of the word at the end. |
| Enter | On the rule: create it as a live rule of the environment shown, armed, in one operation. On a completion or a view: take it. |
| Esc | Close the palette. |

A sentence with a word that the control plane could not read creates nothing, so a typo never becomes a rule that does less than it says.

## Pad layout

A pad layout maps a grid to names.

| Axis | Maps to |
|---|---|
| **Rows** | Selectors, by name. |
| **Columns** | Presets, by name. |
| **Layers** | Scopes. The bottom row of the grid switches layers. |

A layer is a page of the grid, not a priority.
A rule on a client layer does not win over a rule on the all-clients layer.
See [modes](scenarios.md#modes).

```yaml
name: order-desk
title: Order desk
rows: [order-producers, fetch, metadata, find-coordinator, join-and-sync, heartbeat, offset-commit, transactions]
columns: [error-retriable, error-fatal, delay-100ms, delay-1s, delay-10s, blackhole, disconnect, throttle]
layers: [All clients, order-ingest, payments-svc, analytics-etl, kafka-ui]
```

A layer is a scope and no definition, so `order-ingest` is the `client.id` of a client, and not a name.

### A pad is a position, not a rule

A pad is the intersection of a row, a column and a layer.
It points at zero or one rule.

- Pressing an empty pad creates a live rule from the row's selector, the column's preset and the layer's scope, and arms it.
- Pressing a pad with an armed rule disarms the rule.
- Pressing a pad with a disarmed rule arms it again.

The rule gets a generated name, such as `delay-1s-on-order-producers-for-order-ingest`,
and a generated title, such as *Delay 1 s on Order producers for order-ingest*.
The name follows the [rule for names](rules.md#names): what a name cannot hold is left out, and a name that is too long is cut.
Someone can change the title, and the name stays.
Moving a pad never changes what a rule does.

A rule that a pad creates is a [live rule](rules.md#where-rules-live).
It stays in the engine's memory and never touches the workspace files,
until someone keeps it or saves the live set as a scenario.

### Encoders

Eight encoders tune the rule behind the focused pad.
Each encoder maps to one parameter of the rule's action, trigger or lifetime.

| Encoder | Parameter |
|---|---|
| Probability | The trigger's `chance`. |
| Delay | The delay of a timing lie. |
| Jitter | The jitter of a timing lie. |
| Error code | The error of a refusal. |
| Throttle | The `throttle_time_ms` of a timing lie. |
| Every | The trigger's `every` count. |
| Duration | A duration [lifetime](rules.md#lifetime). |
| Firings | A firings lifetime. |

Encoders that do not apply to the focused preset are disabled.
An empty pad has no rule to tune, and a pad whose rule someone kept refuses turns,
so the encoders of both are disabled too.
Turning an encoder on an armed rule does not reset the rule's counters.

### Keys

The grid plays by keyboard as well as by mouse.

| Key | Does |
|---|---|
| Arrows | Move the focus one pad. The focus stays at the edge of the grid. |
| Space | Press and release the focused pad. |
| 1 to 9 | Show that layer of the layout. |
| ⌘Z, ⇧⌘Z | [Undo and redo](#undo-and-redo), except while a pad is held. |

### Modes of play

- **Latch**: a press toggles the pad.
- **Hold**: the pad is armed while pressed, and disarms on release.

Pad presses act on the [live set](scenarios.md#the-live-set).

## Undo and redo

⌘Z takes back the last change that someone made in this tab, and ⇧⌘Z makes it again.
On other systems the keys are Ctrl-Z and Ctrl-Shift-Z.

Undo reverses what someone set, and never what happened.

| Undo takes back | Undo does not take back |
|---|---|
| Creating, changing or deleting a definition. | A lie or an incident that fired, by a trigger or by hand. |
| Making, changing or deleting a live rule. | A step that advanced. |
| Arming and disarming a rule. | What the traffic counted and spent: counters and budgets. |
| A pad press and an encoder turn. | Keeping a live rule. |
| Activating and deactivating a scenario. | Starting or stopping an environment, a recording or a capture, and closing a session. |

A client that got an error, or that waited, has already reacted.
Undoing an arm disarms the rule, and takes back nothing that the rule did.

### The history of a tab

The undo history belongs to the UI, and the engine keeps none.

- **Each tab has a history of its own, in memory.**
  It holds the changes that this tab made.
  It never holds a change of another tab, of `gmq`, of a scenario or of someone who edited a file.
- **It holds as many steps as `undo.depth` says.**
  That is a property of the UI's configuration, and 100 unless someone sets it.
  The oldest step goes when a new one would be one too many.
- **It lives as long as the tab.**
  A live stream that drops and opens again keeps it.
  Closing or reloading the tab ends it.
- **A new change ends what could be redone.**
- **A step goes with its environment.**
  When an environment stops, the tab drops the steps that changed it.

### One gesture is one step

One gesture sends one operation, and one operation is one step.

| Gesture | One step |
|---|---|
| Enter in ⌘K | The live rule that the sentence made, armed. Undo deletes it. |
| A press in latch | The press. Undo disarms the rule, or deletes it when the press made it. |
| A press and its release in hold | Both together. They leave a step only when the press made the rule, and undo then deletes it. |
| Turns of one encoder on one rule, one after another | All of them. Undo sets the rule back to what it was before the first. |

⌘Z does nothing while a pad is held.
While a text field has the focus, and in the ⌘K palette, ⌘Z and ⇧⌘Z belong to the text.

### When the state has moved on

An undo is an ordinary operation of the [control plane](../architecture/control-plane.md#undo).
The UI works out the inverse of each change from what the control plane answered to it, and never from what it expects a press or a turn to do.

Someone else can change the same rule in between: another tab, `gmq`, a step, a lifetime or a file edit.
Each undo therefore names the [revision](../architecture/control-plane.md#revisions) that its change left.

- **When the revision still holds, the undo applies.**
  The UI says what it did, by name: *Armed slow-order-producers again*.
  An undo can arm a fault, so it never happens without a word.
- **When the revision has moved on, the undo changes nothing.**
  The UI drops the step, and says that the rule changed since.
  It never forces an undo over a change that the tab did not make.
  The steps before it stay, and the next ⌘Z takes the next one.
