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
| **Pads** | A grid for playing rules live, by mouse, keyboard or hardware. |
| **Scenarios** | The scenarios in the workspace, their mode and steps, and the active one. |
| **World** | Only with the Simulated backend: topics, partitions, groups, brokers and generators. |
| **Quick inject (⌘K)** | Type a sentence such as `delay produce orders.* 750ms 40% order-ingest` and get a live rule. |

The environment switcher picks the backend:
a proxy in front of a real cluster, or a fake cluster.
The views stay the same.
Views that a backend cannot support are hidden,
and actions that need emulation are marked.

Names are unique per kind, so a selector and a rule can share a name.
Wherever the UI lists several kinds together, such as in ⌘K,
it shows the kind next to the name.

The [mockups](../../../mockups/proxy-ui-v2.html) show most of these views.

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
name: Push 2
rows: [Order producers, Fetch, Metadata, FindCoordinator, Join and sync, Heartbeat, OffsetCommit, Transactions]
columns: [Error retriable, Error fatal, Delay 100 ms, Delay 1 s, Delay 10 s, Blackhole, Disconnect, Throttle]
layers: [All clients, order-ingest, payments-svc, analytics-etl, kafka-ui]
```

### A pad is a position, not a rule

A pad is the intersection of a row, a column and a layer.
It points at zero or one rule.

- Pressing an empty pad creates a live rule from the row's selector, the column's preset and the layer's scope, and arms it.
- Pressing a pad with an armed rule disarms the rule.
- Pressing a pad with a disarmed rule arms it again.

The rule gets a generated name, such as *Delay 1 s on Order producers for order-ingest*,
which someone can rename.
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
Turning an encoder on an armed rule does not reset the rule's counters.

### Modes of play

- **Latch**: a press toggles the pad.
- **Hold**: the pad is armed while pressed, and disarms on release.

Pad presses act on the [live set](scenarios.md#the-live-set).

## Hardware mapping

A hardware mapping connects a controller to a pad layout.

| Control | Maps to |
|---|---|
| 8×8 pads | The grid of the current layer. |
| Encoders 1–8 | The encoders of the focused pad. |
| Scene buttons | Mute a row. |
| Track select buttons | Switch layers. |
| Pad colours | Preset colour, brightness for probability, a flash when the rule fires. |

The mapping is a translation between controller messages and surface commands.
It never talks to Injection directly.

The mapping runs in the browser, over Web MIDI.
Hardware therefore only works while the UI is open.
A bridge in the CLI can follow later as one more client of the [control plane](../architecture/control-plane.md).
It does not change the engine.
