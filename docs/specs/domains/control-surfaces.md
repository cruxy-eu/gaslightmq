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
| **Quick inject (⌘K)** | Type a sentence such as `delay produce orders.* 750ms 40% order-ingest` and get a rule. |

The environment switcher picks the backend:
a proxy in front of a real cluster, or a fake cluster.
The views stay the same.
Views that a backend cannot support are hidden,
and actions that need emulation are marked.

The [mockups](../../../mockups/proxy-ui.html) show most of these views.

## Pad layout

A pad layout maps a grid to names.

| Axis | Maps to |
|---|---|
| **Rows** | Selectors, by name. |
| **Columns** | Presets, by name. |
| **Layers** | Scopes. The bottom row of the grid switches layers. |

```yaml
name: Push 2
rows: [Order producers, Fetch, Metadata, FindCoordinator, Join and sync, Heartbeat, OffsetCommit, Transactions]
columns: [Error retriable, Error fatal, Delay 100 ms, Delay 1 s, Delay 10 s, Blackhole, Disconnect, Throttle]
layers: [All clients, order-ingest, payments-svc, analytics-etl, kafka-ui]
```

### A pad is a position, not a rule

A pad is the intersection of a row, a column and a layer.
It points at zero or one rule.

- Pressing an empty pad creates a rule from the row's selector, the column's preset and the layer's scope, and arms it.
- Pressing a pad with an armed rule disarms the rule.
- Pressing a pad with a disarmed rule arms it again.

The rule gets a generated name, such as *Delay 1 s on Order producers for order-ingest*,
which someone can rename.
Moving a pad never changes what a rule does.

### Encoders

Eight encoders tune the rule behind the focused pad:
probability, delay, jitter, error code, throttle, burst, hold and ramp.
Encoders that do not apply to the focused preset are disabled.

### Modes of play

- **Latch**: a press toggles the pad.
- **Hold**: the pad is armed while pressed, and disarms on release.

Pad presses act on the [live layer](scenarios.md#the-live-layer).

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
