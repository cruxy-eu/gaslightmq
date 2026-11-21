# Storage

GaslightMQ stores three kinds of data with different lifetimes.

| Kind | Examples | Lives in | Persisted |
|---|---|---|---|
| **Definitions** | Selectors, presets, rules, scenarios, pad layouts, worlds | Workspace files | Always |
| **Live state** | Sessions, armed state, counters, dice, live layer, World state while running | Engine memory | Only when someone saves it |
| **Recordings** | Calls with requests, responses, verdicts and explanations | Recording files | While recording is on |

## Workspace

A workspace is a directory of plain-text files.
The files are the source of truth:
the engine loads them, watches them for changes,
and writes edits from the UI back to them.

```
gaslight/
├── gaslight.yaml            # workspace settings: default backend, default seed
├── selectors.yaml           # every selector, by name
├── presets.yaml             # every preset, by name
├── rules/
│   └── background-noise.yaml
├── scenarios/
│   ├── leader-election.yaml
│   └── producer-recovers-from-leader-churn.yaml
├── layouts/
│   └── push-2.yaml
├── worlds/
│   └── orders-sim.yaml
└── recordings/              # gitignored
    └── 2026-11-21T14-02-11.rec
```

The workspace works in git.
Teams review scenarios in pull requests,
and CI runs them with `gmq run`.

### Names

- Every definition has a name, unique within its kind.
- Definitions reference each other by name.
- File names derive from names, but the name inside the file is authoritative.
- Renaming in the UI rewrites every reference in the workspace in one change.

### Validation

The engine validates the whole workspace on load and on every change:

- every reference resolves,
- every selector uses known fields,
- every scenario's events are supported by the current backend.

An invalid file does not stop the engine.
The engine keeps the last valid version and reports the error in the UI.

## Live state

Live state stays in memory.
It is lost when the engine stops,
unless someone saves it:

- **Save as scenario** saves the current armed rules as a Chain scenario.
- **Capture** saves the live layer's changes over time as a Sequence scenario.
- **Save world** saves the current World as a world definition.

## Recordings

A recording file is append-only.
It holds a header, the sessions, and the calls in arrival order.
It is split into chunks, so a long recording can be opened from the end.

Each call holds:

- session reference and timing,
- the raw request and response frames, or headers only,
- the decoded fields,
- the truth and the delivered response,
- the verdict and the explanation.

The index is not stored.
The engine rebuilds it when it opens a recording.

### Privacy

Records can hold personal or secret data.
A **headers-only** recording drops record keys, values and headers,
and keeps everything else.
The workspace setting decides the default.
