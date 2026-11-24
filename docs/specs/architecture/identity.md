# Identity: names, titles and IDs

This document lists everything GaslightMQ identifies,
and how it identifies each one.

## Three kinds of identifier

| Identifier | Example | Who gives it | What it is for |
|---|---|---|---|
| **Name** | `order-producers` | The user | The identity of a definition between domains. Files, the control plane, ⌘K and explanations refer to a definition by its name. |
| **Title** | `Order producers` | The user, optionally | A readable label in the UI. Nothing refers to a title. |
| **UUID** | `0193b6c2-…` | GaslightMQ | The identity of a definition inside the workspace. It lets the workspace track and store definitions without one overwriting another. |

Things that nobody defines and that have no name, such as a call, have an ID of their own.
See [runtime things](#runtime-things).

## Definitions

A definition is what a user writes: in a workspace file, in the UI or with ⌘K.
Every definition has a name, an optional title and a UUID.

| Kind | Name | Title | UUID |
|---|---|---|---|
| Selector | `SelectorName` | `SelectorTitle` | `SelectorUuid` |
| Preset | `PresetName` | `PresetTitle` | `PresetUuid` |
| Rule, in the workspace or live | `RuleName` | `RuleTitle` | `RuleUuid` |
| Scenario | `ScenarioName` | `ScenarioTitle` | `ScenarioUuid` |
| Pad layout | `LayoutName` | `LayoutTitle` | `LayoutUuid` |
| Environment | `EnvironmentName` | `EnvironmentTitle` | `EnvironmentUuid` |
| World | `WorldName` | `WorldTitle` | `WorldUuid` |

Each kind has a type of its own for each identifier,
so the name of a selector never stands where the name of a rule belongs.

```yaml
name: slow-order-producers
title: Slow down order producers
uuid: 0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10
selector: order-producers
action: delay-1s
```

### Name

A name is a slug, like the name of a Kubernetes object.

- It holds lowercase letters, digits and `-`.
- It starts and ends with a letter or a digit.
- It has at most 63 characters.
- It is unique within its kind.
  A selector and a rule can share a name, because every reference says which kind it means.

A name is a name and nothing else.
It carries no title and no UUID.

### Title

A title is optional.
It belongs to the definition itself, next to its name.

- It has at least 1 and at most 2000 characters.
- It does not start or end with white space.
- It holds no control character, such as a line break,
  and no format character, such as a zero-width space.

The UI shows the title when a definition has one, and the name otherwise.

### UUID

A UUID is a UUID v7, so UUIDs sort in the order the definitions were created.

- GaslightMQ gives a definition its UUID when someone creates it, in the workspace or as a live rule.
  Nobody writes a UUID by hand.
- The UUID is stored in the definition's file. A live rule keeps it in memory, and in its file once someone keeps it.
  A file without a UUID gets one when the workspace first loads it.
- Two definitions with the same UUID make the workspace invalid.
  That happens when someone copies a file.

## References

Domains refer to each other by name:

- a rule refers to its selector and its preset,
- the explanation of a call refers to the rules that touched it,
- statistics count per rule,
- a pad points at a rule.

The UUID stays inside the workspace.

Creating a definition never overwrites another one:

- creating fails when the name is already taken within its kind,
- updating fails when no definition has that name.

## Runtime things

These things exist while GaslightMQ runs, and nobody defines them.
They have no name, no title and no UUID.

| Thing | Identified by | Lives as long as |
|---|---|---|
| Call | `CallId`, a number | Its environment's run, and every recording that holds it. |
| Session | Its connection number | Its connection. A client that reconnects gets a new session and a new number. |
| Pad | Its position: layout, layer, row and column | Its layout. A pad is [a position, not a rule](../domains/control-surfaces.md#a-pad-is-a-position-not-a-rule). |

### Call ID

A `CallId` is one number.
Each environment counts its calls from 1, across all its connections,
so a higher number is a later call.

A call also keeps the number of its connection,
which says which connection it came on.
That number is no part of the call's identity.

A call ID is unique within one run of an environment.
Within a recording, it is unique too.

## Not decided yet

- How a recording and a journal entry are identified.
- Whether a definition can be renamed and keep its UUID,
  or whether a new name makes a new definition.
- How two clients that update the same definition at once are kept from overwriting each other.
