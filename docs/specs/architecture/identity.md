# Identity: names and IDs

This document lists everything GaslightMQ identifies,
and how it identifies each one.

GaslightMQ knows two types of things:

| Type | What it is | Examples | Identified by |
|---|---|---|---|
| **User-defined** | What a user writes: in a workspace file, in the UI or with ⌘K. | A selector, a preset, a rule | A name and a UUID |
| **Recorded** | What GaslightMQ observes while it runs. Nobody defines it. | A call, a session | An ID of its own |

## User-defined things

A user-defined thing is a definition.
It has a name and a UUID, and it can have a title.

| Kind | Name | UUID | Title |
|---|---|---|---|
| Selector | `SelectorName` | `SelectorUuid` | `SelectorTitle` |
| Preset | `PresetName` | `PresetUuid` | `PresetTitle` |
| Rule, in the workspace or live | `RuleName` | `RuleUuid` | `RuleTitle` |
| Scenario | `ScenarioName` | `ScenarioUuid` | `ScenarioTitle` |
| Pad layout | `LayoutName` | `LayoutUuid` | `LayoutTitle` |
| Environment | `EnvironmentName` | `EnvironmentUuid` | `EnvironmentTitle` |
| World | `WorldName` | `WorldUuid` | `WorldTitle` |

Each kind has a type of its own for each of them,
so the name of a selector never stands where the name of a rule belongs.

```yaml
name: slow-order-producers
uuid: 0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10
title: Slow down order producers
selector: order-producers
action: delay-1s
```

### Name

The name is the identity of a definition between domains.
Files, the control plane, ⌘K and explanations refer to a definition by its name.

A name is a slug, like the name of a Kubernetes object.

- It holds lowercase letters, digits and `-`.
- It starts and ends with a letter or a digit.
- It has at most 63 characters.
- It is unique within its kind.
  A selector and a rule can share a name, because every reference says which kind it means.

A name is a name and nothing else.

### UUID

The UUID is the identity of a definition inside the workspace.
It lets the workspace track and store definitions without one overwriting another.

A UUID is a UUID v7, so UUIDs sort in the order the definitions were created.

- GaslightMQ gives a definition its UUID when someone creates it, in the workspace or as a live rule.
  Nobody writes a UUID by hand.
- The UUID is stored in the definition's file.
  A live rule keeps it in memory, and in its file once someone keeps it.
  A file without a UUID gets one when the workspace first loads it.
- Two definitions with the same UUID make the workspace invalid.
  That happens when someone copies a file.

### Title

A title is no identifier.
It is an optional, readable label of the definition itself, and nothing refers to it.
The UI shows the title when a definition has one, and the name otherwise.

- It has at least 1 and at most 2000 characters.
- It does not start or end with white space.
- It holds no control character, such as a line break,
  and no format character, such as a zero-width space.

### References

Domains refer to definitions by name:

- a rule refers to its selector and its preset,
- the explanation of a call refers to the rules that touched it,
- statistics count per rule,
- a pad points at a rule.

The UUID stays inside the workspace.

Creating a definition never overwrites another one:

- creating fails when the name is already taken within its kind,
- updating fails when no definition has that name.

A pad has no identity of its own.
It is a position in a pad layout: its layer, row and column.
See [a pad is a position, not a rule](../domains/control-surfaces.md#a-pad-is-a-position-not-a-rule).

## Recorded things

A recorded thing exists because GaslightMQ observed it.
It has no name, no UUID and no title.

| Thing | Identified by | Lives as long as |
|---|---|---|
| Call | `CallId`, a number | Its environment's run, and every recording that holds it. |
| Session | Its connection number | Its connection. A client that reconnects gets a new session and a new number. |

### Call ID

A `CallId` is one number.
Each environment counts its calls from 1, across all its connections,
so a higher number is a later call.

A call also keeps the number of its connection,
which says which connection it came on.
That number is no part of the call's identity.

A call ID is unique within one run of an environment,
and within a recording.

### What a recorded thing says about a definition

A recorded thing can refer to a definition, such as the rules in the explanation of a call.
It keeps the definition's name as it was at that moment: a `RecordedRuleName`.

A recorded name follows the rule for names,
but nothing promises that a definition with that name still exists.

## Not decided yet

- How a recording and a journal entry are identified.
- Whether a definition can be renamed and keep its UUID,
  or whether a new name makes a new definition.
- How two clients that update the same definition at once are kept from overwriting each other.
