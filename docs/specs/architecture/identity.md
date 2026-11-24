# Identity: names and IDs

This document lists every entity of GaslightMQ per district of the [town plan](../townplan/README.md),
and how GaslightMQ identifies each one.

## Types of entities

| Type                   | What it is                                                     | Identified by            |
|------------------------|----------------------------------------------------------------|--------------------------|
| User-defined           | What a user writes: in a workspace file, in the UI or with ⌘K. | A name and a UUID        |
| Observed               | What GaslightMQ observes while it runs. Nobody defines it.     | An ID of its own         |
| Part of another entity | A piece of an entity, such as the trigger of a rule.           | The entity it is part of |

## Entities per district

A user-defined entity has a name, a UUID and an optional title.
The [title](#title) is no identifier.

### Session

See [session](../domains/session.md).

| Entity         | Type     | Name | ID                            | Title |
|----------------|----------|------|-------------------------------|-------|
| Session        | Observed | —    | Its connection number         | —     |
| Client context | Observed | —    | Its `client.id` with its host | —     |

### Targeting

See [targeting](../domains/targeting.md).

| Entity    | Type               | Name           | ID             | Title           |
|-----------|--------------------|----------------|----------------|-----------------|
| Selector  | User-defined       | `SelectorName` | `SelectorUuid` | `SelectorTitle` |
| Condition | Part of a selector | —              | —              | —               |
| Scope     | Part of a rule     | —              | —              | —               |

### Lies and incidents

See [lies and incidents](../domains/lies-and-incidents.md).

| Entity   | Type                       | Name         | ID           | Title         |
|----------|----------------------------|--------------|--------------|---------------|
| Preset   | User-defined               | `PresetName` | `PresetUuid` | `PresetTitle` |
| Lie      | Part of a rule or a preset | —            | —            | —             |
| Incident | Part of a rule or a preset | —            | —            | —             |

### Injection

See [injection](../domains/rules.md).

| Entity                 | Type           | Name       | ID         | Title       |
|------------------------|----------------|------------|------------|-------------|
| Rule, in the workspace | User-defined   | `RuleName` | `RuleUuid` | `RuleTitle` |
| Live rule              | User-defined   | `RuleName` | `RuleUuid` | `RuleTitle` |
| Trigger                | Part of a rule | —          | —          | —           |
| Lifetime               | Part of a rule | —          | —          | —           |
| Counter                | Part of a rule | —          | —          | —           |
| Budget                 | Part of a rule | —          | —          | —           |

### Scenarios

See [scenarios](../domains/scenarios.md).

| Entity   | Type                   | Name           | ID                        | Title           |
|----------|------------------------|----------------|---------------------------|-----------------|
| Scenario | User-defined           | `ScenarioName` | `ScenarioUuid`            | `ScenarioTitle` |
| Step     | Part of a scenario     | —              | Its place in the scenario | —               |
| Live set | Part of an environment | —              | Its environment           | —               |

### Observation

See [observation](../domains/observation.md).

| Entity        | Type                | Name | ID                 | Title |
|---------------|---------------------|------|--------------------|-------|
| Call          | Observed            | —    | `CallId`, a number | —     |
| Verdict       | Part of a call      | —    | —                  | —     |
| Explanation   | Part of a call      | —    | —                  | —     |
| Recording     | Observed            | —    | Not decided yet    | —     |
| Journal entry | Observed            | —    | Not decided yet    | —     |
| Index         | Part of a recording | —    | —                  | —     |

### World

See [world](../domains/world.md).

| Entity      | Type            | Name                                | ID                             | Title        |
|-------------|-----------------|-------------------------------------|--------------------------------|--------------|
| World       | User-defined    | `WorldName`                         | `WorldUuid`                    | `WorldTitle` |
| Broker node | Part of a world | —                                   | Its broker ID, as in Kafka     | —            |
| Topic       | Part of a world | Its topic name, as in Kafka         | —                              | —            |
| Partition   | Part of a topic | —                                   | Its topic and partition number | —            |
| Group       | Part of a world | Its `group.id`, as in Kafka         | —                              | —            |
| Producer    | Part of a world | —                                   | Its producer ID, as in Kafka   | —            |
| Transaction | Part of a world | Its `transactional.id`, as in Kafka | —                              | —            |

### Control surfaces

See [control surfaces](../domains/control-surfaces.md).

| Entity     | Type                 | Name         | ID                                                           | Title         |
|------------|----------------------|--------------|--------------------------------------------------------------|---------------|
| Pad layout | User-defined         | `LayoutName` | `LayoutUuid`                                                 | `LayoutTitle` |
| Layer      | Part of a pad layout | —            | Its scope: all clients, a `client.id`, or a scope of its own | —             |
| Pad        | Part of a pad layout | —            | Its position: layer, row and column                          | —             |

### Workspace

See [workspace](../architecture/storage.md).

| Entity      | Type                | Name              | ID                   | Title              |
|-------------|---------------------|-------------------|----------------------|--------------------|
| Environment | User-defined        | `EnvironmentName` | `EnvironmentUuid`    | `EnvironmentTitle` |
| Workspace   | One per directory   | —                 | Its directory        | —                  |
| Secret      | Part of a workspace | —                 | The path of its file | —                  |

## User-defined entities

```yaml
name: slow-order-producers
uuid: 0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10
title: Slow down order producers
selector: order-producers
action: delay-1s
```

Each kind has a type of its own for its name, its UUID and its title,
so the name of a selector never stands where the name of a rule belongs.

### Name

The name is the identity of a user-defined entity between domains.
Files, the control plane, ⌘K and explanations refer to an entity by its name.

A name is a slug, like the name of a Kubernetes object.

- It holds lowercase letters, digits and `-`.
- It starts and ends with a letter or a digit.
- It has at most 63 characters.
- It is unique within its kind.
  A selector and a rule can share a name, because every reference says which kind it means.

A name is a name and nothing else.

### UUID

The UUID is the identity of a user-defined entity inside the workspace.
It lets the workspace track and store entities without one overwriting another.

A UUID is a UUID v7, so UUIDs sort in the order the entities were created, to the millisecond.
A file holds it as text, in lower case and with hyphens.
A `uuid` that is no UUID v7 is a [reading error](storage.md#reading).

- GaslightMQ gives an entity its UUID when someone creates it, in the workspace or as a live rule.
  Nobody writes a UUID by hand.
- The UUID is stored in the entity's file.
  A live rule keeps it in memory, and in its file once someone keeps it.
  A file without a UUID gets one when the workspace first loads it.
- Two entities with the same UUID make the workspace invalid.
  That happens when someone copies a file.
  The `workspace` crate checks it, with the rest of the [validation](storage.md#validation).

#### Making a UUID

A UUID v7 is made of the current time and of random bits.
The crates that hold the definitions are [sans-IO](crates.md#sans-io-code), so they read neither.
The IO edge reads the clock and the randomness of the system, and hands both in:

```rust
let uuid = RuleUuid::new(unix_millis, random);
let rule = rule.with_uuid(uuid);
```

The same time and the same bits make the same UUID, so a test writes its UUIDs down.

A definition therefore has no UUID until GaslightMQ gave it one.
A file that someone wrote by hand has none, and neither has the rule that a pad or ⌘K drafts.
Each definition type holds its UUID as an optional part.
The workspace holds no entity without one, and neither does the live set.

### Title

A title is no identifier.
It is an optional, readable label of the entity itself, and nothing refers to it.
The UI shows the title when an entity has one, and the name otherwise.

- It has at least 1 and at most 2000 characters.
- It does not start or end with white space.
- It holds no control character, such as a line break,
  and no format character, such as a zero-width space.

### References

Domains refer to user-defined entities by name:

- a rule refers to its selector and its preset,
- the explanation of a call refers to the rules that touched it,
- statistics count per rule,
- a pad points at a rule.

The UUID stays inside the workspace.

Creating an entity never overwrites another one:

- creating fails when the name is already taken within its kind,
- updating fails when no entity has that name.

## Observed entities

An observed entity exists because GaslightMQ observed it.
It has no name, no UUID and no title.

### Call ID

A `CallId` is one number.
Each environment counts its calls from 1, across all its connections,
so a higher number is a later call.

A call also keeps the number of its connection,
which says which connection it came on.
That number is no part of the call's identity.

A call ID is unique within one run of an environment,
and within a recording.

A call ID is written as the number alone, such as `42`, and a file keeps it as a number.
It has no prefix: `#7` is the form of a connection number, and a call ID next to one must not look like it.
A sentence that names a call says `call 42`.

The engine keeps the count of its environment, and every session takes the ID of a call from it
when the request arrives.
A run that takes the same requests in the same order gives every call the same ID again.

### What an observed entity says about a user-defined one

An observed entity can refer to a user-defined one, such as the rules in the explanation of a call.
It keeps the entity's name as it was at that moment: a `RecordedRuleName`.

A recorded name follows the rule for names,
but nothing promises that an entity with that name still exists.

## Not decided yet

- How a recording and a journal entry are identified.
- Whether an entity can be renamed and keep its UUID,
  or whether a new name makes a new entity.
- How two clients that update the same entity at once are kept from overwriting each other.
- Which UUID a file without one gets in a [read-only workspace](storage.md#read-only-workspaces),
  where the engine never writes one back.
