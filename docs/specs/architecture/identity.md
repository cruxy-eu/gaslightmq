# Identity: names and IDs

This document lists every entity of GaslightMQ per district of the [town plan](../townplan/README.md),
and how GaslightMQ identifies each one.

## Types of entities

| Type                   | What it is                                                     | Identified by            |
|------------------------|----------------------------------------------------------------|--------------------------|
| User-defined           | What a user writes: in a workspace file, in the UI or with ⌘K. | A name                   |
| Observed               | What GaslightMQ observes while it runs. Nobody defines it.     | An ID of its own         |
| Part of another entity | A piece of an entity, such as the trigger of a rule.           | The entity it is part of |

## Entities per district

A user-defined entity has a name and an optional title.
The [title](#title) is no identifier.
A process that holds the entity gives it a [UUID](#uuid) and a [revision](#revision) for as long as it runs.
The ID column names the type of that UUID.

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
| Step     | Part of a scenario     | Its name, unique within its scenario | —   | Its title       |
| Live set | Part of an environment | —              | Its environment           | —               |

### Observation

See [observation](../domains/observation.md).

| Entity        | Type                | Name | ID                 | Title |
|---------------|---------------------|------|--------------------|-------|
| Call          | Observed            | —    | `CallId`, a number | —     |
| Verdict       | Part of a call      | —    | —                  | —     |
| Explanation   | Part of a call      | —    | —                  | —     |
| Recording     | Observed            | —    | Its environment and the instant it started | —     |
| Journal entry | Observed            | —    | A number, counted per environment          | —     |
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

The UUID tells one definition from every other definition that a process holds, or held, in one run.
A rule that someone deleted and the rule that someone made under its name afterwards have different UUIDs.

A UUID is live state, as the armed state of a rule is.
The workspace keeps it in memory, next to the definition.
A manifest in Kubernetes holds no `metadata.uid` either: the cluster gives the object one, and keeps it in a store of its own.
Here, that store is the memory of the process.

- **No file holds a UUID.**
  A definition that someone wrote by hand and one that GaslightMQ saved look the same.
  A `uuid` key in a definition is a [reading error](storage.md#reading), as every key is that a definition does not have.
- **A definition gets its UUID when the process first holds it.**
  That is when the workspace loads it, when someone creates it, or when someone makes it as a live rule.
- **A definition keeps its UUID for as long as it keeps its name.**
  An update keeps it, whoever makes it: the UI, or someone who edits the file.
  *Keep* saves a live rule in the workspace under the UUID it has.
- **A name that goes and comes back is a new definition.**
  Deleting a definition forgets its UUID, and a definition that someone makes under that name gets a new one.
  A file that someone renames by hand is one definition that went and another one that came.
- **The next run gives new UUIDs.**
  A restart is a [clean slate](storage.md#a-restart-is-a-clean-slate), and the name is what two runs share.

A [journal entry](../domains/observation.md#journal) names a definition by its name and its UUID.
Within one run, and within one recording, the UUID says which definition the name meant at that moment.

A UUID is a UUID v7, so UUIDs sort in the order the process gave them, to the millisecond.
As text, it is in lower case and with hyphens.

#### Making a UUID

A UUID v7 is made of the current time and of random bits.
The crates that hold the definitions are [sans-IO](crates.md#sans-io-code), so they read neither.
The IO edge reads the clock and the randomness of the system, and hands both in:

```rust
let uuid = RuleUuid::new(unix_millis, random);
```

The same time and the same bits make the same UUID, so a test writes its UUIDs down.

A definition type holds no UUID.
The workspace holds one for every definition it holds, and an environment holds one for every live rule.
A live rule gets its UUID from the workspace when someone makes it.

### Revision

A revision says which state of an entity someone saw.
It is a number, and the process raises it each time the entity changes, whoever changes it.

| Has a revision | The revision rises when |
|---|---|
| A definition of the workspace | Someone creates or updates it, through the control plane or in its file. |
| A rule in an environment | It arms or disarms, whether a person, a step or a lifetime does it. For a live rule, also when its definition changes. |
| The scenario of an environment | Someone activates or deactivates a scenario, or a step advances. |

What traffic does raises no revision: not a match, a firing, a counter or a budget.

A revision is live state, as the UUID is.
The process keeps it in memory, no file holds one, and every run starts it again.
A change through the control plane can name the revision it expects, and changes nothing when the entity has another one.
See [revisions](control-plane.md#revisions).

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

No reference uses a UUID.
A journal entry holds the UUID next to the name, and nothing looks a definition up by it.

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

### Recording ID

A recording is identified by its environment and the instant it started, in UTC and to the millisecond.
An environment writes one recording at a time, so no two recordings share both.
The name of its file holds both: `orders-sim.2026-11-21T14-02-11.527Z.rec`.
A name holds no `.`, so the first one ends the name of the environment.

### Journal entry ID

A journal entry has one number.
Each environment counts its entries from 1, so a higher number is a later entry.
The number is unique within one run of an environment, and within a recording, as a call ID is.

### What an observed entity says about a user-defined one

An observed entity can refer to a user-defined one, such as the rules in the explanation of a call.
It keeps the entity's name as it was at that moment: a `RecordedRuleName`.

A recorded name follows the rule for names,
but nothing promises that an entity with that name still exists.

A call keeps the name alone.
A journal entry keeps the UUID too, so the entries around a call say which definition had that name when the call arrived.

## Not decided yet

- Whether an entity can be renamed.
  The UUID can carry a rename within one run: one operation would change the name, keep the UUID and rewrite every reference.
  No operation does that yet, so a new name still makes a new entity.
- Whether GaslightMQ recognises a definition in a later run, or in another copy of the workspace.
  A file next to the definitions that maps each name to a UUID would do that, and people would commit it with them.
  Nothing needs it yet, and the definition files would not change.
