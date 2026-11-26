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
The [name](#name) is its identity, and the [title](#title) is no identifier.
It has no ID: a table shows an ID column only where an observed entity or a part has one.
A process that holds the entity keeps a [revision](#revision) beside it for as long as it runs.

### Session

See [session](../domains/session.md).

| Entity         | Type     | Name | ID                            | Title |
|----------------|----------|------|-------------------------------|-------|
| Session        | Observed | —    | Its connection number         | —     |
| Client context | Observed | —    | Its `client.id` with its host | —     |

### Targeting

See [targeting](../domains/targeting.md).

| Entity    | Type               | Name           | Title           |
|-----------|--------------------|----------------|-----------------|
| Selector  | User-defined       | `SelectorName` | `SelectorTitle` |
| Condition | Part of a selector | —              | —               |
| Scope     | Part of a rule     | —              | —               |

### Lies and incidents

See [lies and incidents](../domains/lies-and-incidents.md).

| Entity   | Type                       | Name         | Title         |
|----------|----------------------------|--------------|---------------|
| Preset   | User-defined               | `PresetName` | `PresetTitle` |
| Lie      | Part of a rule or a preset | —            | —             |
| Incident | Part of a rule or a preset | —            | —             |

### Injection

See [injection](../domains/rules.md).

| Entity                 | Type           | Name       | Title       |
|------------------------|----------------|------------|-------------|
| Rule, in the workspace | User-defined   | `RuleName` | `RuleTitle` |
| Live rule              | User-defined   | `RuleName` | `RuleTitle` |
| Trigger                | Part of a rule | —          | —           |
| Lifetime               | Part of a rule | —          | —           |
| Counter                | Part of a rule | —          | —           |
| Budget                 | Part of a rule | —          | —           |

### Scenarios

See [scenarios](../domains/scenarios.md).

| Entity   | Type                   | Name           | ID                        | Title           |
|----------|------------------------|----------------|---------------------------|-----------------|
| Scenario | User-defined           | `ScenarioName` | —                         | `ScenarioTitle` |
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
| World       | User-defined    | `WorldName`                         | —                              | `WorldTitle` |
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
| Pad layout | User-defined         | `LayoutName` | —                                                            | `LayoutTitle` |
| Layer      | Part of a pad layout | —            | Its scope: all clients, a `client.id`, or a scope of its own | —             |
| Pad        | Part of a pad layout | —            | Its position: layer, row and column                          | —             |

### Workspace

See [workspace](../architecture/storage.md).

| Entity      | Type                | Name              | ID                   | Title              |
|-------------|---------------------|-------------------|----------------------|--------------------|
| Environment | User-defined        | `EnvironmentName` | —                    | `EnvironmentTitle` |
| Workspace   | One per directory   | —                 | Its directory        | —                  |
| Run         | One per process     | —                 | `RunId`              | —                  |
| Secret      | Part of a workspace | —                 | The path of its file | —                  |

## User-defined entities

```yaml
name: slow-order-producers
title: Slow down order producers
selector: order-producers
action: delay-1s
```

Each kind has a type of its own for its name and its title,
so the name of a selector never stands where the name of a rule belongs.

### What a definition holds

A definition holds what a person writes, and nothing else: its name, its title and its content.
A file holds definitions, so a file holds nothing else either.

What the process keeps while it runs sits beside a definition, and never inside it:

| Beside a definition | Kept by |
|---|---|
| Its [revision](#revision) | The workspace, in memory. |
| Whether a rule is armed, its counters and its budget | Each environment, in memory. |
| Whether a write would [lose a comment](storage.md#writing-back) of its file | The store of files. |

The [run](#run) and the live rules of an environment are live state of the same kind: the process keeps them, and no definition and no workspace file holds them.

- **The type of a definition has no field for live state.**
  A rule has no revision and no armed state, in memory as in a file.
- **A file that someone wrote by hand and one that GaslightMQ saved look the same.**
  A key that a definition does not have is a [reading error](storage.md#reading), whatever it is called.
- **On the control plane, live state travels beside the definition.**
  An answer holds a definition and its revision as two things next to each other, such as `{ "definition": { … }, "revision": 7 }`.
  A [request](control-plane.md#operations) holds the definition as its file holds it.

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

### Revision

A revision says which state of something someone saw.
It is a number, and a process has one counter for all of them.
The counter starts at 1 with every [run](#run), and every change takes the next number, whoever makes the change.

| Has a revision | Its revision is the number of |
|---|---|
| A definition of the workspace | The change that last created or updated it: the load, a request, or a [reload](storage.md#reloading). |
| A rule in an environment | The last change of its definition or of its state there. Its state changes when the environment starts, and when the rule arms or disarms, whether a person, a step or a lifetime does it. A disarm by a lifetime or a budget takes its number when the process [next looks](control-plane.md#revisions) at the armed rules. |
| The scenario of an environment | The last time the environment started, someone activated or deactivated a scenario, or a step advanced. |

- **One change takes one number.**
  A load is one change, so every definition that it brings has the same revision.
  A request is one change too, whatever it changes.
  A delete takes a number as well, and nothing has that revision afterwards.
- **A number never comes back in one run.**
  A definition that someone deletes and makes again under its name has a higher revision than the one that went.
  An environment that stops and starts again gives every rule a higher revision than it had before.
  So a revision that someone saw earlier never fits something else.
- **A rule in an environment has one revision.**
  It is the later of two numbers: the revision of the rule's definition, and the number of the last change of its state in that environment.
  Whoever arms a rule therefore says which definition they saw, and not only which state.
- **A revision orders changes.**
  A higher revision is a later change, across everything the process holds.
- **What traffic does takes no number.**
  Not a match, a firing, a counter or a budget.

A revision checks nothing: it is a number, and any number is one.
A number that the counter never gave is the revision of nothing, so a change that expects it changes nothing.

A revision is live state.
The process keeps it in memory, [beside the definition](#what-a-definition-holds), and no file holds one.
A change through the control plane can name the revision it expects, and changes nothing when the thing has another one.
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

Nothing refers to a user-defined entity by anything else.

Creating an entity never overwrites another one:

- creating fails when the name is already taken within its kind,
- updating fails when no entity has that name.

A rule of the workspace and a live rule share the names of rules.
See [live rules](../domains/rules.md#where-rules-live).

### Two definitions of one name

A name that goes and comes back is a new definition.
Someone who deletes `slow-order-producers` and makes a rule under that name has made another rule.
Nothing renames a definition, so a file that someone renames by hand is one definition that went and another one that came.

Two things tell the new definition from the old one within a run:

- **Its revision.**
  The new definition has a higher [revision](#revision) than the old one ever had, so a change that expects the old one changes nothing.
- **The journal.**
  The [journal](../domains/observation.md#journal) records that the old definition was deleted and that the new one was created, each with its revision.
  The entries between a creation and the next deletion are the history of one definition.

Across runs, the name is all that two definitions share.

## Observed entities

An observed entity exists because GaslightMQ observed it.
It has no name and no title.

### Run

A run is one life of a process, from its start to its stop.

Every run has a run ID, a `RunId`.
It is 128 random bits, written as 32 hexadecimal digits in lower case.
Whoever puts the process together reads the randomness of the system once, at the start, and hands the ID to the host.
A test names its own.
A text of another length, or with a character that is no digit and no letter from `a` to `f`, is no run ID.

Revisions start again with every run.
A definition that the workspace loads has the revision of the load, which is the same number in every run.
A revision alone therefore does not say which run it is from, and it means something only together with the run that gave it.

- **The [snapshot](control-plane.md#live-stream) holds the run ID.**
  A client learns the run when it connects, and learns of a restart when the next snapshot names another one.
- **A change that expects a revision names its run.**
  A process changes nothing for a request that names another run.
  See [revisions](control-plane.md#revisions).
- **No file holds a run ID**, and nothing refers to a run after it ended.

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
The journal entries around a call say [which definition](#two-definitions-of-one-name) had that name when the call arrived.

## Not decided yet

- **Whether a definition gets an ID of its own, next to its name.**
  A UUID that the process gives each definition, and keeps in memory beside it, would tell one definition from every other one that had its name.
  Nothing needs that today: the [revision](#revision) and the [journal](#two-definitions-of-one-name) tell them apart within a run.
  Two things would need it, and neither is decided.
- **Whether an entity can be renamed.**
  An ID could carry a rename within one run: one operation would change the name, keep the ID and rewrite every reference.
  No operation does that, so a new name makes a new entity.
- **Whether GaslightMQ recognises a definition in a later run, or in another copy of the workspace.**
  A file next to the definitions that maps each name to an ID would do that, and people would commit it with them.
  The definition files would not change: a definition holds [what a person writes](#what-a-definition-holds), with or without such a file.
