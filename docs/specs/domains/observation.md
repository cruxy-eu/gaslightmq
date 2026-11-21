# Observation

Observation shows what happened and why.
It is downstream of everything:
sessions emit events, and Observation stores, indexes and shows them.
Nothing depends on Observation.

## Call

A call is one request and its response on one session.

| Part | Meaning |
|---|---|
| **Session** | The session the call ran on, with its identity. |
| **Request** | The decoded request, with every [field](targeting.md#fields). |
| **Upstream** | What the real cluster answered, when the Upstream backend's [overlay](../architecture/backends.md#overlay) changed it. |
| **Truth** | What the backend answered, if the request reached it. |
| **Delivered** | What the client actually received. |
| **Verdict** | Pass, or the lies that applied. |
| **Explanation** | Why the verdict came out this way. |
| **Timing** | When the call arrived, how long the backend took, how long the client waited. |

Truth and delivered only differ when a lie applied.
The UI can show the two side by side.
Some lies leave them equal: a delay changes when the answer arrives, not what it says.

The overlay is not a lie.
Address translation and emulated events are part of the truth,
so a Metadata call that no rule touched has the verdict Pass.

## Verdict

The verdict is the outcome of evaluating the call:

- **Pass**: no rule fired, or no rule that fired changed anything.
- **Lied**: one or more lies applied.
  The verdict lists them, such as `+750 ms` and `NOT_LEADER_OR_FOLLOWER on orders.eu/3`.

Events a call fired are part of the explanation, not the verdict,
because they change the world and not this call.

## Explanation

An explanation records every rule the call touched:

| Status | Meaning |
|---|---|
| **Matched** | The selector matched, with the target it matched. |
| **Rolled** | The trigger was evaluated, with its result: the match number, the die roll, the time window, the budget. |
| **Fired** | The trigger said yes. |
| **Applied** | The action took effect. |
| **Shadowed** | The rule fired, but a rule with a higher priority won the conflict. The explanation names the winner and the target. |
| **Pre-empted** | The rule had nothing left to change: a terminal lie ended the call, or a refusal replaced the rule's partition. The explanation names the lie that got there first. |

A rule that waits for the truth is also pre-empted
when a lie ends the call before forwarding.

Without explanations, any combination rule feels like magic.
With them, the UI answers *why did this call fail?*
and *why did my rule not fire?*

Shadowed and pre-empted rules do not count as firings for their [lifetime](rules.md#lifetime).
A rule that is shadowed on one partition and applies on another counts as one firing.

## Recording

A recording is a captured stretch of calls across all sessions.
Someone starts and stops it.

A recording keeps:

- every session with its identity, open and close times,
- every call with its request, truth, delivered response, verdict and explanation,
- the raw frames, unless the recording is metadata-only.

A **metadata-only** recording drops record keys, values and headers,
for traffic that must not leave the machine.
It is the default on the Upstream backend.
See [privacy](../architecture/storage.md#privacy).

## Index

The index is built automatically from a recording.
It groups calls into facets, each with counts.

| Facet | Values |
|---|---|
| Topics | `orders.eu`, `orders.us`, … and suggested patterns such as `orders.*` |
| APIs | `Produce`, `Fetch`, `Heartbeat`, … |
| Consumer groups | `payments`, `analytics-etl` |
| Transactional IDs | `payments-tx-1` |
| Clients | `order-ingest`, `payments-svc`, … |
| Brokers | `broker 1`, `broker 2`, `broker 3` |
| Outcomes | `ok`, `error`, delayed, throttled, … |

Facet counts respect the other active filters,
so the index always answers *what is left if I narrow down further?*

Clicking a facet value adds a condition to the current filter.
The current filter can be saved as a [selector](targeting.md#selectors),
or turned into a rule straight away.

## From observation to action

Observation feeds the rest of the system in three ways:

- **Filter → selector**: a filter built on the index becomes a named selector.
- **Call → selector**: a single call becomes a selector by ticking its fields.
  See [building selectors from traffic](targeting.md#building-selectors-from-traffic).
- **Recording → world**: a recording seeds a simulated cluster.
  See [seeding](world.md#seeding).
