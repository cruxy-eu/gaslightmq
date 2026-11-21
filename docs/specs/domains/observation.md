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
| **Truth** | What the backend answered, if the request reached it. |
| **Delivered** | What the client actually received. |
| **Verdict** | Pass, or the lies that applied. |
| **Explanation** | Why the verdict came out this way. |
| **Timing** | When the call arrived, how long the backend took, how long the client waited. |

Truth and delivered differ exactly when a lie applied.
The UI can show the two side by side.

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
| **Matched** | The selector matched. |
| **Rolled** | The trigger was evaluated, with its result: the die roll, the count, the time window. |
| **Fired** | The trigger said yes. |
| **Applied** | The action took effect. |
| **Shadowed** | The rule fired, but the mode picked another rule. The explanation names the winner. |

Without explanations, any combination rule feels like magic.
With them, the UI answers *why did this call fail?*
and *why did my rule not fire?*

Shadowed rules do not count as firings for their [lifetime](rules.md#lifetime).

## Recording

A recording is a captured stretch of calls across all sessions.
Someone starts and stops it.

A recording keeps:

- every session with its identity, open and close times,
- every call with its request, truth, delivered response, verdict and explanation,
- the raw frames, unless the recording keeps headers only.

A **headers-only** recording drops record payloads,
for traffic that must not leave the machine.

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
