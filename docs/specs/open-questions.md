# Open questions

These decisions are still open.
Each question links to the spec it affects.

## Combining rules

- **Does scope specificity always come first?**
  [Scenarios](domains/scenarios.md#scope-and-layers) proposes that a client-scoped rule beats an all-clients rule before the mode is consulted.
  That fits Live and Chain mode.
  It may surprise someone in Sequence mode who ordered rules deliberately.
- **What does a chance trigger mean in Chain mode?**
  Each rule rolling its own die makes effective probabilities shrink down the chain.
  An alternative is one roll per call,
  with each rule taking a slice of the 0–100 range.
  That needs a policy when the slices add up to more than 100%.
- **Can rewrites of the same target merge?**
  Two data lies on the same partition could chain:
  first drop records, then reorder what is left.
  Or one could win, like a terminal lie.
- **Do delays add up or take the maximum?**
  Adding up is simpler to explain.
  The maximum is closer to how independent slowdowns overlap in reality.

## Triggers and counters

- **What is the default counting scope?**
  Session is the easiest to reason about.
  Global is closer to what people mean by "the 3rd Produce".
  See [counters](domains/session.md#counters).
- **Do count triggers count matches or firings of other rules?**
  For example: "the 3rd call that another rule did not already lie about".

## Scenarios

- **Can several scenarios be active at once?**
  The current proposal is one active scenario plus the [live layer](domains/scenarios.md#the-live-layer).
  Running two scenarios side by side needs a rule for conflicts between them.
- **What does Capture do with encoder turns?**
  Every small change could become a step,
  or Capture could keep only the value at each arm and disarm.

## Backends

- **How far should event emulation go on Upstream?**
  "Rebalance" can only be emulated partially,
  because the real group coordinator keeps its own state.
  See [capabilities](architecture/backends.md#capabilities).
- **Can a proxy front a fake broker?**
  It would work, but adds nothing over the Simulated backend
  and doubles the places where lies can happen.

## Storage and naming

- **Are names unique across kinds?**
  A selector and a rule both called *Order producers* are unambiguous in files,
  but can be confusing in the UI and in ⌘K.
- **What is the default for recordings: full or headers-only?**
  See [privacy](architecture/storage.md#privacy).

## Control surfaces

- **Where does the hardware bridge live?**
  Web MIDI in the browser keeps the engine headless,
  but only works while the UI is open.
  A bridge in the CLI would work without a browser.
