//! Scenarios: failure stories that group rules, decide how they combine, and can play them out over time.
//!
//! This crate holds the scenario as a definition: its mode, its rules and the steps of a Sequence,
//! and the store trait of the scenarios.
//!
//! | Type | Is |
//! |---|---|
//! | [`Scenario`] | A scenario as a file writes it: a name, a title, a mode, a seed and its rules. |
//! | [`ScenarioName`], [`ScenarioTitle`] | The identity of a scenario, and the free text that says what it shows. |
//! | [`ScenarioRule`] | A rule of a scenario: one it owns, or one of the workspace that it names. |
//! | [`Mode`] | How the rules combine: Live, Chain, or a Sequence with its steps. |
//! | [`Step`] | One stage of a Sequence: the rules it arms, and the condition that ends it. |
//! | [`StepName`], [`StepTitle`] | The identity of a step within its scenario, and the free text that says what happens in it. |
//! | [`Until`] | The condition that ends a step: a number of calls, a number of firings, or a time. |
//! | [`ScenarioStore`] | What keeps the scenarios: the store contract, and which scenarios name a rule, a selector or a preset. |
//! | [`ResolvedScenario`], [`ResolvedStep`], [`ResolvedUntil`] | A scenario as an environment runs it: its rules and the rules and conditions of its steps, resolved against the workspace. |
//! | [`ScenarioRun`], [`ScenarioPosition`], [`StepAdvance`], [`StepBudget`] | A scenario that runs: the step where it stands, what ends that step, and the order in which its rules win a conflict. |
//! | [`Capture`], [`CapturedRule`], [`Captured`] | A capture that runs: the arms and the disarms of the live set, read from the journal; a rule of the live set as a capture holds it; and the steps of a Sequence that it captured. |
//!
//! The [`prelude`] holds all of them, and the error of each.
//!
//! ```
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_scenario::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_trigger::prelude::*;
//!
//! // A Chain owns one rule and names another of the workspace. The rule it lists first wins a conflict.
//! let stale_metadata = Rule::new(
//!     "stale-metadata".parse().expect("a rule name"),
//!     Conditions::from(Condition::is(ApiKey::METADATA)),
//!     LieDefinition::Silence,
//! );
//! let leader_moves: RuleName = "leader-moves-away".parse().expect("a rule name");
//! let scenario = Scenario::new(
//!     "leader-election".parse().expect("a scenario name"),
//!     Mode::Chain,
//!     Seed::new(42),
//!     [ScenarioRule::from(leader_moves.clone()), ScenarioRule::from(stale_metadata.clone())],
//! )?;
//!
//! assert_eq!(scenario.mode().name(), "chain");
//! assert_eq!(scenario.owned_rules().collect::<Vec<_>>(), [&stale_metadata]);
//! assert_eq!(scenario.rule_references(), [&leader_moves]);
//! assert!(scenario.steps().is_empty());
//! # Ok::<(), ScenarioError>(())
//! ```
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`ScenarioName`], [`StepName`] | [`ScenarioNameError`], [`StepNameError`] | The text breaks a part of the [rule for names](ScenarioName#the-rule-for-names). |
//! | [`ScenarioTitle`], [`StepTitle`] | [`ScenarioTitleError`], [`StepTitleError`] | The text breaks a part of the [rule for titles](ScenarioTitle#the-rule-for-titles). |
//! | [`Scenario`] | [`ScenarioError`] | A rule or a step stands in it twice, a Sequence has no step, or a step arms or ends on a rule it cannot. |
//! | [`Scenario::resolve`] | [`ResolveScenarioError`] | A rule that the scenario names is not in the workspace, a rule does not resolve, or the selector of a condition does not. |
//!
//! A [`Step`], a [`Mode`] and an [`Until`] check nothing on their own: the scenario they are part of checks them.
//! The message of an error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! # Who checks what
//!
//! | Check | Where |
//! |---|---|
//! | A name, a title, a count and a time are valid. | Their types, when they are built or read. |
//! | A scenario holds each rule and each step once, a Sequence has a step, and a step arms and ends on rules it can. | [`Scenario::new`] |
//! | The rules, the selectors and the presets that a scenario names exist, and the rules it owns can fire. | Whoever holds the whole workspace. |
//! | No rule that a scenario owns has the name of another rule, of the workspace or of another scenario. | Whoever holds the whole workspace. |
//! | The backend of an environment can apply or emulate the incidents of the rules. | That backend, when someone activates the scenario there: [`ResolvedScenario::requirements`] says what it needs. |
//!
//! # A store
//!
//! A [`Scenario`] is a definition that a store holds under its name.
//! A [`ScenarioStore`] is the store contract of `gaslightmq_definition_api` for scenarios.
//! It also answers what only a store of scenarios can answer without reading every scenario:
//! which scenarios name a rule, a selector or a preset.
//! The trait answers them for every store, over the scenarios that the store lists.
//! The crate holds the trait, and no store.
//!
//! | Feature | Adds |
//! |---|---|
//! | `memory` | The store of scenarios in memory: [`ScenarioStore`] for the `MemoryStore` of `gaslightmq_definition_memory`. |
//!
//! # The `serde` feature
//!
//! With the `serde` feature, the definitions have a `serde` form.
//! The forms need a format that describes itself, such as YAML or JSON.
//!
//! | Type | Form |
//! |---|---|
//! | [`ScenarioName`], [`ScenarioTitle`], [`StepName`], [`StepTitle`] | The text. |
//! | [`Scenario`] | A map with `name`, `mode` and `seed`, with `title` when it has one, `rules` when it has any, and `steps` for a Sequence. |
//! | [`ScenarioRule`] | The name of a rule of the workspace as text, or the map of a rule that the scenario owns. |
//! | [`Step`] | A map with `name`, and with `title`, `arm` and `until` when it has them. |
//! | [`Until`] | `{ calls: 100, selector: order-producers }`, `{ firings: 3, rule: refuse-with-not-leader }` or `{ after: 20s }`. |
//!
//! ```yaml
//! name: producer-recovers-from-leader-churn
//! title: Producer recovers from leader churn
//! mode: sequence
//! seed: 7
//! rules:
//!   - background-jitter
//! steps:
//!   - name: warm-up
//!     until: { calls: 100, selector: order-producers }
//!   - name: leader-churn
//!     arm: [refuse-with-not-leader]
//!     until: { firings: 3, rule: refuse-with-not-leader }
//!   - name: slow-recovery
//!     title: Slow recovery of the producer
//!     arm: [slow-order-producers]
//!     until: { after: 20s }
//!   - name: back-to-normal
//! ```
//!
//! Reading rejects a key it does not know, and steps in a scenario that is no Sequence.
//! [`Mode`] has no form of its own: the key `mode` writes its word, and the key `steps` the steps of a Sequence.
//!
//! # The run
//!
//! A [`ScenarioRun`] is the step machine of a scenario, without IO: it takes the calls of the environment and the
//! time, and says when a step ends and which rules the scenario keeps armed, in the order in which they win a
//! conflict. Whoever holds the armed rules of an environment drives it, and arms and disarms what it says.
//!
//! # Capture
//!
//! A [`Capture`] turns what someone played live into the steps of a Sequence, without IO: it reads the entries of
//! the journal of an environment, marks every arm and disarm in the live set with its time, and gives one step for
//! every change when it stops. Whoever runs the environment hands it every entry, and the definition of a live rule
//! as it arms. See [`Capture`].
//!
//! # No IO
//!
//! The crate depends on the rules and on the vocabulary of selectors, triggers, calls and incidents, on
//! `gaslightmq_clock` for the time that ends a step, and on `gaslightmq_journal` for the entries that a capture reads.
//! It has no IO, no system clock and no global randomness, and it compiles to WebAssembly.
//! Its only async code is in [`ScenarioStore`]: the trait awaits the store it is handed, and nothing else.

mod capture;
mod mode;
mod name;
mod resolved;
mod run;
mod scenario;
#[cfg(feature = "serde")]
mod serde;
mod step;
mod store;

pub use capture::{Capture, Captured, CapturedRule};
pub use mode::Mode;
pub use name::{
    ScenarioName, ScenarioNameError, ScenarioTitle, ScenarioTitleError, StepName, StepNameError, StepTitle,
    StepTitleError,
};
pub use resolved::{ResolveScenarioError, ResolvedScenario, ResolvedStep, ResolvedUntil};
pub use run::{ScenarioPosition, ScenarioRun, StepAdvance, StepBudget};
pub use scenario::{Scenario, ScenarioError, ScenarioRule};
pub use step::{Step, Until};
pub use store::ScenarioStore;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in scenarios, next to the preludes of the crates a scenario is made of.
/// No name in it is a name of the standard prelude or of those preludes, so the import shadows nothing.
///
/// ```
/// use gaslightmq_scenario::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let scenario = Scenario::new("flaky-orders".parse()?, Mode::Chain, Seed::new(42), []);
/// assert_eq!(scenario.map(|scenario| scenario.rules().len()), Ok(0));
/// # Ok::<(), ScenarioNameError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Capture, Captured, CapturedRule, Mode, ResolveScenarioError, ResolvedScenario, ResolvedStep, ResolvedUntil,
        Scenario, ScenarioError, ScenarioName, ScenarioNameError, ScenarioPosition, ScenarioRule, ScenarioRun,
        ScenarioStore, ScenarioTitle, ScenarioTitleError, Step, StepAdvance, StepBudget, StepName, StepNameError,
        StepTitle, StepTitleError, Until,
    };
}
