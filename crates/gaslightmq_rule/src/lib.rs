//! Rules: what happens to which calls, when, and for how long.
//!
//! A rule is the unit that fires, that shows up in the explanation of a call, and that has a history.
//! This crate holds the rule as a definition, the preset its action can name, its state,
//! and the snapshot of armed rules that evaluates a call.
//!
//! | Type | Is |
//! |---|---|
//! | [`Rule`] | A rule as a file writes it: a name, a selector, a scope, an action, a trigger, a lifetime and a dice key. |
//! | [`RuleName`], [`PresetName`] | The names that other things reference a rule and a preset by. |
//! | [`Action`] | What happens when a rule fires: a lie or an incident. |
//! | [`ActionRef`] | How a rule says it: a preset name, or an inline action. |
//! | [`Preset`] | A named, reusable lie or incident. |
//! | [`PresetSet`], [`RuleSet`] | The presets and the rules by name. Each name is there once. |
//! | [`RuleState`] | Draft, armed or disarmed, and the changes between them. |
//! | [`ResolvedRule`] | A rule without names to look up: its effective selector and its action. |
//! | [`ArmedRule`] | A resolved rule with the live state of one arming. It also fires by hand. |
//! | [`HandFiring`] | What firing a rule by hand did. |
//! | [`Snapshot`] | The armed rules at one moment, in priority order. It evaluates calls. |
//! | [`Priority`] | Where a rule stands among the armed rules. |
//! | [`PassEvaluation`] | What one evaluation pass gave for one call: every rule that matched. |
//! | [`RuleEvaluation`] | One of those rules: its target, what its trigger rolled, and what it fired. |
//! | [`Effect`] | What a rule that fired does: its lie on its target, or its incidents. |
//! | [`RuleError`] | Why a value cannot become a part of a rule, or why a rule does not resolve. |
//!
//! The [`prelude`] holds all of them.
//!
//! # From a definition to a firing
//!
//! A rule passes through three types on its way from a file to a call.
//!
//! ```text
//! Rule ── resolve(selectors, presets) ──► ResolvedRule ── arm(seed, now) ──► ArmedRule ──► Snapshot
//! ```
//!
//! ```
//! use gaslightmq_clock::{Clock, Duration, VirtualClock};
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_rule::prelude::*;
//! use gaslightmq_selector::prelude::*;
//! use gaslightmq_trigger::prelude::*;
//!
//! // A workspace has selectors and presets, and rules that name them.
//! let selectors = SelectorSet::new([Selector::new(
//!     "Order producers".parse()?,
//!     Conditions::new()
//!         .with(Condition::is(ApiKey::PRODUCE))
//!         .with(Condition::parse(Field::Topic, "orders.*")?),
//! )])?;
//! let presets = PresetSet::new([Preset::new(
//!     "Leader flap".parse()?,
//!     RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
//! )])?;
//! let rule = Rule::new(
//!     "Refuse the 3rd order".parse()?,
//!     "Order producers".parse::<SelectorName>()?,
//!     "Leader flap".parse::<PresetName>()?,
//! )
//! .with_trigger(TriggerParts::new().with_nth(MatchNumber::new(3)?))
//! .with_lifetime(Lifetime::Firings(Count::ONE));
//!
//! // Someone arms the rule, and the engine publishes a snapshot of the armed rules.
//! let mut clock = VirtualClock::new();
//! let armed = rule.resolve(&selectors, &presets)?.arm(Seed::new(42), clock.now());
//! let snapshot = Snapshot::new([armed])?;
//!
//! // A session evaluates each of its calls. This Produce names an orders topic and another one.
//! let produce = Fields::new()
//!     .with(ApiKey::PRODUCE)
//!     .with_part("orders.eu/3".parse::<TopicPartition>()?)
//!     .with_part("payments.settled/0".parse::<TopicPartition>()?);
//! let mut session = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1")?);
//!
//! for _ in 0..2 {
//!     clock.advance(Duration::from_millis(5));
//!     let pass = snapshot.evaluate(Pass::Request, &produce, clock.now(), &mut session);
//!     assert_eq!(pass.rules()[0].rolled().outcome(), TriggerOutcome::Held);
//! }
//!
//! // The 3rd matching call fires the rule, and the refusal is about the orders partition only.
//! let mut pass = snapshot.evaluate(Pass::Request, &produce, clock.now(), &mut session);
//! let third = &mut pass.rules_mut()[0];
//! assert_eq!(third.target().to_string(), "orders.eu/3");
//! assert_eq!(third.rolled().match_number(), Some(MatchNumber::new(3)?));
//! assert_eq!(third.lie().map(ToString::to_string), Some("NOT_LEADER_OR_FOLLOWER on orders.eu/3".into()));
//!
//! // No other lie shadowed it, so the call confirms the firing, which was the one the lifetime allows.
//! let confirmed = third.confirm().expect("the rule fired");
//! assert!(confirmed.ends_lifetime());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Who checks what
//!
//! Every definition type only holds valid values.
//! A rule can still name a selector or a preset that does not exist.
//!
//! | Check | Where |
//! |---|---|
//! | A name, a scope, a trigger and a lifetime are valid. | Their types, when they are built or read. |
//! | The selector and the preset of one rule exist, and the rule can fire: its lie can act when its selector is decided, and a manual rule names the subject of its incident. | [`Rule::resolve`] |
//! | The names of presets, of rules and of armed rules are unique. | [`PresetSet::new`], [`RuleSet::new`], [`Snapshot::new`] |
//! | A scenario, a step or a pad names a rule that exists. A rename reaches every reference. | Whoever holds the whole workspace. |
//!
//! A snapshot only holds resolved rules, so evaluating a call never looks up a name.
//!
//! # Two passes
//!
//! A rule whose selector tests a response field cannot match before the backend answers.
//! [`ResolvedRule::pass`] says which pass decides a rule, and [`Snapshot::evaluate`] runs one pass.
//! A rule is in exactly one pass, so one call is one match of it.
//!
//! # What this crate leaves to others
//!
//! - It resolves no conflict between lies. [`PassEvaluation::lies`] gives the lies in priority order to whoever does.
//! - It applies nothing, and it expands no incident into its consequences.
//! - It settles no firing: the call [confirms or withdraws](RuleEvaluation#settling) each one.
//!   A firing that is withdrawn goes back to the budget, and a hand firing waits for the next matching call.
//! - It keeps no order of its own. A snapshot takes its rules in the order it is given.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every definition type has a `serde` form.
//! The forms need a format that describes itself, such as YAML or JSON.
//!
//! | Type | Form |
//! |---|---|
//! | [`RuleName`], [`PresetName`] | The text. |
//! | [`Action`] | The flat map of the lie or of the incident: `{ lie: refusal, error: NOT_LEADER_OR_FOLLOWER }`. |
//! | [`ActionRef`] | The name of a preset as text, or the map of an inline action. |
//! | [`Preset`] | The map of its action, with a `name` key next to the others. |
//! | [`Rule`] | A map with `name`, `selector` and `action`, and with `scope`, `trigger`, `lifetime` and `dice` when it has them. |
//! | [`RuleState`] | Its name: `draft`, `armed` or `disarmed`. |
//!
//! ```yaml
//! name: Slow down order producers
//! selector: Order producers
//! scope:
//!   client.id: order-ingest
//! action: Delay 1 s
//! trigger:
//!   chance: 40%
//! lifetime: until disarmed
//! ```
//!
//! Reading a rule rejects a key it does not know.
//! Writing leaves out an empty scope, a trigger that fires on every match, the lifetime `until disarmed`,
//! and a dice key that the rule does not set.
//!
//! Live state has no form: an [`ArmedRule`], a [`Snapshot`] and an evaluation exist in memory only.
//!
//! # No IO
//!
//! The crate depends on the vocabulary of selectors, lies, incidents and triggers,
//! and on `gaslightmq_field`, `gaslightmq_kernel` and `gaslightmq_clock` for calls, API keys and instants.
//! It does not depend on the Kafka codec.
//! It has no async code, no IO, no system clock and no global randomness, and it compiles to WebAssembly.
//! The current time comes in as an [`Instant`](gaslightmq_clock::Instant) argument.

mod action;
mod armed;
mod error;
mod evaluation;
mod name;
mod preset;
mod resolved;
mod rule;
#[cfg(feature = "serde")]
mod serde;
mod set;
mod snapshot;
mod state;

pub use action::{Action, ActionRef};
pub use armed::{ArmedRule, HandFiring};
pub use error::RuleError;
pub use evaluation::{Effect, PassEvaluation, RuleEvaluation};
pub use name::{PresetName, RuleName};
pub use preset::{Preset, PresetSet};
pub use resolved::ResolvedRule;
pub use rule::Rule;
pub use set::RuleSet;
pub use snapshot::{Priority, Snapshot};
pub use state::RuleState;

/// Every item of this crate.
///
/// Import it whole wherever code speaks in rules, next to the preludes of the crates a rule is made of.
/// No name in it is a name of the standard prelude or of those preludes, so the import shadows nothing.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let rule: Result<Rule, RuleError> = "Blackhole".parse().map(|name| Rule::new(name, Conditions::new(), LieDefinition::Silence));
/// assert_eq!(rule?.name(), "Blackhole");
/// # Ok::<(), RuleError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Action, ActionRef, ArmedRule, Effect, HandFiring, PassEvaluation, Preset, PresetName, PresetSet, Priority,
        ResolvedRule, Rule, RuleError, RuleEvaluation, RuleName, RuleSet, RuleState, Snapshot,
    };
}
