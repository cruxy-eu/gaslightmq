//! The call record: one request and its response on one session, and what GaslightMQ did to it and why.
//!
//! The pipeline builds a record for every call.
//! The UI shows it, a scenario counts it, a recording keeps it, and a replay compares it with a new one.
//! This crate holds the record as data, and it evaluates nothing.
//!
//! | Type | Is |
//! |---|---|
//! | [`Call`] | The record: the fields of the call, what the client received, the explanation and the timing. |
//! | [`CallNumber`], [`CallId`] | Which call of its session this is, and which call of its engine. |
//! | [`Answer`], [`ResponseSummary`] | A response without its payload: the answer for the call, and for each part with its own. |
//! | [`Delivered`] | What the client actually received: a response, a response that nobody could read, nothing, or a closed connection. |
//! | [`Verdict`] | Pass, or the lies that applied. |
//! | [`Explanation`] | Why the verdict came out this way: every rule the call touched. |
//! | [`RuleExplanation`] | One of those rules: what its selector matched, what its trigger decided, and what its action did. |
//! | [`ExplanationStatus`] | Matched, rolled, fired, applied, shadowed or pre-empted. |
//! | [`TriggerExplanation`] | What a trigger looked at and decided: the match number, the checks, the budget. |
//! | [`TriggerDecision`] | Whether the rule fired, and why not when it did not. |
//! | [`TriggerChecks`], [`DieCheck`], [`WindowCheck`], [`BudgetCheck`] | What the parts of a trigger said, as plain numbers. |
//! | [`LieFate`], [`LostTarget`] | What became of a lie that fired: where it applied, where it lost to which rule, and why it could not be told. |
//! | [`RecordedRuleName`] | The name a rule had when the call met it. |
//! | [`Failure`] | What went wrong inside GaslightMQ while it handled the call. |
//! | [`CallTiming`] | When the call arrived, how long the backend took, and how long the client waited. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! # The parts of a call
//!
//! | Part | Meaning | Here |
//! |---|---|---|
//! | Session | The session the call ran on, with its identity. | The connection fields of [`Call::fields`]. |
//! | Request | The decoded request, with every field. | The request and learned fields of [`Call::fields`], and its parts. |
//! | Truth | What the backend answered, if the request reached it. | The response fields of [`Call::fields`], for the call and for each part. |
//! | Upstream | What the real cluster answered, when the overlay changed it. | [`Call::upstream`] |
//! | Delivered | What the client actually received. | [`Call::delivered`] |
//! | Verdict | Pass, or the lies that applied. | [`Call::verdict`] |
//! | Explanation | Why the verdict came out this way. | [`Call::explanation`] |
//! | Timing | When the call arrived, how long the backend took, how long the client waited. | [`Call::timing`] |
//!
//! A record is a [`FieldSource`](gaslightmq_field::FieldSource),
//! so one selector targets a live call and filters a recorded one.
//!
//! # An explanation speaks in its own words
//!
//! An explanation describes what the rules and their triggers did, and this crate knows neither.
//! It depends on vocabulary only, and the crates that evaluate rules and triggers are logic.
//! The explanation therefore has a vocabulary of its own for what it reports:
//!
//! - A rule is the [name it had](RecordedRuleName) when the call met it.
//! - A trigger is a [`TriggerExplanation`]: a [`TriggerDecision`], the match number as a number that is at least one,
//!   and one check per part of the trigger.
//!   A [`DieCheck`] holds the roll and the chance as parts per million,
//!   a [`WindowCheck`] holds the window and the time since arming, and a [`BudgetCheck`] holds what was left of how much.
//! - A lie is the [`TargetedLie`](gaslightmq_lie::TargetedLie) the rule fired, in a [`LieFate`]
//!   that says where it applied and where it lost, and each [`LostTarget`] names the rule that won.
//!   A lie that GaslightMQ could not tell is [untold](LieFate::untold), with the [`Failure`] that says why.
//!
//! Whoever evaluates a call writes these down.
//! A record only checks that it does not contradict itself:
//! a trigger does not fire against one of its own checks, and a rule that did not fire has no lie.
//!
//! ```
//! use std::num::NonZeroU64;
//!
//! use gaslightmq_call::prelude::*;
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//!
//! let on = |part: &str| Target::of_parts([part.parse().expect("a part")]).expect("one part");
//! let number = |value| NonZeroU64::new(value).expect("at least one");
//!
//! // Three rules matched one Fetch. `Leader flap` has `chance: 40%` and `lifetime: 3 firings`.
//! let flap = TriggerExplanation::new(
//!     TriggerDecision::Fired,
//!     Some(number(7)),
//!     TriggerChecks::new().with_die(DieCheck::new(374_213, 400_000)?),
//! )?
//! .with_budget(BudgetCheck::new(2, number(3))?);
//! // `Slow fetch` has no trigger, and `Refuse every 2nd` has `every: 2`.
//! let slow = TriggerExplanation::new(TriggerDecision::Fired, Some(number(12)), TriggerChecks::new())?;
//! let every = TriggerExplanation::new(TriggerDecision::Held, Some(number(5)), TriggerChecks::new().with_count(false))?;
//!
//! let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(on("orders.eu/3"));
//! let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_millis(750))).on(Target::Call);
//!
//! let explanation = Explanation::new([
//!     RuleExplanation::lied("Leader flap".parse()?, Priority::new(0), on("orders.eu/3"), flap, LieFate::all_applied(refusal))?,
//!     RuleExplanation::lied("Slow fetch".parse()?, Priority::new(1), on("orders.eu/3"), slow, LieFate::all_applied(delay))?,
//!     RuleExplanation::held("Refuse every 2nd".parse()?, Priority::new(2), on("orders.eu/3"), every)?,
//! ])?;
//!
//! // Why did this call fail?
//! assert_eq!(explanation.verdict().to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3; +750 ms");
//!
//! // Why did my rule not fire?
//! let line = explanation.rule("Refuse every 2nd").expect("the rule matched");
//! assert_eq!(line.to_string(), "Refuse every 2nd: matched orders.eu/3; match 5, count did not agree: held");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Errors
//!
//! Every type that checks its value has an error of its own, named after it.
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`CallNumber`] | [`CallNumberError`] | The number is zero, or the text is no number. |
//! | [`CallId`] | [`CallIdError`] | The text is not a connection and a call number. |
//! | [`RecordedRuleName`] | [`RecordedRuleNameError`] | The text is empty. |
//! | [`TriggerDecision`] | [`TriggerDecisionError`] | No decision has that name. |
//! | [`DieCheck`] | [`DieCheckError`] | The roll or the chance is more parts per million than it can be. |
//! | [`WindowCheck`] | [`WindowCheckError`] | The window is never open. |
//! | [`BudgetCheck`] | [`BudgetCheckError`] | More is left than the budget allows. |
//! | [`TriggerExplanation`] | [`TriggerExplanationError`] | The decision contradicts the match number or a check. |
//! | [`LieFate`] | [`LieFateError`] | The lie went nowhere, it applied outside its target, or it applied although it could not be told. |
//! | [`RuleExplanation`] | [`RuleExplanationError`] | The trigger and the action contradict each other. |
//! | [`Explanation`] | [`ExplanationError`] | A rule has two lines, or a line names a rule that has none. |
//! | [`CallTiming`] | [`CallTimingError`] | A moment comes before the one it follows. |
//! | [`Call`] | [`CallError`] | The fields name no connection, or they still wait for the backend. |
//!
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//! An error that holds another one writes that message once, and keeps the error in its field `error`.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, every type of a record has a `serde` form.
//! The forms need a format that describes itself, such as JSON or YAML.
//!
//! | Type | Form |
//! |---|---|
//! | [`CallNumber`] | A number. |
//! | [`CallId`], [`RecordedRuleName`], [`TriggerDecision`], [`Failure`] | The text. |
//! | [`Answer`] | `ok`, or `{ error: NOT_LEADER_OR_FOLLOWER }`. |
//! | [`ResponseSummary`] | A map with `answer`, and with `parts` when a part has its own answer: a map from the part to its answer. |
//! | [`Delivered`] | `{ response: … }`, `unread-response`, `nothing` or `connection-closed`. |
//! | [`Verdict`] | The list of its lies. A pass is the empty list. |
//! | [`DieCheck`] | `{ roll: 374213, chance: 400000 }`, in parts per million. |
//! | [`WindowCheck`] | `{ opens-after: 10s, closes-after: 40s, since-armed: 12s }`. |
//! | [`BudgetCheck`] | `{ left: 2, limit: 3 }`. |
//! | [`TriggerExplanation`] | A map with `decision`, and with `match-number`, `count-agreed`, `die`, `window`, `budget` and `reason` when it has them. |
//! | [`LostTarget`] | `{ on: …, to: Leader flap }`. |
//! | [`LieFate`] | A map with `fired`, and with `applied`, `shadowed`, `pre-empted` and `untold` when it has them. `untold` is the failure of a lie that GaslightMQ could not tell. |
//! | [`RuleExplanation`] | A map with `rule` and `priority`, and the keys of how far the rule got. |
//! | [`Explanation`] | A map with `rules` and `failure`, each when it has it. |
//! | [`CallTiming`] | A map with `arrived`, and with `forwarded`, `answered` and `delivered` when it has them. |
//! | [`Call`] | A map with `number`, `fields`, `delivered`, `verdict`, `explanation` and `timing`, and with `upstream` when it has it. |
//!
//! A line of an explanation says how far its rule got with the keys it has:
//!
//! | The rule | Keys next to `rule` and `priority` |
//! |---|---|
//! | Matched, and did not fire. | `matched`, `trigger` |
//! | Fired its lie. | `matched`, `trigger`, `lie` |
//! | Fired its incidents. | `matched`, `trigger`, `incidents` |
//! | Waited for the truth of a call that ended before it. | `ended-by` |
//!
//! The `fields` of a call are a map with three lists, each when it has entries:
//! `values` for what the call and its session have, `learned` for what the client context provides,
//! and `parts` for the topics and partitions of the request.
//! A value is a map with one entry, such as `{ api: Produce }`.
//! A part is `{ part: orders.eu/3 }`, with a `truth` when it has one of its own: `ok`, `{ error: … }` or `none`.
//! Fields without a response field are those of a call that never reached the backend.
//!
//! A call writes its `verdict` for whoever reads the file.
//! Reading takes the verdict from the explanation, and skips the key.
//!
//! Reading builds every type through its own checks.
//! A record that contradicts itself does not read, and the message says what is wrong.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_field` for the fields of a call and its targets,
//! on `gaslightmq_lie` for the lies of a verdict, on `gaslightmq_incident` for the incidents a call fired,
//! on `gaslightmq_kernel` for the error code of an answer, and on `gaslightmq_clock` for instants and time spans.
//! It does not depend on the Kafka codec.
//! It has no async code, no IO, no system clock and no randomness, and it compiles to WebAssembly.

mod call;
mod explanation;
#[cfg(feature = "serde")]
mod form;
mod id;
mod response;
mod rule;
mod timing;
mod trigger;
mod verdict;

pub use call::{Call, CallError};
pub use explanation::{
    Explanation, ExplanationError, ExplanationStatus, Failure, LieFate, LieFateError, LostTarget, RuleExplanation,
    RuleExplanationError,
};
pub use id::{CallId, CallIdError, CallNumber, CallNumberError};
pub use response::{Answer, Delivered, ResponseSummary};
pub use rule::{RecordedRuleName, RecordedRuleNameError};
pub use timing::{CallTiming, CallTimingError};
pub use trigger::{
    BudgetCheck, BudgetCheckError, DieCheck, DieCheckError, TriggerChecks, TriggerDecision, TriggerDecisionError,
    TriggerExplanation, TriggerExplanationError, WindowCheck, WindowCheckError,
};
pub use verdict::Verdict;

/// Every type of this crate, with its error.
///
/// Import it whole wherever code speaks in call records,
/// next to the preludes of `gaslightmq_kernel`, `gaslightmq_field`, `gaslightmq_lie` and `gaslightmq_incident`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_call::prelude::*;
///
/// let number: Result<CallNumber, CallNumberError> = "3".parse();
/// assert_eq!(number?, CallNumber::FIRST.next().next());
/// # Ok::<(), CallNumberError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Answer, BudgetCheck, BudgetCheckError, Call, CallError, CallId, CallIdError, CallNumber, CallNumberError,
        CallTiming, CallTimingError, Delivered, DieCheck, DieCheckError, Explanation, ExplanationError,
        ExplanationStatus, Failure, LieFate, LieFateError, LostTarget, RecordedRuleName, RecordedRuleNameError,
        ResponseSummary, RuleExplanation, RuleExplanationError, TriggerChecks, TriggerDecision, TriggerDecisionError,
        TriggerExplanation, TriggerExplanationError, Verdict, WindowCheck, WindowCheckError,
    };
}
