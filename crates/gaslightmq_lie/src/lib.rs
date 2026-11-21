//! The lie vocabulary: what a client can be told that differs from the truth.
//!
//! A lie affects what one client sees on one call, and nothing changes in the cluster.
//! This crate holds every lie as data, and it holds no behaviour.
//! The crates of behaviour read a definition and do the telling.
//!
//! | Type | Is |
//! |---|---|
//! | [`LieKind`] | Which of the eight kinds of lie, without parameters. |
//! | [`Stage`] | Where in a call a lie acts: before forwarding, or after the response. |
//! | [`CombinationClass`] | How a lie combines with the other lies on a call. |
//! | [`Aspect`] | What a rewrite changes about its target. |
//! | [`LieDefinition`] | A lie as a rule's action or a preset writes it. |
//! | [`RefusalLie`], [`TimingLie`], [`HangUpLie`] | The parameters of a refusal, a timing lie and a hang-up. |
//! | [`TargetedLie`] | A lie with the target it applies to: one line of a verdict. |
//! | [`LieError`] | Why a value cannot become a part of a lie. |
//!
//! The [`prelude`] holds all of them.
//!
//! # From a definition to a verdict
//!
//! ```
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//!
//! // Two rules, each with a lie as its action.
//! let flap = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
//! let slow = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_millis(750)));
//!
//! // The kind says how the two combine: a refusal and a modifier do not conflict.
//! assert_eq!(flap.class(), CombinationClass::Refusal);
//! assert_eq!(slow.class(), CombinationClass::Modifier);
//!
//! // It also says when they act: the refused partition never reaches the backend, and the answer waits.
//! assert_eq!(flap.stage(), Stage::BeforeForwarding);
//! assert_eq!(slow.stage(), Stage::AfterResponse);
//!
//! // Both rules match a Fetch, and their selector matched one of its partitions.
//! let matched = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
//! let verdict = [slow.on(matched.clone()), flap.on(matched)];
//!
//! // The refusal changes that partition, and the delay holds the whole call back.
//! assert_eq!(verdict[0].to_string(), "+750 ms");
//! assert_eq!(verdict[1].to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
//! # Ok::<(), FieldError>(())
//! ```
//!
//! # Kinds without a definition
//!
//! [`LieKind`] names all eight kinds, and [`Aspect`] names what the rewrites change.
//! Whoever combines lies or draws a pad grid can therefore speak of every kind today.
//!
//! [`LieDefinition`] has the four kinds that need no payload: refusal, timing, silence and hang-up.
//! Topology, data, progress and configuration get their definition here when their behaviour arrives.
//!
//! # The `serde` feature
//!
//! With the `serde` feature, a definition reads and writes the map a rule or a preset holds.
//! The `lie` key names the kind, and the other keys are the parameters:
//!
//! ```yaml
//! { lie: refusal, error: NOT_LEADER_OR_FOLLOWER }
//! { lie: refusal, error: REQUEST_TIMED_OUT, stage: after }
//! { lie: timing, delay: 1s, jitter: 200ms }
//! { lie: timing, throttle: 500ms, stage: before }
//! { lie: silence }
//! { lie: hang-up, stage: after }
//! ```
//!
//! | Key | Holds | On |
//! |---|---|---|
//! | `error` | The error code, by the name Kafka gives it. Required. | `refusal` |
//! | `delay`, `jitter`, `throttle` | A time span, such as `200ms`. Zero when left out. | `timing` |
//! | `stage` | `before` or `after`. The default stage of the kind when left out. | `refusal`, `timing`, `hang-up` |
//!
//! Writing leaves out the default stage of the kind, and a time span of zero.
//!
//! Reading rejects a key that does not belong to the lie, and a stage at which the lie never acts.
//! A `delay` on a refusal would otherwise silently do nothing.
//! Reading also rejects `topology`, `data`, `progress` and `configuration`, which have no definition yet.
//!
//! The map is flat, so a preset or a rule holds the keys of a lie next to its own.
//! `#[serde(flatten)]` on a field of type [`LieDefinition`] reads `{ name: Delay 1 s, lie: timing, delay: 1s }`.
//!
//! The other types write these forms:
//!
//! | Type | Form |
//! |---|---|
//! | [`LieKind`] | Its name, such as `hang-up`. |
//! | [`Stage`] | `before` or `after`. |
//! | [`CombinationClass`] | Its name, such as `modifier`. |
//! | [`Aspect`] | Its name, such as `high-watermark`. |
//! | [`TargetedLie`] | The map of the definition, with a `target` key unless the target is the whole call: `{ lie: refusal, error: NOT_LEADER_OR_FOLLOWER, target: { parts: [orders.eu/3] } }`. |
//!
//! [`RefusalLie`], [`TimingLie`] and [`HangUpLie`] have no form of their own.
//! Without the `lie` key, nothing tells their maps apart.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_field` for the target, on `gaslightmq_kernel` for the error code,
//! and on `gaslightmq_clock` for the time span.
//! It does not depend on the Kafka codec.
//! It has no async code, no IO, no clock and no randomness, and it compiles to WebAssembly.

mod aspect;
mod class;
mod definition;
mod error;
#[cfg(feature = "serde")]
mod form;
mod kind;
mod stage;
mod targeted;

pub use aspect::Aspect;
pub use class::CombinationClass;
pub use definition::{HangUpLie, LieDefinition, RefusalLie, TimingLie};
pub use error::LieError;
pub use kind::LieKind;
pub use stage::Stage;
pub use targeted::TargetedLie;

/// Every type of this crate.
///
/// Import it whole wherever code speaks in lies, next to the preludes of `gaslightmq_kernel` and `gaslightmq_field`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let lie = LieDefinition::from(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse));
/// assert_eq!(lie.kind(), LieKind::Refusal);
/// assert_eq!(lie.to_string(), "REQUEST_TIMED_OUT after the response");
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Aspect, CombinationClass, HangUpLie, LieDefinition, LieError, LieKind, RefusalLie, Stage, TargetedLie,
        TimingLie,
    };
}
