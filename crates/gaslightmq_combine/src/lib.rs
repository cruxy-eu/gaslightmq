//! How lies combine: which lies conflict, which win by priority, which are shadowed or pre-empted,
//! and what the lies that apply do to the call.
//!
//! Several rules can fire on one call, each with a lie.
//! This crate decides between those lies.
//! It does not know what a rule is: every lie comes with an identity the caller chooses,
//! and every answer names lies by that identity.
//!
//! | Type | Is |
//! |---|---|
//! | [`Claim`] | What a lie claims of a call: its class, its stage, and its target or its aspect. |
//! | [`FiredLie`] | A lie that fired: an identity, a priority and a claim. |
//! | [`RequestPass`] | The lies of the request pass, combined. It says what happens before forwarding. |
//! | [`Combination`] | The lies of the whole call, combined. |
//! | [`Fate`] | What became of one lie: where it applied, and where it was shadowed or pre-empted. |
//! | [`Loss`] | A target a lie lost, and the lie it lost it to. |
//! | [`StageEffect`] | What the lies that apply do to the call at one stage. |
//! | [`Winners`] | Which lie holds each target, among lies that conflict there. |
//! | [`Delay`] | How long the call waits at one stage. |
//!
//! The [`prelude`] holds all of them, with the one [error](#errors).
//!
//! # The rules
//!
//! The [`CombinationClass`](gaslightmq_lie::CombinationClass) of a lie decides how it combines.
//!
//! | Class | Combination |
//! |---|---|
//! | Modifier | Modifiers stack. Delays add up, with their jitters. Throttle takes the maximum. |
//! | Rewrite | Two rewrites of the same aspect of the same target conflict. |
//! | Refusal | Two refusals of the same target conflict. A refusal pre-empts the rewrites of its target. |
//! | Terminal | A terminal lie conflicts with every other terminal lie and with every refusal on the call. It pre-empts modifiers and rewrites. |
//!
//! - **Lies that do not conflict all apply.**
//! - **Priority decides a conflict.**
//!   The lies go from the highest priority down, and each takes the targets that are still free.
//!   The loser is **shadowed** on the target the two share.
//! - **Within one pass, a loser wins nothing.**
//!   A lie only shadows another lie on a target it applies to itself.
//!   A refusal above a blackhole shadows the blackhole, and a refusal of another partition below the two then applies.
//!   [Two passes](#two-passes) have one exception.
//! - **A lie with nothing left to change is pre-empted**, whatever its priority.
//!   Pre-emption comes before shadowing:
//!   a rewrite of a refused partition is pre-empted, even when a rewrite above it changes the same aspect.
//! - **A terminal lie pre-empts what acts at its stage or later.**
//!   A delay before forwarding still holds the request back when a hang-up after the response ends the call.
//!   Its throttle reaches no client, because the call has no answer.
//! - **The whole call is one target.**
//!   A refusal of the whole call and a refusal of a part conflict, and the loser loses its whole target.
//!   A rewrite of the whole call keeps something to change until the whole call is refused.
//!
//! ```
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_combine::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_lie::prelude::*;
//!
//! let [three, four]: [Part; 2] = ["orders.eu/3".parse().expect("a part"), "orders.eu/4".parse().expect("a part")];
//! let on = |part: &Part| Target::of_parts([part.clone()]).expect("one part");
//!
//! // Three rules fired on one Fetch. The pipeline hands over each lie with its rule's place in the priority order.
//! let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_millis(750))).on(on(&three));
//! let flap = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(on(&three));
//! let stale = Claim::Rewrite { aspect: Aspect::HighWatermark, target: on(&four) };
//!
//! let request = RequestPass::of([
//!     FiredLie::new("Slow fetch", Priority::new(0), Claim::try_from(&delay)?),
//!     FiredLie::new("Leader flap", Priority::new(1), Claim::try_from(&flap)?),
//!     FiredLie::new("Stale watermark", Priority::new(2), stale),
//! ]);
//!
//! // Before forwarding: `orders.eu/3` leaves the request, and nothing ends the call.
//! let before = request.before_forwarding();
//! assert_eq!(before.terminal(), None);
//! assert_eq!(before.refusals().of(&three), Some(&"Leader flap"));
//!
//! // No rule waited for the truth, so the response pass adds nothing.
//! let call = request.without_response_pass();
//!
//! // After the response: the watermark of `orders.eu/4` goes stale, and the answer waits.
//! let after = call.after_response();
//! assert_eq!(after.rewrite(Aspect::HighWatermark, &four), Some(&"Stale watermark"));
//! assert_eq!(after.delay().length(), TimeSpan::from_millis(750));
//!
//! // None of the three conflict, so all of them apply, and every firing counts.
//! assert!(call.fates().iter().all(Fate::counts));
//! # Ok::<(), ClaimError>(())
//! ```
//!
//! # Two passes
//!
//! Some rules wait for the truth, so the pipeline evaluates its rules in two passes.
//! [`RequestPass::of`] combines the lies of the request pass,
//! and [`RequestPass::response_pass`] adds the lies of the response pass.
//!
//! Priority orders the lies within one pass.
//! Between the passes, the stage decides.
//! What took effect before forwarding stays: the fate of such a lie is final after the request pass.
//! A lie that acts after the response is open until the response pass ran.
//! [`RequestPass::response_pass`] lists what happens between the passes.
//!
//! A loss before forwarding stays as well, and that is the one time a loser keeps a win.
//! A hang-up after the response shadows a refusal before forwarding in the request pass,
//! and a refusal of the response pass, with a higher priority, then shadows the hang-up.
//! The request was forwarded whole by then, so the first refusal stays shadowed.
//! Its [`Loss`] names the hang-up, which ended nothing, and its firing does not count.
//!
//! ```
//! use gaslightmq_combine::prelude::*;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_lie::prelude::*;
//!
//! // A scenario rule blackholes the call. A live rule, with a higher priority, waits for the truth.
//! let request = RequestPass::of([
//!     FiredLie::new("Blackhole", Priority::new(1), Claim::Terminal { stage: Stage::BeforeForwarding }),
//! ]);
//! assert_eq!(request.before_forwarding().terminal(), Some(&"Blackhole"));
//!
//! // The call ended before the truth existed, so the live rule is pre-empted.
//! let lost = Claim::Refusal { stage: Stage::AfterResponse, target: Target::Call };
//! let call = request.response_pass([FiredLie::new("Lost acknowledgement", Priority::new(0), lost)]);
//! let lost = call.fate_of(&"Lost acknowledgement").expect("the lie fired");
//! assert!(!lost.counts());
//! assert_eq!(lost.pre_empted()[0].to(), &"Blackhole");
//! ```
//!
//! # Firings
//!
//! A firing counts when its lie applied on at least one target, and [`Fate::counts`] says so.
//! A lie that is shadowed on one partition and applies on another counts as one firing.
//! A lie that was shadowed or pre-empted on every target gives its firing back to the budget.
//!
//! # Errors
//!
//! Combining never fails: every lie gets a [`Fate`], and a lie that loses is no error.
//! Only [`Claim`] checks a value, so it has the one error of the crate, [`ClaimError`].
//! A [`TargetedLie`](gaslightmq_lie::TargetedLie) does not become a claim when its definition breaks its own word:
//! it is a rewrite without an aspect, or a modifier without a timing.
//! No definition of today's `gaslightmq_lie` does.
//! The message of the error is complete, so it has no [`source`](std::error::Error::source).
//!
//! # No `serde` feature
//!
//! Nothing in this crate is a definition, and nothing travels in a recording.
//! The pipeline turns a [`Fate`] into the explanation of a call record.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_lie` for the vocabulary of lies, on `gaslightmq_field` for the target,
//! and on `gaslightmq_clock` for the time span.
//! It does not depend on the Kafka codec.
//! Combining is a pure function of the lies that fired:
//! it has no async code, no IO, no clock and no randomness, and it compiles to WebAssembly.

mod board;
mod claim;
mod combination;
mod effect;
mod fate;

pub use claim::{Claim, ClaimError, FiredLie};
pub use combination::{Combination, RequestPass};
pub use effect::{Delay, StageEffect, Winners};
pub use fate::{Fate, Loss};

/// Every type of this crate, with the error of a claim.
///
/// Import it whole wherever code combines lies, next to the preludes of `gaslightmq_field` and `gaslightmq_lie`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let call = RequestPass::of([FiredLie::new(1, Priority::HIGHEST, TimingLie::new())]).without_response_pass();
/// assert!(call.fates()[0].counts());
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Claim, ClaimError, Combination, Delay, Fate, FiredLie, Loss, RequestPass, StageEffect, Winners};
}
