//! The contract between the pipeline and a kind of lie: what one lie asks the pipeline for.
//!
//! A lie has a definition and a behaviour.
//! The definition is data in `gaslightmq_lie`, and it says what the client is told.
//! The behaviour does the telling: it reads the request or the response, and it asks the pipeline for an [`Effect`].
//! This crate holds the trait that every behaviour implements, and the behaviour of the three kinds that every build has.
//!
//! | Type | Is |
//! |---|---|
//! | [`Lie`] | The behaviour of one kind of lie: what a lie of that kind asks for, before forwarding and after the response. |
//! | [`Effect`] | What one lie asks the pipeline for, at one stage of one call. |
//! | [`Instruction`] | An effect without what it carries: nothing, hold, drop, close, forward or answer. |
//! | [`Lies`] | The lies a pipeline can tell: one behaviour per kind, as the engine registers them. |
//! | [`Timing`], [`Silence`], [`HangUp`] | The behaviour of a timing lie, a silence and a hang-up: hold, drop and close. |
//!
//! The [`prelude`] holds all of them, with their [errors](#errors).
//!
//! # Asking a lie
//!
//! The pipeline holds a set of [`Lies`] and names no kind.
//! It asks the set with a lie and its target, and it does what the effect says.
//!
//! ```
//! use gaslightmq_clock::TimeSpan;
//! use gaslightmq_field::prelude::*;
//! use gaslightmq_lie::prelude::*;
//! use gaslightmq_lie_api::prelude::*;
//! use gaslightmq_wire::Request;
//! use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
//!
//! // The engine registers the kinds of its build. These three are in every build.
//! let lies = Lies::of_every_build();
//!
//! // A Heartbeat arrives, and two rules fire on it.
//! let request = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default())?;
//! let slow = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_secs(45)).at(Stage::BeforeForwarding));
//! let late_hang_up = LieDefinition::from(HangUpLie::new().at(Stage::AfterResponse));
//!
//! // Before forwarding, the delay asks to hold the request back.
//! let effect = lies.before_forwarding(&slow.on(Target::Call), &request)?;
//! assert_eq!(effect.to_string(), "hold +45 s before forwarding");
//!
//! // The backend answers, and the hang-up asks to close the connection instead of sending the answer.
//! let truth = request.response(&HeartbeatResponse::default())?;
//! let effect = lies.after_response(&late_hang_up.on(Target::Call), &request, &truth)?;
//! assert_eq!(effect.instruction(), Instruction::Close);
//! assert!(effect.ends_call());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [`Lie`] says when the pipeline asks, and [`Effect`] says what each effect means at each stage.
//!
//! # One lie and the lies of a call
//!
//! Several lies can apply to one call, and `gaslightmq_combine` decides between them.
//! Combining comes first, and it reads definitions only.
//! Asking comes second, and it is the only place where a lie meets a request or a response.
//!
//! | Question | Who answers | From |
//! |---|---|---|
//! | Which lies apply, and which are shadowed or pre-empted? | `gaslightmq_combine` | The definitions and the priorities. |
//! | Which target does a lie hold at a stage? | `gaslightmq_combine` | The same. |
//! | How long does the call wait at a stage, and which throttle does the answer carry? | `gaslightmq_combine` | The timing lies that apply. |
//! | What does this one lie do to this request or this response? | A [`Lie`], with an [`Effect`] | The definition, the target, the request and the response. |
//!
//! Three rules keep the two apart:
//!
//! - **The pipeline only asks a lie that applies, with the target it holds.**
//!   A behaviour never learns of another lie, and it never decides whether it applies.
//! - **An effect is one lie's.**
//!   Nothing in this crate adds two effects up, or picks between them.
//! - **An effect stays within the class of its lie.**
//!   Lies combine by their [`CombinationClass`](gaslightmq_lie::CombinationClass), so a refusal that closed the connection
//!   would end a call on which combining let other lies apply.
//!   [`Lies`] rejects such an effect with [`LieError::InstructionNotAllowed`], and [`Instruction::fits`] has the table.
//!
//! A timing lie is all definition, so the two meet there.
//! [`Timing`] asks to [hold](Effect::Hold) the call as its own lie says, and it knows no other timing lie.
//! The wait of a stage is the sum of the delays that apply there, and the throttle is their maximum:
//! both are sums over lies, so they come from the stage effect of `gaslightmq_combine`.
//! A pipeline that combined its lies takes them from there, and it does not add up holds.
//!
//! # A new kind
//!
//! A new kind of lie implements [`Lie`] in a crate of its own, and the engine registers it with [`Lies::with`].
//! The pipeline does not change, because it asks the set and reads the [`Effect`].
//!
//! # Errors
//!
//! | Type | Error | What is wrong |
//! |---|---|---|
//! | [`Lie`] | [`LieError`] | A lie could not be told: no behaviour is registered for its kind, the kind has no behaviour at the stage, the effect is not allowed, the request or the response could not be read or written, the request does not name a part the lie targets, the response has no place for an error code, or the response answers another request. |
//! | [`Lies`] | [`LiesError`] | Two behaviours tell the same kind of lie. |
//!
//! [`Effect`] and [`Instruction`] take every value, so they have no error.
//! [`LieError::Unreadable`] and [`LieError::Unwritable`] write the message of the error they hold, once.
//! The message of every error is complete, so no error has a [`source`](std::error::Error::source).
//!
//! A lie that fails changes nothing.
//! The pipeline passes the call on untouched and writes the error into the explanation of the call,
//! so that a mistake of GaslightMQ never looks like a lie.
//!
//! # No `serde` feature
//!
//! Nothing in this crate is a definition, and nothing travels in a file or in a recording.
//! An effect lives from the moment a lie asks for it to the moment the pipeline did it.
//!
//! # No IO
//!
//! The crate depends on `gaslightmq_lie` for the definitions, on `gaslightmq_wire` for the request and the response,
//! and on `gaslightmq_field` for the parts of a call that an error names.
//! A behaviour is a pure function of what it is given:
//! the crate has no async code, no IO, no clock and no randomness.
//! It reaches the Kafka codec, so it does not compile to WebAssembly, and no crate that the UI shares depends on it.

mod effect;
mod every_build;
mod instruction;
mod lie;
mod lies;

pub use effect::Effect;
pub use every_build::{HangUp, Silence, Timing};
pub use instruction::Instruction;
pub use lie::{Lie, LieError};
pub use lies::{Lies, LiesError};

/// Every item of this crate, with its error.
///
/// Import it whole wherever code tells lies, next to the preludes of `gaslightmq_field` and `gaslightmq_lie`.
/// No name in it is a name of those preludes or of the standard one, so the import shadows nothing.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
///
/// let lies: Result<Lies, LiesError> = Lies::new().with(Timing);
/// assert!(lies?.tells(LieKind::Timing));
/// # Ok::<(), LiesError>(())
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{Effect, HangUp, Instruction, Lie, LieError, Lies, LiesError, Silence, Timing};
}
