use std::fmt;

use gaslightmq_lie::prelude::*;
use gaslightmq_wire::{Request, Response};

use crate::Instruction;

/// What one lie asks the pipeline for, at one stage of one call.
///
/// A [`Lie`](crate::Lie) returns it, and the pipeline does it.
/// The lie touches no socket, sets no timer and keeps no state.
///
/// | Effect | Before forwarding | After the response |
/// |---|---|---|
/// | [`Nothing`](Self::Nothing) | The call goes on as it is. | The call goes on as it is. |
/// | [`Hold`](Self::Hold) | The request waits, and the backend sees it late. | The answer waits. |
/// | [`Drop`](Self::Drop) | The backend never sees the request. The client gets no answer, and the connection stalls. | The client gets no answer, and the connection stalls. |
/// | [`Close`](Self::Close) | The backend never sees the request, and the connection closes. | The connection closes instead of sending the answer. |
/// | [`Forward`](Self::Forward) | The backend sees this request instead. The pipeline asks the lie again after the response. | Never: the backend already answered. |
/// | [`Answer`](Self::Answer) | The backend never sees the request, and this is the response. | This is the response instead. |
///
/// An effect is one lie's.
/// It is never the sum of two: adding up is combining, and that happens before any lie is asked.
/// See [the crate](crate#one-lie-and-the-lies-of-a-call).
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
///
/// let effect = Effect::Hold(TimingLie::new().with_delay(TimeSpan::from_millis(750)));
/// assert_eq!(effect.instruction(), Instruction::Hold);
/// assert!(!effect.ends_call());
/// assert_eq!(effect.to_string(), "hold +750 ms");
///
/// assert!(Effect::Drop.ends_call());
/// assert!(Effect::Close.ends_call());
/// ```
#[derive(Debug, Clone)]
pub enum Effect {
    /// The lie leaves the call as it is.
    ///
    /// A rewrite asks for it when the response holds nothing of what it rewrites.
    Nothing,
    /// Hold the call back as this timing lie says.
    ///
    /// The call waits for the delay, give or take the jitter, at the stage the lie was asked at.
    /// The throttle is a field of the answer, so it reaches the client with the response, whatever the stage.
    Hold(TimingLie),
    /// Never answer the call, and stall the connection.
    ///
    /// Later requests on the connection never reach the backend, and never get an answer.
    Drop,
    /// Close the connection.
    Close,
    /// Forward this request instead of the one the lie was given.
    ///
    /// The backend then answers another request than the client sent.
    /// The pipeline therefore asks the lie again after the response, with the request of the client,
    /// so that the lie makes the response fit that request.
    /// A refusal takes its partitions out of the request here, and puts their errors into the response there.
    Forward(Request),
    /// Send this response instead of the one the lie was given.
    ///
    /// Before forwarding, the lie was given no response.
    /// The backend then never sees the request, and this response is all the client gets to know.
    /// A refusal asks for it when no partition is left to forward.
    Answer(Response),
}

impl Effect {
    /// Which of the six things the lie asks for, without the request or the response it carries.
    pub const fn instruction(&self) -> Instruction {
        match self {
            Self::Nothing => Instruction::Nothing,
            Self::Hold(_) => Instruction::Hold,
            Self::Drop => Instruction::Drop,
            Self::Close => Instruction::Close,
            Self::Forward(_) => Instruction::Forward,
            Self::Answer(_) => Instruction::Answer,
        }
    }

    /// Whether the effect ends the call: the client gets no answer to it.
    pub const fn ends_call(&self) -> bool {
        self.instruction().ends_call()
    }

    /// Whether the lie leaves the call as it is.
    pub const fn is_nothing(&self) -> bool {
        matches!(self, Self::Nothing)
    }
}

/// Writes the instruction, and what it carries:
/// `nothing`, `hold +750 ms`, `drop`, `close`, `forward Produce v9` or `answer Produce v9`.
impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let instruction = self.instruction();
        match self {
            Self::Nothing | Self::Drop | Self::Close => write!(f, "{instruction}"),
            Self::Hold(timing) => write!(f, "{instruction} {timing}"),
            Self::Forward(request) => write!(f, "{instruction} {}", request.api()),
            Self::Answer(response) => write!(f, "{instruction} {}", response.api()),
        }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};

    use super::*;

    fn heartbeat() -> Request {
        Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap()
    }

    /// One effect of every instruction, in the order of [`Instruction::ALL`].
    fn every_effect() -> Vec<Effect> {
        let request = heartbeat();
        let response = request.response(&HeartbeatResponse::default()).unwrap();
        vec![
            Effect::Nothing,
            Effect::Hold(TimingLie::new().with_delay(TimeSpan::from_secs(1))),
            Effect::Drop,
            Effect::Close,
            Effect::Forward(request),
            Effect::Answer(response),
        ]
    }

    #[test]
    fn every_effect_has_its_instruction() {
        let instructions: Vec<Instruction> = every_effect().iter().map(Effect::instruction).collect();
        assert_eq!(instructions, Instruction::ALL);
    }

    #[test]
    fn an_effect_ends_the_call_when_its_instruction_does() {
        for effect in every_effect() {
            assert_eq!(effect.ends_call(), effect.instruction().ends_call(), "{effect}");
            assert_eq!(
                effect.is_nothing(),
                effect.instruction() == Instruction::Nothing,
                "{effect}"
            );
        }
    }

    #[test]
    fn an_effect_writes_its_instruction_and_what_it_carries() {
        let texts: Vec<String> = every_effect().iter().map(ToString::to_string).collect();
        assert_eq!(
            texts,
            [
                "nothing",
                "hold +1 s",
                "drop",
                "close",
                "forward Heartbeat v4",
                "answer Heartbeat v4",
            ]
        );
    }

    #[test]
    fn a_hold_writes_its_jitter_its_throttle_and_its_stage() {
        let timing = TimingLie::new()
            .with_delay(TimeSpan::from_secs(1))
            .with_jitter(TimeSpan::from_millis(200))
            .with_throttle(TimeSpan::from_millis(500))
            .at(Stage::BeforeForwarding);

        assert_eq!(
            Effect::Hold(timing).to_string(),
            "hold +1 s ±200 ms, throttle 500 ms before forwarding"
        );
    }
}
