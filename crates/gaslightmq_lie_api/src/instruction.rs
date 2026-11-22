use std::fmt;

use gaslightmq_lie::prelude::*;

/// An effect without what it carries: which of the six things a lie asks the pipeline for.
///
/// | Instruction | The pipeline |
/// |---|---|
/// | Nothing | Leaves the call as it is. |
/// | Hold | Holds the call back. |
/// | Drop | Never answers the call, and stalls the connection. |
/// | Close | Closes the connection. |
/// | Forward | Forwards another request than the one the client sent. |
/// | Answer | Sends another response than the truth. |
///
/// The instruction decides everything about an effect that the request or the response it carries does not:
/// the [stages](Self::allowed_at) it can happen at, and the [classes](Self::fits) of lie that can ask for it.
///
/// ```
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_lie_api::prelude::*;
///
/// // The backend already answered, so no lie forwards another request after the response.
/// assert!(Instruction::Forward.allowed_at(Stage::BeforeForwarding));
/// assert!(!Instruction::Forward.allowed_at(Stage::AfterResponse));
///
/// // Lies combine by their class, so a refusal never ends the call.
/// assert!(Instruction::Close.fits(CombinationClass::Terminal));
/// assert!(!Instruction::Close.fits(CombinationClass::Refusal));
/// assert_eq!(Instruction::Close.to_string(), "close");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Instruction {
    /// Leave the call as it is.
    Nothing,
    /// Hold the call back.
    Hold,
    /// Never answer the call, and stall the connection.
    Drop,
    /// Close the connection.
    Close,
    /// Forward another request than the one the client sent.
    Forward,
    /// Send another response than the truth.
    Answer,
}

impl Instruction {
    /// Every instruction, from the one that changes nothing to the ones that carry a message.
    pub const ALL: [Self; 6] = [
        Self::Nothing,
        Self::Hold,
        Self::Drop,
        Self::Close,
        Self::Forward,
        Self::Answer,
    ];

    /// The instruction as one word, such as `close`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Nothing => "nothing",
            Self::Hold => "hold",
            Self::Drop => "drop",
            Self::Close => "close",
            Self::Forward => "forward",
            Self::Answer => "answer",
        }
    }

    /// The instruction as the words of a sentence, after "asks to": `close the connection`.
    pub const fn phrase(self) -> &'static str {
        match self {
            Self::Nothing => "change nothing",
            Self::Hold => "hold the call back",
            Self::Drop => "drop the call",
            Self::Close => "close the connection",
            Self::Forward => "forward another request",
            Self::Answer => "send another response",
        }
    }

    /// Whether the instruction ends the call: the client gets no answer to it.
    pub const fn ends_call(self) -> bool {
        matches!(self, Self::Drop | Self::Close)
    }

    /// Whether the pipeline can follow the instruction at `stage`.
    ///
    /// Only [`Forward`](Self::Forward) has one stage.
    /// After the response, the backend already saw the request.
    pub const fn allowed_at(self, stage: Stage) -> bool {
        match self {
            Self::Forward => matches!(stage, Stage::BeforeForwarding),
            Self::Nothing | Self::Hold | Self::Drop | Self::Close | Self::Answer => true,
        }
    }

    /// Whether a lie of `class` can ask for the instruction.
    ///
    /// | Class | Can ask to |
    /// |---|---|
    /// | Modifier | Hold, or nothing. |
    /// | Rewrite, refusal | Forward, answer, or nothing. |
    /// | Terminal | Drop or close. |
    ///
    /// Lies combine by their class before any of them is asked.
    /// A lie that asked outside its class would break what combining decided:
    /// a refusal that closed the connection would end a call on which other lies still apply.
    /// A terminal lie cannot ask for nothing either, because every other lie already made way for it.
    pub const fn fits(self, class: CombinationClass) -> bool {
        match class {
            CombinationClass::Modifier => matches!(self, Self::Hold | Self::Nothing),
            CombinationClass::Rewrite | CombinationClass::Refusal => {
                matches!(self, Self::Forward | Self::Answer | Self::Nothing)
            }
            CombinationClass::Terminal => self.ends_call(),
        }
    }
}

/// Writes the instruction as one word, such as `close`.
impl fmt::Display for Instruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_are_six_instructions() {
        let names: Vec<&str> = Instruction::ALL.into_iter().map(Instruction::name).collect();
        assert_eq!(names, ["nothing", "hold", "drop", "close", "forward", "answer"]);

        let mut sorted = Instruction::ALL;
        sorted.sort();
        assert_eq!(sorted, Instruction::ALL);
    }

    #[test]
    fn an_instruction_writes_its_name() {
        for instruction in Instruction::ALL {
            assert_eq!(instruction.to_string(), instruction.name());
        }
        assert_eq!(format!("[{:>7}]", Instruction::Close), "[  close]");
    }

    #[test]
    fn a_phrase_ends_the_sentence_of_what_a_lie_asks() {
        let phrases: Vec<&str> = Instruction::ALL.into_iter().map(Instruction::phrase).collect();
        assert_eq!(
            phrases,
            [
                "change nothing",
                "hold the call back",
                "drop the call",
                "close the connection",
                "forward another request",
                "send another response",
            ]
        );
    }

    #[test]
    fn drop_and_close_end_the_call() {
        let ending: Vec<Instruction> = Instruction::ALL
            .into_iter()
            .filter(|instruction| instruction.ends_call())
            .collect();
        assert_eq!(ending, [Instruction::Drop, Instruction::Close]);
    }

    #[test]
    fn only_forward_has_one_stage() {
        for instruction in Instruction::ALL {
            assert!(instruction.allowed_at(Stage::BeforeForwarding), "{instruction}");
            assert_eq!(
                instruction.allowed_at(Stage::AfterResponse),
                instruction != Instruction::Forward,
                "{instruction}"
            );
        }
    }

    #[test]
    fn every_class_can_ask_for_its_own_instructions() {
        use CombinationClass::{Modifier, Refusal, Rewrite, Terminal};
        use Instruction::{Answer, Close, Drop, Forward, Hold, Nothing};

        let allowed = |class: CombinationClass| -> Vec<Instruction> {
            Instruction::ALL
                .into_iter()
                .filter(|instruction| instruction.fits(class))
                .collect()
        };

        assert_eq!(allowed(Modifier), [Nothing, Hold]);
        assert_eq!(allowed(Rewrite), [Nothing, Forward, Answer]);
        assert_eq!(allowed(Refusal), [Nothing, Forward, Answer]);
        assert_eq!(allowed(Terminal), [Drop, Close]);
    }

    #[test]
    fn only_a_terminal_lie_ends_the_call() {
        for class in CombinationClass::ALL {
            for instruction in Instruction::ALL
                .into_iter()
                .filter(|instruction| instruction.fits(class))
            {
                assert_eq!(
                    instruction.ends_call(),
                    class == CombinationClass::Terminal,
                    "{class} {instruction}"
                );
            }
        }
    }

    #[test]
    fn every_instruction_fits_a_class() {
        for instruction in Instruction::ALL {
            assert!(
                CombinationClass::ALL.into_iter().any(|class| instruction.fits(class)),
                "{instruction}"
            );
        }
    }
}
