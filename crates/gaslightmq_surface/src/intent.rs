use std::fmt;

use crate::{EncoderTurn, Hold, Pad};

/// What a surface asks for: the state that someone wants for the rule of a [`Pad`], or a change of one parameter.
///
/// A surface holds no rule, so it does not say what a rule becomes.
/// A press says the state it wants, *arm* or *disarm*, and never *the other state*,
/// so two people who press one pad to arm it both find it armed.
/// Whoever holds the rules acts on the intent, and an intent whose state is there already changes nothing.
///
/// | Intent | Whoever holds the rules |
/// |---|---|
/// | [`Arm`](Self::Arm) | Arms the rule the pad points at, unless it is armed. A pad without a rule gets [the rule of its position](crate::PadLayout::rule) first, as a live rule. |
/// | [`Disarm`](Self::Disarm) | Disarms the rule the pad points at, unless it is not armed. A disarm ends every hold of the rule. |
/// | [`Hold`](Self::Hold) | Adds a [`Hold`] to the rule the pad points at, and arms it as `Arm` does. It answers with the hold. |
/// | [`Release`](Self::Release) | Takes the hold away, and disarms the rule when it was its last hold. A hold that a disarm ended changes nothing. |
/// | [`Turn`](Self::Turn) | [Applies the turn](crate::EncoderMapping::apply) to the rule the pad points at. |
///
/// A turn says how far an encoder moved, and not the value it reached,
/// so two surfaces that turn the same encoder do not fight.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let pad = Pad::new(
///     "order-desk".parse().expect("a layout name"),
///     "order-ingest".parse()?,
///     Row::from_index(0),
///     Column::from_index(3),
/// );
///
/// let arm = Intent::Arm { pad: pad.clone() };
/// assert_eq!(arm.to_string(), "arm the rule at row 1, column 4 on layer order-ingest of layout order-desk");
///
/// let release = Intent::Release { pad: pad.clone(), hold: Hold::new(7) };
/// assert_eq!(release.to_string(), "release hold 7 of the rule at row 1, column 4 on layer order-ingest of layout order-desk");
///
/// let turn = Intent::Turn { pad, turn: EncoderTurn::new(Encoder::Probability, Steps::new(3)) };
/// assert_eq!(
///     turn.to_string(),
///     "change probability by +3 for the rule at row 1, column 4 on layer order-ingest of layout order-desk",
/// );
/// # Ok::<(), LayerError>(())
/// ```
///
/// With the `serde` feature, an intent serializes as a map, and its `intent` key says which one it is:
/// `{ intent: arm, pad: { … } }`, `{ intent: disarm, pad: { … } }`, `{ intent: hold, pad: { … } }`,
/// `{ intent: release, pad: { … }, hold: 7 }` or `{ intent: turn, pad: { … }, turn: { encoder: probability, steps: 3 } }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "intent", rename_all = "kebab-case", deny_unknown_fields)
)]
pub enum Intent {
    /// The rule at the pad is to be armed, and made first when the pad has none.
    Arm { pad: Pad },
    /// The rule at the pad is to be disarmed.
    Disarm { pad: Pad },
    /// Someone holds the pad: the rule at it is to be armed for as long as anyone holds it, and made first when the pad
    /// has none.
    Hold { pad: Pad },
    /// Someone let go of the pad: the hold that their press added ends.
    Release { pad: Pad, hold: Hold },
    /// Change one parameter of the rule at the pad, by as far as an encoder turned.
    Turn { pad: Pad, turn: EncoderTurn },
}

impl Intent {
    /// The pad the intent is about.
    pub const fn pad(&self) -> &Pad {
        match self {
            Intent::Arm { pad }
            | Intent::Disarm { pad }
            | Intent::Hold { pad }
            | Intent::Release { pad, .. }
            | Intent::Turn { pad, .. } => pad,
        }
    }
}

/// Writes the intent as a sentence: `arm the rule at row 1, column 4 on layer order-ingest of layout order-desk`.
impl fmt::Display for Intent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Intent::Arm { pad } => write!(f, "arm the rule at {pad}"),
            Intent::Disarm { pad } => write!(f, "disarm the rule at {pad}"),
            Intent::Hold { pad } => write!(f, "hold the rule at {pad}"),
            Intent::Release { pad, hold } => write!(f, "release {hold} of the rule at {pad}"),
            Intent::Turn { pad, turn } => write!(f, "change {turn} for the rule at {pad}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, Encoder, Row, Steps};

    fn pad() -> Pad {
        Pad::new(
            "order-desk".parse().unwrap(),
            "All clients".parse().unwrap(),
            Row::from_index(1),
            Column::from_index(0),
        )
    }

    #[test]
    fn every_intent_is_about_a_pad_and_reads_as_a_sentence() {
        let turn = EncoderTurn::new(Encoder::Delay, Steps::new(-2));
        let intents = [
            (
                Intent::Arm { pad: pad() },
                "arm the rule at row 2, column 1 on layer All clients of layout order-desk",
            ),
            (
                Intent::Disarm { pad: pad() },
                "disarm the rule at row 2, column 1 on layer All clients of layout order-desk",
            ),
            (
                Intent::Hold { pad: pad() },
                "hold the rule at row 2, column 1 on layer All clients of layout order-desk",
            ),
            (
                Intent::Release {
                    pad: pad(),
                    hold: Hold::new(3),
                },
                "release hold 3 of the rule at row 2, column 1 on layer All clients of layout order-desk",
            ),
            (
                Intent::Turn { pad: pad(), turn },
                "change delay by -2 for the rule at row 2, column 1 on layer All clients of layout order-desk",
            ),
        ];

        for (intent, sentence) in intents {
            assert_eq!(intent.pad(), &pad());
            assert_eq!(intent.to_string(), sentence);
        }
    }
}
