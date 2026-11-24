use std::fmt;
use std::str::FromStr;

use gaslightmq_lie::LieDefinition;
use gaslightmq_rule::Action;

/// One of the eight encoders that tune the rule behind the focused pad.
///
/// Each encoder changes one parameter of the rule's action, of its trigger or of its lifetime.
///
/// | Encoder | Name | Parameter |
/// |---|---|---|
/// | [`Probability`](Self::Probability) | `probability` | The trigger's `chance`. |
/// | [`Delay`](Self::Delay) | `delay` | The delay of a timing lie. |
/// | [`Jitter`](Self::Jitter) | `jitter` | The jitter of a timing lie. |
/// | [`ErrorCode`](Self::ErrorCode) | `error-code` | The error of a refusal. |
/// | [`Throttle`](Self::Throttle) | `throttle` | The `throttle_time_ms` of a timing lie. |
/// | [`Every`](Self::Every) | `every` | The trigger's `every` count. |
/// | [`Duration`](Self::Duration) | `duration` | A duration lifetime. |
/// | [`Firings`](Self::Firings) | `firings` | A firings lifetime. |
///
/// An encoder that does not apply to the preset of the focused pad is disabled: [`applies_to`](Self::applies_to).
/// An [`EncoderMapping`](crate::EncoderMapping) says what one step of each encoder is worth.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// let delay = Action::from(TimingLie::new().with_delay(TimeSpan::from_secs(1)));
/// let refusal = Action::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
///
/// assert!(Encoder::Delay.applies_to(&delay));
/// assert!(!Encoder::Delay.applies_to(&refusal));
/// assert!(Encoder::ErrorCode.applies_to(&refusal));
/// // Every action has a trigger and a lifetime next to it.
/// assert!(Encoder::Probability.applies_to(&refusal));
///
/// assert_eq!("error-code".parse(), Ok(Encoder::ErrorCode));
/// ```
///
/// With the `serde` feature, an encoder serializes as its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Encoder {
    /// How often the rule fires among its matching calls.
    Probability,
    /// How long a timing lie holds the call.
    Delay,
    /// How far the wait of a timing lie strays from its delay.
    Jitter,
    /// Which error a refusal tells the client.
    ErrorCode,
    /// How long a timing lie tells the client to back off.
    Throttle,
    /// The step between the matching calls the rule fires on.
    Every,
    /// How long the rule stays armed.
    Duration,
    /// How many firings the rule stays armed for.
    Firings,
}

impl Encoder {
    /// Every encoder, from the first to the eighth, in the order a surface shows them.
    pub const ALL: [Encoder; 8] = [
        Encoder::Probability,
        Encoder::Delay,
        Encoder::Jitter,
        Encoder::ErrorCode,
        Encoder::Throttle,
        Encoder::Every,
        Encoder::Duration,
        Encoder::Firings,
    ];

    /// The name of the encoder in lower case, with a hyphen between two words.
    pub const fn name(self) -> &'static str {
        match self {
            Encoder::Probability => "probability",
            Encoder::Delay => "delay",
            Encoder::Jitter => "jitter",
            Encoder::ErrorCode => "error-code",
            Encoder::Throttle => "throttle",
            Encoder::Every => "every",
            Encoder::Duration => "duration",
            Encoder::Firings => "firings",
        }
    }

    /// What the encoder changes, as a sentence names it.
    pub const fn parameter(self) -> &'static str {
        match self {
            Encoder::Probability => "the chance of a trigger",
            Encoder::Delay => "the delay of a timing lie",
            Encoder::Jitter => "the jitter of a timing lie",
            Encoder::ErrorCode => "the error of a refusal",
            Encoder::Throttle => "the throttle of a timing lie",
            Encoder::Every => "the `every` count of a trigger",
            Encoder::Duration => "a duration lifetime",
            Encoder::Firings => "a firings lifetime",
        }
    }

    /// Whether the encoder applies to that action, which is the action of a preset or of a rule.
    ///
    /// The encoders of a timing lie apply to a timing lie, and the encoder of the error to a refusal.
    /// The encoders of the trigger and of the lifetime apply to every action, because a preset says nothing about either.
    pub const fn applies_to(self, action: &Action) -> bool {
        match self {
            Encoder::Delay | Encoder::Jitter | Encoder::Throttle => {
                matches!(action, Action::Lie(LieDefinition::Timing(_)))
            }
            Encoder::ErrorCode => matches!(action, Action::Lie(LieDefinition::Refusal(_))),
            Encoder::Probability | Encoder::Every | Encoder::Duration | Encoder::Firings => true,
        }
    }
}

/// Writes the name of the encoder.
impl fmt::Display for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name of an encoder.
impl FromStr for Encoder {
    type Err = EncoderError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Encoder::ALL
            .into_iter()
            .find(|encoder| encoder.name() == name)
            .ok_or_else(|| EncoderError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not an [`Encoder`].
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let error = "volume".parse::<Encoder>().unwrap_err();
/// assert_eq!(error, EncoderError::Unknown { name: "volume".to_owned() });
/// assert_eq!(error.to_string(), "no encoder is called \"volume\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncoderError {
    /// No encoder has this name.
    #[error("no encoder is called {name:?}")]
    Unknown { name: String },
}

/// How far someone turned an encoder: a number of steps, up or down.
///
/// A surface sends how far an encoder turned, and never the value it reached,
/// so two surfaces that turn the same encoder do not fight.
/// Any number is a value: turning down is negative, and zero steps change nothing.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// assert_eq!(Steps::new(3).to_string(), "+3");
/// assert_eq!(Steps::new(-2).count(), -2);
/// ```
///
/// With the `serde` feature, it serializes as its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(transparent))]
pub struct Steps(i32);

impl Steps {
    /// That many steps: up when positive, down when negative.
    pub const fn new(count: i32) -> Self {
        Self(count)
    }

    /// The number of steps: positive for up, negative for down.
    pub const fn count(self) -> i32 {
        self.0
    }

    /// `value` after these steps of `step` each. It stops at zero and at the highest number.
    pub(crate) fn moved(self, value: u64, step: u64) -> u64 {
        let moved = i128::from(value) + i128::from(self.0) * i128::from(step);
        u64::try_from(moved.max(0)).unwrap_or(u64::MAX)
    }
}

/// Writes the number with its sign: `+3` or `-2`.
impl fmt::Display for Steps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:+}", self.0)
    }
}

/// One turn of one encoder: which one, and how far.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let turn = EncoderTurn::new(Encoder::Probability, Steps::new(3));
/// assert_eq!(turn.to_string(), "probability by +3");
/// ```
///
/// With the `serde` feature, a turn serializes as a map: `{ encoder: probability, steps: 3 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct EncoderTurn {
    encoder: Encoder,
    steps: Steps,
}

impl EncoderTurn {
    /// A turn of `encoder` by `steps`.
    pub const fn new(encoder: Encoder, steps: Steps) -> Self {
        Self { encoder, steps }
    }

    /// The encoder someone turned.
    pub const fn encoder(self) -> Encoder {
        self.encoder
    }

    /// How far it turned.
    pub const fn steps(self) -> Steps {
        self.steps
    }
}

/// Writes the encoder and the steps: `probability by +3`.
impl fmt::Display for EncoderTurn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} by {}", self.encoder, self.steps)
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_incident::IncidentDefinition;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    use super::*;

    #[test]
    fn there_are_eight_encoders_and_each_has_a_name_of_its_own() {
        let names = Encoder::ALL.map(Encoder::name);

        assert_eq!(
            names,
            [
                "probability",
                "delay",
                "jitter",
                "error-code",
                "throttle",
                "every",
                "duration",
                "firings"
            ]
        );
        for encoder in Encoder::ALL {
            assert_eq!(encoder.to_string(), encoder.name());
            assert_eq!(encoder.name().parse(), Ok(encoder));
        }
    }

    #[test]
    fn a_name_of_no_encoder_is_an_error() {
        for name in ["", "Probability", "error code", "volume"] {
            assert_eq!(
                name.parse::<Encoder>(),
                Err(EncoderError::Unknown { name: name.to_owned() })
            );
        }
    }

    #[test]
    fn an_encoder_applies_to_the_actions_that_have_its_parameter() {
        let timing = Action::from(TimingLie::new().with_delay(TimeSpan::from_secs(1)));
        let refusal = Action::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
        let silence = Action::from(LieDefinition::Silence);
        let incident = Action::from(IncidentDefinition::LeaderMoves {
            partition: None,
            to: None,
        });

        let applying = |action: &Action| -> Vec<Encoder> {
            Encoder::ALL
                .into_iter()
                .filter(|encoder| encoder.applies_to(action))
                .collect()
        };
        let always = [
            Encoder::Probability,
            Encoder::Every,
            Encoder::Duration,
            Encoder::Firings,
        ];

        assert_eq!(
            applying(&timing),
            [
                Encoder::Probability,
                Encoder::Delay,
                Encoder::Jitter,
                Encoder::Throttle,
                Encoder::Every,
                Encoder::Duration,
                Encoder::Firings
            ]
        );
        assert_eq!(
            applying(&refusal),
            [
                Encoder::Probability,
                Encoder::ErrorCode,
                Encoder::Every,
                Encoder::Duration,
                Encoder::Firings
            ]
        );
        assert_eq!(applying(&silence), always);
        assert_eq!(applying(&incident), always);
    }

    #[test]
    fn steps_move_a_value_up_and_down_and_stop_at_its_ends() {
        assert_eq!(Steps::new(3).moved(40, 1), 43);
        assert_eq!(Steps::new(-3).moved(1_000, 50), 850);
        assert_eq!(Steps::new(0).moved(40, 50), 40);
        assert_eq!(Steps::new(-3).moved(100, 50), 0);
        assert_eq!(Steps::new(i32::MIN).moved(u64::MAX, u64::MAX), 0);
        assert_eq!(Steps::new(1).moved(u64::MAX, 1), u64::MAX);
        assert_eq!(Steps::new(i32::MAX).moved(u64::MAX, u64::MAX), u64::MAX);
    }

    #[test]
    fn a_turn_says_which_encoder_and_how_far() {
        let turn = EncoderTurn::new(Encoder::Delay, Steps::new(-2));

        assert_eq!(turn.encoder(), Encoder::Delay);
        assert_eq!(turn.steps(), Steps::new(-2));
        assert_eq!(turn.to_string(), "delay by -2");
        assert_eq!(Steps::new(0).to_string(), "+0");
    }
}
