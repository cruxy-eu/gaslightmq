use gaslightmq_clock::TimeSpan;
use gaslightmq_kernel::ErrorCode;
use gaslightmq_lie::{LieDefinition, RefusalLie, TimingLie};
use gaslightmq_rule::{Action, ActionRef, Preset, PresetName, PresetSet, Rule, RuleName};
use gaslightmq_trigger::{Chance, Count, Lifetime, TriggerParts};

use crate::{Encoder, EncoderTurn, Steps};

/// The encoder mapping: what one step of each encoder is worth, and what a turn does to a rule.
///
/// A surface sends how far an encoder turned, and whoever holds the rule [applies](Self::apply) the turn.
/// Two surfaces that turn the same encoder therefore add up, and neither overwrites the other.
///
/// Every step is a property with a default.
///
/// | Property | One step of | Default |
/// |---|---|---|
/// | [`probability_step`](Self::probability_step) | [`Encoder::Probability`], on the trigger's chance. | `1%` |
/// | [`delay_step`](Self::delay_step) | [`Encoder::Delay`], on the delay of a timing lie. | `50ms` |
/// | [`jitter_step`](Self::jitter_step) | [`Encoder::Jitter`], on the jitter of a timing lie. | `25ms` |
/// | [`throttle_step`](Self::throttle_step) | [`Encoder::Throttle`], on the throttle of a timing lie. | `100ms` |
/// | [`every_step`](Self::every_step) | [`Encoder::Every`], on the trigger's `every` count. | `1` |
/// | [`duration_step`](Self::duration_step) | [`Encoder::Duration`], on a duration lifetime. | `5s` |
/// | [`firings_step`](Self::firings_step) | [`Encoder::Firings`], on a firings lifetime. | `1` |
/// | [`errors`](Self::errors) | [`Encoder::ErrorCode`]: the errors it steps through, in order. | Every error Kafka names. |
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_surface::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let presets = PresetSet::new([Preset::new(
///     "delay-1s".parse().expect("a preset name"),
///     TimingLie::new().with_delay(TimeSpan::from_secs(1)),
/// )]).expect("a preset set");
/// let rule = Rule::new(
///     "slow-order-producers".parse().expect("a rule name"),
///     "order-producers".parse::<SelectorName>().expect("a selector name"),
///     "delay-1s".parse::<PresetName>().expect("a preset name"),
/// );
/// let mapping = EncoderMapping::new();
///
/// // Three steps down on the probability encoder: from every matching call to 97% of them.
/// let turn = EncoderTurn::new(Encoder::Probability, Steps::new(-3));
/// let rarer = mapping.apply(turn, &rule, &presets)?;
/// assert_eq!(rarer.trigger().parts().and_then(TriggerParts::chance), Some("97%".parse().expect("a chance")));
///
/// // Two steps up on the delay encoder. The rule gets the changed lie, and the preset stays as it is.
/// let turn = EncoderTurn::new(Encoder::Delay, Steps::new(2));
/// let slower = mapping.apply(turn, &rule, &presets)?;
/// let lie = TimingLie::new().with_delay(TimeSpan::from_millis(1_100));
/// assert_eq!(slower.action(), &ActionRef::from(lie));
/// # Ok::<(), TurnError>(())
/// ```
///
/// With the `serde` feature, a mapping serializes as a map of its properties.
/// Reading takes the default of a property that the map leaves out, and rejects a key it does not know.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(default, deny_unknown_fields)
)]
pub struct EncoderMapping {
    probability_step: Chance,
    delay_step: TimeSpan,
    jitter_step: TimeSpan,
    throttle_step: TimeSpan,
    every_step: Count,
    duration_step: TimeSpan,
    firings_step: Count,
    #[cfg_attr(feature = "serde", serde(deserialize_with = "crate::serde::errors_once_each"))]
    errors: Vec<ErrorCode>,
}

impl EncoderMapping {
    /// The mapping with the default of every property.
    pub fn new() -> Self {
        Self {
            probability_step: match Chance::from_percent(1) {
                Ok(chance) => chance,
                Err(error) => unreachable!("one percent is a chance: {error}"),
            },
            delay_step: TimeSpan::from_millis(50),
            jitter_step: TimeSpan::from_millis(25),
            throttle_step: TimeSpan::from_millis(100),
            every_step: Count::ONE,
            duration_step: TimeSpan::from_secs(5),
            firings_step: Count::ONE,
            errors: ErrorCode::known().collect(),
        }
    }

    /// The same mapping with that step of the probability encoder.
    #[must_use]
    pub fn with_probability_step(self, step: Chance) -> Self {
        Self {
            probability_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the delay encoder.
    #[must_use]
    pub fn with_delay_step(self, step: TimeSpan) -> Self {
        Self {
            delay_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the jitter encoder.
    #[must_use]
    pub fn with_jitter_step(self, step: TimeSpan) -> Self {
        Self {
            jitter_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the throttle encoder.
    #[must_use]
    pub fn with_throttle_step(self, step: TimeSpan) -> Self {
        Self {
            throttle_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the `every` encoder.
    #[must_use]
    pub fn with_every_step(self, step: Count) -> Self {
        Self {
            every_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the duration encoder.
    #[must_use]
    pub fn with_duration_step(self, step: TimeSpan) -> Self {
        Self {
            duration_step: step,
            ..self
        }
    }

    /// The same mapping with that step of the firings encoder.
    #[must_use]
    pub fn with_firings_step(self, step: Count) -> Self {
        Self {
            firings_step: step,
            ..self
        }
    }

    /// The same mapping with those errors for the error encoder to step through, in that order.
    ///
    /// An error that the list names twice counts once, where it stands first.
    #[must_use]
    pub fn with_errors(self, errors: impl IntoIterator<Item = ErrorCode>) -> Self {
        Self {
            errors: once_each(errors),
            ..self
        }
    }

    /// How much chance one step of the probability encoder adds or takes.
    pub const fn probability_step(&self) -> Chance {
        self.probability_step
    }

    /// How much delay one step of the delay encoder adds or takes.
    pub const fn delay_step(&self) -> TimeSpan {
        self.delay_step
    }

    /// How much jitter one step of the jitter encoder adds or takes.
    pub const fn jitter_step(&self) -> TimeSpan {
        self.jitter_step
    }

    /// How much throttle one step of the throttle encoder adds or takes.
    pub const fn throttle_step(&self) -> TimeSpan {
        self.throttle_step
    }

    /// How many matches one step of the `every` encoder adds to the step between firings, or takes from it.
    pub const fn every_step(&self) -> Count {
        self.every_step
    }

    /// How much time one step of the duration encoder adds to the lifetime, or takes from it.
    pub const fn duration_step(&self) -> TimeSpan {
        self.duration_step
    }

    /// How many firings one step of the firings encoder adds to the lifetime, or takes from it.
    pub const fn firings_step(&self) -> Count {
        self.firings_step
    }

    /// The errors that the error encoder steps through, in order. Each stands there once.
    pub fn errors(&self) -> &[ErrorCode] {
        &self.errors
    }

    /// The rule after that turn of an encoder.
    ///
    /// A value stops at its ends, and a turn past an end changes nothing more.
    /// A turn of zero steps changes nothing.
    ///
    /// | Encoder | What the turn does |
    /// |---|---|
    /// | Probability | Moves the trigger's chance between `0%` and `100%`. A rule without a chance fires at `100%`, and at `100%` the rule has no chance part. |
    /// | Every | Moves the trigger's `every` count. A rule without one fires on each match, and at one the rule has no `every` part. |
    /// | Delay, jitter, throttle | Moves that parameter of the timing lie. The jitter stops at the delay, and the throttle at the longest one Kafka can tell a client. |
    /// | Error code | Steps through [`errors`](Self::errors), and stops at the first and at the last. An error that the list does not hold moves to the first. |
    /// | Duration | Moves the duration of the lifetime. Any other lifetime counts as no time, and at no time the rule stays armed until disarmed. |
    /// | Firings | Moves the firings of the lifetime. Any other lifetime counts as no firings, and at none the rule stays armed until disarmed. |
    ///
    /// A preset stays as it is.
    /// A turn that changes the action of a rule that names a preset gives the rule the changed action inline,
    /// and `presets` says what to start from. A turn that changes nothing leaves the name of the preset in the rule.
    ///
    /// The result is a definition.
    /// Whoever holds the armed rule keeps its arming, so a turn does not reset the counters of an armed rule.
    ///
    /// # Errors
    ///
    /// - [`TurnError::Disabled`] when the rule has nothing for the encoder to change:
    ///   its action is not the lie the encoder tunes, or its trigger is manual and has no chance and no count.
    /// - [`TurnError::UnknownPreset`] when the encoder tunes the action, and the rule names a preset that `presets` does not hold.
    pub fn apply(&self, turn: EncoderTurn, rule: &Rule, presets: &PresetSet) -> Result<Rule, TurnError> {
        let steps = turn.steps();
        let turned = match turn.encoder() {
            Encoder::Probability => {
                let parts = parts_of(turn, rule)?;
                let chance = self.chance_after(parts.chance().unwrap_or(Chance::ALWAYS), steps);
                rule.clone().with_trigger(if chance == Chance::ALWAYS {
                    parts.without_chance()
                } else {
                    parts.with_chance(chance)
                })
            }
            Encoder::Every => {
                let parts = parts_of(turn, rule)?;
                let every = parts.every().unwrap_or(Count::ONE).value();
                let every = Count::new(steps.moved(every, self.every_step.value())).unwrap_or(Count::ONE);
                rule.clone().with_trigger(if every == Count::ONE {
                    parts.without_every()
                } else {
                    parts.with_every(every)
                })
            }
            Encoder::Delay => {
                let lie = timing_of(turn, rule, presets)?;
                let delay = span_after(lie.delay(), self.delay_step, steps);
                with_lie(rule, &lie, lie.with_delay(delay))
            }
            Encoder::Jitter => {
                let lie = timing_of(turn, rule, presets)?;
                let jitter = span_after(lie.jitter(), self.jitter_step, steps);
                with_lie(rule, &lie, lie.with_jitter(jitter))
            }
            Encoder::Throttle => {
                let lie = timing_of(turn, rule, presets)?;
                let throttle = span_after(lie.throttle(), self.throttle_step, steps);
                with_lie(rule, &lie, lie.with_throttle(throttle))
            }
            Encoder::ErrorCode => {
                let lie = refusal_of(turn, rule, presets)?;
                let error = self.error_after(lie.error, steps);
                with_lie(rule, &lie, RefusalLie { error, ..lie })
            }
            Encoder::Duration => {
                let duration = match rule.lifetime() {
                    Lifetime::Duration(duration) => duration,
                    _ => TimeSpan::ZERO,
                };
                let duration = span_after(duration, self.duration_step, steps);
                rule.clone().with_lifetime(if duration == TimeSpan::ZERO {
                    Lifetime::UntilDisarmed
                } else {
                    Lifetime::Duration(duration)
                })
            }
            Encoder::Firings => {
                let firings = match rule.lifetime() {
                    Lifetime::Firings(firings) => firings.value(),
                    _ => 0,
                };
                let firings = steps.moved(firings, self.firings_step.value());
                rule.clone()
                    .with_lifetime(Count::new(firings).map_or(Lifetime::UntilDisarmed, Lifetime::Firings))
            }
        };
        Ok(if steps == Steps::new(0) { rule.clone() } else { turned })
    }

    /// The chance after those steps, between `0%` and `100%`.
    fn chance_after(&self, chance: Chance, steps: Steps) -> Chance {
        let ppm = steps.moved(u64::from(chance.ppm()), u64::from(self.probability_step.ppm()));
        u32::try_from(ppm)
            .ok()
            .and_then(|ppm| Chance::from_ppm(ppm).ok())
            .unwrap_or(Chance::ALWAYS)
    }

    /// The error after those steps through the list, or `error` itself when the list is empty.
    fn error_after(&self, error: ErrorCode, steps: Steps) -> ErrorCode {
        let Some(last) = self.errors.len().checked_sub(1) else {
            return error;
        };
        let index = self
            .errors
            .iter()
            .position(|listed| *listed == error)
            .map_or(0, |index| {
                let moved = steps.moved(u64::try_from(index).unwrap_or(u64::MAX), 1);
                usize::try_from(moved).unwrap_or(usize::MAX).min(last)
            });
        self.errors.get(index).copied().unwrap_or(error)
    }
}

/// The mapping of [`EncoderMapping::new`].
impl Default for EncoderMapping {
    fn default() -> Self {
        Self::new()
    }
}

/// The items in their order, each one where it stands first.
pub(crate) fn once_each(errors: impl IntoIterator<Item = ErrorCode>) -> Vec<ErrorCode> {
    let mut once = Vec::new();
    for error in errors {
        if !once.contains(&error) {
            once.push(error);
        }
    }
    once
}

/// The rule with the lie that a turn made of the lie it had.
///
/// A turn that leaves the lie as it was leaves the rule as it was, so a rule that names a preset still names it.
fn with_lie<L: PartialEq + Into<ActionRef>>(rule: &Rule, was: &L, turned: L) -> Rule {
    if turned == *was {
        rule.clone()
    } else {
        rule.clone().with_action(turned)
    }
}

/// The time span after those steps of `step` each.
fn span_after(span: TimeSpan, step: TimeSpan, steps: Steps) -> TimeSpan {
    TimeSpan::from_millis(steps.moved(span.as_millis(), step.as_millis()))
}

/// The parts of the rule's trigger, which a manual trigger does not have.
fn parts_of(turn: EncoderTurn, rule: &Rule) -> Result<TriggerParts, TurnError> {
    rule.trigger().parts().copied().ok_or_else(|| disabled(turn, rule))
}

/// The timing lie of the rule, when its action is one.
fn timing_of(turn: EncoderTurn, rule: &Rule, presets: &PresetSet) -> Result<TimingLie, TurnError> {
    match action_of(rule, presets)? {
        Action::Lie(LieDefinition::Timing(lie)) => Ok(*lie),
        _ => Err(disabled(turn, rule)),
    }
}

/// The refusal of the rule, when its action is one.
fn refusal_of(turn: EncoderTurn, rule: &Rule, presets: &PresetSet) -> Result<RefusalLie, TurnError> {
    match action_of(rule, presets)? {
        Action::Lie(LieDefinition::Refusal(lie)) => Ok(*lie),
        _ => Err(disabled(turn, rule)),
    }
}

/// The action of the rule: its own, or the one of the preset it names.
fn action_of<'a>(rule: &'a Rule, presets: &'a PresetSet) -> Result<&'a Action, TurnError> {
    match rule.action() {
        ActionRef::Inline(action) => Ok(action),
        ActionRef::Preset(name) => presets
            .get(name)
            .map(Preset::action)
            .ok_or_else(|| TurnError::UnknownPreset {
                rule: rule.name().clone(),
                preset: name.clone(),
            }),
    }
}

fn disabled(turn: EncoderTurn, rule: &Rule) -> TurnError {
    TurnError::Disabled {
        encoder: turn.encoder(),
        rule: rule.name().clone(),
    }
}

/// Why a turn of an encoder does not change a rule.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_surface::prelude::*;
///
/// let flap = Rule::new(
///     "leader-flap".parse().expect("a rule name"),
///     Conditions::new(),
///     RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER),
/// );
/// let turn = EncoderTurn::new(Encoder::Delay, Steps::new(1));
///
/// let error = EncoderMapping::new().apply(turn, &flap, &PresetSet::default()).unwrap_err();
/// assert_eq!(error, TurnError::Disabled { encoder: Encoder::Delay, rule: flap.name().clone() });
/// assert_eq!(
///     error.to_string(),
///     "the delay encoder changes the delay of a timing lie, and rule \"leader-flap\" has none",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TurnError {
    /// The rule has nothing for the encoder to change, so a surface shows the encoder disabled.
    #[error("the {encoder} encoder changes {}, and rule {:?} has none", .encoder.parameter(), .rule.as_str())]
    Disabled { encoder: Encoder, rule: RuleName },
    /// The encoder tunes the action, and the rule names a preset that does not exist.
    #[error("rule {:?}: no preset is named {:?}", .rule.as_str(), .preset.as_str())]
    UnknownPreset { rule: RuleName, preset: PresetName },
}
