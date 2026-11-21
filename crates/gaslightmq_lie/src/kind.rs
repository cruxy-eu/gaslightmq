use std::fmt;
use std::str::FromStr;

use crate::{Aspect, CombinationClass, LieError, Stage};

/// A lie without its parameters: which of the eight kinds it is.
///
/// | Kind | The client is told |
/// |---|---|
/// | Refusal | The request failed. |
/// | Timing | The answer took longer, or the client must back off. |
/// | Silence | Nothing at all. |
/// | Hang-up | The connection is gone. |
/// | Topology | The cluster looks different. |
/// | Data | The records are different. |
/// | Progress | Offsets and positions are different. |
/// | Configuration | Settings are different. |
///
/// The kind decides everything about a lie that its parameters do not:
/// the [stages](Self::stages) it can act at, how it [combines](Self::class) with other lies,
/// whether it [targets the whole call](Self::targets_whole_call), and which [aspects](Self::aspects) it rewrites.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// let kind: LieKind = "hang-up".parse()?;
/// assert_eq!(kind, LieKind::HangUp);
/// assert_eq!(kind.title(), "Hang-up");
/// assert_eq!(kind.class(), CombinationClass::Terminal);
/// assert!(kind.targets_whole_call());
///
/// // A hang-up names its stage, and a silence has only one.
/// assert_eq!(kind.stage_for(None)?, Stage::BeforeForwarding);
/// assert_eq!(kind.stage_for(Some(Stage::AfterResponse))?, Stage::AfterResponse);
/// assert!(LieKind::Silence.stage_for(Some(Stage::AfterResponse)).is_err());
/// # Ok::<(), LieError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LieKind {
    /// The request failed: an error code on the parts a rule targets, or on the whole response.
    Refusal,
    /// The answer took longer, or the client must back off.
    Timing,
    /// Nothing at all: GaslightMQ reads the request and never answers it.
    Silence,
    /// The connection is gone.
    HangUp,
    /// The cluster looks different: another leader, a missing broker, another controller.
    Topology,
    /// The records are different: altered, dropped, duplicated, reordered or corrupted.
    Data,
    /// Offsets and positions are different: a stale high watermark, another committed offset.
    Progress,
    /// Settings are different: rewritten topic or broker configs.
    Configuration,
}

impl LieKind {
    /// Every kind, in the order of the specification.
    pub const ALL: [Self; 8] = [
        Self::Refusal,
        Self::Timing,
        Self::Silence,
        Self::HangUp,
        Self::Topology,
        Self::Data,
        Self::Progress,
        Self::Configuration,
    ];

    /// The name a file uses, such as `hang-up`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Refusal => "refusal",
            Self::Timing => "timing",
            Self::Silence => "silence",
            Self::HangUp => "hang-up",
            Self::Topology => "topology",
            Self::Data => "data",
            Self::Progress => "progress",
            Self::Configuration => "configuration",
        }
    }

    /// The name a person reads, such as `Hang-up`.
    pub const fn title(self) -> &'static str {
        match self {
            Self::Refusal => "Refusal",
            Self::Timing => "Timing",
            Self::Silence => "Silence",
            Self::HangUp => "Hang-up",
            Self::Topology => "Topology",
            Self::Data => "Data",
            Self::Progress => "Progress",
            Self::Configuration => "Configuration",
        }
    }

    /// How a lie of this kind combines with the other lies on a call.
    pub const fn class(self) -> CombinationClass {
        match self {
            Self::Refusal => CombinationClass::Refusal,
            Self::Timing => CombinationClass::Modifier,
            Self::Silence | Self::HangUp => CombinationClass::Terminal,
            Self::Topology | Self::Data | Self::Progress | Self::Configuration => CombinationClass::Rewrite,
        }
    }

    /// The stage a lie of this kind acts at when its definition names none.
    ///
    /// A refusal and a hang-up act before forwarding, so nothing happens in the cluster.
    /// A delay acts after the response, so the backend answers on time and only the client waits.
    pub const fn default_stage(self) -> Stage {
        match self {
            Self::Refusal | Self::Silence | Self::HangUp => Stage::BeforeForwarding,
            Self::Timing | Self::Topology | Self::Data | Self::Progress | Self::Configuration => Stage::AfterResponse,
        }
    }

    /// The stages a lie of this kind can act at, in the order a call passes them.
    ///
    /// Silence always acts before forwarding, because nothing is ever answered.
    /// A rewrite always acts after the response, because it rewrites the truth.
    pub const fn stages(self) -> &'static [Stage] {
        match self {
            Self::Refusal | Self::Timing | Self::HangUp => &Stage::ALL,
            Self::Silence => &[Stage::BeforeForwarding],
            Self::Topology | Self::Data | Self::Progress | Self::Configuration => &[Stage::AfterResponse],
        }
    }

    /// Whether a lie of this kind can act at `stage`.
    pub fn allows(self, stage: Stage) -> bool {
        self.stages().contains(&stage)
    }

    /// Whether a definition of this kind names its stage.
    ///
    /// Refusal, timing and hang-up do, because the stage changes what really happens in the cluster.
    /// Every other kind has one stage only.
    pub const fn names_stage(self) -> bool {
        self.stages().len() > 1
    }

    /// The stage of a definition of this kind that names `stage`, or names none.
    ///
    /// # Errors
    ///
    /// [`LieError::StageNotAllowed`] when a lie of this kind never acts at `stage`.
    pub fn stage_for(self, stage: Option<Stage>) -> Result<Stage, LieError> {
        match stage {
            None => Ok(self.default_stage()),
            Some(stage) if self.allows(stage) => Ok(stage),
            Some(stage) => Err(LieError::StageNotAllowed { lie: self, stage }),
        }
    }

    /// Whether a lie of this kind always targets the whole call.
    ///
    /// Timing, silence and hang-up do: a call has one answer, which arrives late, never, or on a closed connection.
    /// Every other kind targets what the rule's selector matched.
    pub const fn targets_whole_call(self) -> bool {
        matches!(self, Self::Timing | Self::Silence | Self::HangUp)
    }

    /// The aspects a lie of this kind can rewrite.
    /// Only the kinds that rewrite have any.
    pub const fn aspects(self) -> &'static [Aspect] {
        match self {
            Self::Refusal | Self::Timing | Self::Silence | Self::HangUp => &[],
            Self::Topology => &[Aspect::Leader, Aspect::Brokers, Aspect::Controller],
            Self::Data => &[Aspect::Records],
            Self::Progress => &[Aspect::HighWatermark, Aspect::LogStartOffset, Aspect::CommittedOffset],
            Self::Configuration => &[Aspect::Configs],
        }
    }
}

/// Writes the name a file uses, such as `hang-up`.
impl fmt::Display for LieKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name a file uses, as [`Display`](fmt::Display) writes it.
impl FromStr for LieKind {
    type Err = LieError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.name() == name)
            .ok_or_else(|| LieError::UnknownName {
                what: "lie",
                name: name.to_owned(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REWRITES: [LieKind; 4] = [
        LieKind::Topology,
        LieKind::Data,
        LieKind::Progress,
        LieKind::Configuration,
    ];

    #[test]
    fn there_are_eight_kinds_in_the_order_of_the_specification() {
        let titles: Vec<&str> = LieKind::ALL.into_iter().map(LieKind::title).collect();
        assert_eq!(
            titles,
            [
                "Refusal",
                "Timing",
                "Silence",
                "Hang-up",
                "Topology",
                "Data",
                "Progress",
                "Configuration",
            ]
        );

        let mut sorted = LieKind::ALL;
        sorted.sort();
        assert_eq!(sorted, LieKind::ALL);
    }

    #[test]
    fn the_name_in_a_file_is_the_title_in_lower_case() {
        for kind in LieKind::ALL {
            assert_eq!(kind.name(), kind.title().to_lowercase());
        }
        assert_eq!(LieKind::HangUp.name(), "hang-up");
    }

    #[test]
    fn a_kind_reads_back_from_the_name_it_writes() {
        for kind in LieKind::ALL {
            assert_eq!(kind.to_string().parse(), Ok(kind));
        }
    }

    #[test]
    fn a_name_that_no_kind_has_is_an_error() {
        for name in ["hangup", "Hang-up", "REFUSAL", "delay", ""] {
            assert_eq!(
                name.parse::<LieKind>(),
                Err(LieError::UnknownName {
                    what: "lie",
                    name: name.to_owned()
                }),
            );
        }
    }

    #[test]
    fn every_kind_has_the_class_of_the_specification() {
        use CombinationClass::{Modifier, Refusal, Rewrite, Terminal};

        let classes: Vec<CombinationClass> = LieKind::ALL.into_iter().map(LieKind::class).collect();
        assert_eq!(
            classes,
            [
                Refusal, Modifier, Terminal, Terminal, Rewrite, Rewrite, Rewrite, Rewrite
            ]
        );
    }

    #[test]
    fn refusal_timing_and_hang_up_name_their_stage() {
        let naming: Vec<LieKind> = LieKind::ALL.into_iter().filter(|kind| kind.names_stage()).collect();
        assert_eq!(naming, [LieKind::Refusal, LieKind::Timing, LieKind::HangUp]);

        for kind in naming {
            assert_eq!(kind.stages(), Stage::ALL);
            for stage in Stage::ALL {
                assert!(kind.allows(stage));
                assert_eq!(kind.stage_for(Some(stage)), Ok(stage));
            }
        }
    }

    #[test]
    fn a_refusal_and_a_hang_up_act_before_forwarding_unless_they_say_otherwise() {
        assert_eq!(LieKind::Refusal.default_stage(), Stage::BeforeForwarding);
        assert_eq!(LieKind::HangUp.default_stage(), Stage::BeforeForwarding);
        assert_eq!(LieKind::Refusal.stage_for(None), Ok(Stage::BeforeForwarding));
    }

    #[test]
    fn a_delay_acts_after_the_response_unless_it_says_otherwise() {
        assert_eq!(LieKind::Timing.default_stage(), Stage::AfterResponse);
        assert_eq!(LieKind::Timing.stage_for(None), Ok(Stage::AfterResponse));
    }

    #[test]
    fn silence_always_acts_before_forwarding() {
        let silence = LieKind::Silence;

        assert_eq!(silence.stages(), [Stage::BeforeForwarding]);
        assert!(!silence.names_stage());
        assert_eq!(silence.stage_for(None), Ok(Stage::BeforeForwarding));
        assert_eq!(
            silence.stage_for(Some(Stage::BeforeForwarding)),
            Ok(Stage::BeforeForwarding)
        );
        assert_eq!(
            silence.stage_for(Some(Stage::AfterResponse)),
            Err(LieError::StageNotAllowed {
                lie: silence,
                stage: Stage::AfterResponse
            })
        );
    }

    #[test]
    fn a_rewrite_always_acts_after_the_response() {
        for kind in REWRITES {
            assert_eq!(kind.stages(), [Stage::AfterResponse], "{kind}");
            assert!(!kind.names_stage(), "{kind}");
            assert_eq!(kind.stage_for(None), Ok(Stage::AfterResponse), "{kind}");
            assert_eq!(
                kind.stage_for(Some(Stage::BeforeForwarding)),
                Err(LieError::StageNotAllowed {
                    lie: kind,
                    stage: Stage::BeforeForwarding
                })
            );
        }
    }

    #[test]
    fn every_kind_can_act_at_its_default_stage() {
        for kind in LieKind::ALL {
            assert!(kind.allows(kind.default_stage()), "{kind}");
            assert!(!kind.stages().is_empty(), "{kind}");
        }
    }

    #[test]
    fn timing_silence_and_hang_up_target_the_whole_call() {
        let whole: Vec<LieKind> = LieKind::ALL
            .into_iter()
            .filter(|kind| kind.targets_whole_call())
            .collect();
        assert_eq!(whole, [LieKind::Timing, LieKind::Silence, LieKind::HangUp]);

        // A modifier and a terminal lie act on the one answer of a call, and the other classes on its parts.
        for kind in LieKind::ALL {
            let on_the_answer = matches!(kind.class(), CombinationClass::Modifier | CombinationClass::Terminal);
            assert_eq!(kind.targets_whole_call(), on_the_answer, "{kind}");
        }
    }

    #[test]
    fn only_the_kinds_that_rewrite_have_aspects() {
        for kind in LieKind::ALL {
            let rewrites = kind.class() == CombinationClass::Rewrite;
            assert_eq!(!kind.aspects().is_empty(), rewrites, "{kind}");
            assert_eq!(REWRITES.contains(&kind), rewrites, "{kind}");
        }
        assert_eq!(
            LieKind::Progress.aspects(),
            [Aspect::HighWatermark, Aspect::LogStartOffset, Aspect::CommittedOffset]
        );
    }
}
