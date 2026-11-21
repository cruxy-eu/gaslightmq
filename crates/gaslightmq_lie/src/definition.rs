use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_kernel::prelude::*;

use crate::{Aspect, CombinationClass, LieKind, Stage, TargetedLie, TargetedLieError};

/// A refusal: the client is told that the request failed.
///
/// The error lands on the parts a rule targets, or on the whole response when the request has no parts.
/// A refusal acts before forwarding unless it says otherwise, so the backend never sees the refused part.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // "Leader flap": nothing is written, and the client refreshes its metadata.
/// let flap = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);
/// assert_eq!(flap.stage, Stage::BeforeForwarding);
/// assert_eq!(flap.to_string(), "NOT_LEADER_OR_FOLLOWER");
///
/// // "Lost acknowledgement": the backend writes the records, and the client is told it failed.
/// let lost = RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(Stage::AfterResponse);
/// assert_eq!(lost.to_string(), "REQUEST_TIMED_OUT after the response");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RefusalLie {
    /// The error the client is told.
    pub error: ErrorCode,
    /// Whether the backend handles the refused part.
    pub stage: Stage,
}

impl RefusalLie {
    /// A refusal with `error` before forwarding.
    pub const fn new(error: ErrorCode) -> Self {
        Self {
            error,
            stage: LieKind::Refusal.default_stage(),
        }
    }

    /// The same refusal at `stage`.
    #[must_use]
    pub const fn at(mut self, stage: Stage) -> Self {
        self.stage = stage;
        self
    }
}

/// Writes the error, and the stage unless the refusal acts before forwarding.
impl fmt::Display for RefusalLie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.error, AtStage::of(LieKind::Refusal, self.stage))
    }
}

/// A timing lie: the answer takes longer, or the client is told to back off.
///
/// Every parameter is a time span, and zero means that the lie leaves it alone.
/// A timing lie acts after the response unless it says otherwise, so the backend answers on time and the client waits.
///
/// A timing lie whose parameters are all zero changes nothing.
/// It is a valid lie: it is where the `with_` methods start from, and it harms no call.
///
/// # What always holds
///
/// - The [jitter](Self::jitter) is at most the [delay](Self::delay), so the shortest wait is never less than no time.
/// - The [throttle](Self::throttle) is at most [`MAX_THROTTLE`](Self::MAX_THROTTLE), so it fits `throttle_time_ms`.
///
/// Whoever does the telling can therefore compute `delay - jitter`, and cast the throttle to an `i32`.
/// The parameters are private for that reason, unlike those of a [`RefusalLie`], which take any value.
///
/// The `with_` methods never fail.
/// A value that is too long stops at the longest one that holds, as [`TimeSpan::from_secs`] stops at the longest time span.
/// Code that builds a lie has no user to report an error to, and a chain of `with_` calls stays a `const` expression.
/// Reading a file is different: with the `serde` feature, a value that is too long is a [`TimingLieError`],
/// because the person who wrote it meant something else.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
///
/// // "Delay 1 s": between 800 ms and 1200 ms.
/// let delay = TimingLie::new()
///     .with_delay(TimeSpan::from_secs(1))
///     .with_jitter(TimeSpan::from_millis(200));
/// assert_eq!(delay.stage(), Stage::AfterResponse);
/// assert_eq!(delay.throttle(), TimeSpan::ZERO);
/// assert_eq!(delay.to_string(), "+1 s ±200 ms");
///
/// // A late Heartbeat can expire the member, and cause a real rebalance.
/// let late = TimingLie::new().with_delay(TimeSpan::from_secs(45)).at(Stage::BeforeForwarding);
/// assert_eq!(late.to_string(), "+45 s before forwarding");
///
/// // The jitter stops at the delay.
/// let wild = TimingLie::new()
///     .with_delay(TimeSpan::from_millis(100))
///     .with_jitter(TimeSpan::from_millis(200));
/// assert_eq!(wild.jitter(), TimeSpan::from_millis(100));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimingLie {
    delay: TimeSpan,
    jitter: TimeSpan,
    throttle: TimeSpan,
    stage: Stage,
}

impl TimingLie {
    /// The longest throttle: `throttle_time_ms` is an `i32`, so it holds 2147483647 ms at most, which is nearly 25 days.
    pub const MAX_THROTTLE: TimeSpan = TimeSpan::from_millis(i32::MAX as u64);

    /// A timing lie that changes nothing yet, after the response.
    pub const fn new() -> Self {
        Self {
            delay: TimeSpan::ZERO,
            jitter: TimeSpan::ZERO,
            throttle: TimeSpan::ZERO,
            stage: LieKind::Timing.default_stage(),
        }
    }

    /// A timing lie of exactly those parameters, as a file writes them.
    ///
    /// # Errors
    ///
    /// - [`TimingLieError::JitterExceedsDelay`] when `jitter` is longer than `delay`.
    /// - [`TimingLieError::ThrottleTooLong`] when `throttle` is longer than [`MAX_THROTTLE`](Self::MAX_THROTTLE).
    #[cfg(feature = "serde")]
    pub(crate) fn checked(
        delay: TimeSpan,
        jitter: TimeSpan,
        throttle: TimeSpan,
        stage: Stage,
    ) -> Result<Self, TimingLieError> {
        if jitter > delay {
            return Err(TimingLieError::JitterExceedsDelay { jitter, delay });
        }
        if throttle > Self::MAX_THROTTLE {
            return Err(TimingLieError::ThrottleTooLong { throttle });
        }
        Ok(Self {
            delay,
            jitter,
            throttle,
            stage,
        })
    }

    /// The same lie with that delay.
    ///
    /// The jitter never exceeds the delay, so a jitter that is longer than `delay` stops at `delay`.
    #[must_use]
    pub const fn with_delay(mut self, delay: TimeSpan) -> Self {
        self.delay = delay;
        self.jitter = shorter(self.jitter, delay);
        self
    }

    /// The same lie with that jitter, which stops at the delay.
    ///
    /// Set the delay first: a lie without a delay has no wait to stray from, and its jitter stays zero.
    ///
    /// ```
    /// use gaslightmq_clock::TimeSpan;
    /// use gaslightmq_lie::prelude::*;
    ///
    /// let (second, jitter) = (TimeSpan::from_secs(1), TimeSpan::from_millis(200));
    ///
    /// assert_eq!(TimingLie::new().with_delay(second).with_jitter(jitter).jitter(), jitter);
    /// assert_eq!(TimingLie::new().with_jitter(jitter).with_delay(second).jitter(), TimeSpan::ZERO);
    /// ```
    #[must_use]
    pub const fn with_jitter(mut self, jitter: TimeSpan) -> Self {
        self.jitter = shorter(jitter, self.delay);
        self
    }

    /// The same lie with that throttle, which stops at [`MAX_THROTTLE`](Self::MAX_THROTTLE).
    #[must_use]
    pub const fn with_throttle(mut self, throttle: TimeSpan) -> Self {
        self.throttle = shorter(throttle, Self::MAX_THROTTLE);
        self
    }

    /// The same lie at `stage`.
    #[must_use]
    pub const fn at(mut self, stage: Stage) -> Self {
        self.stage = stage;
        self
    }

    /// How long the call waits.
    pub const fn delay(&self) -> TimeSpan {
        self.delay
    }

    /// How far the wait strays from the delay, either way, and at most the delay.
    /// A delay of `1s` with a jitter of `200ms` waits between 800 ms and 1200 ms.
    pub const fn jitter(&self) -> TimeSpan {
        self.jitter
    }

    /// The `throttle_time_ms` of the answer, which tells the client to back off for that long.
    /// It is at most [`MAX_THROTTLE`](Self::MAX_THROTTLE).
    pub const fn throttle(&self) -> TimeSpan {
        self.throttle
    }

    /// Where the call waits: before the backend sees the request, or before the client sees the answer.
    pub const fn stage(&self) -> Stage {
        self.stage
    }
}

/// The shorter of two time spans.
const fn shorter(one: TimeSpan, other: TimeSpan) -> TimeSpan {
    if one.as_millis() <= other.as_millis() {
        one
    } else {
        other
    }
}

/// The lie of [`TimingLie::new`].
impl Default for TimingLie {
    fn default() -> Self {
        Self::new()
    }
}

/// Writes the delay as `+750 ms`, the jitter as `±200 ms` and the throttle as `throttle 1 s`.
/// It leaves out what is zero, and the stage unless the lie acts before forwarding.
impl fmt::Display for TimingLie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A jitter needs a delay, so the delay alone says whether the call waits.
        let waits = self.delay != TimeSpan::ZERO;
        let throttles = self.throttle != TimeSpan::ZERO;

        if waits || !throttles {
            write!(f, "+{}", Spaced(self.delay))?;
        }
        if self.jitter != TimeSpan::ZERO {
            write!(f, " ±{}", Spaced(self.jitter))?;
        }
        if throttles {
            let separator = if waits { ", " } else { "" };
            write!(f, "{separator}throttle {}", Spaced(self.throttle))?;
        }
        AtStage::of(LieKind::Timing, self.stage).fmt(f)
    }
}

/// Why the parameters that a file writes are not a [`TimingLie`].
///
/// Only reading returns it, with the `serde` feature.
/// The `with_` methods never fail, because they stop a value at the longest one that holds.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
///
/// let error = TimingLieError::JitterExceedsDelay {
///     jitter: TimeSpan::from_millis(200),
///     delay: TimeSpan::ZERO,
/// };
/// assert_eq!(error.to_string(), "a jitter of 200ms is longer than the delay of 0s");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimingLieError {
    /// The jitter is longer than the delay, so the shortest wait would be less than no time.
    JitterExceedsDelay { jitter: TimeSpan, delay: TimeSpan },
    /// The throttle is longer than [`TimingLie::MAX_THROTTLE`].
    ThrottleTooLong { throttle: TimeSpan },
}

impl fmt::Display for TimingLieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JitterExceedsDelay { jitter, delay } => {
                write!(f, "a jitter of {jitter} is longer than the delay of {delay}")
            }
            Self::ThrottleTooLong { throttle } => write!(
                f,
                "a throttle of {throttle} is longer than {}, the longest that Kafka can tell a client",
                TimingLie::MAX_THROTTLE
            ),
        }
    }
}

impl std::error::Error for TimingLieError {}

/// A hang-up: the connection closes.
///
/// A hang-up acts before forwarding unless it says otherwise, so the backend never sees the request.
/// After the response, the backend handles the request, and the connection closes before the client reads the answer.
///
/// ```
/// use gaslightmq_lie::prelude::*;
///
/// assert_eq!(HangUpLie::new().to_string(), "hang-up");
/// assert_eq!(HangUpLie::new().at(Stage::AfterResponse).to_string(), "hang-up after the response");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HangUpLie {
    /// Whether the backend handles the request before the connection closes.
    pub stage: Stage,
}

impl HangUpLie {
    /// A hang-up before forwarding.
    pub const fn new() -> Self {
        Self {
            stage: LieKind::HangUp.default_stage(),
        }
    }

    /// The same hang-up at `stage`.
    #[must_use]
    pub const fn at(mut self, stage: Stage) -> Self {
        self.stage = stage;
        self
    }
}

/// The lie of [`HangUpLie::new`].
impl Default for HangUpLie {
    fn default() -> Self {
        Self::new()
    }
}

/// Writes `hang-up`, and the stage unless the connection closes before forwarding.
impl fmt::Display for HangUpLie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", LieKind::HangUp, AtStage::of(LieKind::HangUp, self.stage))
    }
}

/// A lie as a rule's action or a preset writes it: its kind with its parameters.
///
/// A definition is data.
/// It says what the client is told, and a crate of behaviour does the telling.
///
/// Refusal, timing, silence and hang-up have a definition.
/// Topology, data, progress and configuration get theirs with their behaviour,
/// so a `match` on a definition needs a wildcard arm.
///
/// A definition does not say what it lies about.
/// The rule's selector yields a target on every call it matches,
/// and [`on`](Self::on) binds the definition to that target.
///
/// ```
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let lie = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// assert_eq!(lie.kind(), LieKind::Refusal);
/// assert_eq!(lie.stage(), Stage::BeforeForwarding);
/// assert_eq!(lie.class(), CombinationClass::Refusal);
///
/// // The rule matched one partition of a Produce.
/// let matched = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
/// assert_eq!(lie.on(matched)?.to_string(), "NOT_LEADER_OR_FOLLOWER on orders.eu/3");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LieDefinition {
    /// The request failed.
    Refusal(RefusalLie),
    /// The answer took longer, or the client must back off.
    Timing(TimingLie),
    /// Nothing at all: GaslightMQ reads the request and never answers it.
    /// A silence has no parameters.
    Silence,
    /// The connection is gone.
    HangUp(HangUpLie),
}

impl LieDefinition {
    /// Which kind of lie this is.
    pub const fn kind(&self) -> LieKind {
        match self {
            Self::Refusal(_) => LieKind::Refusal,
            Self::Timing(_) => LieKind::Timing,
            Self::Silence => LieKind::Silence,
            Self::HangUp(_) => LieKind::HangUp,
        }
    }

    /// The stage the lie acts at: the one it names, or the only one its kind has.
    pub const fn stage(&self) -> Stage {
        match self {
            Self::Refusal(lie) => lie.stage,
            Self::Timing(lie) => lie.stage(),
            Self::Silence => LieKind::Silence.default_stage(),
            Self::HangUp(lie) => lie.stage,
        }
    }

    /// How the lie combines with the other lies on a call.
    pub const fn class(&self) -> CombinationClass {
        self.kind().class()
    }

    /// What the lie rewrites about its target, or `None` when it is not a rewrite.
    ///
    /// Two rewrites conflict when they have the same aspect and the same target.
    pub const fn aspect(&self) -> Option<Aspect> {
        match self {
            Self::Refusal(_) | Self::Timing(_) | Self::Silence | Self::HangUp(_) => None,
        }
    }

    /// The lie on what a selector matched.
    /// See [`TargetedLie::new`].
    ///
    /// # Errors
    ///
    /// [`TargetedLieError::EmptyTarget`] when `target` is a set of parts without a part.
    pub fn on(self, target: Target) -> Result<TargetedLie, TargetedLieError> {
        TargetedLie::new(self, target)
    }
}

impl From<RefusalLie> for LieDefinition {
    fn from(lie: RefusalLie) -> Self {
        Self::Refusal(lie)
    }
}

impl From<TimingLie> for LieDefinition {
    fn from(lie: TimingLie) -> Self {
        Self::Timing(lie)
    }
}

impl From<HangUpLie> for LieDefinition {
    fn from(lie: HangUpLie) -> Self {
        Self::HangUp(lie)
    }
}

/// Writes the lie as a verdict lists it: `NOT_LEADER_OR_FOLLOWER`, `+750 ms`, `silence` or `hang-up`.
/// The stage follows when the definition names another one than its kind's default.
impl fmt::Display for LieDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refusal(lie) => lie.fmt(f),
            Self::Timing(lie) => lie.fmt(f),
            Self::Silence => f.write_str(LieKind::Silence.name()),
            Self::HangUp(lie) => lie.fmt(f),
        }
    }
}

/// The stage as the end of a sentence about a lie: ` after the response`.
/// It writes nothing at the stage the kind acts at by default, because a reader assumes that one.
struct AtStage {
    kind: LieKind,
    stage: Stage,
}

impl AtStage {
    const fn of(kind: LieKind, stage: Stage) -> Self {
        Self { kind, stage }
    }
}

impl fmt::Display for AtStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.stage == self.kind.default_stage() {
            Ok(())
        } else {
            write!(f, " {}", self.stage.phrase())
        }
    }
}

/// A time span with a space before its unit, as a sentence writes it: `750 ms`.
struct Spaced(TimeSpan);

impl fmt::Display for Spaced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = self.0.to_string();
        let digits = text.bytes().take_while(u8::is_ascii_digit).count();
        let (number, unit) = text.split_at(digits);
        write!(f, "{number} {unit}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEFORE: Stage = Stage::BeforeForwarding;
    const AFTER: Stage = Stage::AfterResponse;

    fn millis(millis: u64) -> TimeSpan {
        TimeSpan::from_millis(millis)
    }

    /// One definition of every kind that has one, at every stage it can name.
    fn every_definition() -> Vec<LieDefinition> {
        vec![
            RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER).into(),
            RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(AFTER).into(),
            TimingLie::new().with_delay(millis(750)).into(),
            TimingLie::new().with_delay(millis(750)).at(BEFORE).into(),
            LieDefinition::Silence,
            HangUpLie::new().into(),
            HangUpLie::new().at(AFTER).into(),
        ]
    }

    #[test]
    fn a_new_lie_acts_at_the_default_stage_of_its_kind() {
        assert_eq!(RefusalLie::new(ErrorCode::UNKNOWN_SERVER_ERROR).stage, BEFORE);
        assert_eq!(TimingLie::new().stage(), AFTER);
        assert_eq!(HangUpLie::new().stage, BEFORE);
        assert_eq!(TimingLie::default(), TimingLie::new());
        assert_eq!(HangUpLie::default(), HangUpLie::new());

        for lie in [
            RefusalLie::new(ErrorCode::UNKNOWN_SERVER_ERROR).into(),
            TimingLie::new().into(),
            LieDefinition::Silence,
            HangUpLie::new().into(),
        ] {
            assert_eq!(lie.stage(), lie.kind().default_stage(), "{lie}");
        }
    }

    #[test]
    fn a_lie_that_names_its_stage_acts_there() {
        assert_eq!(RefusalLie::new(ErrorCode::REQUEST_TIMED_OUT).at(AFTER).stage, AFTER);
        assert_eq!(TimingLie::new().at(BEFORE).stage(), BEFORE);
        assert_eq!(HangUpLie::new().at(AFTER).stage, AFTER);
        assert_eq!(LieDefinition::from(HangUpLie::new().at(AFTER)).stage(), AFTER);
    }

    #[test]
    fn a_new_timing_lie_changes_nothing_until_it_gets_a_parameter() {
        let lie = TimingLie::new();
        assert_eq!(
            (lie.delay(), lie.jitter(), lie.throttle()),
            (millis(0), millis(0), millis(0))
        );

        let lie = lie
            .with_delay(millis(1000))
            .with_jitter(millis(200))
            .with_throttle(millis(500));
        assert_eq!(
            (lie.delay(), lie.jitter(), lie.throttle(), lie.stage()),
            (millis(1000), millis(200), millis(500), AFTER)
        );
    }

    #[test]
    fn the_jitter_of_a_timing_lie_stops_at_its_delay() {
        let lie = TimingLie::new().with_delay(millis(100));

        assert_eq!(lie.with_jitter(millis(99)).jitter(), millis(99));
        assert_eq!(lie.with_jitter(millis(100)).jitter(), millis(100));
        assert_eq!(lie.with_jitter(millis(200)).jitter(), millis(100));
        assert_eq!(lie.with_jitter(millis(u64::MAX)).jitter(), millis(100));
    }

    #[test]
    fn a_timing_lie_without_a_delay_has_no_jitter() {
        assert_eq!(TimingLie::new().with_jitter(millis(200)).jitter(), millis(0));
        assert_eq!(TimingLie::new().with_jitter(millis(200)), TimingLie::new());

        // The jitter comes after the delay it strays from.
        let lie = TimingLie::new().with_jitter(millis(200)).with_delay(millis(1000));
        assert_eq!((lie.delay(), lie.jitter()), (millis(1000), millis(0)));
    }

    #[test]
    fn a_shorter_delay_shortens_the_jitter_with_it() {
        let lie = TimingLie::new().with_delay(millis(1000)).with_jitter(millis(200));

        let shorter = lie.with_delay(millis(50));
        assert_eq!((shorter.delay(), shorter.jitter()), (millis(50), millis(50)));

        let longer = lie.with_delay(millis(5000));
        assert_eq!((longer.delay(), longer.jitter()), (millis(5000), millis(200)));

        assert_eq!(lie.with_delay(millis(0)), TimingLie::new());
    }

    #[test]
    fn the_throttle_of_a_timing_lie_stops_at_what_kafka_can_tell() {
        let longest = millis(u64::from(i32::MAX.unsigned_abs()));
        assert_eq!(TimingLie::MAX_THROTTLE, longest);
        assert!(i32::try_from(TimingLie::MAX_THROTTLE.as_millis()).is_ok());

        let lie = TimingLie::new();
        assert_eq!(lie.with_throttle(longest).throttle(), longest);
        assert_eq!(lie.with_throttle(millis(longest.as_millis() + 1)).throttle(), longest);
        assert_eq!(lie.with_throttle(millis(u64::MAX)).throttle(), longest);
        // The delay has no such bound.
        assert_eq!(lie.with_delay(millis(u64::MAX)).delay(), millis(u64::MAX));
    }

    #[test]
    fn a_timing_lie_error_says_what_is_wrong() {
        let cases = [
            (
                TimingLieError::JitterExceedsDelay {
                    jitter: millis(200),
                    delay: TimeSpan::ZERO,
                },
                "a jitter of 200ms is longer than the delay of 0s",
            ),
            (
                TimingLieError::ThrottleTooLong {
                    throttle: TimeSpan::from_secs(3_000_000),
                },
                "a throttle of 50000m is longer than 2147483647ms, the longest that Kafka can tell a client",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_chain_of_parameters_is_a_constant() {
        const DELAY: TimingLie = TimingLie::new()
            .with_delay(TimeSpan::from_secs(1))
            .with_jitter(TimeSpan::from_secs(5))
            .with_throttle(TimeSpan::from_millis(u64::MAX))
            .at(BEFORE);

        assert_eq!(DELAY.jitter(), DELAY.delay());
        assert_eq!(DELAY.throttle(), TimingLie::MAX_THROTTLE);
    }

    #[test]
    fn a_definition_has_the_kind_of_its_variant() {
        let kinds: Vec<LieKind> = every_definition().iter().map(LieDefinition::kind).collect();
        assert_eq!(
            kinds,
            [
                LieKind::Refusal,
                LieKind::Refusal,
                LieKind::Timing,
                LieKind::Timing,
                LieKind::Silence,
                LieKind::HangUp,
                LieKind::HangUp,
            ]
        );
    }

    #[test]
    fn every_definition_acts_at_a_stage_its_kind_allows() {
        for lie in every_definition() {
            assert!(lie.kind().allows(lie.stage()), "{lie}");
            assert_eq!(lie.class(), lie.kind().class(), "{lie}");
        }
    }

    #[test]
    fn no_definition_rewrites_an_aspect_yet() {
        for lie in every_definition() {
            assert_eq!(lie.aspect(), None, "{lie}");
            assert_ne!(lie.class(), CombinationClass::Rewrite, "{lie}");
        }
    }

    #[test]
    fn a_definition_writes_what_a_verdict_lists() {
        let texts: Vec<String> = every_definition().iter().map(ToString::to_string).collect();
        assert_eq!(
            texts,
            [
                "NOT_LEADER_OR_FOLLOWER",
                "REQUEST_TIMED_OUT after the response",
                "+750 ms",
                "+750 ms before forwarding",
                "silence",
                "hang-up",
                "hang-up after the response",
            ]
        );
    }

    #[test]
    fn a_refusal_with_a_code_kafka_gives_no_name_writes_the_number() {
        let lie = RefusalLie::new(ErrorCode::new(999).unwrap());
        assert_eq!(lie.to_string(), "999");
    }

    #[test]
    fn a_timing_lie_writes_the_parameters_that_are_not_zero() {
        let lie = TimingLie::new;
        let cases = [
            (lie(), "+0 s"),
            (lie().with_delay(millis(750)), "+750 ms"),
            (lie().with_delay(millis(1000)), "+1 s"),
            (lie().with_delay(millis(1500)), "+1500 ms"),
            (lie().with_delay(millis(120_000)), "+2 m"),
            (lie().with_delay(millis(1000)).with_jitter(millis(200)), "+1 s ±200 ms"),
            (lie().with_jitter(millis(200)), "+0 s"),
            (
                lie().with_delay(millis(100)).with_jitter(millis(200)),
                "+100 ms ±100 ms",
            ),
            (lie().with_throttle(millis(500)), "throttle 500 ms"),
            (
                lie().with_delay(millis(1000)).with_throttle(millis(500)),
                "+1 s, throttle 500 ms",
            ),
            (
                lie()
                    .with_delay(millis(1000))
                    .with_jitter(millis(200))
                    .with_throttle(millis(500))
                    .at(BEFORE),
                "+1 s ±200 ms, throttle 500 ms before forwarding",
            ),
            (lie().at(BEFORE), "+0 s before forwarding"),
        ];

        for (lie, text) in cases {
            assert_eq!(lie.to_string(), text);
            assert_eq!(LieDefinition::from(lie).to_string(), text);
        }
    }

    #[test]
    fn a_definition_converts_from_the_lie_it_holds() {
        let refusal = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);
        let timing = TimingLie::new().with_delay(millis(750));
        let hang_up = HangUpLie::new();

        assert_eq!(LieDefinition::from(refusal), LieDefinition::Refusal(refusal));
        assert_eq!(LieDefinition::from(timing), LieDefinition::Timing(timing));
        assert_eq!(LieDefinition::from(hang_up), LieDefinition::HangUp(hang_up));
    }
}
