use std::collections::BTreeMap;
use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

/// Which lie won each target, among lies that conflict there.
///
/// An [`Effect`] holds one for the refusals of its stage, and one for every aspect that a rewrite changes.
/// Either one lie holds the whole call, or lies hold parts of it, and never both:
/// a lie on the whole call is one target, so it wins or loses all of it.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// let refusal = |part: &Part| Claim::Refusal {
///     stage: Stage::BeforeForwarding,
///     target: Target::of_parts([part.clone()]).expect("one part"),
/// };
///
/// let request = RequestPass::of([
///     FiredLie::new("Leader flap", Priority::new(0), refusal(&three)),
///     FiredLie::new("Refuse the 3rd Produce", Priority::new(1), refusal(&three)),
/// ]);
/// let refusals = request.before_forwarding().refusals();
///
/// assert_eq!(refusals.of(&three), Some(&"Leader flap"));
/// assert_eq!(refusals.of(&four), None);
/// assert_eq!(refusals.of_call(), None);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Winners<Id> {
    call: Option<Id>,
    parts: BTreeMap<Part, Id>,
}

impl<Id> Winners<Id> {
    /// The lie that holds `part`: the one that won that part, or the one that won the whole call.
    pub fn of(&self, part: &Part) -> Option<&Id> {
        self.call.as_ref().or_else(|| self.parts.get(part))
    }

    /// The lie that holds the whole call.
    /// It is `None` when lies hold parts of the call, and when no lie holds anything.
    pub const fn of_call(&self) -> Option<&Id> {
        self.call.as_ref()
    }

    /// The parts that lies hold one by one, in order, each with its lie.
    /// A lie that holds the whole call names no parts.
    pub fn parts(&self) -> impl Iterator<Item = (&Part, &Id)> {
        self.parts.iter()
    }

    /// Whether no lie holds anything.
    pub fn is_empty(&self) -> bool {
        self.call.is_none() && self.parts.is_empty()
    }

    /// Gives the whole call to `id`.
    pub(crate) fn give_call(&mut self, id: Id) {
        self.call = Some(id);
    }

    /// Gives `part` to `id`.
    pub(crate) fn give(&mut self, part: Part, id: Id) {
        self.parts.insert(part, id);
    }

    /// The same winners under other names, without the ones that `rename` drops.
    pub(crate) fn filter_map<Other>(&self, mut rename: impl FnMut(&Id) -> Option<Other>) -> Winners<Other> {
        Winners {
            call: self.call.as_ref().and_then(&mut rename),
            parts: self
                .parts
                .iter()
                .filter_map(|(part, id)| Some((part.clone(), rename(id)?)))
                .collect(),
        }
    }
}

/// No lie holds anything.
impl<Id> Default for Winners<Id> {
    fn default() -> Self {
        Self {
            call: None,
            parts: BTreeMap::new(),
        }
    }
}

/// How long a call waits at one stage: the delays of every timing lie that applies there, added up.
///
/// The jitters add up with their delays.
/// Each wait strays from its delay by at most its jitter, so the sum strays by at most the sum of the jitters.
/// Whoever holds the call back picks a wait between the [shortest](Self::shortest) and the [longest](Self::longest).
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let second = TimingLie::new().with_delay(TimeSpan::from_secs(1)).with_jitter(TimeSpan::from_millis(200));
/// let more = TimingLie::new().with_delay(TimeSpan::from_millis(500));
///
/// let request = RequestPass::of([
///     FiredLie::new("Delay 1 s", Priority::new(0), second),
///     FiredLie::new("Background jitter", Priority::new(1), more),
/// ]);
/// let delay = request.without_response_pass().after_response().delay();
///
/// assert_eq!(delay.length(), TimeSpan::from_millis(1500));
/// assert_eq!(delay.jitter(), TimeSpan::from_millis(200));
/// assert_eq!(delay.shortest(), TimeSpan::from_millis(1300));
/// assert_eq!(delay.longest(), TimeSpan::from_millis(1700));
/// assert_eq!(delay.to_string(), "+1500 ms ±200 ms");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Delay {
    length: TimeSpan,
    jitter: TimeSpan,
}

impl Delay {
    /// No wait at all.
    pub const ZERO: Self = Self {
        length: TimeSpan::ZERO,
        jitter: TimeSpan::ZERO,
    };

    /// How long the call waits, before any jitter.
    pub const fn length(self) -> TimeSpan {
        self.length
    }

    /// How far the wait strays from the length, either way.
    /// It is at most the length.
    pub const fn jitter(self) -> TimeSpan {
        self.jitter
    }

    /// The shortest wait: the length less the jitter.
    pub const fn shortest(self) -> TimeSpan {
        TimeSpan::from_millis(self.length.as_millis().saturating_sub(self.jitter.as_millis()))
    }

    /// The longest wait: the length and the jitter.
    /// It stops at the longest time span.
    pub const fn longest(self) -> TimeSpan {
        sum(self.length, self.jitter)
    }

    /// Whether the call does not wait.
    pub const fn is_zero(self) -> bool {
        self.length.as_millis() == 0
    }

    /// This delay with the delay and the jitter of `timing` on top.
    /// A sum stops at the longest time span.
    pub(crate) const fn plus(self, timing: &TimingLie) -> Self {
        Self {
            length: sum(self.length, timing.delay()),
            jitter: sum(self.jitter, timing.jitter()),
        }
    }
}

/// Writes the delay as a timing lie writes its own: `+1500 ms ±200 ms`.
impl fmt::Display for Delay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timing = TimingLie::new().with_delay(self.length).with_jitter(self.jitter);
        fmt::Display::fmt(&timing, f)
    }
}

/// The two time spans added up, stopping at the longest time span.
const fn sum(one: TimeSpan, other: TimeSpan) -> TimeSpan {
    TimeSpan::from_millis(one.as_millis().saturating_add(other.as_millis()))
}

/// What the lies that apply do to a call at one stage.
///
/// A call has two effects: one [before forwarding](Stage::BeforeForwarding),
/// and one [after the response](Stage::AfterResponse).
/// Each names the lies to tell at its stage, by the identity the caller gave them.
///
/// | Part | Is |
/// |---|---|
/// | [`terminal`](Self::terminal) | The silence or the hang-up that ends the call at this stage. |
/// | [`refusals`](Self::refusals) | The refusal that holds each target. |
/// | [`rewrites`](Self::rewrites) | For every aspect, the rewrite that holds each target. Rewrites act after the response only. |
/// | [`delay`](Self::delay) | How long the call waits at this stage. |
/// | [`throttle`](Self::throttle) | The throttle the answer tells the client. It is a field of the answer, so it is zero before forwarding, and zero when the call ends without an answer. |
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // A delay, a refusal of `orders.eu/3` and a stale watermark on `orders.eu/4` combine on one Fetch.
/// let [three, four]: [Part; 2] = ["orders.eu/3".parse()?, "orders.eu/4".parse()?];
/// let on = |part: &Part| Target::of_parts([part.clone()]).expect("one part");
///
/// let delay = TimingLie::new().with_delay(TimeSpan::from_secs(1));
/// let refusal = Claim::Refusal { stage: Stage::BeforeForwarding, target: on(&three) };
/// let stale = Claim::Rewrite { aspect: Aspect::HighWatermark, target: on(&four) };
///
/// let request = RequestPass::of([
///     FiredLie::new("Delay 1 s", Priority::new(0), delay),
///     FiredLie::new("Leader flap", Priority::new(1), refusal),
///     FiredLie::new("Stale watermark", Priority::new(2), stale),
/// ]);
///
/// // Before forwarding, `orders.eu/3` leaves the request.
/// let before = request.before_forwarding();
/// assert_eq!(before.refusals().of(&three), Some(&"Leader flap"));
/// assert!(before.delay().is_zero());
///
/// // After the response, the watermark of `orders.eu/4` goes stale, and the answer waits.
/// let call = request.without_response_pass();
/// let after = call.after_response();
/// assert_eq!(after.rewrite(Aspect::HighWatermark, &four), Some(&"Stale watermark"));
/// assert_eq!(after.delay().length(), TimeSpan::from_secs(1));
/// assert_eq!(after.terminal(), None);
/// # Ok::<(), FieldError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effect<Id> {
    pub(crate) stage: Stage,
    pub(crate) terminal: Option<Id>,
    pub(crate) refusals: Winners<Id>,
    pub(crate) rewrites: BTreeMap<Aspect, Winners<Id>>,
    pub(crate) delay: Delay,
    pub(crate) throttle: TimeSpan,
}

impl<Id> Effect<Id> {
    /// The effect of no lie at `stage`.
    pub(crate) const fn nothing(stage: Stage) -> Self {
        Self {
            stage,
            terminal: None,
            refusals: Winners {
                call: None,
                parts: BTreeMap::new(),
            },
            rewrites: BTreeMap::new(),
            delay: Delay::ZERO,
            throttle: TimeSpan::ZERO,
        }
    }

    /// The stage at which this happens to the call.
    pub const fn stage(&self) -> Stage {
        self.stage
    }

    /// The silence or the hang-up that ends the call at this stage.
    ///
    /// Before forwarding, the backend never sees the request, and the response pass does not run.
    /// After the response, the connection closes instead of sending the answer.
    pub const fn terminal(&self) -> Option<&Id> {
        self.terminal.as_ref()
    }

    /// The refusal that holds each target at this stage.
    ///
    /// Before forwarding, these parts leave the request, and their refusals merge into the response.
    /// After the response, these refusals replace the truth of their parts.
    pub const fn refusals(&self) -> &Winners<Id> {
        &self.refusals
    }

    /// The aspects that rewrites change at this stage, in order, each with the rewrite that holds each target.
    pub fn rewrites(&self) -> impl Iterator<Item = (Aspect, &Winners<Id>)> {
        self.rewrites.iter().map(|(aspect, winners)| (*aspect, winners))
    }

    /// The rewrite that changes `aspect` of `part` at this stage.
    pub fn rewrite(&self, aspect: Aspect, part: &Part) -> Option<&Id> {
        self.rewrites.get(&aspect)?.of(part)
    }

    /// The rewrite that changes `aspect` of the whole call at this stage.
    pub fn rewrite_of_call(&self, aspect: Aspect) -> Option<&Id> {
        self.rewrites.get(&aspect)?.of_call()
    }

    /// How long the call waits at this stage.
    ///
    /// Before forwarding, the request waits, and the backend sees it late.
    /// After the response, the answer waits.
    pub const fn delay(&self) -> Delay {
        self.delay
    }

    /// The `throttle_time_ms` of the answer: the longest throttle of the timing lies that apply, at either stage.
    ///
    /// The throttle is a field of the answer, so it is zero before forwarding.
    /// It is also zero when a [terminal lie](Self::terminal) ends the call at this stage: no answer carries it.
    /// It is at most [`TimingLie::MAX_THROTTLE`].
    pub const fn throttle(&self) -> TimeSpan {
        self.throttle
    }

    /// Whether no lie changes anything at this stage.
    pub fn changes_nothing(&self) -> bool {
        self.terminal.is_none()
            && self.refusals.is_empty()
            && self.rewrites.is_empty()
            && self.delay.is_zero()
            && self.throttle == TimeSpan::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn millis(millis: u64) -> TimeSpan {
        TimeSpan::from_millis(millis)
    }

    fn part(text: &str) -> Part {
        text.parse().unwrap()
    }

    #[test]
    fn no_winners_hold_nothing() {
        let winners = Winners::<u32>::default();

        assert!(winners.is_empty());
        assert_eq!(winners.of_call(), None);
        assert_eq!(winners.of(&part("orders.eu/3")), None);
        assert_eq!(winners.parts().count(), 0);
    }

    #[test]
    fn the_winner_of_the_whole_call_holds_every_part() {
        let mut winners = Winners::default();
        winners.give_call(7);

        assert!(!winners.is_empty());
        assert_eq!(winners.of_call(), Some(&7));
        assert_eq!(winners.of(&part("orders.eu/3")), Some(&7));
        assert_eq!(winners.of(&part("payments.settled")), Some(&7));
        assert_eq!(winners.parts().count(), 0);
    }

    #[test]
    fn the_winner_of_a_part_holds_that_part_only() {
        let mut winners = Winners::default();
        winners.give(part("orders.eu/4"), 2);
        winners.give(part("orders.eu/3"), 1);

        assert_eq!(winners.of(&part("orders.eu/3")), Some(&1));
        assert_eq!(winners.of(&part("orders.eu/5")), None);
        assert_eq!(winners.of_call(), None);
        assert_eq!(
            winners.parts().collect::<Vec<_>>(),
            [(&part("orders.eu/3"), &1), (&part("orders.eu/4"), &2)]
        );
    }

    #[test]
    fn winners_rename_and_drop() {
        let mut winners = Winners::default();
        winners.give(part("orders.eu/3"), 1);
        winners.give(part("orders.eu/4"), 2);

        let odd = winners.filter_map(|id| (id % 2 == 1).then(|| format!("#{id}")));

        assert_eq!(odd.of(&part("orders.eu/3")), Some(&"#1".to_owned()));
        assert_eq!(odd.of(&part("orders.eu/4")), None);
    }

    #[test]
    fn no_delay_waits_no_time() {
        assert_eq!(Delay::ZERO, Delay::default());
        assert!(Delay::ZERO.is_zero());
        assert_eq!(Delay::ZERO.shortest(), millis(0));
        assert_eq!(Delay::ZERO.longest(), millis(0));
        assert_eq!(Delay::ZERO.to_string(), "+0 s");
    }

    #[test]
    fn delays_and_their_jitters_add_up() {
        let one = TimingLie::new().with_delay(millis(1000)).with_jitter(millis(200));
        let other = TimingLie::new().with_delay(millis(100)).with_jitter(millis(100));
        let delay = Delay::ZERO.plus(&one).plus(&other);

        assert_eq!(delay.length(), millis(1100));
        assert_eq!(delay.jitter(), millis(300));
        assert_eq!(delay.shortest(), millis(800));
        assert_eq!(delay.longest(), millis(1400));
        assert!(!delay.is_zero());
        assert_eq!(delay.to_string(), "+1100 ms ±300 ms");
    }

    #[test]
    fn a_sum_of_delays_stops_at_the_longest_time_span() {
        let forever = TimingLie::new()
            .with_delay(millis(u64::MAX))
            .with_jitter(millis(u64::MAX));
        let delay = Delay::ZERO.plus(&forever).plus(&forever);

        assert_eq!(delay.length(), millis(u64::MAX));
        assert_eq!(delay.jitter(), millis(u64::MAX));
        assert_eq!(delay.shortest(), millis(0));
        assert_eq!(delay.longest(), millis(u64::MAX));
    }

    #[test]
    fn the_effect_of_no_lie_changes_nothing() {
        let effect = Effect::<u32>::nothing(Stage::AfterResponse);

        assert!(effect.changes_nothing());
        assert_eq!(effect.stage(), Stage::AfterResponse);
        assert_eq!(effect.terminal(), None);
        assert_eq!(effect.rewrites().count(), 0);
        assert_eq!(effect.rewrite(Aspect::Records, &part("orders.eu/3")), None);
        assert_eq!(effect.rewrite_of_call(Aspect::Records), None);
        assert_eq!(effect.throttle(), millis(0));
    }
}
