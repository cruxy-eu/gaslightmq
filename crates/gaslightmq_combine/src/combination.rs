use gaslightmq_clock::TimeSpan;
use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

use crate::board::{Board, Draft, Entry, Holders, How, Pass, Round, in_priority_order};
use crate::{Claim, Delay, Fate, FiredLie, Loss, StageEffect};

/// The lies of the request pass, combined: what happens to the call before forwarding.
///
/// The pipeline evaluates its rules in two passes.
/// The request pass runs before forwarding, and the response pass runs once the backend answered.
/// This type is the call between the two.
///
/// 1. [`of`](Self::of) combines the lies that fired in the request pass.
/// 2. [`before_forwarding`](Self::before_forwarding) says what to do to the request.
///    It is final: a lie that acts before forwarding acts now, or never.
/// 3. [`response_pass`](Self::response_pass) adds the lies that fired in the response pass,
///    and [`without_response_pass`](Self::without_response_pass) adds none.
///    Either gives the [`Combination`] of the whole call.
///
/// The fate of a lie that acts after the response is open until then,
/// because a lie of the response pass can still win a conflict with it.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// let three = Target::of_parts(["orders.eu/3".parse()?]).expect("one part");
///
/// // A chain of "Delay 1 s" above "Refuse the 3rd Produce": the rule's position is its priority.
/// let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_secs(1))).on(three.clone());
/// let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER)).on(three.clone());
///
/// let request = RequestPass::of([
///     FiredLie::new("Delay 1 s", Priority::new(0), Claim::try_from(&delay)?),
///     FiredLie::new("Refuse the 3rd Produce", Priority::new(1), Claim::try_from(&refusal)?),
/// ]);
///
/// // Before forwarding, the refused partition leaves the request.
/// let part = three.parts().next().expect("one part");
/// assert_eq!(request.before_forwarding().refusals().of(part), Some(&"Refuse the 3rd Produce"));
/// assert_eq!(request.before_forwarding().terminal(), None);
///
/// // No rule waited for the truth. A delay and a refusal do not conflict, so both apply.
/// let call = request.without_response_pass();
/// assert!(call.fates().iter().all(Fate::counts));
/// assert_eq!(call.after_response().delay().length(), TimeSpan::from_secs(1));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct RequestPass<Id> {
    entries: Vec<Entry<Id>>,
    board: Board,
    drafts: Vec<Draft>,
    before: StageEffect<Id>,
}

impl<Id: Clone> RequestPass<Id> {
    /// Combines the lies that fired in the request pass.
    ///
    /// The lies come in any order, and their [`Priority`] orders them.
    pub fn of(fired: impl IntoIterator<Item = FiredLie<Id>>) -> Self {
        let entries: Vec<Entry<Id>> = fired
            .into_iter()
            .map(|lie| Entry {
                lie,
                pass: Pass::Request,
            })
            .collect();
        let order = in_priority_order(&entries);
        let drafts = vec![Draft::default(); entries.len()];

        let (board, drafts) = Round::new(&entries, &Holders::default(), drafts).run(&order);
        let before = before_forwarding(&entries, &board);

        Self {
            entries,
            board,
            drafts,
            before,
        }
    }

    /// What the lies do to the call before forwarding.
    ///
    /// When it names a [terminal lie](StageEffect::terminal), the call ends here:
    /// the backend never sees the request, and the response pass does not run.
    pub const fn before_forwarding(&self) -> &StageEffect<Id> {
        &self.before
    }

    /// Adds the lies that fired in the response pass, and combines the whole call.
    ///
    /// Priority orders the lies within one pass.
    /// Between the passes, the stage decides:
    ///
    /// - The fate of a request-pass lie that acts before forwarding is final.
    ///   It took effect, or it lost to a lie of its own pass.
    ///   It keeps that loss when its winner then loses in the response pass, because the request was forwarded by then.
    ///   Its [`Loss`] then names a lie that does not apply there.
    /// - A request-pass lie that acts after the response meets the lies of the response pass,
    ///   and their conflicts resolve by priority as usual.
    ///   A hang-up after the response that won the request pass can still lose to a refusal of the response pass,
    ///   and a refusal it shadowed then applies after all.
    /// - A part that was refused before forwarding has no truth.
    ///   A response-pass lie on that part is pre-empted by the refusal, whatever its priority.
    /// - A response-pass lie on the whole call does not conflict with a refusal of a part before forwarding,
    ///   which already happened.
    ///   A refusal of the whole call replaces the truth of every part that was forwarded, and both apply.
    ///   Only a refusal of the whole call before forwarding leaves no truth, and pre-empts it.
    /// - A response-pass hang-up does not conflict with a refusal before forwarding, which already happened.
    ///   Both apply.
    /// - After a silence or a hang-up before forwarding, there is no truth at all.
    ///   Every response-pass lie is pre-empted by it, whatever its priority.
    ///
    /// A lie of the response pass acts after the response, whatever stage it names:
    /// the truth exists by the time it fires.
    /// A rule that waits for the truth should not carry a lie before forwarding, and whoever reads rules rejects it.
    pub fn response_pass(self, fired: impl IntoIterator<Item = FiredLie<Id>>) -> Combination<Id> {
        let Self {
            mut entries,
            board: first,
            drafts: settled,
            before,
        } = self;

        let carried = entries.len();
        entries.extend(fired.into_iter().map(|lie| Entry {
            lie,
            pass: Pass::Response,
        }));
        let order = in_priority_order(&entries);

        let ended = first
            .terminal
            .filter(|&terminal| entries[terminal].stage() == Stage::BeforeForwarding);

        let (drafts, after) = if let Some(terminal) = ended {
            let mut drafts = settled;
            drafts.extend(
                entries[carried..]
                    .iter()
                    .map(|entry| Draft::lost(How::PreEmpted, terminal, entry.lie.claim().target().clone())),
            );
            (drafts, StageEffect::nothing(Stage::AfterResponse))
        } else {
            let seated = first.refusals.settled(&entries);
            let drafts: Vec<Draft> = entries
                .iter()
                .zip(settled.into_iter().map(Some).chain(std::iter::repeat(None)))
                .map(|(entry, settled)| settled.filter(|_| entry.is_settled()).unwrap_or_default())
                .collect();
            let open: Vec<usize> = order
                .iter()
                .copied()
                .filter(|&lie| !entries[lie].is_settled())
                .collect();

            let (second, drafts) = Round::new(&entries, &seated, drafts).run(&open);
            let after = after_response(&entries, &first, &second);
            (drafts, after)
        };

        Combination {
            fates: fates(entries, drafts, &order),
            before,
            after,
        }
    }

    /// Combines the whole call without a response pass.
    ///
    /// That is the call on which no rule waited for the truth,
    /// and the call that a silence or a hang-up ended before forwarding.
    pub fn without_response_pass(self) -> Combination<Id> {
        self.response_pass([])
    }
}

/// The lies of one call, combined: the fate of every lie, and what the lies that apply do to the call.
///
/// A [`RequestPass`] gives it, with or without the lies of the response pass.
///
/// ```
/// use gaslightmq_combine::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_lie::prelude::*;
///
/// // A chain of "Blackhole" above "Refuse": both are ways to fail the call, so they conflict.
/// let silence = Claim::Terminal { stage: Stage::BeforeForwarding };
/// let refusal = Claim::Refusal { stage: Stage::BeforeForwarding, target: Target::Call };
///
/// let call = RequestPass::of([
///     FiredLie::new("Blackhole", Priority::new(0), silence),
///     FiredLie::new("Refuse", Priority::new(1), refusal),
/// ])
/// .without_response_pass();
///
/// assert_eq!(call.before_forwarding().terminal(), Some(&"Blackhole"));
///
/// let refuse = call.fate_of(&"Refuse").expect("the lie fired");
/// assert!(!refuse.counts());
/// assert_eq!(refuse.shadowed()[0].to(), &"Blackhole");
/// assert_eq!(refuse.shadowed()[0].on(), &Target::Call);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Combination<Id> {
    fates: Vec<Fate<Id>>,
    before: StageEffect<Id>,
    after: StageEffect<Id>,
}

impl<Id> Combination<Id> {
    /// The fate of every lie that fired on the call, in either pass, from the highest priority down.
    pub fn fates(&self) -> &[Fate<Id>] {
        &self.fates
    }

    /// The fate of the lie `id`, or `None` when no lie of that identity fired.
    pub fn fate_of(&self, id: &Id) -> Option<&Fate<Id>>
    where
        Id: PartialEq,
    {
        self.fates.iter().find(|fate| fate.id() == id)
    }

    /// What the lies did to the call before forwarding.
    /// It is the effect the [`RequestPass`] gave.
    pub const fn before_forwarding(&self) -> &StageEffect<Id> {
        &self.before
    }

    /// What the lies do to the call after the response.
    ///
    /// The refusals of this effect are the ones that act after the response.
    /// The refusals [before forwarding](Self::before_forwarding) merge into the response as well.
    /// A part that was refused before forwarding keeps that refusal,
    /// also when a refusal of the response pass holds the whole call here: that one replaces the truth of the rest.
    pub const fn after_response(&self) -> &StageEffect<Id> {
        &self.after
    }
}

/// The timing lie at `lie`, if that lie is one.
fn timing<Id>(entries: &[Entry<Id>], lie: usize) -> Option<&TimingLie> {
    match entries[lie].lie.claim() {
        Claim::Modifier(timing) => Some(timing),
        _ => None,
    }
}

/// The delays of those modifiers, added up.
fn delay<Id>(entries: &[Entry<Id>], modifiers: impl IntoIterator<Item = usize>) -> Delay {
    modifiers
        .into_iter()
        .filter_map(|lie| timing(entries, lie))
        .fold(Delay::ZERO, Delay::plus)
}

/// The effect before forwarding of what the request pass decided.
fn before_forwarding<Id: Clone>(entries: &[Entry<Id>], board: &Board) -> StageEffect<Id> {
    let acts_now = |lie: &usize| entries[*lie].is_settled();
    let id = |lie: &usize| acts_now(lie).then(|| entries[*lie].lie.id().clone());

    StageEffect {
        terminal: board.terminal.as_ref().and_then(id),
        refusals: board.refusals.winners.filter_map(id),
        delay: delay(entries, board.modifiers.iter().copied().filter(acts_now)),
        ..StageEffect::nothing(Stage::BeforeForwarding)
    }
}

/// The effect after the response of what the second round decided.
///
/// The throttle also counts the modifiers that applied before forwarding, in the `first` round.
/// A call that ends after the response has no answer to carry a throttle, so its throttle is zero.
fn after_response<Id: Clone>(entries: &[Entry<Id>], first: &Board, second: &Board) -> StageEffect<Id> {
    let id = |lie: &usize| Some(entries[*lie].lie.id().clone());
    let held_back = first.modifiers.iter().copied().filter(|&lie| entries[lie].is_settled());
    let answered = second.terminal.is_none();

    StageEffect {
        terminal: second.terminal.as_ref().and_then(id),
        refusals: second.refusals.winners.filter_map(id),
        rewrites: second
            .rewrites
            .iter()
            .map(|(aspect, holders)| (*aspect, holders.winners.filter_map(id)))
            .filter(|(_, winners)| !winners.is_empty())
            .collect(),
        delay: delay(entries, second.modifiers.iter().copied()),
        throttle: held_back
            .chain(second.modifiers.iter().copied())
            .filter(|_| answered)
            .filter_map(|lie| timing(entries, lie))
            .map(TimingLie::throttle)
            .max()
            .unwrap_or(TimeSpan::ZERO),
        ..StageEffect::nothing(Stage::AfterResponse)
    }
}

/// The fate of every lie, from the highest priority down.
fn fates<Id: Clone>(entries: Vec<Entry<Id>>, drafts: Vec<Draft>, order: &[usize]) -> Vec<Fate<Id>> {
    let losses = |mut losses: Vec<(usize, Target)>| -> Vec<Loss<Id>> {
        losses.sort_by_key(|&(winner, _)| entries[winner].rank(winner));
        losses
            .into_iter()
            .map(|(winner, on)| Loss {
                on,
                to: entries[winner].lie.id().clone(),
            })
            .collect()
    };

    let lost: Vec<_> = drafts
        .into_iter()
        .map(|draft| {
            let applied = if draft.applied_call {
                Some(Target::Call)
            } else {
                Target::of_parts(draft.applied_parts)
            };
            (applied, losses(draft.shadowed), losses(draft.pre_empted))
        })
        .collect();

    let mut fates: Vec<Option<Fate<Id>>> = entries
        .into_iter()
        .zip(lost)
        .map(|(entry, (applied, shadowed, pre_empted))| {
            Some(Fate {
                stage: entry.stage(),
                lie: entry.lie,
                applied,
                shadowed,
                pre_empted,
            })
        })
        .collect();

    order.iter().filter_map(|&lie| fates[lie].take()).collect()
}

/// What holds for every combination, checked on every short list of lies from a catalogue.
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::Winners;

    const BEFORE: Stage = Stage::BeforeForwarding;
    const AFTER: Stage = Stage::AfterResponse;

    fn parts(texts: &[&str]) -> Target {
        Target::of_parts(texts.iter().map(|text| text.parse().unwrap())).unwrap()
    }

    fn timing(delay: u64, throttle: u64, stage: Stage) -> Claim {
        TimingLie::new()
            .with_delay(TimeSpan::from_millis(delay))
            .with_jitter(TimeSpan::from_millis(delay / 10))
            .with_throttle(TimeSpan::from_millis(throttle))
            .at(stage)
            .into()
    }

    /// Lies of every class, at both stages, on parts that overlap and on the whole call.
    fn catalogue() -> Vec<Claim> {
        let refusal = |stage, target| Claim::Refusal { stage, target };
        let rewrite = |aspect, target| Claim::Rewrite { aspect, target };

        vec![
            Claim::Terminal { stage: BEFORE },
            Claim::Terminal { stage: AFTER },
            refusal(BEFORE, parts(&["orders.eu/3"])),
            refusal(AFTER, parts(&["orders.eu/3"])),
            refusal(BEFORE, parts(&["orders.eu/3", "orders.eu/4"])),
            refusal(AFTER, parts(&["orders.eu/4"])),
            refusal(BEFORE, Target::Call),
            refusal(AFTER, Target::Call),
            rewrite(Aspect::HighWatermark, parts(&["orders.eu/3"])),
            rewrite(Aspect::HighWatermark, parts(&["orders.eu/3", "orders.eu/4"])),
            rewrite(Aspect::HighWatermark, Target::Call),
            rewrite(Aspect::Records, parts(&["orders.eu/4"])),
            timing(200, 700, BEFORE),
            timing(1000, 500, AFTER),
        ]
    }

    /// The fewest lies that still meet in every way, to afford longer lists.
    fn short_catalogue() -> Vec<Claim> {
        let both = || parts(&["orders.eu/3", "orders.eu/4"]);

        vec![
            Claim::Terminal { stage: BEFORE },
            Claim::Terminal { stage: AFTER },
            Claim::Refusal {
                stage: BEFORE,
                target: parts(&["orders.eu/3"]),
            },
            Claim::Refusal {
                stage: AFTER,
                target: parts(&["orders.eu/3"]),
            },
            Claim::Refusal {
                stage: AFTER,
                target: both(),
            },
            Claim::Rewrite {
                aspect: Aspect::HighWatermark,
                target: both(),
            },
            timing(200, 700, BEFORE),
            timing(1000, 500, AFTER),
        ]
    }

    /// Every list of exactly `length` claims of the catalogue.
    fn lists(catalogue: &[Claim], length: usize) -> Vec<Vec<Claim>> {
        (0..length).fold(vec![Vec::new()], |lists, _| {
            lists
                .iter()
                .flat_map(|list| {
                    catalogue.iter().map(move |claim| {
                        let mut longer = list.clone();
                        longer.push(claim.clone());
                        longer
                    })
                })
                .collect()
        })
    }

    /// Every list of at most `length` claims of the catalogue.
    fn lists_up_to(catalogue: &[Claim], length: usize) -> Vec<Vec<Claim>> {
        (0..=length).flat_map(|length| lists(catalogue, length)).collect()
    }

    /// The lies of a list: the place of a claim is its identity and its priority.
    fn fired(claims: &[Claim]) -> Vec<FiredLie<usize>> {
        claims
            .iter()
            .enumerate()
            .map(|(place, claim)| FiredLie::new(place, Priority::new(place), claim.clone()))
            .collect()
    }

    /// The lies of a list in two passes: bit `place` of `mask` puts that lie in the response pass.
    fn in_two_passes(claims: &[Claim], mask: usize) -> (Vec<FiredLie<usize>>, Vec<FiredLie<usize>>) {
        fired(claims).into_iter().partition(|lie| mask >> lie.id() & 1 == 0)
    }

    /// The targets of a fate: where the lie applied, and every target it lost.
    fn pieces(fate: &Fate<usize>) -> Vec<&Target> {
        fate.applied()
            .into_iter()
            .chain(fate.shadowed().iter().chain(fate.pre_empted()).map(Loss::on))
            .collect()
    }

    fn assert_the_pieces_are_the_claimed_target(fate: &Fate<usize>) {
        match fate.claim().target() {
            Target::Call => assert_eq!(pieces(fate), [&Target::Call], "{fate:?}"),
            Target::Parts(claimed) => {
                let pieces: Vec<&Part> = pieces(fate).into_iter().flat_map(Target::parts).collect();
                let distinct: BTreeSet<&Part> = pieces.iter().copied().collect();

                assert!(pieces.iter().all(|part| claimed.contains(part)), "{fate:?}");
                assert_eq!(pieces.len(), distinct.len(), "{fate:?}");
                assert_eq!(distinct.len(), claimed.len(), "{fate:?}");
            }
        }
    }

    /// The winners that the fates tell: every lie of that kind that applies at `stage`, with what it applies to.
    fn winners_by_fate(
        call: &Combination<usize>,
        stage: Stage,
        is_that_kind: impl Fn(&Claim) -> bool,
    ) -> Winners<usize> {
        let mut winners = Winners::default();
        for fate in call.fates() {
            if fate.stage() != stage || !is_that_kind(fate.claim()) {
                continue;
            }
            match fate.applied() {
                Some(Target::Call) => {
                    assert_eq!(winners.of_call(), None, "two lies hold the whole call");
                    winners.give_call(*fate.id());
                }
                Some(target) => {
                    for part in target.parts() {
                        assert_eq!(winners.of(part), None, "two lies hold {part}");
                        winners.give(part.clone(), *fate.id());
                    }
                }
                None => {}
            }
        }
        assert!(
            winners.of_call().is_none() || winners.parts().next().is_none(),
            "lies hold the whole call and parts of it"
        );
        winners
    }

    /// The effect at `stage` that the fates tell.
    fn effect_by_fate(call: &Combination<usize>, stage: Stage) -> StageEffect<usize> {
        let applies = |fate: &&Fate<usize>| fate.counts();
        let timings = |stage: Option<Stage>| {
            call.fates()
                .iter()
                .filter(applies)
                .filter(move |fate| stage.is_none_or(|stage| fate.stage() == stage))
                .filter_map(|fate| match fate.claim() {
                    Claim::Modifier(timing) => Some(timing),
                    _ => None,
                })
        };

        let terminals: Vec<usize> = call
            .fates()
            .iter()
            .filter(applies)
            .filter(|fate| fate.stage() == stage && fate.claim().class() == CombinationClass::Terminal)
            .map(|fate| *fate.id())
            .collect();
        assert!(terminals.len() <= 1, "two terminal lies apply");

        let rewrites: BTreeMap<Aspect, Winners<usize>> = Aspect::ALL
            .into_iter()
            .map(|aspect| {
                (
                    aspect,
                    winners_by_fate(call, stage, |claim| claim.aspect() == Some(aspect)),
                )
            })
            .filter(|(_, winners)| !winners.is_empty())
            .collect();

        StageEffect {
            stage,
            terminal: terminals.first().copied(),
            refusals: winners_by_fate(call, stage, |claim| claim.class() == CombinationClass::Refusal),
            rewrites,
            delay: timings(Some(stage)).fold(Delay::ZERO, Delay::plus),
            // The throttle is a field of the answer, and a call that a terminal lie ends has no answer.
            throttle: match stage {
                Stage::AfterResponse if terminals.is_empty() => {
                    timings(None).map(TimingLie::throttle).max().unwrap_or(TimeSpan::ZERO)
                }
                _ => TimeSpan::ZERO,
            },
        }
    }

    fn assert_the_combination_holds_together(call: &Combination<usize>, lies: usize) {
        assert_eq!(call.fates().len(), lies);
        for fate in call.fates() {
            assert_the_pieces_are_the_claimed_target(fate);
        }
        assert_eq!(call.before_forwarding(), &effect_by_fate(call, BEFORE), "{call:?}");
        assert_eq!(call.after_response(), &effect_by_fate(call, AFTER), "{call:?}");
    }

    #[test]
    fn every_fate_splits_the_claimed_target_and_the_effects_tell_what_the_fates_tell() {
        for claims in lists_up_to(&catalogue(), 3) {
            let call = RequestPass::of(fired(&claims)).without_response_pass();
            assert_the_combination_holds_together(&call, claims.len());
        }
    }

    #[test]
    fn in_one_pass_a_lie_only_loses_to_a_lie_that_applies() {
        for claims in lists_up_to(&catalogue(), 3) {
            let call = RequestPass::of(fired(&claims)).without_response_pass();
            let applies = |id: &usize| call.fate_of(id).unwrap().counts();

            for fate in call.fates() {
                for loss in fate.shadowed() {
                    assert!(loss.to() < fate.id(), "shadowed by a lower priority: {call:?}");
                    assert!(applies(loss.to()), "shadowed by a loser: {call:?}");
                }
                for loss in fate.pre_empted() {
                    assert!(applies(loss.to()), "pre-empted by a loser: {call:?}");
                }
            }
        }
    }

    #[test]
    fn in_one_pass_a_terminal_lie_that_applies_leaves_only_what_acts_before_it() {
        for claims in lists_up_to(&catalogue(), 3) {
            let call = RequestPass::of(fired(&claims)).without_response_pass();
            let terminal = call
                .fates()
                .iter()
                .find(|fate| fate.counts() && fate.claim().class() == CombinationClass::Terminal);

            if let Some(terminal) = terminal {
                for fate in call.fates().iter().filter(|fate| fate.id() != terminal.id()) {
                    let held_the_request_back =
                        fate.claim().class() == CombinationClass::Modifier && fate.stage() < terminal.stage();
                    assert_eq!(fate.counts(), held_the_request_back, "{call:?}");
                }
            }
        }
    }

    #[test]
    fn the_response_pass_without_lies_decides_what_the_request_pass_decided() {
        let with_the_whole_call = lists_up_to(&catalogue(), 3);
        let longer = lists(&short_catalogue(), 4);

        for claims in with_the_whole_call.into_iter().chain(longer) {
            let request = RequestPass::of(fired(&claims));
            let order = in_priority_order(&request.entries);
            let decided = fates(request.entries.clone(), request.drafts.clone(), &order);

            assert_eq!(request.without_response_pass().fates(), decided);
        }
    }

    #[test]
    fn two_passes_hold_together_and_the_response_pass_changes_nothing_before_forwarding() {
        for claims in lists(&short_catalogue(), 4) {
            for mask in 0..16 {
                let (request, response) = in_two_passes(&claims, mask);
                let request = RequestPass::of(request);
                let alone = request.clone().without_response_pass();
                let call = request.response_pass(response);

                assert_the_combination_holds_together(&call, claims.len());
                assert_eq!(call.before_forwarding(), alone.before_forwarding());

                for fate in alone.fates().iter().filter(|fate| fate.stage() == BEFORE) {
                    assert_eq!(call.fate_of(fate.id()), Some(fate), "{call:?}");
                }
            }
        }
    }

    #[test]
    fn two_passes_with_the_whole_call_as_a_target_hold_together() {
        for claims in lists(&catalogue(), 3) {
            for mask in 0..8 {
                let (request, response) = in_two_passes(&claims, mask);
                let call = RequestPass::of(request).response_pass(response);

                assert_the_combination_holds_together(&call, claims.len());
            }
        }
    }

    #[test]
    fn a_lie_is_only_pre_empted_on_what_the_other_lie_holds() {
        for claims in lists(&catalogue(), 3) {
            for mask in 0..8 {
                let (request, response) = in_two_passes(&claims, mask);
                let call = RequestPass::of(request).response_pass(response);

                for fate in call.fates() {
                    for loss in fate.pre_empted() {
                        let held = call.fate_of(loss.to()).unwrap().applied();
                        let holds_it = match (held, loss.on()) {
                            (Some(Target::Call), _) => true,
                            (Some(Target::Parts(held)), Target::Parts(lost)) => lost.is_subset(held),
                            (Some(Target::Parts(_)), Target::Call) | (None, _) => false,
                        };
                        assert!(holds_it, "pre-empted on more than the other lie holds: {call:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn a_call_that_ended_before_forwarding_does_nothing_after_the_response() {
        for claims in lists(&short_catalogue(), 3) {
            for mask in 0..8 {
                let (request, response) = in_two_passes(&claims, mask);
                let request = RequestPass::of(request);
                let Some(&terminal) = request.before_forwarding().terminal() else {
                    continue;
                };
                let call = request.response_pass(response.clone());

                assert!(call.after_response().changes_nothing(), "{call:?}");
                for lie in response {
                    let fate = call.fate_of(lie.id()).unwrap();
                    assert!(!fate.counts(), "{call:?}");
                    assert_eq!(fate.pre_empted().len(), 1, "{call:?}");
                    assert_eq!(fate.pre_empted()[0].to(), &terminal, "{call:?}");
                }
            }
        }
    }
}
