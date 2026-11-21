//! The rules of combination, as one round over the lies of a call.
//!
//! A round takes the lies from the highest priority down, and each lie takes what is still free.
//! A lie that lost a target holds nothing there, so it shadows no lie below it.
//!
//! The lies in this module are numbers: the place of a lie among the lies of the call.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_field::prelude::*;
use gaslightmq_lie::prelude::*;

use crate::{Claim, FiredLie, Winners};

/// The evaluation pass in which a lie fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pass {
    Request,
    Response,
}

/// A lie that fired, with the pass it fired in.
#[derive(Debug, Clone)]
pub(crate) struct Entry<Id> {
    pub(crate) lie: FiredLie<Id>,
    pub(crate) pass: Pass,
}

impl<Id> Entry<Id> {
    /// The stage the lie acts at.
    /// The truth exists by the time a lie of the response pass fires, so that lie acts after the response.
    pub(crate) fn stage(&self) -> Stage {
        match self.pass {
            Pass::Request => self.lie.claim().stage(),
            Pass::Response => Stage::AfterResponse,
        }
    }

    /// Whether the request pass decides the lie for good.
    /// A lie before forwarding has acted, or lost its chance, by the time the response pass runs.
    pub(crate) fn is_settled(&self) -> bool {
        self.pass == Pass::Request && self.stage() == Stage::BeforeForwarding
    }

    /// What orders the lies of a call: the priority, and then the order in which the lies came in.
    pub(crate) fn rank(&self, place: usize) -> (usize, usize) {
        (self.lie.priority().position(), place)
    }
}

/// The places of the lies, from the highest priority down.
pub(crate) fn in_priority_order<Id>(entries: &[Entry<Id>]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..entries.len()).collect();
    order.sort_by_key(|&place| entries[place].rank(place));
    order
}

/// Why a lie lost a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum How {
    /// A lie with a higher priority won the conflict.
    Shadowed,
    /// The lie had nothing left to change.
    PreEmpted,
}

/// The fate of one lie while a round decides it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Draft {
    pub(crate) applied_call: bool,
    pub(crate) applied_parts: BTreeSet<Part>,
    /// Each target the lie lost a conflict on, with the place of the winner.
    pub(crate) shadowed: Vec<(usize, Target)>,
    /// Each target on which the lie had nothing left to change, with the place of the lie that got there first.
    pub(crate) pre_empted: Vec<(usize, Target)>,
}

impl Draft {
    /// The draft of a lie that lost all of `target` to the lie at `to`.
    pub(crate) fn lost(how: How, to: usize, target: Target) -> Self {
        let mut draft = Self::default();
        draft.lose(how, to, target);
        draft
    }

    fn losses(&mut self, how: How) -> &mut Vec<(usize, Target)> {
        match how {
            How::Shadowed => &mut self.shadowed,
            How::PreEmpted => &mut self.pre_empted,
        }
    }

    /// The lie loses all of `target` to the lie at `to`.
    fn lose(&mut self, how: How, to: usize, target: Target) {
        self.losses(how).push((to, target));
    }

    /// The lie loses `part` to the lie at `to`.
    /// The parts a lie loses to one winner make one target.
    fn lose_part(&mut self, how: How, to: usize, part: &Part) {
        let losses = self.losses(how);
        for (winner, target) in losses.iter_mut() {
            if *winner == to
                && let Target::Parts(parts) = target
            {
                parts.insert(part.clone());
                return;
            }
        }
        losses.push((to, Target::Parts(PartSet::one(part.clone()))));
    }
}

/// Who holds each target among lies that conflict there, and who took a target first.
#[derive(Debug, Clone, Default)]
pub(crate) struct Holders {
    pub(crate) winners: Winners<usize>,
    /// The holder with the highest priority.
    /// A round takes the lies from the highest priority down, so it is the first lie that took anything.
    first: Option<usize>,
}

impl Holders {
    fn of(&self, part: &Part) -> Option<usize> {
        self.winners.of(part).copied()
    }

    fn of_call(&self) -> Option<usize> {
        self.winners.of_call().copied()
    }

    fn take_call(&mut self, lie: usize) {
        self.winners.give_call(lie);
        self.first.get_or_insert(lie);
    }

    fn take(&mut self, part: &Part, lie: usize) {
        self.winners.give(part.clone(), lie);
        self.first.get_or_insert(lie);
    }

    /// The holders among these that the request pass decided for good.
    pub(crate) fn settled<Id>(&self, entries: &[Entry<Id>]) -> Self {
        let winners = self.winners.filter_map(|&lie| entries[lie].is_settled().then_some(lie));
        let first = winners
            .of_call()
            .into_iter()
            .chain(winners.parts().map(|(_, lie)| lie))
            .copied()
            .min_by_key(|&lie| entries[lie].rank(lie));
        Self { winners, first }
    }
}

/// What a round decided: which lie ends the call, which lies hold which targets, and which modifiers apply.
#[derive(Debug, Clone, Default)]
pub(crate) struct Board {
    pub(crate) terminal: Option<usize>,
    pub(crate) refusals: Holders,
    pub(crate) rewrites: BTreeMap<Aspect, Holders>,
    /// The modifiers that apply, from the highest priority down.
    pub(crate) modifiers: Vec<usize>,
}

/// One round of combining.
///
/// The request pass is one round over its lies.
/// The response pass is a second round, over the lies that act after the response:
/// those the request pass carried over, and those of the response pass.
/// What took effect before forwarding stays, and the second round finds it `seated`.
pub(crate) struct Round<'a, Id> {
    entries: &'a [Entry<Id>],
    /// The refusals that took effect before forwarding, in an earlier round.
    seated: &'a Holders,
    board: Board,
    drafts: Vec<Draft>,
}

impl<'a, Id> Round<'a, Id> {
    /// A round over `entries`, in which `drafts` holds the fate of every lie that an earlier round decided for good.
    pub(crate) fn new(entries: &'a [Entry<Id>], seated: &'a Holders, drafts: Vec<Draft>) -> Self {
        Self {
            entries,
            seated,
            board: Board::default(),
            drafts,
        }
    }

    /// Decides the lies at the places of `todo`, which come from the highest priority down.
    ///
    /// Terminal lies and refusals go first, because they decide what is left for the others.
    /// A rewrite of a refused part is pre-empted, even when a rewrite above it would have shadowed it.
    pub(crate) fn run(mut self, todo: &[usize]) -> (Board, Vec<Draft>) {
        let entries = self.entries;

        for &lie in todo {
            match entries[lie].lie.claim() {
                Claim::Terminal { .. } => self.terminal(lie),
                Claim::Refusal { target, .. } => self.refusal(lie, target),
                Claim::Rewrite { .. } | Claim::Modifier(_) => {}
            }
        }
        for &lie in todo {
            if let Claim::Rewrite { aspect, target } = entries[lie].lie.claim() {
                self.rewrite(lie, *aspect, target);
            }
        }
        for &lie in todo {
            if let Claim::Modifier(_) = entries[lie].lie.claim() {
                self.modifier(lie);
            }
        }

        (self.board, self.drafts)
    }

    /// Whether the lie fired in the pass of the seated refusals.
    /// Priority holds within one pass, so such a lie conflicts with them.
    /// A lie of the response pass fired after they took effect, and the stage decides between the two.
    fn fired_with_the_seated(&self, lie: usize) -> bool {
        self.entries[lie].pass == Pass::Request
    }

    /// The one of the two lies with the higher priority.
    fn higher(&self, one: Option<usize>, other: Option<usize>) -> Option<usize> {
        [one, other]
            .into_iter()
            .flatten()
            .min_by_key(|&lie| self.entries[lie].rank(lie))
    }

    /// A terminal lie ends the whole call.
    /// It conflicts with every terminal lie and with every refusal on the call.
    fn terminal(&mut self, lie: usize) {
        let seated = if self.fired_with_the_seated(lie) {
            self.seated.first
        } else {
            // The refusal already happened, and the hang-up closes the connection after it: both apply.
            None
        };
        let rival = self
            .board
            .terminal
            .or_else(|| self.higher(self.board.refusals.first, seated));

        if let Some(winner) = rival {
            self.drafts[lie].lose(How::Shadowed, winner, Target::Call);
        } else {
            self.board.terminal = Some(lie);
            self.drafts[lie].applied_call = true;
        }
    }

    /// A refusal replaces everything about its target.
    /// It conflicts with a terminal lie, and with a refusal of the same target.
    fn refusal(&mut self, lie: usize, target: &Target) {
        if let Some(terminal) = self.board.terminal {
            self.drafts[lie].lose(How::Shadowed, terminal, target.clone());
            return;
        }

        let with_the_seated = self.fired_with_the_seated(lie);
        // A part that was refused before forwarding has no truth, so a lie of the response pass finds nothing there.
        let to_the_seated = if with_the_seated { How::Shadowed } else { How::PreEmpted };

        match target {
            Target::Call => {
                let held = self.board.refusals.first;
                let (seated, rival) = if with_the_seated {
                    // The whole call is one target, so any refusal on the call stands in its way.
                    let seated = self.seated.first;
                    (seated, self.higher(seated, held))
                } else {
                    // A seated refusal of a part already happened, and the rest of the call has a truth to replace.
                    // Only a seated refusal of the whole call leaves a lie of the response pass nothing.
                    let seated = self.seated.of_call();
                    (seated, seated.or(held))
                };

                match rival {
                    Some(winner) if rival == seated => self.drafts[lie].lose(to_the_seated, winner, Target::Call),
                    Some(winner) => self.drafts[lie].lose(How::Shadowed, winner, Target::Call),
                    None => {
                        self.board.refusals.take_call(lie);
                        self.drafts[lie].applied_call = true;
                    }
                }
            }
            Target::Parts(parts) => {
                for part in parts {
                    if let Some(winner) = self.seated.of(part) {
                        self.drafts[lie].lose_part(to_the_seated, winner, part);
                    } else if let Some(winner) = self.board.refusals.of(part) {
                        self.drafts[lie].lose_part(How::Shadowed, winner, part);
                    } else {
                        self.board.refusals.take(part, lie);
                        self.drafts[lie].applied_parts.insert(part.clone());
                    }
                }
            }
        }
    }

    /// A rewrite changes one aspect of its target.
    /// A terminal lie and a refusal of the target pre-empt it, and it conflicts with a rewrite of the same aspect.
    fn rewrite(&mut self, lie: usize, aspect: Aspect, target: &Target) {
        if let Some(terminal) = self.board.terminal {
            self.drafts[lie].lose(How::PreEmpted, terminal, target.clone());
            return;
        }

        let draft = &mut self.drafts[lie];
        let refusals = &self.board.refusals;
        let holders = self.board.rewrites.entry(aspect).or_default();

        match target {
            // A rewrite of the whole call has something left to change until the whole call is refused.
            Target::Call => {
                if let Some(refusal) = self.seated.of_call().or_else(|| refusals.of_call()) {
                    draft.lose(How::PreEmpted, refusal, Target::Call);
                } else if let Some(winner) = holders.first {
                    draft.lose(How::Shadowed, winner, Target::Call);
                } else {
                    holders.take_call(lie);
                    draft.applied_call = true;
                }
            }
            Target::Parts(parts) => {
                for part in parts {
                    if let Some(refusal) = self.seated.of(part).or_else(|| refusals.of(part)) {
                        draft.lose_part(How::PreEmpted, refusal, part);
                    } else if let Some(winner) = holders.of(part) {
                        draft.lose_part(How::Shadowed, winner, part);
                    } else {
                        holders.take(part, lie);
                        draft.applied_parts.insert(part.clone());
                    }
                }
            }
        }
    }

    /// Modifiers stack, so a modifier conflicts with no lie.
    /// A terminal lie pre-empts it, unless the modifier acts at an earlier stage than the call ends.
    fn modifier(&mut self, lie: usize) {
        let stage = self.entries[lie].stage();

        match self.board.terminal {
            Some(terminal) if self.entries[terminal].stage() <= stage => {
                self.drafts[lie].lose(How::PreEmpted, terminal, Target::Call);
            }
            _ => {
                self.board.modifiers.push(lie);
                self.drafts[lie].applied_call = true;
            }
        }
    }
}
