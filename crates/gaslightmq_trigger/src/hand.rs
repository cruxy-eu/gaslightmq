//! Hand firings: the firings someone asked for, which wait for a matching call.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The hand firings of one arming that wait for a matching call.
///
/// It is a handle to one atomic number, like a [`Budget`](crate::Budget).
/// Cloning it gives a second handle to the same hand firings.
#[derive(Clone, Debug, Default)]
pub(crate) struct HandFirings(Arc<AtomicU64>);

impl HandFirings {
    /// Adds one hand firing, which the next matching call takes.
    pub(crate) fn add(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// How many hand firings wait for a matching call.
    pub(crate) fn waiting(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Takes one hand firing for a matching call, when one waits.
    pub(crate) fn take(&self) -> Option<TakenHandFiring> {
        self.0
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |waiting| waiting.checked_sub(1))
            .ok()?;
        Some(TakenHandFiring {
            from: self.clone(),
            applied: false,
        })
    }
}

/// One hand firing that a matching call took.
///
/// Every hand firing lies on a matching call, so it is only used up when the action applied.
/// Dropping it puts it back with the hand firings that wait, and the next matching call takes it.
#[derive(Debug)]
#[must_use = "a hand firing that is dropped waits for the next matching call"]
pub(crate) struct TakenHandFiring {
    from: HandFirings,
    applied: bool,
}

impl TakenHandFiring {
    /// Uses the hand firing up: the action applied.
    pub(crate) fn confirm(mut self) {
        self.applied = true;
    }
}

impl Drop for TakenHandFiring {
    fn drop(&mut self) {
        if !self.applied {
            self.from.add();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_takes_one_hand_firing_at_a_time() {
        let hand_firings = HandFirings::default();
        assert!(hand_firings.take().is_none());

        hand_firings.add();
        hand_firings.add();
        assert_eq!(hand_firings.waiting(), 2);

        let first = hand_firings.take().unwrap();
        let second = hand_firings.take().unwrap();
        assert_eq!(hand_firings.waiting(), 0);
        assert!(hand_firings.take().is_none());
        first.confirm();
        second.confirm();
    }

    #[test]
    fn a_confirmed_hand_firing_is_used_up() {
        let hand_firings = HandFirings::default();
        hand_firings.add();

        hand_firings.take().unwrap().confirm();

        assert_eq!(hand_firings.waiting(), 0);
    }

    #[test]
    fn a_dropped_hand_firing_waits_again() {
        let hand_firings = HandFirings::default();
        hand_firings.add();

        drop(hand_firings.take().unwrap());

        assert_eq!(hand_firings.waiting(), 1);
    }

    #[test]
    fn clones_share_the_hand_firings() {
        let engine = HandFirings::default();
        let session = engine.clone();
        engine.add();

        drop(session.take().unwrap());

        assert_eq!(engine.waiting(), 1);
        session.take().unwrap().confirm();
        assert_eq!(engine.waiting(), 0);
    }
}
