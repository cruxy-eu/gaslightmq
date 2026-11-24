//! The timers that the pipeline of one session asked for, and that did not fire yet.

use std::collections::BTreeSet;

use gaslightmq_clock::Instant;
use gaslightmq_pipeline::TimerId;

/// The timers of one session: every wait that its pipeline asked for, in the order in which the waits end.
///
/// The session task waits for the [next](Self::next) instant on the clock of its engine,
/// and reports every timer that is [due](Self::due) to the pipeline.
/// A timer is never taken back: the pipeline ignores one that fires for a call that waits no more.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    /// Two timers that end at one instant fire in the order of their calls.
    waiting: BTreeSet<(Instant, TimerId)>,
}

impl Timers {
    /// The pipeline asked for the timer `id`, which fires at `at`.
    pub fn set(&mut self, id: TimerId, at: Instant) {
        self.waiting.insert((at, id));
    }

    /// The instant at which the next timer fires, or `None` while the session waits for none.
    pub fn next(&self) -> Option<Instant> {
        self.waiting.first().map(|(at, _)| *at)
    }

    /// Takes every timer that fires at `now` or fired before, in the order in which they fire.
    pub fn due(&mut self, now: Instant) -> Vec<TimerId> {
        let mut due = Vec::new();
        while let Some((at, id)) = self.waiting.first().copied()
            && at <= now
        {
            self.waiting.remove(&(at, id));
            due.push(id);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_call::CallId;
    use gaslightmq_clock::Duration;
    use gaslightmq_lie::Stage;

    use super::*;

    /// The timer of the answer that the call waits with.
    fn timer(call: CallId) -> TimerId {
        TimerId::new(call, Stage::AfterResponse)
    }

    /// The first four calls of a proxy.
    fn four_calls() -> [CallId; 4] {
        let first = CallId::FIRST;
        let second = first.next();
        let third = second.next();
        [first, second, third, third.next()]
    }

    fn at(millis: u64) -> Instant {
        Instant::ZERO + Duration::from_millis(millis)
    }

    #[test]
    fn a_session_without_timers_waits_for_none() {
        let mut timers = Timers::default();

        assert_eq!(timers.next(), None);
        assert_eq!(timers.due(at(1_000)), []);
    }

    #[test]
    fn the_next_timer_is_the_one_that_ends_first() {
        let mut timers = Timers::default();
        let [first, second, ..] = four_calls();
        timers.set(timer(first), at(500));
        timers.set(TimerId::new(second, Stage::BeforeForwarding), at(200));

        assert_eq!(timers.next(), Some(at(200)));
    }

    #[test]
    fn the_timers_that_are_due_come_in_the_order_in_which_they_fire() {
        let mut timers = Timers::default();
        let [first, second, third, fourth] = four_calls();
        timers.set(timer(third), at(300));
        timers.set(timer(second), at(200));
        timers.set(timer(first), at(200));
        timers.set(timer(fourth), at(301));

        assert_eq!(timers.due(at(199)), []);
        assert_eq!(timers.due(at(300)), [timer(first), timer(second), timer(third)]);
        assert_eq!(timers.next(), Some(at(301)));
        assert_eq!(timers.due(at(301)), [timer(fourth)]);
        assert_eq!(timers.next(), None);
    }
}
