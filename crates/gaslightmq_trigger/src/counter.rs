//! Counters: the shared one of a rule, and the ones a session keeps for itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crate::{CounterKey, MatchNumber};

/// The counter of one armed rule, which every session shares.
///
/// A counter is a handle to one atomic number.
/// Cloning it gives a second handle to the same number, so the engine keeps one and every session counts on it.
/// When several sessions count at once, the order in which they arrive decides which call gets which match number,
/// and no two calls get the same one.
///
/// A counter starts at zero and never goes back.
/// Arming a rule again gives it a new counter.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let engine = Counter::new();
/// let session = engine.clone();
/// assert_eq!(engine.count(), MatchNumber::FIRST);
/// assert_eq!(session.count().value(), 2);
/// assert_eq!(engine.matches(), 2);
/// ```
#[derive(Clone, Debug, Default)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    /// A counter that has counted no match yet.
    pub fn new() -> Counter {
        Counter::default()
    }

    /// Counts one match, and returns its number.
    pub fn count(&self) -> MatchNumber {
        // The number stands alone, so no other memory has to be in step with it.
        MatchNumber::following(self.0.fetch_add(1, Ordering::Relaxed))
    }

    /// How many matches the counter has counted.
    pub fn matches(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Whether the two handles count on the same number.
    pub fn is(&self, other: &Counter) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// What one session keeps for the rules that count `per: session`: its counter key and its own match counts.
///
/// A session owns one of these from the moment it opens, and hands it to [`Arming::evaluate`](crate::Arming::evaluate).
/// It dies with the session, so a client that reconnects starts at zero.
///
/// The counts are per armed rule.
/// The session's count for a rule belongs to the rule's shared [`Counter`]:
/// a rule that is armed again has a new counter, and so the session counts it from one again.
/// Counts of rules that are no longer armed anywhere go away by themselves.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let rule = Counter::new();
/// let mut first = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #1")?);
/// let mut second = SessionCounters::new(CounterKey::new("10.0.4.27 broker 2 #2")?);
///
/// assert_eq!(first.count(&rule).value(), 1);
/// assert_eq!(first.count(&rule).value(), 2);
/// assert_eq!(second.count(&rule).value(), 1);
///
/// // Arming the rule again gives it a new counter.
/// let armed_again = Counter::new();
/// assert_eq!(first.count(&armed_again).value(), 1);
/// # Ok::<(), TriggerError>(())
/// ```
#[derive(Debug)]
pub struct SessionCounters {
    key: CounterKey,
    /// The session's count for each shared counter.
    /// A weak handle keeps the counter's address taken, so no other counter is ever mistaken for it.
    counts: Vec<(Weak<AtomicU64>, u64)>,
}

impl SessionCounters {
    /// The counters of a session with that counter key, all at zero.
    pub fn new(key: CounterKey) -> SessionCounters {
        SessionCounters {
            key,
            counts: Vec::new(),
        }
    }

    /// The counter key of the session, which its dice use.
    pub fn key(&self) -> &CounterKey {
        &self.key
    }

    /// Counts one match of the rule with that shared counter on this session, and returns its number.
    pub fn count(&mut self, rule: &Counter) -> MatchNumber {
        let index = self.position(rule).unwrap_or_else(|| {
            // A session only meets a new armed rule now and then, which is the moment to forget the ones that are gone.
            self.counts.retain(|(counter, _)| counter.strong_count() > 0);
            self.counts.push((Arc::downgrade(&rule.0), 0));
            self.counts.len() - 1
        });
        let matches = &mut self.counts[index].1;
        let number = MatchNumber::following(*matches);
        *matches = number.value();
        number
    }

    /// How many matches of the rule with that shared counter this session has counted.
    pub fn matches(&self, rule: &Counter) -> u64 {
        self.position(rule).map_or(0, |index| self.counts[index].1)
    }

    /// How many armed rules this session keeps a count for.
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Whether this session has counted no match of any rule.
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    fn position(&self, rule: &Counter) -> Option<usize> {
        let address = Arc::as_ptr(&rule.0);
        self.counts.iter().position(|(counter, _)| counter.as_ptr() == address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(key: &str) -> SessionCounters {
        SessionCounters::new(CounterKey::new(key).unwrap())
    }

    #[test]
    fn a_counter_numbers_its_matches_from_one() {
        let counter = Counter::new();
        assert_eq!(counter.matches(), 0);
        assert_eq!(counter.count(), MatchNumber::FIRST);
        assert_eq!(counter.count().value(), 2);
        assert_eq!(counter.count().value(), 3);
        assert_eq!(counter.matches(), 3);
    }

    #[test]
    fn clones_share_one_number() {
        let engine = Counter::new();
        let session = engine.clone();
        assert!(engine.is(&session));
        assert_eq!(session.count().value(), 1);
        assert_eq!(engine.count().value(), 2);
        assert_eq!(session.matches(), 2);
    }

    #[test]
    fn two_counters_count_apart() {
        let one = Counter::new();
        let other = Counter::new();
        assert!(!one.is(&other));
        one.count();
        assert_eq!(other.matches(), 0);
    }

    #[test]
    fn a_session_counts_each_rule_apart() {
        let (delay, refuse) = (Counter::new(), Counter::new());
        let mut session = session("a");
        assert!(session.is_empty());
        assert_eq!(session.count(&delay).value(), 1);
        assert_eq!(session.count(&delay).value(), 2);
        assert_eq!(session.count(&refuse).value(), 1);
        assert_eq!(session.matches(&delay), 2);
        assert_eq!(session.matches(&refuse), 1);
        assert_eq!(session.len(), 2);
    }

    #[test]
    fn a_session_count_leaves_the_shared_counter_alone() {
        let rule = Counter::new();
        let mut session = session("a");
        session.count(&rule);
        assert_eq!(rule.matches(), 0);
    }

    #[test]
    fn a_session_finds_its_count_through_any_handle_of_the_counter() {
        let rule = Counter::new();
        let mut session = session("a");
        assert_eq!(session.count(&rule).value(), 1);
        assert_eq!(session.count(&rule.clone()).value(), 2);
    }

    #[test]
    fn a_session_forgets_the_rules_that_are_gone() {
        let mut session = session("a");
        let kept = Counter::new();
        session.count(&kept);
        for _ in 0..100 {
            let armed_and_disarmed = Counter::new();
            assert_eq!(session.count(&armed_and_disarmed).value(), 1);
        }
        // Only the counter that still exists and the last one that was counted remain.
        assert!(session.len() <= 2, "{} counts remain", session.len());
        assert_eq!(session.count(&kept).value(), 2);
    }

    #[test]
    fn a_session_keeps_its_key() {
        assert_eq!(session("10.0.4.27 broker 2 #1").key().as_str(), "10.0.4.27 broker 2 #1");
    }
}
