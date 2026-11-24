//! The call IDs that the sessions of one environment hand out.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use gaslightmq_call::CallId;

/// The call IDs of one environment: one count of its calls, across all of its connections.
///
/// A clone is a handle to the same count.
/// The engine makes one per environment, and every session holds a clone of it.
/// A session [assigns](Self::assign) an ID to each request that arrives,
/// so the first call of the environment is [`CallId::FIRST`], and a higher ID is a later call.
/// A call that ends does not give its ID back.
///
/// The handle never waits: the count is an atomic number.
/// A run that takes the same requests in the same order gives every call the same ID again.
///
/// ```
/// use gaslightmq_call::CallId;
/// use gaslightmq_session::prelude::*;
///
/// let call_ids = CallIds::new();
/// let of_another_session = call_ids.clone();
///
/// let first = call_ids.assign();
/// let second = of_another_session.assign();
///
/// assert_eq!(first, CallId::FIRST);
/// assert_eq!(second, first.next());
/// assert_eq!(call_ids.assigned(), 2);
/// ```
#[derive(Debug, Clone, Default)]
pub struct CallIds {
    /// How many IDs were handed out, which is the ID of the last call.
    assigned: Arc<AtomicU64>,
}

impl CallIds {
    /// The call IDs of an environment that took no request yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts one more call, and returns its ID.
    pub fn assign(&self) -> CallId {
        let before = self.assigned.fetch_add(1, Ordering::Relaxed);
        NonZeroU64::new(before.wrapping_add(1)).map_or(CallId::FIRST, CallId::from)
    }

    /// How many calls got an ID.
    pub fn assigned(&self) -> u64 {
        self.assigned.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn the_first_call_of_an_environment_is_the_first_id() {
        let call_ids = CallIds::new();

        assert_eq!(call_ids.assigned(), 0);
        let first = call_ids.assign();
        let second = call_ids.assign();

        assert_eq!(first, CallId::FIRST);
        assert_eq!(second, first.next());
        assert_eq!(call_ids.assigned(), 2);
    }

    #[test]
    fn every_clone_hands_out_ids_of_the_same_count() {
        let call_ids = CallIds::new();
        let handles: Vec<CallIds> = (0..4).map(|_| call_ids.clone()).collect();

        let threads: Vec<_> = handles
            .into_iter()
            .map(|handle| std::thread::spawn(move || (0..250).map(|_| handle.assign()).collect::<Vec<_>>()))
            .collect();
        let per_thread: Vec<Vec<CallId>> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();

        // Whoever holds a handle gets its IDs in rising order, and no two handles get the same one.
        for assigned in &per_thread {
            assert!(assigned.is_sorted());
        }
        let assigned: BTreeSet<CallId> = per_thread.into_iter().flatten().collect();
        assert_eq!(assigned.len(), 1000);
        assert_eq!(assigned.first(), Some(&CallId::FIRST));
        assert_eq!(assigned.last().map(|last| last.value()), Some(1000));
        assert_eq!(call_ids.assigned(), 1000);
    }
}
