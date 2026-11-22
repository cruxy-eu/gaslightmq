//! The numbers of the connections that listeners accept.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use gaslightmq_field::ConnectionNumber;

/// The numbers of the connections that the listeners of one instance accept.
///
/// A clone is a handle to the same numbers.
/// The listeners of one proxy share one, so a number names one connection of the proxy,
/// whichever endpoint the client connected to.
/// The first connection is `#1`, and a connection that closes does not give its number back.
///
/// The handle never waits: the count is an atomic number.
///
/// ```
/// use gaslightmq_listener::prelude::*;
///
/// let numbers = ConnectionNumbers::new();
/// let shared = numbers.clone();
///
/// assert_eq!(numbers.assign().to_string(), "#1");
/// assert_eq!(shared.assign().to_string(), "#2");
/// assert_eq!(numbers.assigned(), 2);
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConnectionNumbers {
    /// How many numbers were handed out, which is the number of the last connection.
    assigned: Arc<AtomicU64>,
}

impl ConnectionNumbers {
    /// The numbers of an instance that accepted no connection yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts one more connection, and returns its number.
    pub fn assign(&self) -> ConnectionNumber {
        let before = self.assigned.fetch_add(1, Ordering::Relaxed);
        ConnectionNumber::new(before.wrapping_add(1))
    }

    /// How many connections got a number.
    pub fn assigned(&self) -> u64 {
        self.assigned.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn the_first_connection_is_number_one() {
        let numbers = ConnectionNumbers::new();

        assert_eq!(numbers.assigned(), 0);
        assert_eq!(numbers.assign(), ConnectionNumber::new(1));
        assert_eq!(numbers.assign(), ConnectionNumber::new(2));
        assert_eq!(numbers.assigned(), 2);
    }

    #[test]
    fn every_clone_hands_out_the_same_numbers() {
        let numbers = ConnectionNumbers::new();
        let handles: Vec<ConnectionNumbers> = (0..4).map(|_| numbers.clone()).collect();

        let threads: Vec<_> = handles
            .into_iter()
            .map(|handle| std::thread::spawn(move || (0..250).map(|_| handle.assign()).collect::<Vec<_>>()))
            .collect();
        let assigned: BTreeSet<ConnectionNumber> =
            threads.into_iter().flat_map(|thread| thread.join().unwrap()).collect();

        assert_eq!(assigned.len(), 1000);
        assert_eq!(assigned.first(), Some(&ConnectionNumber::new(1)));
        assert_eq!(assigned.last(), Some(&ConnectionNumber::new(1000)));
        assert_eq!(numbers.assigned(), 1000);
    }
}
