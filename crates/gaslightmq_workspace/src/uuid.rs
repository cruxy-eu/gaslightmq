//! Where the UUIDs of a workspace come from.

/// What a new UUID is made of: the time, and random bits.
///
/// A workspace gives a definition its UUID, and it reads neither a clock nor the randomness of the system.
/// Whoever does reads both for each UUID, and hands them over as a seed.
/// The same seed makes the same UUID, so a test writes its seeds down.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// // The milliseconds since the Unix epoch, and ten random bytes.
/// let seed = UuidSeed::new(1_733_938_011_966, [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10]);
/// assert_eq!(seed.unix_millis(), 1_733_938_011_966);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UuidSeed {
    unix_millis: u64,
    random: [u8; 10],
}

impl UuidSeed {
    /// The seed of a UUID that is made at `unix_millis`, with those random bits.
    ///
    /// `unix_millis` is the time in milliseconds since the Unix epoch.
    /// The random bits tell the UUIDs of one millisecond apart, so they come from a source that does not repeat
    /// itself.
    pub const fn new(unix_millis: u64, random: [u8; 10]) -> Self {
        Self { unix_millis, random }
    }

    /// The time, in milliseconds since the Unix epoch.
    pub const fn unix_millis(&self) -> u64 {
        self.unix_millis
    }

    /// The random bits.
    pub const fn random(&self) -> [u8; 10] {
        self.random
    }
}

/// Where a workspace takes the seeds of new UUIDs from.
///
/// A [`Workspace`](crate::Workspace) asks for one seed for each UUID it makes:
/// when someone creates a definition, when it finds a definition in a store that it did not hold yet,
/// and when someone makes a live rule.
///
/// The IO edge implements this with the clock and the randomness of the system.
/// A closure that returns seeds is a source too, which is how a test hands in the ones it wrote down:
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// // Every UUID of this test is made one millisecond after the one before, with the same bits.
/// let mut unix_millis = 1_733_938_011_966;
/// let mut uuids = || {
///     unix_millis += 1;
///     UuidSeed::new(unix_millis, [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10])
/// };
///
/// assert_eq!(uuids.seed().unix_millis(), 1_733_938_011_967);
/// assert_eq!(uuids.seed().unix_millis(), 1_733_938_011_968);
/// ```
pub trait UuidSource {
    /// The seed of the next UUID: the time now, and random bits that no seed before had.
    fn seed(&mut self) -> UuidSeed;
}

impl<F: FnMut() -> UuidSeed> UuidSource for F {
    fn seed(&mut self) -> UuidSeed {
        self()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The time of a seed, in milliseconds since the Unix epoch, and its random bits.
    const UNIX_MILLIS: u64 = 0x0193_b6c2_5f3e;
    const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

    #[test]
    fn a_seed_keeps_its_time_and_its_random_bits() {
        let seed = UuidSeed::new(UNIX_MILLIS, RANDOM);

        assert_eq!(seed.unix_millis(), UNIX_MILLIS);
        assert_eq!(seed.random(), RANDOM);
    }

    #[test]
    fn a_closure_that_returns_seeds_is_a_source() {
        let mut unix_millis = UNIX_MILLIS;
        let mut uuids = || {
            unix_millis += 1;
            UuidSeed::new(unix_millis, RANDOM)
        };

        assert_eq!(uuids.seed(), UuidSeed::new(UNIX_MILLIS + 1, RANDOM));
        assert_eq!(uuids.seed(), UuidSeed::new(UNIX_MILLIS + 2, RANDOM));
    }
}
