//! A source of UUIDs that are known before they are made.

use gaslightmq_rule::RuleUuid;
use gaslightmq_workspace::{UuidSeed, UuidSource};

/// A source whose UUIDs are known before a workspace makes them.
///
/// The first seed has the time [`CREATED`](Self::CREATED), and each one after it is one millisecond later.
/// All of them have the random bits [`RANDOM`](Self::RANDOM).
/// A test therefore knows every UUID of its workspace, and [`nth`](Self::nth) says which one it is.
///
/// The source reads no clock and no randomness, so nothing outside a test takes its UUIDs from it:
/// two processes would make the same ones.
///
/// ```
/// use gaslightmq_definition_memory::KnownUuids;
/// use gaslightmq_workspace::prelude::*;
///
/// let mut uuids = KnownUuids::new();
///
/// assert_eq!(uuids.seed(), UuidSeed::new(KnownUuids::CREATED, KnownUuids::RANDOM));
/// assert_eq!(uuids.seed(), UuidSeed::new(KnownUuids::CREATED + 1, KnownUuids::RANDOM));
/// assert_eq!(KnownUuids::nth(1), "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10");
/// assert_eq!(KnownUuids::nth(2), "0193b6c2-5f3f-7a41-9c2d-8e4b1f6a7d10");
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KnownUuids {
    /// How many seeds the source handed out.
    made: u64,
}

impl KnownUuids {
    /// The time of the first seed: 11 December 2024, in milliseconds since the Unix epoch.
    pub const CREATED: u64 = 0x0193_b6c2_5f3e;

    /// The random bits of every seed.
    pub const RANDOM: [u8; 10] = [0x0a, 0x41, 0x1c, 0x2d, 0x8e, 0x4b, 0x1f, 0x6a, 0x7d, 0x10];

    /// A source that has made no UUID yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The UUID that a source makes as its `nth`, counted from one, as text.
    ///
    /// Every kind of definition has a UUID type of its own, and the text is the same for each of them.
    pub fn nth(nth: u64) -> String {
        RuleUuid::new(Self::CREATED + nth.saturating_sub(1), Self::RANDOM).to_string()
    }
}

impl UuidSource for KnownUuids {
    fn seed(&mut self) -> UuidSeed {
        let seed = UuidSeed::new(Self::CREATED + self.made, Self::RANDOM);
        self.made += 1;
        seed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_seed_is_one_millisecond_after_the_one_before() {
        let mut uuids = KnownUuids::new();

        let seeds = [uuids.seed(), uuids.seed(), uuids.seed()];

        assert_eq!(
            seeds,
            [
                UuidSeed::new(KnownUuids::CREATED, KnownUuids::RANDOM),
                UuidSeed::new(KnownUuids::CREATED + 1, KnownUuids::RANDOM),
                UuidSeed::new(KnownUuids::CREATED + 2, KnownUuids::RANDOM),
            ]
        );
    }

    #[test]
    fn the_nth_uuid_is_the_one_that_the_nth_seed_makes() {
        let mut uuids = KnownUuids::new();
        uuids.seed();
        let second = uuids.seed();

        assert_eq!(
            KnownUuids::nth(2),
            RuleUuid::new(second.unix_millis(), second.random()).to_string()
        );
    }
}
