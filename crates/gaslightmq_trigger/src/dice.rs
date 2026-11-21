//! Dice: the seed, the keys, and the fixed derivation of every roll.

use std::fmt;
use std::str::FromStr;

use crate::{MatchNumber, Roll, TriggerError};

/// The offset basis of 64-bit FNV-1a.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// The prime of 64-bit FNV-1a.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
/// The increment of `SplitMix64`: 2^64 divided by the golden ratio.
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// 64-bit FNV-1a over the bytes.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    })
}

/// The finalizer of `SplitMix64`: every bit of the input changes every bit of the output.
const fn mix(bits: u64) -> u64 {
    let bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    bits ^ (bits >> 31)
}

/// The root of every die in a run: `seed: 42`.
///
/// A scenario names its seed, and the workspace has a default one for when no scenario is active.
/// The same seed makes the same match numbers fire, in every run.
///
/// With the `serde` feature, a seed serializes as a number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(u64);

impl Seed {
    /// The seed with that number.
    pub const fn new(value: u64) -> Seed {
        Seed(value)
    }

    /// The seed as a plain number.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl From<u64> for Seed {
    fn from(value: u64) -> Seed {
        Seed(value)
    }
}

impl From<Seed> for u64 {
    fn from(seed: Seed) -> u64 {
        seed.0
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Define a key that holds text of at least one character.
macro_rules! text_key {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Check that `text` is not empty, and keep it.
            ///
            /// # Errors
            ///
            /// [`TriggerError::EmptyKey`] when `text` is empty.
            pub fn new(text: impl Into<String>) -> Result<$name, TriggerError> {
                let text = text.into();
                if text.is_empty() {
                    return Err(TriggerError::EmptyKey { what: $what });
                }
                Ok($name(text))
            }

            /// The key as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = TriggerError;

            fn from_str(text: &str) -> Result<$name, TriggerError> {
                $name::new(text)
            }
        }

        impl TryFrom<String> for $name {
            type Error = TriggerError;

            fn try_from(text: String) -> Result<$name, TriggerError> {
                $name::new(text)
            }
        }

        impl From<$name> for String {
            fn from(key: $name) -> String {
                key.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

text_key! {
    /// What tells the die of one rule from the die of another.
    ///
    /// The dice key is the rule's name, unless the rule sets `dice`.
    /// Renaming a rule writes the old name as its `dice` key, so a rename does not change which calls the rule hits.
    ///
    /// A [`Die`] hashes the UTF-8 bytes of the key, so two keys roll the same only when they are the same text.
    ///
    /// ```
    /// use gaslightmq_trigger::prelude::*;
    ///
    /// let key = DiceKey::new("Slow down order producers")?;
    /// assert_eq!(key.as_str(), "Slow down order producers");
    /// assert!(DiceKey::new("").is_err());
    /// # Ok::<(), TriggerError>(())
    /// ```
    ///
    /// With the `serde` feature, a dice key serializes as its text.
    DiceKey, "dice key"
}

text_key! {
    /// What tells one session's counter, and its rolls, from another's.
    ///
    /// A rule that counts `per: session` rolls a die of its own for every session.
    /// The counter key is the text form of the session key: the host, the endpoint,
    /// and the ordinal of the connection among the connections from that host to that endpoint.
    /// That key is stable across runs, so two runs give each connection the same rolls.
    ///
    /// This crate does not know sessions.
    /// A session builds its counter key once, and hands it over inside its [`SessionCounters`](crate::SessionCounters).
    /// A [`Die`] hashes the UTF-8 bytes of the key, so whoever builds the key decides what "the same session" means.
    ///
    /// ```
    /// use gaslightmq_trigger::prelude::*;
    ///
    /// let key = CounterKey::new("10.0.4.27 broker 2 #1")?;
    /// assert_eq!(key.to_string(), "10.0.4.27 broker 2 #1");
    /// assert!(CounterKey::new("").is_err());
    /// # Ok::<(), TriggerError>(())
    /// ```
    CounterKey, "counter key"
}

/// The die of one rule, or of one rule on one session.
///
/// A die rolls once for every match number, and the roll derives from four values:
///
/// ```text
/// scenario seed
/// └─ dice key (the rule's name, unless the rule sets `dice`)
///    └─ counter key (none, or the session key with `per: session`)
///       └─ match number
/// ```
///
/// The derivation is fixed.
/// It uses 64-bit integer arithmetic that wraps, and nothing of the platform or of the standard library's hashers.
/// A recording made on one machine replays with the same rolls on any other, with any version of Rust.
///
/// | Step | Value |
/// |---|---|
/// | The die of a rule | `mix(seed ^ fnv1a(dice key))` |
/// | The die of a rule on one session | `mix(die ^ fnv1a(counter key))` |
/// | The bits for one match | `mix(die + match number × 0x9e3779b97f4a7c15)` |
/// | The roll | `(bits × 1 000 000) >> 64`, in parts per million |
///
/// - `fnv1a` is 64-bit FNV-1a over the UTF-8 bytes of the key:
///   start at `0xcbf29ce484222325`, and for every byte XOR it in and multiply by `0x100000001b3`.
/// - `mix` is the finalizer of `SplitMix64`:
///   `x ^= x >> 30`, `x *= 0xbf58476d1ce4e5b9`, `x ^= x >> 27`, `x *= 0x94d049bb133111eb`, `x ^= x >> 31`.
/// - The bits for match `n` are output number `n` of the `SplitMix64` generator that starts at the die.
///   A die therefore needs no state: any match number rolls on its own, in any order.
///
/// Each rule rolls its own die, so adding or removing a rule does not shift the rolls of the others.
/// A roll does not depend on the order in which connections open.
///
/// ```
/// use gaslightmq_trigger::prelude::*;
///
/// let die = Die::new(Seed::new(42), &DiceKey::new("Slow down order producers")?);
///
/// // The same seed, key and match number roll the same, in every run and on every machine.
/// let third = MatchNumber::new(3)?;
/// assert_eq!(die.roll(third), die.roll(third));
/// assert_eq!(die.roll(third).to_string(), "73.8645%");
///
/// // With `per: session`, every session rolls a die of its own.
/// let session = die.for_counter(&CounterKey::new("10.0.4.27 broker 2 #1")?);
/// assert_ne!(session.roll(third), die.roll(third));
/// # Ok::<(), TriggerError>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Die(u64);

impl Die {
    /// The die of the rule with that dice key, in a run with that seed.
    pub fn new(seed: Seed, key: &DiceKey) -> Die {
        Die(mix(seed.value() ^ fnv1a(key.as_str().as_bytes())))
    }

    /// The die this rule rolls for the session with that counter key.
    #[must_use]
    pub fn for_counter(self, key: &CounterKey) -> Die {
        Die(mix(self.0 ^ fnv1a(key.as_str().as_bytes())))
    }

    /// What the die rolls for that match.
    pub fn roll(self, match_number: MatchNumber) -> Roll {
        let step = match_number.value().wrapping_mul(GOLDEN_GAMMA);
        Roll::from_bits(mix(self.0.wrapping_add(step)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_matches_its_reference_vectors() {
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn the_bits_of_a_die_are_the_outputs_of_split_mix_64() {
        // The reference outputs of SplitMix64 that starts at zero.
        let outputs = [0xe220_a839_7b1d_cdaf_u64, 0x6e78_9e6a_a1b9_65f4, 0x06c4_5d18_8009_454f];
        for (number, output) in (1_u64..).zip(outputs) {
            assert_eq!(mix(number.wrapping_mul(GOLDEN_GAMMA)), output);
            assert_eq!(Die(0).roll(MatchNumber::new(number).unwrap()), Roll::from_bits(output));
        }
    }

    #[test]
    fn a_key_is_not_empty() {
        assert_eq!(DiceKey::new(""), Err(TriggerError::EmptyKey { what: "dice key" }));
        assert_eq!(CounterKey::new(""), Err(TriggerError::EmptyKey { what: "counter key" }));
        assert_eq!("".parse::<DiceKey>(), Err(TriggerError::EmptyKey { what: "dice key" }));
    }

    #[test]
    fn a_key_converts_from_and_to_text() {
        let key = DiceKey::new("Stale metadata").unwrap();
        assert_eq!(key.as_str(), "Stale metadata");
        assert_eq!(key.as_ref(), "Stale metadata");
        assert_eq!(key.to_string(), "Stale metadata");
        assert_eq!("Stale metadata".parse(), Ok(key.clone()));
        assert_eq!(DiceKey::try_from("Stale metadata".to_owned()), Ok(key.clone()));
        assert_eq!(String::from(key), "Stale metadata");
    }

    #[test]
    fn a_seed_converts_from_and_to_its_number() {
        let seed = Seed::new(42);
        assert_eq!(seed.value(), 42);
        assert_eq!(Seed::from(42), seed);
        assert_eq!(u64::from(seed), 42);
        assert_eq!(seed.to_string(), "42");
        assert_eq!(Seed::default(), Seed::new(0));
    }
}
