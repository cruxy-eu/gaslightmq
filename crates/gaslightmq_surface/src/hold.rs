use std::fmt;

/// One hold of a pad: what a press in [hold](crate::PlayMode::Hold) added to its rule, and what the release takes away.
///
/// Whoever holds the rules counts the holds of a rule, and gives each one a number when the press adds it.
/// No number comes back while it runs, so a release names exactly the hold that its press added.
/// A release of a hold that a disarm ended since changes nothing, also when someone held the rule again.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let hold = Hold::new(7);
/// assert_eq!(hold.number(), 7);
/// assert_eq!(hold.to_string(), "hold 7");
/// ```
///
/// With the `serde` feature, a hold serializes as its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(transparent))]
pub struct Hold(u64);

impl Hold {
    /// The hold of that number.
    pub const fn new(number: u64) -> Self {
        Self(number)
    }

    /// The number of the hold.
    pub const fn number(self) -> u64 {
        self.0
    }
}

/// Writes `hold` and the number: `hold 7`.
impl fmt::Display for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hold {}", self.0)
    }
}
