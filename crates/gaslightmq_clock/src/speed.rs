use core::fmt;

/// How fast a virtual clock runs against real time.
///
/// A speed is a whole multiplier.
/// At `60x`, a second of real time is a minute on the clock.
/// The multiplier is whole, so virtual time is an exact product of real time,
/// and nothing rounds.
///
/// ```
/// use gaslightmq_clock::Speed;
///
/// assert_eq!(Speed::default(), Speed::REAL_TIME);
/// assert_eq!(Speed::times(3600).to_string(), "3600x");
/// assert!(Speed::times(0).is_paused());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize), serde(transparent))]
pub struct Speed(u32);

impl Speed {
    /// The clock stands still while real time passes.
    /// It moves only when someone steps it.
    pub const PAUSED: Speed = Speed(0);

    /// The clock runs as fast as real time.
    /// This is the default.
    pub const REAL_TIME: Speed = Speed(1);

    /// Returns the speed at which the clock runs `factor` times as fast as real time.
    /// With `Speed::times(3600)`, an hour on the clock takes a second.
    pub const fn times(factor: u32) -> Speed {
        Speed(factor)
    }

    /// Returns how many times as fast as real time the clock runs.
    pub const fn factor(self) -> u32 {
        self.0
    }

    /// Returns whether the clock stands still while real time passes.
    pub const fn is_paused(self) -> bool {
        self.0 == 0
    }
}

/// Returns [`Speed::REAL_TIME`].
impl Default for Speed {
    fn default() -> Speed {
        Speed::REAL_TIME
    }
}

/// Writes the speed as its multiplier, such as `60x`.
impl fmt::Display for Speed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_speed_is_real_time() {
        assert_eq!(Speed::default(), Speed::REAL_TIME);
        assert_eq!(Speed::REAL_TIME, Speed::times(1));
        assert_eq!(Speed::REAL_TIME.factor(), 1);
        assert!(!Speed::REAL_TIME.is_paused());
    }

    #[test]
    fn a_paused_clock_has_factor_zero() {
        assert_eq!(Speed::PAUSED, Speed::times(0));
        assert_eq!(Speed::PAUSED.factor(), 0);
        assert!(Speed::PAUSED.is_paused());
    }

    #[test]
    fn a_speed_keeps_its_factor() {
        assert_eq!(Speed::times(3600).factor(), 3600);
        assert!(!Speed::times(3600).is_paused());
        assert!(Speed::PAUSED < Speed::REAL_TIME);
        assert!(Speed::REAL_TIME < Speed::times(60));
    }

    #[test]
    fn display_writes_the_multiplier() {
        assert_eq!(Speed::times(60).to_string(), "60x");
        assert_eq!(Speed::REAL_TIME.to_string(), "1x");
        assert_eq!(Speed::PAUSED.to_string(), "0x");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_maps_a_speed_to_its_factor() {
        assert_eq!(serde_json::to_string(&Speed::times(60)).unwrap(), "60");
        assert_eq!(serde_json::from_str::<Speed>("60").unwrap(), Speed::times(60));
        assert_eq!(serde_json::from_str::<Speed>("0").unwrap(), Speed::PAUSED);
        assert!(serde_json::from_str::<Speed>("-1").is_err());
        assert!(serde_json::from_str::<Speed>("1.5").is_err());
    }
}
