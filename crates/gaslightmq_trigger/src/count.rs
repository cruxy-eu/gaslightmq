//! How many, and which one: the two numbers of counting matches.

use std::fmt;
use std::num::NonZeroU64;

use crate::TriggerError;

/// Define a number that is at least one.
macro_rules! positive_number {
    (
        $(#[$meta:meta])*
        $name:ident, $what:literal, $(#[$one_meta:meta])* $one:ident
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            $(#[$one_meta])*
            pub const $one: $name = $name(NonZeroU64::MIN);

            /// Check that `value` is at least one, and keep it.
            ///
            /// # Errors
            ///
            /// [`TriggerError::Zero`] when `value` is zero.
            pub fn new(value: u64) -> Result<$name, TriggerError> {
                NonZeroU64::new(value).map($name).ok_or(TriggerError::Zero { what: $what })
            }

            #[doc = concat!("The ", $what, " as a plain number.")]
            pub const fn value(self) -> u64 {
                self.0.get()
            }
        }

        impl TryFrom<u64> for $name {
            type Error = TriggerError;

            fn try_from(value: u64) -> Result<$name, TriggerError> {
                $name::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(number: $name) -> u64 {
                number.value()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

positive_number! {
    /// How many matches or firings, as a definition writes it: `every: 10`, `first: 5`, `skip: 100`, `3 firings`.
    ///
    /// A count is at least one.
    /// A definition that means "none" leaves the part out.
    ///
    /// ```
    /// use gaslightmq_trigger::prelude::*;
    ///
    /// assert_eq!(Count::new(10)?.value(), 10);
    /// assert_eq!(Count::ONE.to_string(), "1");
    /// assert!(Count::new(0).is_err());
    /// # Ok::<(), TriggerError>(())
    /// ```
    ///
    /// With the `serde` feature, a count serializes as a number.
    Count, "count",
    /// One.
    ONE
}

positive_number! {
    /// Which matching call this is, for one counter: the 1st, the 2nd, the 3rd.
    ///
    /// Every armed rule counts the calls its selector matches.
    /// Count parts and dice use that number, and the first match has number one.
    /// A definition writes one as `nth: 3`.
    ///
    /// ```
    /// use gaslightmq_trigger::prelude::*;
    ///
    /// let third = MatchNumber::new(3)?;
    /// assert!(MatchNumber::FIRST < third);
    /// assert_eq!(third.value(), 3);
    /// assert!(MatchNumber::new(0).is_err());
    /// # Ok::<(), TriggerError>(())
    /// ```
    ///
    /// With the `serde` feature, a match number serializes as a number.
    MatchNumber, "match number",
    /// The first match.
    FIRST
}

impl MatchNumber {
    /// The match number that follows `matches` earlier matches.
    pub(crate) fn following(matches: u64) -> MatchNumber {
        MatchNumber(NonZeroU64::new(matches.saturating_add(1)).unwrap_or(NonZeroU64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_is_at_least_one() {
        assert_eq!(Count::new(1), Ok(Count::ONE));
        assert_eq!(Count::new(0), Err(TriggerError::Zero { what: "count" }));
        assert_eq!(Count::try_from(u64::MAX).unwrap().value(), u64::MAX);
    }

    #[test]
    fn the_first_match_has_number_one() {
        assert_eq!(MatchNumber::new(1), Ok(MatchNumber::FIRST));
        assert_eq!(MatchNumber::new(0), Err(TriggerError::Zero { what: "match number" }));
        assert_eq!(MatchNumber::following(0), MatchNumber::FIRST);
        assert_eq!(MatchNumber::following(2).value(), 3);
        assert_eq!(MatchNumber::following(u64::MAX).value(), u64::MAX);
    }

    #[test]
    fn numbers_convert_to_plain_numbers_and_text() {
        let count = Count::try_from(100_u64).unwrap();
        assert_eq!(u64::from(count), 100);
        assert_eq!(count.to_string(), "100");

        let third = MatchNumber::try_from(3_u64).unwrap();
        assert_eq!(u64::from(third), 3);
        assert_eq!(third.to_string(), "3");
    }

    #[test]
    fn numbers_order_by_value() {
        assert!(Count::ONE < Count::new(2).unwrap());
        assert!(MatchNumber::FIRST < MatchNumber::new(2).unwrap());
    }
}
