//! How many of something an incident adds.

/// Define a count that holds `1..=max`, and the error that says why a number is not one.
macro_rules! count_value {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal, value: $value:ty, max: $max:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($value);

        impl $name {
            /// One, which a definition means when it names no count.
            pub const ONE: Self = Self(1);

            #[doc = concat!("The highest ", $what, ".")]
            pub const MAX: Self = Self($max);

            #[doc = concat!("Check that `value` is at least one and at most [`MAX`](Self::MAX), and keep it.")]
            ///
            /// # Errors
            ///
            #[doc = concat!("[`", stringify!($error), "::OutOfRange`] when `value` is outside that range.")]
            pub fn new(value: $value) -> Result<Self, $error> {
                if !(1..=Self::MAX.0).contains(&value) {
                    return Err($error::OutOfRange {
                        value: value.into(),
                        max: Self::MAX.0.into(),
                    });
                }
                Ok(Self(value))
            }

            #[doc = concat!("The ", $what, " as a plain number.")]
            pub fn value(self) -> $value {
                self.0
            }
        }

        #[doc = concat!("Why a number is not a [`", stringify!($name), "`].")]
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            #[doc = concat!("The number is zero, or above the highest ", $what, ".")]
            OutOfRange { value: u64, max: u64 },
        }

        impl std::fmt::Display for $error {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::OutOfRange { value, max } => {
                        write!(f, concat!($what, " {} is not between 1 and {}"), value, max)
                    }
                }
            }
        }

        impl std::error::Error for $error {}

        /// One.
        impl Default for $name {
            fn default() -> Self {
                Self::ONE
            }
        }

        impl TryFrom<$value> for $name {
            type Error = $error;

            fn try_from(value: $value) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for $value {
            fn from(count: $name) -> Self {
                count.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = <$value as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

count_value! {
    /// How many partitions a "partition added" incident adds to a topic.
    ///
    /// It is at least one.
    /// A topic numbers its partitions with an `i32`, so no topic takes more than [`i32::MAX`].
    ///
    /// ```
    /// use gaslightmq_incident::prelude::*;
    ///
    /// assert_eq!(PartitionCount::new(3)?.value(), 3);
    /// assert_eq!(PartitionCount::default(), PartitionCount::ONE);
    /// assert!(PartitionCount::new(0).is_err());
    /// # Ok::<(), PartitionCountError>(())
    /// ```
    PartitionCount, PartitionCountError, "partition count", value: u32, max: i32::MAX as u32
}

count_value! {
    /// How many records a "records appended" incident adds to a partition.
    ///
    /// It is at least one.
    /// A partition numbers its records with an `i64`, so no log takes more than [`i64::MAX`].
    ///
    /// ```
    /// use gaslightmq_incident::prelude::*;
    ///
    /// assert_eq!(RecordCount::new(500)?.value(), 500);
    /// assert_eq!(RecordCount::default(), RecordCount::ONE);
    /// assert!(RecordCount::new(0).is_err());
    /// # Ok::<(), RecordCountError>(())
    /// ```
    RecordCount, RecordCountError, "record count", value: u64, max: i64::MAX as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_is_at_least_one() {
        assert_eq!(PartitionCount::new(1), Ok(PartitionCount::ONE));
        assert_eq!(RecordCount::new(1), Ok(RecordCount::ONE));
        assert_eq!(
            PartitionCount::new(0),
            Err(PartitionCountError::OutOfRange {
                value: 0,
                max: 2_147_483_647
            }),
        );
        assert_eq!(
            RecordCount::new(0),
            Err(RecordCountError::OutOfRange {
                value: 0,
                max: i64::MAX as u64
            }),
        );
    }

    #[test]
    fn a_count_is_at_most_what_kafka_can_number() {
        assert_eq!(PartitionCount::new(i32::MAX as u32), Ok(PartitionCount::MAX));
        assert_eq!(RecordCount::new(i64::MAX as u64), Ok(RecordCount::MAX));
        assert_eq!(
            PartitionCount::new(i32::MAX as u32 + 1),
            Err(PartitionCountError::OutOfRange {
                value: 2_147_483_648,
                max: 2_147_483_647
            }),
        );
        assert_eq!(
            RecordCount::try_from(u64::MAX),
            Err(RecordCountError::OutOfRange {
                value: u64::MAX,
                max: i64::MAX as u64
            }),
        );
    }

    #[test]
    fn the_error_of_a_count_names_the_count_the_number_and_the_range() {
        use std::error::Error;

        /// What an error says, which is everything: it has no source.
        fn said<E: Error>(error: E) -> String {
            assert!(error.source().is_none(), "the message is complete: {error}");
            error.to_string()
        }

        let cases: [(String, &str); 4] = [
            (
                said(PartitionCount::new(0).unwrap_err()),
                "partition count 0 is not between 1 and 2147483647",
            ),
            (
                said(PartitionCount::new(u32::MAX).unwrap_err()),
                "partition count 4294967295 is not between 1 and 2147483647",
            ),
            (
                said(RecordCount::new(0).unwrap_err()),
                "record count 0 is not between 1 and 9223372036854775807",
            ),
            (
                said(RecordCount::new(u64::MAX).unwrap_err()),
                "record count 18446744073709551615 is not between 1 and 9223372036854775807",
            ),
        ];

        for (said, message) in cases {
            assert_eq!(said, message);
        }
    }

    #[test]
    fn a_count_is_one_unless_someone_says_otherwise() {
        assert_eq!(PartitionCount::default().value(), 1);
        assert_eq!(RecordCount::default().value(), 1);
    }

    #[test]
    fn a_count_converts_to_its_number_and_its_text() {
        let partitions = PartitionCount::try_from(3_u32).unwrap();
        assert_eq!(u32::from(partitions), 3);
        assert_eq!(partitions.to_string(), "3");

        let records = RecordCount::try_from(500_u64).unwrap();
        assert_eq!(u64::from(records), 500);
        assert_eq!(records.to_string(), "500");
    }
}
