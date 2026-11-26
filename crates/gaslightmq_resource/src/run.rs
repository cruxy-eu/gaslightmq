/// Write the ID of a run, and its error, into the crate that calls it.
///
/// A run is one life of a process, from its start to its stop.
/// Its ID is 128 random bits, written as 32 hexadecimal digits in lower case.
/// A revision means something only together with the run that gave it, so whatever names a revision that it
/// expects names the run too.
///
/// ```
/// gaslightmq_resource::run_id!(RunId, RunIdError);
///
/// // Whoever puts a process together reads the randomness of the system once, and hands the bits in.
/// let run = RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd);
/// assert_eq!(run.to_string(), "4be0643f1d98573b97cdca98a65347dd");
/// assert_eq!(format!("{run:?}"), "RunId(4be0643f1d98573b97cdca98a65347dd)");
///
/// // A test names its own.
/// let named: RunId = "4be0643f1d98573b97cdca98a65347dd".parse()?;
/// assert_eq!(named, run);
///
/// let error = RunId::new("4BE0643F1D98573B97CDCA98A65347DD").unwrap_err();
/// assert_eq!(
///     error.to_string(),
///     "run ID \"4BE0643F1D98573B97CDCA98A65347DD\" holds 'B' at position 1, which is no digit and no letter from a to f"
/// );
/// # Ok::<(), RunIdError>(())
/// ```
///
/// The call has up to three arguments:
///
/// - The type of the ID. It checks its text in `new`, in `FromStr` and in `TryFrom`.
///   This crate reads no randomness: `from_random_bits` takes the bits from its caller.
/// - The type of its error, an enum with one variant for each part of the rule.
/// - The word `serde`, as the last argument, for a crate whose run IDs travel.
///   The ID then serializes as its text, and the calling crate depends on `serde`:
///   `gaslightmq_resource::run_id!(RunId, RunIdError, serde);`
///
/// The error derives its message, so the calling crate depends on `thiserror`.
/// The documentation that the macro writes names neither this crate nor a type of it.
/// Doc comments in front of the arguments join the documentation of the ID, after its first line.
#[macro_export]
macro_rules! run_id {
    (
        $(#[$meta:meta])*
        $run:ident, $error:ident $(,)?
    ) => {
        /// The ID of a run: of one life of a process, from its start to its stop.
        ///
        $(#[$meta])*
        ///
        /// # The rule for run IDs
        ///
        /// A run ID is 128 random bits.
        /// Its text is 32 hexadecimal digits in lower case: the digits `0` to `9` and the letters `a` to `f`.
        ///
        /// Whoever puts a process together reads the randomness of the system once, at the start,
        /// and makes the ID [from those bits](Self::from_random_bits). A test names its own.
        /// No file holds a run ID, and nothing refers to a run after it ended.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $run(u128);

        impl $run {
            /// The number of hexadecimal digits that the text of a run ID has.
            pub const LENGTH: usize = 32;

            /// The ID that those 128 random bits make.
            ///
            /// The caller reads them from the randomness of the system: nothing here reads it.
            pub const fn from_random_bits(bits: u128) -> Self {
                Self(bits)
            }

            /// The 128 bits of the ID.
            pub const fn bits(self) -> u128 {
                self.0
            }

            /// Checks the [rule for run IDs](Self#the-rule-for-run-ids), and reads the ID that the text writes.
            ///
            /// # Errors
            ///
            /// The error of the ID, which says which part of the rule the text breaks:
            /// it has another length than 32 characters, or it holds a character that is no hexadecimal digit
            /// in lower case. A text that breaks both gets the first, in this order.
            pub fn new(text: impl ::core::convert::AsRef<str>) -> ::core::result::Result<Self, $error> {
                let text = text.as_ref();
                let length = text.chars().count();
                if length != Self::LENGTH {
                    return Err($error::WrongLength { length });
                }
                let mut bits: u128 = 0;
                for (position, character) in text.chars().enumerate() {
                    let digit = match character {
                        '0'..='9' | 'a'..='f' => character.to_digit(16),
                        _ => None,
                    };
                    let Some(digit) = digit else {
                        return Err($error::InvalidCharacter {
                            input: text.to_owned(),
                            character,
                            position,
                        });
                    };
                    bits = (bits << 4) | u128::from(digit);
                }
                Ok(Self(bits))
            }
        }

        impl ::core::fmt::Debug for $run {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                write!(f, "{}({self})", stringify!($run))
            }
        }

        /// Writes the 32 hexadecimal digits, in lower case.
        impl ::core::fmt::Display for $run {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }

        impl ::core::str::FromStr for $run {
            type Err = $error;

            fn from_str(text: &str) -> ::core::result::Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<&str> for $run {
            type Error = $error;

            fn try_from(text: &str) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<::std::string::String> for $run {
            type Error = $error;

            fn try_from(text: ::std::string::String) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::From<$run> for ::std::string::String {
            fn from(run: $run) -> Self {
                ::std::string::ToString::to_string(&run)
            }
        }

        #[doc = concat!(
            "Why a text is not a [`", stringify!($run), "`]: the part of the [rule for run IDs](",
            stringify!($run), "#the-rule-for-run-ids) it breaks."
        )]
        #[derive(Debug, Clone, PartialEq, Eq, ::thiserror::Error)]
        pub enum $error {
            /// The text has `length` characters, and a run ID has 32.
            #[error("a run ID has {} hexadecimal digits, and not {length} characters", $run::LENGTH)]
            WrongLength { length: usize },
            /// The text holds a character that is no hexadecimal digit in lower case, such as `G` or `B`.
            /// It is the first such character, and `position` counts characters from zero.
            #[error(
                "run ID {input:?} holds {character:?} at position {position}, which is no digit and no letter from a to f"
            )]
            InvalidCharacter {
                input: ::std::string::String,
                character: char,
                position: usize,
            },
        }
    };
    (
        $(#[$meta:meta])*
        $run:ident, $error:ident, serde $(,)?
    ) => {
        $crate::run_id! {
            $(#[$meta])*
            ///
            /// With `serde`, a run ID is its text.
            $run, $error
        }

        impl ::serde::Serialize for $run {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $run {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                let text = <::std::string::String as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::new(text).map_err(::serde::de::Error::custom)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    crate::run_id!(RunId, RunIdError);

    mod travelling {
        #[cfg(feature = "serde")]
        crate::run_id!(RunId, RunIdError, serde);
    }

    /// The text of a run ID, as a snapshot of the control plane names it.
    const TEXT: &str = "4be0643f1d98573b97cdca98a65347dd";

    /// The 128 bits that [`TEXT`] writes.
    const BITS: u128 = 0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd;

    #[test]
    fn a_run_id_is_its_128_bits_as_32_hexadecimal_digits_in_lower_case() {
        let run = RunId::from_random_bits(BITS);

        assert_eq!(run.to_string(), TEXT);
        assert_eq!(run.bits(), BITS);
        assert_eq!(String::from(run), TEXT);
        assert_eq!(format!("{run:?}"), format!("RunId({TEXT})"));
    }

    #[test]
    fn a_run_id_of_few_bits_still_has_32_digits() {
        let run = RunId::from_random_bits(0xa);

        assert_eq!(run.to_string(), "0000000000000000000000000000000a");
        assert_eq!(run.to_string().parse::<RunId>(), Ok(run));
    }

    #[test]
    fn a_text_reads_as_the_run_id_that_writes_it() {
        let run = RunId::from_random_bits(BITS);

        assert_eq!(RunId::new(TEXT), Ok(run));
        assert_eq!(TEXT.parse::<RunId>(), Ok(run));
        assert_eq!(RunId::try_from(TEXT), Ok(run));
        assert_eq!(RunId::try_from(TEXT.to_owned()), Ok(run));
    }

    #[test]
    fn the_error_of_a_run_id_has_a_variant_for_each_part_of_the_rule() {
        use RunIdError::{InvalidCharacter, WrongLength};

        let upper = TEXT.to_uppercase();
        // As many characters as a run ID has, in the form of a UUID.
        let dashed = "4be0643f-1d98-573b-97cd-ca98a653";
        let cases = [
            (
                "",
                WrongLength { length: 0 },
                "a run ID has 32 hexadecimal digits, and not 0 characters".to_owned(),
            ),
            (
                &TEXT[1..],
                WrongLength { length: 31 },
                "a run ID has 32 hexadecimal digits, and not 31 characters".to_owned(),
            ),
            (
                upper.as_str(),
                InvalidCharacter {
                    input: upper.clone(),
                    character: 'B',
                    position: 1,
                },
                format!("run ID {upper:?} holds 'B' at position 1, which is no digit and no letter from a to f"),
            ),
            (
                dashed,
                InvalidCharacter {
                    input: dashed.to_owned(),
                    character: '-',
                    position: 8,
                },
                format!("run ID {dashed:?} holds '-' at position 8, which is no digit and no letter from a to f"),
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(RunId::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(text.parse::<RunId>(), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn a_sign_in_front_of_32_characters_is_no_digit() {
        let signed = format!("+{}", &TEXT[1..]);

        assert_eq!(
            RunId::new(&signed),
            Err(RunIdError::InvalidCharacter {
                input: signed.clone(),
                character: '+',
                position: 0,
            })
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_run_id_that_travels_is_its_text_in_serde() {
        use travelling::RunId;

        let run = RunId::from_random_bits(BITS);

        assert_eq!(serde_json::to_string(&run).unwrap(), format!("\"{TEXT}\""));
        assert_eq!(serde_json::from_str::<RunId>(&format!("\"{TEXT}\"")).unwrap(), run);
        assert_eq!(
            serde_json::from_str::<RunId>("\"4be0\"").unwrap_err().to_string(),
            "a run ID has 32 hexadecimal digits, and not 4 characters"
        );
    }
}
