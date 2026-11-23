/// Write the name of one kind of resource, and its error, into the crate that calls it.
///
/// A selector, a rule and an environment each have a name type of their own,
/// so the name of a selector never stands where the name of a rule belongs.
/// Every such type is one call of this macro, in the crate of its kind:
///
/// ```
/// gaslightmq_resource::name!(ScenarioName, ScenarioNameError, "scenario name");
///
/// let name: ScenarioName = "Black Friday".parse()?;
/// assert_eq!(name, "Black Friday");
/// assert_eq!(name.as_str(), "Black Friday");
/// assert_eq!(format!("{name:?}"), "ScenarioName(\"Black Friday\")");
///
/// assert_eq!(ScenarioName::new(""), Err(ScenarioNameError::Empty));
/// let error = ScenarioName::new("Black Friday ").unwrap_err();
/// assert_eq!(error, ScenarioNameError::SurroundingWhiteSpace { input: "Black Friday ".to_owned() });
/// assert_eq!(error.to_string(), "scenario name \"Black Friday \" starts or ends with white space");
/// # Ok::<(), ScenarioNameError>(())
/// ```
///
/// The call has three arguments:
///
/// - The type of the name. It checks the [rule for names](crate::ResourceName#the-rule-for-names)
///   in `new`, in `FromStr` and in `TryFrom`, and keeps the text as it is.
///   It compares, sorts and hashes as its text, so a map by name finds its entry with a `&str`.
///   With the `serde` feature of the calling crate, it serializes as its text.
/// - The type of its error, an enum with one variant for each part of the rule.
/// - The label: how a message and the documentation call the name, such as `scenario name`.
///   An empty name then reads `scenario name "" is empty`.
///
/// The macro writes the documentation of both types from the label, with the rule in the words of the kind.
/// That documentation names neither this crate nor a type of it:
/// the name of a kind shows nothing of what it is built on.
///
/// Doc comments in front of the arguments join the documentation of the name, after its first line.
/// They say what only this kind has to say:
///
/// ```
/// gaslightmq_resource::name! {
///     /// A scenario keeps its name when someone captures it a second time.
///     ScenarioName, ScenarioNameError, "scenario name"
/// }
/// # assert!(ScenarioName::new("Black Friday").is_ok());
/// ```
///
/// # What the calling crate needs
///
/// The error derives its message, so the calling crate depends on `thiserror`.
/// The `serde` form stands behind `#[cfg(feature = "serde")]`, which the calling crate decides:
/// it has a `serde` feature that turns on its optional dependency `serde`.
#[macro_export]
macro_rules! name {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $label:literal $(,)?
    ) => {
        #[doc = concat!("A valid ", $label, ".")]
        ///
        $(#[$meta])*
        ///
        /// # The rule for names
        ///
        #[doc = concat!("Every ", $label, " has at least one character.")]
        /// It does not start or end with white space, and it holds no control character such as a line break.
        /// It holds no format character either, such as a zero-width space or a mark that turns the text direction.
        /// Nobody sees those, so two names that hold them can look the same and still differ.
        ///
        /// A name is exact: upper and lower case differ.
        /// It compares, sorts and hashes as its text, so a map by name finds its entry with plain text.
        /// With the `serde` feature, it serializes as its text.
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($crate::ResourceName);

        impl $name {
            /// Checks the [rule for names](Self#the-rule-for-names), and keeps the text as it is.
            ///
            /// # Errors
            ///
            /// The error of the name, which says which part of the rule the text breaks:
            /// the text is empty, starts or ends with white space, or holds a control character or a format character.
            pub fn new(name: impl ::core::convert::AsRef<str>) -> ::core::result::Result<Self, $error> {
                match $crate::ResourceName::new(name) {
                    Ok(name) => Ok(Self(name)),
                    Err($crate::ResourceNameError::Empty) => Err($error::Empty),
                    Err($crate::ResourceNameError::SurroundingWhiteSpace { input }) => {
                        Err($error::SurroundingWhiteSpace { input })
                    }
                    Err($crate::ResourceNameError::ControlCharacter { input }) => {
                        Err($error::ControlCharacter { input })
                    }
                    Err($crate::ResourceNameError::FormatCharacter { input }) => {
                        Err($error::FormatCharacter { input })
                    }
                }
            }

            /// The name as text.
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl ::core::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.as_str()).finish()
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $error;

            fn from_str(text: &str) -> ::core::result::Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(text: &str) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<::std::string::String> for $name {
            type Error = $error;

            fn try_from(text: ::std::string::String) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::From<$name> for ::std::string::String {
            fn from(name: $name) -> Self {
                name.0.into()
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.pad(self.as_str())
            }
        }

        impl ::core::convert::AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        /// A map by name finds its entry with plain text.
        impl ::core::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl ::core::cmp::PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl ::core::cmp::PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        #[cfg(feature = "serde")]
        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                let text = <::std::string::String as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::new(text).map_err(::serde::de::Error::custom)
            }
        }

        #[doc = concat!(
            "Why a text is not a [`", stringify!($name), "`]: the part of the [rule for names](",
            stringify!($name), "#the-rule-for-names) it breaks."
        )]
        ///
        /// The rule has four parts, and the error has one variant for each.
        #[derive(Debug, Clone, PartialEq, Eq, ::thiserror::Error)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no character.
            #[error("{} \"\" is empty", $label)]
            Empty,
            /// The text starts or ends with white space.
            #[error("{} {input:?} starts or ends with white space", $label)]
            SurroundingWhiteSpace { input: ::std::string::String },
            /// The text holds a control character, such as a line break.
            #[error("{} {input:?} holds a control character", $label)]
            ControlCharacter { input: ::std::string::String },
            /// The text holds a format character, such as a zero-width space, which nobody sees.
            #[error("{} {input:?} holds a format character", $label)]
            FormatCharacter { input: ::std::string::String },
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    crate::name!(ScenarioName, ScenarioNameError, "scenario name");

    crate::name! {
        /// A scenario keeps this name when someone captures it a second time.
        CapturedName, CapturedNameError, "captured scenario name",
    }

    #[test]
    fn the_name_of_a_kind_keeps_its_text() {
        for text in ["Black Friday", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let name = ScenarioName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(name.as_ref(), text);
            assert_eq!(text.parse::<ScenarioName>(), Ok(name.clone()));
            assert_eq!(ScenarioName::try_from(text), Ok(name.clone()));
            assert_eq!(ScenarioName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn the_error_of_a_kind_has_a_variant_for_each_part_of_the_rule() {
        use ScenarioNameError::{ControlCharacter, Empty, FormatCharacter, SurroundingWhiteSpace};

        let input = |text: &str| text.to_owned();
        let cases = [
            ("", Empty, "scenario name \"\" is empty"),
            (
                " Black Friday",
                SurroundingWhiteSpace {
                    input: input(" Black Friday"),
                },
                "scenario name \" Black Friday\" starts or ends with white space",
            ),
            (
                "Black\nFriday",
                ControlCharacter {
                    input: input("Black\nFriday"),
                },
                "scenario name \"Black\\nFriday\" holds a control character",
            ),
            (
                "Black\u{200b}Friday",
                FormatCharacter {
                    input: input("Black\u{200b}Friday"),
                },
                "scenario name \"Black\\u{200b}Friday\" holds a format character",
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(ScenarioName::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(text.parse::<ScenarioName>(), Err(error.clone()), "{text:?}");
            assert_eq!(ScenarioName::try_from(text), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn the_error_of_a_kind_calls_the_name_by_the_label_of_its_call() {
        let message = |text: &str| CapturedName::new(text).unwrap_err().to_string();

        assert_eq!(message(""), "captured scenario name \"\" is empty");
        assert_eq!(
            message("Flap "),
            "captured scenario name \"Flap \" starts or ends with white space"
        );
        assert_eq!(
            message("Leader\tflap"),
            "captured scenario name \"Leader\\tflap\" holds a control character"
        );
        assert_eq!(
            message("Leader\u{feff}flap"),
            "captured scenario name \"Leader\\u{feff}flap\" holds a format character"
        );
    }

    #[test]
    fn a_map_by_the_name_of_a_kind_finds_its_entry_with_text() {
        let name = || ScenarioName::new("Black Friday").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("Black Friday"), Some(&1));
        assert_eq!(sorted.get("Cyber Monday"), None);
        assert_eq!(hashed.get("Black Friday"), Some(&1));
        assert_eq!(hashed.get("Cyber Monday"), None);
    }

    #[test]
    fn the_name_of_a_kind_pads_like_text() {
        assert_eq!(format!("{:>8}|", ScenarioName::new("Flap").unwrap()), "    Flap|");
    }

    #[test]
    fn the_name_of_a_kind_shows_as_its_own_type_when_debugging() {
        assert_eq!(
            format!("{:?}", ScenarioName::new("Black Friday").unwrap()),
            "ScenarioName(\"Black Friday\")"
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn the_name_of_a_kind_is_its_text_in_serde() {
        let name = ScenarioName::new("Black Friday").unwrap();

        assert_eq!(serde_json::to_string(&name).unwrap(), "\"Black Friday\"");
        assert_eq!(serde_json::from_str::<ScenarioName>("\"Black Friday\"").unwrap(), name);
        assert_eq!(
            serde_json::from_str::<ScenarioName>("\" Black Friday\"")
                .unwrap_err()
                .to_string(),
            "scenario name \" Black Friday\" starts or ends with white space"
        );
    }
}
