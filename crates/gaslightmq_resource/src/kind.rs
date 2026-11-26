/// Write the name of one kind of resource, and its error, into the crate that calls it.
///
/// A selector, a rule and an environment each have a name type of their own,
/// so the name of a selector never stands where the name of a rule belongs.
/// Every such type is one call of this macro, in the crate of its kind:
///
/// ```
/// gaslightmq_resource::name!(ScenarioName, ScenarioNameError, "scenario name");
///
/// let name: ScenarioName = "black-friday".parse()?;
/// assert_eq!(name, "black-friday");
/// assert_eq!(name.as_str(), "black-friday");
/// assert_eq!(format!("{name:?}"), "ScenarioName(\"black-friday\")");
///
/// assert_eq!(ScenarioName::new(""), Err(ScenarioNameError::Empty));
/// let error = ScenarioName::new("Black Friday").unwrap_err();
/// assert_eq!(
///     error,
///     ScenarioNameError::InvalidCharacter { input: "Black Friday".to_owned(), character: 'B', position: 0 }
/// );
/// assert_eq!(
///     error.to_string(),
///     "scenario name \"Black Friday\" holds 'B' at position 0, which is no lowercase letter, digit or hyphen"
/// );
/// assert_eq!(ScenarioName::MAX_LENGTH, 63);
/// # Ok::<(), ScenarioNameError>(())
/// ```
///
/// The call has three arguments:
///
/// - The type of the name. It checks the [rule for names](crate::ResourceName#the-rule-for-names)
///   in `new`, in `FromStr` and in `TryFrom`, and keeps the text as it is.
///   Its `MAX_LENGTH` is the most characters a name has.
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
/// # assert!(ScenarioName::new("black-friday").is_ok());
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
        #[doc = concat!("Every ", $label, " is a slug, like the name of a resource in Kubernetes.")]
        /// It holds lowercase letters from `a` to `z`, digits and hyphens.
        /// It starts and ends with a letter or a digit.
        /// It has at least one character, and at most [`MAX_LENGTH`](Self::MAX_LENGTH), which is 63.
        ///
        /// A name is an identity, and nothing else: whatever references a thing does so by its name.
        /// Free text that says what the thing is for is no name.
        ///
        /// A name compares, sorts and hashes as its text, so a map by name finds its entry with plain text.
        /// With the `serde` feature, it serializes as its text.
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($crate::ResourceName);

        impl $name {
            /// The most characters a name has.
            pub const MAX_LENGTH: usize = $crate::ResourceName::MAX_LENGTH;

            /// Checks the [rule for names](Self#the-rule-for-names), and keeps the text as it is.
            ///
            /// # Errors
            ///
            /// The error of the name, which says which part of the rule the text breaks:
            /// the text is empty, holds a character that no name holds, starts or ends with a hyphen, or is too long.
            /// A text that breaks several parts gets the first of them, in this order.
            pub fn new(name: impl ::core::convert::AsRef<str>) -> ::core::result::Result<Self, $error> {
                match $crate::ResourceName::new(name) {
                    Ok(name) => Ok(Self(name)),
                    Err($crate::ResourceNameError::Empty) => Err($error::Empty),
                    Err($crate::ResourceNameError::InvalidCharacter {
                        input,
                        character,
                        position,
                    }) => Err($error::InvalidCharacter {
                        input,
                        character,
                        position,
                    }),
                    Err($crate::ResourceNameError::SurroundingHyphen { input }) => {
                        Err($error::SurroundingHyphen { input })
                    }
                    Err($crate::ResourceNameError::TooLong { length }) => Err($error::TooLong { length }),
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
        pub enum $error {
            /// The text has no character.
            #[error("{} \"\" is empty", $label)]
            Empty,
            /// The text holds a character that no name holds, such as an uppercase letter, a space or a dot.
            /// It is the first such character, and `position` counts characters from zero.
            #[error(
                "{} {input:?} holds {character:?} at position {position}, which is no lowercase letter, digit or hyphen",
                $label
            )]
            InvalidCharacter {
                input: ::std::string::String,
                character: char,
                position: usize,
            },
            /// The text starts or ends with a hyphen.
            #[error("{} {input:?} starts or ends with a hyphen", $label)]
            SurroundingHyphen { input: ::std::string::String },
            /// The text has `length` characters, which is more than a name has.
            #[error("{} of {length} characters is longer than {} characters", $label, $name::MAX_LENGTH)]
            TooLong { length: usize },
        }
    };
}

/// Write the title of one kind of resource, and its error, into the crate that calls it.
///
/// A selector, a rule and an environment each have a title type of their own,
/// so the title of a selector never stands where the title of a rule belongs.
/// Every such type is one call of this macro, in the crate of its kind:
///
/// ```
/// gaslightmq_resource::title!(ScenarioTitle, ScenarioTitleError, "scenario title");
///
/// let title: ScenarioTitle = "Black Friday, with half of the brokers down".parse()?;
/// assert_eq!(title, "Black Friday, with half of the brokers down");
/// assert_eq!(title.as_str(), "Black Friday, with half of the brokers down");
/// assert_eq!(format!("{:?}", ScenarioTitle::new("Black Friday")?), "ScenarioTitle(\"Black Friday\")");
///
/// assert_eq!(ScenarioTitle::new(""), Err(ScenarioTitleError::Empty));
/// let error = ScenarioTitle::new("Black Friday ").unwrap_err();
/// assert_eq!(error, ScenarioTitleError::SurroundingWhiteSpace { input: "Black Friday ".to_owned() });
/// assert_eq!(error.to_string(), "scenario title \"Black Friday \" starts or ends with white space");
/// assert_eq!(ScenarioTitle::MAX_LENGTH, 2000);
/// # Ok::<(), ScenarioTitleError>(())
/// ```
///
/// A title is free text that says what a resource is for.
/// It is a part of the resource, next to its name, and never a part of the name:
/// the type that holds the resource has an optional field of the title type.
///
/// The call has three arguments:
///
/// - The type of the title. It checks the [rule for titles](crate::ResourceTitle#the-rule-for-titles)
///   in `new`, in `FromStr` and in `TryFrom`, and keeps the text as it is.
///   Its `MAX_LENGTH` is the most characters a title has.
///   It compares, sorts and hashes as its text.
///   With the `serde` feature of the calling crate, it serializes as its text.
/// - The type of its error, an enum with one variant for each part of the rule.
/// - The label: how a message and the documentation call the title, such as `scenario title`.
///   An empty title then reads `scenario title "" is empty`.
///
/// The macro writes the documentation of both types from the label, with the rule in the words of the kind.
/// That documentation names neither this crate nor a type of it:
/// the title of a kind shows nothing of what it is built on.
///
/// Doc comments in front of the arguments join the documentation of the title, after its first line.
/// They say what only this kind has to say:
///
/// ```
/// gaslightmq_resource::title! {
///     /// A captured scenario gets a title that says when someone captured it.
///     ScenarioTitle, ScenarioTitleError, "scenario title"
/// }
/// # assert!(ScenarioTitle::new("Captured on Black Friday").is_ok());
/// ```
///
/// # What the calling crate needs
///
/// The error derives its message, so the calling crate depends on `thiserror`.
/// The `serde` form stands behind `#[cfg(feature = "serde")]`, which the calling crate decides:
/// it has a `serde` feature that turns on its optional dependency `serde`.
#[macro_export]
macro_rules! title {
    (
        $(#[$meta:meta])*
        $title:ident, $error:ident, $label:literal $(,)?
    ) => {
        #[doc = concat!("A valid ", $label, ".")]
        ///
        $(#[$meta])*
        ///
        /// A title is free text that says what a thing is for.
        /// Nothing references a thing by its title, so a title changes freely, and two things can have the same one.
        ///
        /// # The rule for titles
        ///
        #[doc = concat!("Every ", $label, " has at least one character, and at most [`MAX_LENGTH`](Self::MAX_LENGTH), which is 2000.")]
        /// The length counts characters, and not bytes.
        /// A title does not start or end with white space, and it holds no control character such as a line break.
        /// It holds no format character either, such as a zero-width space or a mark that turns the text direction.
        /// Nobody sees those.
        ///
        /// A title is exact: upper and lower case differ.
        /// With the `serde` feature, it serializes as its text.
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $title($crate::ResourceTitle);

        impl $title {
            /// The most characters a title has.
            pub const MAX_LENGTH: usize = $crate::ResourceTitle::MAX_LENGTH;

            /// Checks the [rule for titles](Self#the-rule-for-titles), and keeps the text as it is.
            ///
            /// # Errors
            ///
            /// The error of the title, which says which part of the rule the text breaks:
            /// the text is empty or too long, starts or ends with white space,
            /// or holds a control character or a format character.
            /// A text that breaks several parts gets the first of them, in this order.
            pub fn new(title: impl ::core::convert::AsRef<str>) -> ::core::result::Result<Self, $error> {
                match $crate::ResourceTitle::new(title) {
                    Ok(title) => Ok(Self(title)),
                    Err($crate::ResourceTitleError::Empty) => Err($error::Empty),
                    Err($crate::ResourceTitleError::TooLong { length }) => Err($error::TooLong { length }),
                    Err($crate::ResourceTitleError::SurroundingWhiteSpace { input }) => {
                        Err($error::SurroundingWhiteSpace { input })
                    }
                    Err($crate::ResourceTitleError::ControlCharacter { input }) => {
                        Err($error::ControlCharacter { input })
                    }
                    Err($crate::ResourceTitleError::FormatCharacter { input }) => {
                        Err($error::FormatCharacter { input })
                    }
                }
            }

            /// The title as text.
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl ::core::fmt::Debug for $title {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($title)).field(&self.as_str()).finish()
            }
        }

        impl ::core::str::FromStr for $title {
            type Err = $error;

            fn from_str(text: &str) -> ::core::result::Result<Self, Self::Err> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<&str> for $title {
            type Error = $error;

            fn try_from(text: &str) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::TryFrom<::std::string::String> for $title {
            type Error = $error;

            fn try_from(text: ::std::string::String) -> ::core::result::Result<Self, Self::Error> {
                Self::new(text)
            }
        }

        impl ::core::convert::From<$title> for ::std::string::String {
            fn from(title: $title) -> Self {
                title.0.into()
            }
        }

        impl ::core::fmt::Display for $title {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.pad(self.as_str())
            }
        }

        impl ::core::convert::AsRef<str> for $title {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::core::cmp::PartialEq<str> for $title {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl ::core::cmp::PartialEq<&str> for $title {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }

        #[cfg(feature = "serde")]
        impl ::serde::Serialize for $title {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> ::serde::Deserialize<'de> for $title {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                let text = <::std::string::String as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::new(text).map_err(::serde::de::Error::custom)
            }
        }

        #[doc = concat!(
            "Why a text is not a [`", stringify!($title), "`]: the part of the [rule for titles](",
            stringify!($title), "#the-rule-for-titles) it breaks."
        )]
        ///
        /// The rule has five parts, and the error has one variant for each.
        #[derive(Debug, Clone, PartialEq, Eq, ::thiserror::Error)]
        pub enum $error {
            /// The text has no character.
            #[error("{} \"\" is empty", $label)]
            Empty,
            /// The text has `length` characters, which is more than a title has.
            #[error("{} of {length} characters is longer than {} characters", $label, $title::MAX_LENGTH)]
            TooLong { length: usize },
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
        for text in ["black-friday", "x", "7", "a--b", "2nd-produce"] {
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
        use ScenarioNameError::{Empty, InvalidCharacter, SurroundingHyphen, TooLong};

        let input = |text: &str| text.to_owned();
        let long = "a".repeat(ScenarioName::MAX_LENGTH + 1);
        let cases = [
            ("", Empty, "scenario name \"\" is empty"),
            (
                "Black Friday",
                InvalidCharacter {
                    input: input("Black Friday"),
                    character: 'B',
                    position: 0,
                },
                "scenario name \"Black Friday\" holds 'B' at position 0, which is no lowercase letter, digit or hyphen",
            ),
            (
                "black-friday-",
                SurroundingHyphen {
                    input: input("black-friday-"),
                },
                "scenario name \"black-friday-\" starts or ends with a hyphen",
            ),
            (
                long.as_str(),
                TooLong { length: 64 },
                "scenario name of 64 characters is longer than 63 characters",
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
            message("leader flap"),
            "captured scenario name \"leader flap\" holds ' ' at position 6, which is no lowercase letter, digit or hyphen"
        );
        assert_eq!(
            message("-flap"),
            "captured scenario name \"-flap\" starts or ends with a hyphen"
        );
        assert_eq!(
            message(&"a".repeat(64)),
            "captured scenario name of 64 characters is longer than 63 characters"
        );
    }

    #[test]
    fn the_name_of_a_kind_has_at_most_63_characters() {
        let longest = "a".repeat(ScenarioName::MAX_LENGTH);

        assert_eq!(ScenarioName::MAX_LENGTH, 63);
        assert_eq!(ScenarioName::new(&longest).unwrap(), *longest);
        assert!(ScenarioName::new(format!("{longest}a")).is_err());
    }

    #[test]
    fn a_map_by_the_name_of_a_kind_finds_its_entry_with_text() {
        let name = || ScenarioName::new("black-friday").unwrap();
        let sorted = BTreeMap::from([(name(), 1)]);
        let hashed = HashMap::from([(name(), 1)]);

        assert_eq!(sorted.get("black-friday"), Some(&1));
        assert_eq!(sorted.get("cyber-monday"), None);
        assert_eq!(hashed.get("black-friday"), Some(&1));
        assert_eq!(hashed.get("cyber-monday"), None);
    }

    #[test]
    fn the_name_of_a_kind_pads_like_text() {
        assert_eq!(format!("{:>8}|", ScenarioName::new("flap").unwrap()), "    flap|");
    }

    #[test]
    fn the_name_of_a_kind_shows_as_its_own_type_when_debugging() {
        assert_eq!(
            format!("{:?}", ScenarioName::new("black-friday").unwrap()),
            "ScenarioName(\"black-friday\")"
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn the_name_of_a_kind_is_its_text_in_serde() {
        let name = ScenarioName::new("black-friday").unwrap();

        assert_eq!(serde_json::to_string(&name).unwrap(), "\"black-friday\"");
        assert_eq!(serde_json::from_str::<ScenarioName>("\"black-friday\"").unwrap(), name);
        assert_eq!(
            serde_json::from_str::<ScenarioName>("\"black-friday-\"")
                .unwrap_err()
                .to_string(),
            "scenario name \"black-friday-\" starts or ends with a hyphen"
        );
    }

    crate::title!(ScenarioTitle, ScenarioTitleError, "scenario title");

    crate::title! {
        /// A captured scenario gets this title when someone captures it.
        CapturedTitle, CapturedTitleError, "captured scenario title",
    }

    #[test]
    fn the_title_of_a_kind_keeps_its_text() {
        for text in ["Black Friday", "x", "Ünïcode ✓", "a  b", "orders.* (EU)"] {
            let title = ScenarioTitle::new(text).unwrap();
            assert_eq!(title.as_str(), text);
            assert_eq!(title.to_string(), text);
            assert_eq!(title, text);
            assert_eq!(title, *text);
            assert_eq!(title.as_ref(), text);
            assert_eq!(text.parse::<ScenarioTitle>(), Ok(title.clone()));
            assert_eq!(ScenarioTitle::try_from(text), Ok(title.clone()));
            assert_eq!(ScenarioTitle::try_from(text.to_owned()), Ok(title.clone()));
            assert_eq!(String::from(title), text);
        }
    }

    #[test]
    fn the_error_of_a_title_has_a_variant_for_each_part_of_the_rule() {
        use ScenarioTitleError::{ControlCharacter, Empty, FormatCharacter, SurroundingWhiteSpace, TooLong};

        let input = |text: &str| text.to_owned();
        let long = "a".repeat(ScenarioTitle::MAX_LENGTH + 1);
        let cases = [
            ("", Empty, "scenario title \"\" is empty"),
            (
                long.as_str(),
                TooLong { length: 2001 },
                "scenario title of 2001 characters is longer than 2000 characters",
            ),
            (
                " Black Friday",
                SurroundingWhiteSpace {
                    input: input(" Black Friday"),
                },
                "scenario title \" Black Friday\" starts or ends with white space",
            ),
            (
                "Black\nFriday",
                ControlCharacter {
                    input: input("Black\nFriday"),
                },
                "scenario title \"Black\\nFriday\" holds a control character",
            ),
            (
                "Black\u{200b}Friday",
                FormatCharacter {
                    input: input("Black\u{200b}Friday"),
                },
                "scenario title \"Black\\u{200b}Friday\" holds a format character",
            ),
        ];

        for (text, error, message) in cases {
            assert_eq!(ScenarioTitle::new(text), Err(error.clone()), "{text:?}");
            assert_eq!(text.parse::<ScenarioTitle>(), Err(error.clone()), "{text:?}");
            assert_eq!(ScenarioTitle::try_from(text), Err(error.clone()), "{text:?}");
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn the_error_of_a_title_calls_the_title_by_the_label_of_its_call() {
        let message = |text: &str| CapturedTitle::new(text).unwrap_err().to_string();

        assert_eq!(message(""), "captured scenario title \"\" is empty");
        assert_eq!(
            message("Flap "),
            "captured scenario title \"Flap \" starts or ends with white space"
        );
        assert_eq!(
            message("Leader\tflap"),
            "captured scenario title \"Leader\\tflap\" holds a control character"
        );
        assert_eq!(
            message("Leader\u{feff}flap"),
            "captured scenario title \"Leader\\u{feff}flap\" holds a format character"
        );
        assert_eq!(
            message(&"é".repeat(2001)),
            "captured scenario title of 2001 characters is longer than 2000 characters"
        );
    }

    #[test]
    fn the_title_of_a_kind_has_at_most_2000_characters() {
        // Every `é` is one character of two bytes.
        let longest = "é".repeat(ScenarioTitle::MAX_LENGTH);

        assert_eq!(ScenarioTitle::MAX_LENGTH, 2000);
        assert_eq!(ScenarioTitle::new(&longest).unwrap(), *longest);
        assert_eq!(
            ScenarioTitle::new(format!("{longest}é")),
            Err(ScenarioTitleError::TooLong { length: 2001 })
        );
    }

    #[test]
    fn a_title_holds_what_no_name_holds() {
        let text = "Black Friday";

        assert!(ScenarioTitle::new(text).is_ok());
        assert!(ScenarioName::new(text).is_err());
    }

    #[test]
    fn the_title_of_a_kind_pads_like_text() {
        assert_eq!(format!("{:>8}|", ScenarioTitle::new("Flap").unwrap()), "    Flap|");
    }

    #[test]
    fn the_title_of_a_kind_shows_as_its_own_type_when_debugging() {
        assert_eq!(
            format!("{:?}", ScenarioTitle::new("Black Friday").unwrap()),
            "ScenarioTitle(\"Black Friday\")"
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn the_title_of_a_kind_is_its_text_in_serde() {
        let title = ScenarioTitle::new("Black Friday").unwrap();

        assert_eq!(serde_json::to_string(&title).unwrap(), "\"Black Friday\"");
        assert_eq!(
            serde_json::from_str::<ScenarioTitle>("\"Black Friday\"").unwrap(),
            title
        );
        assert_eq!(
            serde_json::from_str::<ScenarioTitle>("\" Black Friday\"")
                .unwrap_err()
                .to_string(),
            "scenario title \" Black Friday\" starts or ends with white space"
        );
    }
}
