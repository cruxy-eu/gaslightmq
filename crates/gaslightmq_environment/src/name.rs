use gaslightmq_resource::{ResourceName, ResourceNameError};

use crate::text::kept_text;

/// Define a name: text that something else references a definition by, and its error.
///
/// The error has a variant for every part of the rule for names, and `what` is how its messages call the name.
macro_rules! name {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal
    ) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(ResourceName);

        impl $name {
            /// Checks the rule for names.
            ///
            /// # Errors
            ///
            /// The error of the name, which says which part of the rule the text breaks:
            /// the text is empty, starts or ends with white space, or holds a control character or a format character.
            pub fn new(name: impl AsRef<str>) -> Result<Self, $error> {
                match ResourceName::new(name) {
                    Ok(name) => Ok(Self(name)),
                    Err(ResourceNameError::Empty) => Err($error::Empty),
                    Err(ResourceNameError::SurroundingWhiteSpace { input }) => {
                        Err($error::SurroundingWhiteSpace { input })
                    }
                    Err(ResourceNameError::ControlCharacter { input }) => Err($error::ControlCharacter { input }),
                    Err(ResourceNameError::FormatCharacter { input }) => Err($error::FormatCharacter { input }),
                }
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.as_str()).finish()
            }
        }

        kept_text!($name, $error);

        #[doc = concat!("Why a text is not a [`", stringify!($name), "`]: the part of the rule for names it breaks.")]
        ///
        /// The rule has four parts, and the error has one variant for each.
        #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no character.
            #[error("{} \"\" is empty", $what)]
            Empty,
            /// The text starts or ends with white space.
            #[error("{} {input:?} starts or ends with white space", $what)]
            SurroundingWhiteSpace { input: String },
            /// The text holds a control character, such as a line break.
            #[error("{} {input:?} holds a control character", $what)]
            ControlCharacter { input: String },
            /// The text holds a format character, such as a zero-width space, which nobody sees.
            #[error("{} {input:?} holds a format character", $what)]
            FormatCharacter { input: String },
        }
    };
}

name! {
    /// The name of an environment, such as `staging-proxy`.
    ///
    /// A command names the environments it starts by this name, and so do the workspace settings,
    /// the environment switcher of the UI, and every command of the control plane on something live.
    /// A name is unique among environments, and it is exact: `Staging` and `staging` are two names.
    ///
    /// # The rule for names
    ///
    /// A name has at least one character.
    /// It does not start or end with white space, and it holds no control character such as a line break.
    /// It holds no format character either, such as a zero-width space or a mark that turns the text direction.
    /// Nobody sees those, so two names that hold them can look the same and still differ.
    ///
    /// This is the rule that the names of selectors, rules and presets follow.
    ///
    /// ```
    /// use gaslightmq_environment::prelude::*;
    ///
    /// let name: EnvironmentName = "staging-proxy".parse()?;
    /// assert_eq!(name, "staging-proxy");
    /// assert_eq!(name.to_string(), "staging-proxy");
    ///
    /// assert!(EnvironmentName::new("").is_err());
    /// assert!(EnvironmentName::new("staging-proxy ").is_err());
    /// // A zero-width space hides between the two words.
    /// assert!(EnvironmentName::new("staging\u{200b}proxy").is_err());
    /// # Ok::<(), EnvironmentNameError>(())
    /// ```
    EnvironmentName, EnvironmentNameError, "environment name"
}

name! {
    /// The name of a world definition, such as `orders-sim`.
    ///
    /// The broker of an environment names the world it answers from.
    /// The name follows [the rule for names](EnvironmentName#the-rule-for-names), like the name of an environment.
    ///
    /// An environment only names a world.
    /// Whether a world of that name exists is for whoever holds the world definitions to check.
    ///
    /// ```
    /// use gaslightmq_environment::prelude::*;
    ///
    /// let world: WorldName = "orders-sim".parse()?;
    /// assert_eq!(world, "orders-sim");
    /// assert_eq!(WorldName::new(""), Err(WorldNameError::Empty));
    /// # Ok::<(), gaslightmq_environment::WorldNameError>(())
    /// ```
    WorldName, WorldNameError, "world name"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn a_name_keeps_its_text() {
        for text in ["staging-proxy", "x", "Ünïcode ✓", "orders sim", "shop (EU)"] {
            let name = EnvironmentName::new(text).unwrap();
            assert_eq!(name.as_str(), text);
            assert_eq!(name.to_string(), text);
            assert_eq!(name, text);
            assert_eq!(name, *text);
            assert_eq!(text.parse::<EnvironmentName>(), Ok(name.clone()));
            assert_eq!(EnvironmentName::try_from(text.to_owned()), Ok(name.clone()));
            assert_eq!(String::from(name), text);
        }
    }

    #[test]
    fn a_name_that_breaks_the_rule_is_an_error() {
        let input = |text: &str| text.to_owned();

        assert_eq!(EnvironmentName::new(""), Err(EnvironmentNameError::Empty));
        assert_eq!(
            EnvironmentName::new(" shop"),
            Err(EnvironmentNameError::SurroundingWhiteSpace { input: input(" shop") })
        );
        assert_eq!(
            EnvironmentName::new("shop\n"),
            Err(EnvironmentNameError::SurroundingWhiteSpace { input: input("shop\n") })
        );
        assert_eq!(
            EnvironmentName::new("orders\tsim"),
            Err(EnvironmentNameError::ControlCharacter {
                input: input("orders\tsim")
            })
        );
        assert_eq!(
            EnvironmentName::new("orders\u{200b}sim"),
            Err(EnvironmentNameError::FormatCharacter {
                input: input("orders\u{200b}sim")
            })
        );
        assert_eq!(
            WorldName::new("\u{feff}shop"),
            Err(WorldNameError::FormatCharacter {
                input: input("\u{feff}shop")
            })
        );
    }

    #[test]
    fn the_error_says_whose_name_breaks_which_part_of_the_rule() {
        let environment = |text: &str| EnvironmentName::new(text).unwrap_err().to_string();
        let world = |text: &str| WorldName::new(text).unwrap_err().to_string();

        assert_eq!(environment(""), "environment name \"\" is empty");
        assert_eq!(
            environment("shop "),
            "environment name \"shop \" starts or ends with white space"
        );
        assert_eq!(
            environment("orders\nsim"),
            "environment name \"orders\\nsim\" holds a control character"
        );
        assert_eq!(
            world("orders\u{2060}sim"),
            "world name \"orders\\u{2060}sim\" holds a format character"
        );
        assert_eq!(world(""), "world name \"\" is empty");
    }

    #[test]
    fn a_name_shows_as_its_text_when_debugging() {
        assert_eq!(
            format!("{:?}", EnvironmentName::new("shop").unwrap()),
            "EnvironmentName(\"shop\")"
        );
        assert_eq!(format!("{:?}", WorldName::new("shop").unwrap()), "WorldName(\"shop\")");
    }

    #[test]
    fn a_name_is_exact() {
        assert_ne!(
            EnvironmentName::new("Staging").unwrap(),
            EnvironmentName::new("staging").unwrap()
        );
    }

    #[test]
    fn a_map_by_name_finds_its_entry_with_text() {
        let mut by_name = BTreeMap::new();
        by_name.insert(EnvironmentName::new("shop").unwrap(), 1);

        assert_eq!(by_name.get("shop"), Some(&1));
        assert_eq!(by_name.get("staging"), None);
    }
}
