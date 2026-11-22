use std::fmt;
use std::sync::Arc;

use crate::text::kept_text;

/// The part of the rule for names that a text breaks.
enum Broken {
    Empty,
    SurroundingWhiteSpace,
    ControlCharacter,
    FormatCharacter,
}

/// Checks the rule for names, which every name of a workspace follows.
///
/// A name has at least one character.
/// It does not start or end with white space, and it holds no control character and no format character.
fn check(text: &str) -> Result<(), Broken> {
    if text.is_empty() {
        Err(Broken::Empty)
    } else if text.trim() != text {
        Err(Broken::SurroundingWhiteSpace)
    } else if text.chars().any(char::is_control) {
        Err(Broken::ControlCharacter)
    } else if text.chars().any(is_format) {
        Err(Broken::FormatCharacter)
    } else {
        Ok(())
    }
}

/// Whether the character is a format character: one of the Unicode general category `Cf`, as of Unicode 16.0.
///
/// The standard library has no table of it, so the ranges stand here.
/// The names of selectors, rules and presets take the same ranges, and a test holds the two tables together.
const fn is_format(character: char) -> bool {
    matches!(
        character,
        // The soft hyphen.
        '\u{00AD}'
        // The Arabic number signs, the Arabic letter mark and the end of ayah.
        | '\u{0600}'..='\u{0605}' | '\u{061C}' | '\u{06DD}' | '\u{0890}'..='\u{0891}' | '\u{08E2}'
        // The Syriac abbreviation mark and the Mongolian vowel separator.
        | '\u{070F}' | '\u{180E}'
        // The zero-width space, non-joiner and joiner, and the left-to-right and right-to-left marks.
        | '\u{200B}'..='\u{200F}'
        // The embeddings and overrides of the text direction.
        | '\u{202A}'..='\u{202E}'
        // The word joiner and the invisible operators.
        | '\u{2060}'..='\u{2064}'
        // The isolates of the text direction, and the deprecated format characters.
        | '\u{2066}'..='\u{206F}'
        // The byte order mark, which is also the zero-width no-break space.
        | '\u{FEFF}'
        // The interlinear annotation marks.
        | '\u{FFF9}'..='\u{FFFB}'
        // The Kaithi number signs, and the format controls of Egyptian hieroglyphs, of shorthand and of music.
        | '\u{110BD}' | '\u{110CD}' | '\u{13430}'..='\u{1343F}' | '\u{1BCA0}'..='\u{1BCA3}'
        | '\u{1D173}'..='\u{1D17A}'
        // The language tag and the tag characters.
        | '\u{E0001}' | '\u{E0020}'..='\u{E007F}'
    )
}

/// Define a name: text that something else references a definition by.
macro_rules! name {
    (
        $(#[$meta:meta])*
        $name:ident, $error:ident, $what:literal
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Arc<str>);

        impl $name {
            /// Checks the rule for names.
            ///
            /// # Errors
            ///
            /// The error of the name, which says which part of the rule the text breaks:
            /// the text is empty, starts or ends with white space, or holds a control character or a format character.
            pub fn new(name: impl AsRef<str>) -> Result<Self, $error> {
                let name = name.as_ref();
                let input = || name.to_owned();
                match check(name) {
                    Ok(()) => Ok(Self(name.into())),
                    Err(Broken::Empty) => Err($error::Empty),
                    Err(Broken::SurroundingWhiteSpace) => Err($error::SurroundingWhiteSpace { input: input() }),
                    Err(Broken::ControlCharacter) => Err($error::ControlCharacter { input: input() }),
                    Err(Broken::FormatCharacter) => Err($error::FormatCharacter { input: input() }),
                }
            }
        }

        kept_text!($name, $error);

        #[doc = concat!("Why a text is not a [`", stringify!($name), "`]: the part of the rule for names it breaks.")]
        ///
        /// The rule has four parts, and the error has one variant for each.
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum $error {
            /// The text has no character.
            Empty,
            /// The text starts or ends with white space.
            SurroundingWhiteSpace { input: String },
            /// The text holds a control character, such as a line break.
            ControlCharacter { input: String },
            /// The text holds a format character, such as a zero-width space, which nobody sees.
            FormatCharacter { input: String },
        }

        impl fmt::Display for $error {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Empty => f.write_str(concat!($what, " \"\" is empty")),
                    Self::SurroundingWhiteSpace { input } => {
                        write!(f, concat!($what, " {:?} starts or ends with white space"), input)
                    }
                    Self::ControlCharacter { input } => {
                        write!(f, concat!($what, " {:?} holds a control character"), input)
                    }
                    Self::FormatCharacter { input } => {
                        write!(f, concat!($what, " {:?} holds a format character"), input)
                    }
                }
            }
        }

        impl std::error::Error for $error {}
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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
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
    /// # Ok::<(), Box<dyn std::error::Error>>(())
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
