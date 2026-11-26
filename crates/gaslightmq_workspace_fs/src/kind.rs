//! The kinds of definitions that the files of a workspace hold, and where the files of each kind are.

use std::fmt;
use std::path::{Path, PathBuf};

use gaslightmq_definition_api::Definition;
use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;

/// A kind of definition that the files of a workspace hold: a selector, a preset, a rule, a scenario, a pad layout
/// or an environment.
///
/// Each kind has its place in the workspace:
///
/// | Kind | Files | A file holds |
/// |---|---|---|
/// | [`Selector`] | `selectors.yaml` | A list of selectors. |
/// | [`Preset`] | `presets.yaml` | A list of presets. |
/// | [`Rule`] | `rules/*.yaml` | A list of rules. A new rule goes to `rules/<name>.yaml`. |
/// | [`Scenario`] | `scenarios/<name>.yaml` | One scenario. |
/// | [`PadLayout`] | `layouts/<name>.yaml` | One pad layout. |
/// | [`Environment`] | `environments/<name>.yaml` | One environment. |
///
/// Nothing outside this crate implements the trait.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace_fs::prelude::*;
///
/// fn kept_in_files<D: FileDefinition>() {}
///
/// kept_in_files::<Rule>();
/// ```
pub trait FileDefinition: Definition + sealed::Placed {}

/// Where the files of a kind are, and how a definition reads and writes. Only this crate says it.
pub(crate) mod sealed {
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::Place;

    pub trait Placed:
        gaslightmq_definition_api::Definition<Name: AsRef<str>> + Serialize + DeserializeOwned + PartialEq
    {
        /// Where the files of the kind are.
        const PLACE: Place;
    }
}

/// Where the files of one kind of definition are, below the directory of the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    /// One file holds every definition of the kind, as a list: `selectors.yaml`.
    File(&'static str),
    /// A directory holds files of the kind, and each holds a list: `rules`.
    Lists(&'static str),
    /// A directory holds one file for each definition: `environments`.
    Each(&'static str),
}

impl Place {
    /// Whether a file of the kind holds a list of definitions, and not one.
    pub(crate) fn holds_lists(self) -> bool {
        !matches!(self, Self::Each(_))
    }

    /// The file that a new definition of this name goes to, below the directory of the workspace.
    pub(crate) fn file_for(self, name: &str) -> PathBuf {
        match self {
            Self::File(file) => PathBuf::from(file),
            Self::Lists(directory) | Self::Each(directory) => Path::new(directory).join(format!("{name}.yaml")),
        }
    }
}

/// Writes the file or the directory: `selectors.yaml`, or `rules/`.
impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(file) => f.write_str(file),
            Self::Lists(directory) | Self::Each(directory) => write!(f, "{directory}/"),
        }
    }
}

/// Makes a definition type a kind that the files of a workspace hold, in its place.
macro_rules! placed {
    ($($definition:ty => $place:expr;)*) => {
        $(
            impl FileDefinition for $definition {}

            impl sealed::Placed for $definition {
                const PLACE: Place = $place;
            }
        )*
    };
}

placed! {
    Selector => Place::File("selectors.yaml");
    Preset => Place::File("presets.yaml");
    Rule => Place::Lists("rules");
    Scenario => Place::Each("scenarios");
    PadLayout => Place::Each("layouts");
    Environment => Place::Each("environments");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_definition_goes_to_the_file_of_its_kind_or_of_its_name() {
        assert_eq!(
            Place::File("selectors.yaml").file_for("fetches"),
            Path::new("selectors.yaml")
        );
        assert_eq!(Place::Lists("rules").file_for("slow"), Path::new("rules/slow.yaml"));
        assert_eq!(
            Place::Each("environments").file_for("shop"),
            Path::new("environments/shop.yaml")
        );
    }
}
