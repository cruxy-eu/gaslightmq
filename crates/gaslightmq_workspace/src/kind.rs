//! The kinds of definitions that a workspace holds, and what it needs of each.

use gaslightmq_definition_api::Definition;
use gaslightmq_environment::{Environment, EnvironmentUuid};
use gaslightmq_rule::{Preset, PresetUuid, Rule, RuleUuid};
use gaslightmq_selector::{Selector, SelectorUuid};
use gaslightmq_surface::{LayoutUuid, PadLayout};

use crate::{DefinitionName, UuidSeed};

/// A definition of a kind that a [`Workspace`](crate::Workspace) holds:
/// a selector, a preset, a rule, a pad layout or an environment.
///
/// Whoever creates or updates a definition hands the definition itself over, with the type of its own crate.
/// The workspace knows from that type which store the definition belongs to,
/// and the name of the definition is a [`DefinitionName`] of its kind.
/// Nothing outside this crate implements the trait.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// fn held_by_a_workspace<D: WorkspaceDefinition>() {}
///
/// held_by_a_workspace::<Selector>();
/// held_by_a_workspace::<Rule>();
/// ```
pub trait WorkspaceDefinition: Definition<Name: Into<DefinitionName>> + Kind {}

/// What a workspace needs of a kind, next to its name: its UUID, and its place among the other kinds.
pub trait Kind: Sized {
    /// The UUID of a definition of this kind.
    type Uuid: Copy;

    /// The definition, as one of any kind.
    fn into_any(self) -> AnyDefinition;

    /// The definition of this kind that `definition` is, or `None` when it is of another kind.
    fn of(definition: &AnyDefinition) -> Option<&Self>;

    /// The UUID of the definition, when it has one.
    fn uuid(&self) -> Option<Self::Uuid>;

    /// The same definition with this UUID.
    #[must_use]
    fn with_uuid(self, uuid: Self::Uuid) -> Self;

    /// The UUID that `seed` makes.
    fn uuid_of(seed: UuidSeed) -> Self::Uuid;
}

/// One definition of a workspace, of any kind.
///
/// Each kind keeps the type of its own crate, and this type only says which kind it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyDefinition {
    Selector(Selector),
    Preset(Preset),
    Rule(Rule),
    Layout(PadLayout),
    Environment(Environment),
}

/// Makes a definition type a kind of the workspace: the variant it has, and the UUID it takes.
macro_rules! kind {
    ($($definition:ident, $uuid:ident => $variant:ident;)*) => {
        $(
            impl WorkspaceDefinition for $definition {}

            impl Kind for $definition {
                type Uuid = $uuid;

                fn into_any(self) -> AnyDefinition {
                    AnyDefinition::$variant(self)
                }

                fn of(definition: &AnyDefinition) -> Option<&Self> {
                    match definition {
                        AnyDefinition::$variant(definition) => Some(definition),
                        _ => None,
                    }
                }

                fn uuid(&self) -> Option<$uuid> {
                    $definition::uuid(self)
                }

                fn with_uuid(self, uuid: $uuid) -> Self {
                    $definition::with_uuid(self, uuid)
                }

                fn uuid_of(seed: UuidSeed) -> $uuid {
                    $uuid::new(seed.unix_millis(), seed.random())
                }
            }
        )*

        impl AnyDefinition {
            /// The name of the definition, with its kind.
            pub(crate) fn name(&self) -> DefinitionName {
                match self {
                    $(AnyDefinition::$variant(definition) => definition.name().clone().into(),)*
                }
            }

            /// The UUID of the definition as text, when the definition has one.
            ///
            /// Every kind has a UUID type of its own. The text is what the UUIDs of two kinds compare by.
            pub(crate) fn uuid(&self) -> Option<String> {
                match self {
                    $(AnyDefinition::$variant(definition) => definition.uuid().map(String::from),)*
                }
            }
        }
    };
}

kind! {
    Selector, SelectorUuid => Selector;
    Preset, PresetUuid => Preset;
    Rule, RuleUuid => Rule;
    PadLayout, LayoutUuid => Layout;
    Environment, EnvironmentUuid => Environment;
}

impl AnyDefinition {
    /// The definitions that this one refers to by name.
    ///
    /// An environment names its world, and no store holds worlds yet, so it refers to nothing here.
    pub(crate) fn references(&self) -> Vec<DefinitionName> {
        match self {
            AnyDefinition::Selector(selector) => selector.include().cloned().map(Into::into).into_iter().collect(),
            AnyDefinition::Rule(rule) => {
                let selector = rule.selector().name().cloned().map(DefinitionName::from);
                let preset = rule.action().preset().cloned().map(DefinitionName::from);
                selector.into_iter().chain(preset).collect()
            }
            AnyDefinition::Layout(layout) => {
                let rows = layout.rows().iter().cloned().map(DefinitionName::from);
                let columns = layout.columns().iter().cloned().map(DefinitionName::from);
                rows.chain(columns).collect()
            }
            AnyDefinition::Preset(_) | AnyDefinition::Environment(_) => Vec::new(),
        }
    }
}
