//! The kinds of definitions that a workspace holds, and what it needs of each.

use std::fmt;

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

/// What a workspace needs of a kind, next to its name: the type of its UUID, and its place among the other kinds.
pub trait Kind: Sized {
    /// The UUID of a definition of this kind, which the workspace keeps in memory next to the name.
    type Uuid: Copy + Eq + fmt::Debug + Send + Sync;

    /// The definition, as one of any kind.
    fn into_any(self) -> AnyDefinition;

    /// The UUID of this kind, as one of any kind.
    fn any_uuid(uuid: Self::Uuid) -> AnyUuid;

    /// The UUID of this kind that `uuid` is, or `None` when it is of another kind.
    fn uuid_of(uuid: AnyUuid) -> Option<Self::Uuid>;

    /// The UUID of this kind that `seed` makes.
    fn new_uuid(seed: UuidSeed) -> Self::Uuid;
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

/// The UUID of one definition of a workspace, of any kind.
///
/// Each kind keeps the UUID type of its own crate, and this type only says which kind it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnyUuid {
    Selector(SelectorUuid),
    Preset(PresetUuid),
    Rule(RuleUuid),
    Layout(LayoutUuid),
    Environment(EnvironmentUuid),
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

                fn any_uuid(uuid: $uuid) -> AnyUuid {
                    AnyUuid::$variant(uuid)
                }

                fn uuid_of(uuid: AnyUuid) -> Option<$uuid> {
                    match uuid {
                        AnyUuid::$variant(uuid) => Some(uuid),
                        _ => None,
                    }
                }

                fn new_uuid(seed: UuidSeed) -> $uuid {
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
        }

        impl AnyUuid {
            /// The UUID that `seed` makes, of the kind of the definition that `name` names.
            pub(crate) fn new(name: &DefinitionName, seed: UuidSeed) -> Self {
                match name {
                    $(DefinitionName::$variant(_) => AnyUuid::$variant($definition::new_uuid(seed)),)*
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
