//! The kinds of definitions that a workspace holds, and what it needs of each.

use gaslightmq_definition_api::Definition;
use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, Rule};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;

use crate::DefinitionName;

/// A definition of a kind that a [`Workspace`](crate::Workspace) holds:
/// a selector, a preset, a rule, a scenario, a pad layout or an environment.
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

/// What a workspace needs of a kind, next to its name: its place among the other kinds.
pub trait Kind: Sized {
    /// The definition, as one of any kind.
    fn into_any(self) -> AnyDefinition;
}

/// One definition of a workspace, of any kind.
///
/// Each kind keeps the type of its own crate, and this type only says which kind it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyDefinition {
    Selector(Selector),
    Preset(Preset),
    Rule(Rule),
    Scenario(Scenario),
    Layout(PadLayout),
    Environment(Environment),
}

/// Makes a definition type a kind of the workspace: the variant it has.
macro_rules! kind {
    ($($definition:ident => $variant:ident;)*) => {
        $(
            impl WorkspaceDefinition for $definition {}

            impl Kind for $definition {
                fn into_any(self) -> AnyDefinition {
                    AnyDefinition::$variant(self)
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
    };
}

kind! {
    Selector => Selector;
    Preset => Preset;
    Rule => Rule;
    Scenario => Scenario;
    PadLayout => Layout;
    Environment => Environment;
}

impl AnyDefinition {
    /// The definitions that this one refers to by name.
    ///
    /// A scenario refers to the rules of the workspace that it names, and to the selectors and the presets that
    /// the rules it owns and the conditions of its steps name. A rule that it owns is its own, and no reference.
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
            AnyDefinition::Scenario(scenario) => {
                let rules = scenario
                    .rule_references()
                    .into_iter()
                    .cloned()
                    .map(DefinitionName::from);
                let selectors = scenario
                    .selector_references()
                    .into_iter()
                    .cloned()
                    .map(DefinitionName::from);
                let presets = scenario
                    .preset_references()
                    .into_iter()
                    .cloned()
                    .map(DefinitionName::from);
                rules.chain(selectors).chain(presets).collect()
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
