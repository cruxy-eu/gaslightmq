//! The name of one definition of any kind.

use std::fmt;

use gaslightmq_definition_store_api::Definition;
use gaslightmq_environment::{Environment, EnvironmentName};
use gaslightmq_rule::{Preset, PresetName, Rule, RuleName};
use gaslightmq_selector::{Selector, SelectorName};
use gaslightmq_surface::{LayoutName, PadLayout};

/// The name of one definition of a workspace, of whichever kind: `rule "slow-order-producers"`.
///
/// A name is unique within its kind, so a selector and a rule can share one.
/// The errors of a workspace speak about definitions of every kind in one breath:
/// a rule that refers to a selector, a preset that a layout uses.
/// This type says which kind each name belongs to, and keeps the name of that kind as it is.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let selector = DefinitionName::from("order-producers".parse::<SelectorName>().expect("a selector name"));
/// let rule = DefinitionName::from("order-producers".parse::<RuleName>()?);
///
/// assert_ne!(selector, rule);
/// assert_eq!(selector.to_string(), "selector \"order-producers\"");
/// assert_eq!(rule.to_string(), "rule \"order-producers\"");
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DefinitionName {
    /// The name of a selector.
    Selector(SelectorName),
    /// The name of a preset.
    Preset(PresetName),
    /// The name of a rule.
    Rule(RuleName),
    /// The name of a pad layout.
    Layout(LayoutName),
    /// The name of an environment.
    Environment(EnvironmentName),
}

/// Writes the word for the kind, and the name in quotes: `rule "slow-order-producers"`.
impl fmt::Display for DefinitionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (label, name) = match self {
            DefinitionName::Selector(name) => (Selector::LABEL, name.as_str()),
            DefinitionName::Preset(name) => (Preset::LABEL, name.as_str()),
            DefinitionName::Rule(name) => (Rule::LABEL, name.as_str()),
            DefinitionName::Layout(name) => (PadLayout::LABEL, name.as_str()),
            DefinitionName::Environment(name) => (Environment::LABEL, name.as_str()),
        };
        write!(f, "{label} {name:?}")
    }
}

impl From<SelectorName> for DefinitionName {
    fn from(name: SelectorName) -> Self {
        DefinitionName::Selector(name)
    }
}

impl From<PresetName> for DefinitionName {
    fn from(name: PresetName) -> Self {
        DefinitionName::Preset(name)
    }
}

impl From<RuleName> for DefinitionName {
    fn from(name: RuleName) -> Self {
        DefinitionName::Rule(name)
    }
}

impl From<LayoutName> for DefinitionName {
    fn from(name: LayoutName) -> Self {
        DefinitionName::Layout(name)
    }
}

impl From<EnvironmentName> for DefinitionName {
    fn from(name: EnvironmentName) -> Self {
        DefinitionName::Environment(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_kinds_can_share_a_name() {
        let selector = DefinitionName::from("metadata".parse::<SelectorName>().unwrap());
        let rule = DefinitionName::from("metadata".parse::<RuleName>().unwrap());

        assert_ne!(selector, rule);
        assert_eq!(selector.to_string(), "selector \"metadata\"");
        assert_eq!(rule.to_string(), "rule \"metadata\"");
    }

    #[test]
    fn names_sort_by_their_kind_and_then_by_their_text() {
        let mut names = [
            DefinitionName::from("blackhole".parse::<RuleName>().unwrap()),
            DefinitionName::from("order-desk".parse::<LayoutName>().unwrap()),
            DefinitionName::from("metadata".parse::<SelectorName>().unwrap()),
            DefinitionName::from("orders-sim".parse::<EnvironmentName>().unwrap()),
            DefinitionName::from("blackhole".parse::<PresetName>().unwrap()),
            DefinitionName::from("fetches".parse::<SelectorName>().unwrap()),
        ];

        names.sort();

        let sorted: Vec<String> = names.iter().map(ToString::to_string).collect();
        assert_eq!(
            sorted,
            [
                "selector \"fetches\"",
                "selector \"metadata\"",
                "preset \"blackhole\"",
                "rule \"blackhole\"",
                "layout \"order-desk\"",
                "environment \"orders-sim\"",
            ]
        );
    }
}
