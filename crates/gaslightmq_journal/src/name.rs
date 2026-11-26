//! The name by which an entry names a definition, and a definition of the workspace with its kind.

use std::fmt;

gaslightmq_resource::name! {
    /// It is the name a definition had when the entry was written: a rule of an environment, or a definition of
    /// the workspace.
    ///
    /// A journal outlives the definitions it names: someone can delete a rule after the entry,
    /// and make another rule under its name.
    /// A recorded name is therefore only a name, and nobody looks it up.
    /// The entries between a creation and the next deletion are the history of one definition.
    RecordedName, RecordedNameError, "recorded name"
}

/// A definition of the workspace as an entry names it: its kind, and the name it had.
///
/// A name is unique within its kind, so a selector and a rule can share one, and an entry says which it means.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let rule = RecordedDefinition::Rule("slow-order-producers".parse()?);
/// assert_eq!(rule.name(), "slow-order-producers");
/// assert_eq!(rule.to_string(), "rule \"slow-order-producers\"");
/// assert_ne!(rule, RecordedDefinition::Selector("slow-order-producers".parse()?));
/// # Ok::<(), RecordedNameError>(())
/// ```
///
/// With the `serde` feature, it is a map with one entry: the kind, and the name. `{ "rule": "slow-order-producers" }`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub enum RecordedDefinition {
    /// A selector.
    Selector(RecordedName),
    /// A preset.
    Preset(RecordedName),
    /// A rule of the workspace.
    Rule(RecordedName),
    /// A pad layout.
    Layout(RecordedName),
    /// An environment.
    Environment(RecordedName),
}

impl RecordedDefinition {
    /// The name that the definition had.
    pub const fn name(&self) -> &RecordedName {
        match self {
            RecordedDefinition::Selector(name)
            | RecordedDefinition::Preset(name)
            | RecordedDefinition::Rule(name)
            | RecordedDefinition::Layout(name)
            | RecordedDefinition::Environment(name) => name,
        }
    }

    /// The word for the kind of the definition: `selector`, `preset`, `rule`, `layout` or `environment`.
    pub const fn kind(&self) -> &'static str {
        match self {
            RecordedDefinition::Selector(_) => "selector",
            RecordedDefinition::Preset(_) => "preset",
            RecordedDefinition::Rule(_) => "rule",
            RecordedDefinition::Layout(_) => "layout",
            RecordedDefinition::Environment(_) => "environment",
        }
    }
}

/// Writes the kind and the name in quotes: `rule "slow-order-producers"`.
impl fmt::Display for RecordedDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?}", self.kind(), self.name().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recorded_name_keeps_its_text() {
        let name = RecordedName::new("slow-order-producers").unwrap();

        assert_eq!(name.as_str(), "slow-order-producers");
        assert_eq!(name, "slow-order-producers");
        assert_eq!("slow-order-producers".parse(), Ok(name));
    }

    #[test]
    fn a_recorded_name_follows_the_rule_for_names() {
        assert_eq!(RecordedName::new(""), Err(RecordedNameError::Empty));
        assert_eq!(
            RecordedName::new("").unwrap_err().to_string(),
            "recorded name \"\" is empty"
        );
        assert!(matches!(
            RecordedName::new("Slow producers"),
            Err(RecordedNameError::InvalidCharacter { .. })
        ));
    }

    #[test]
    fn a_definition_is_named_with_its_kind() {
        let name = || RecordedName::new("orders").unwrap();
        let cases = [
            (RecordedDefinition::Selector(name()), "selector \"orders\""),
            (RecordedDefinition::Preset(name()), "preset \"orders\""),
            (RecordedDefinition::Rule(name()), "rule \"orders\""),
            (RecordedDefinition::Layout(name()), "layout \"orders\""),
            (RecordedDefinition::Environment(name()), "environment \"orders\""),
        ];

        for (definition, text) in cases {
            assert_eq!(definition.to_string(), text);
            assert_eq!(definition.name(), &name());
        }
    }
}
