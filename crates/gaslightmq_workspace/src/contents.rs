//! Everything that the stores hold at one moment, and whether it fits together.
//!
//! The rules of a workspace are here, as plain functions over definitions: nothing in this module waits.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, PresetSet, Rule};
use gaslightmq_selector::{Selector, SelectorName, SelectorSet};
use gaslightmq_surface::PadLayout;

use crate::kind::AnyDefinition;
use crate::{DefinitionName, WorkspaceError};

/// The definitions of every kind, as the stores listed them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Contents {
    /// In the order of their kinds, and within a kind in the order of their names.
    definitions: Vec<AnyDefinition>,
}

impl Contents {
    /// The contents of five stores.
    pub(crate) fn new(
        selectors: Vec<Selector>,
        presets: Vec<Preset>,
        rules: Vec<Rule>,
        layouts: Vec<PadLayout>,
        environments: Vec<Environment>,
    ) -> Self {
        let mut definitions: Vec<AnyDefinition> = (selectors.into_iter().map(AnyDefinition::Selector))
            .chain(presets.into_iter().map(AnyDefinition::Preset))
            .chain(rules.into_iter().map(AnyDefinition::Rule))
            .chain(layouts.into_iter().map(AnyDefinition::Layout))
            .chain(environments.into_iter().map(AnyDefinition::Environment))
            .collect();
        definitions.sort_by_cached_key(AnyDefinition::name);
        Self { definitions }
    }

    /// The name of every definition, with its kind, in the order of their kinds and names.
    pub(crate) fn names(&self) -> impl Iterator<Item = DefinitionName> {
        self.definitions.iter().map(AnyDefinition::name)
    }

    /// The definition of this kind and name.
    pub(crate) fn get(&self, name: &DefinitionName) -> Option<&AnyDefinition> {
        self.definitions.iter().find(|definition| definition.name() == *name)
    }

    /// What is wrong with the contents once they hold `definition`.
    ///
    /// The definition takes the place of the one of its name, or comes next to the others.
    /// The errors are those of the definition itself, whenever they began:
    /// it refers to a name that no definition has, or it is a rule that can never work.
    /// After them come the errors that the definition brings to the rest, such as a rule that no longer works with it.
    /// What was wrong with the rest before stays out: a change is refused for what it breaks, and for nothing else.
    pub(crate) fn errors_with(&self, definition: AnyDefinition) -> Vec<WorkspaceError> {
        let name = definition.name();
        let before = self.errors();
        let after = self.with(definition);
        let own = (after.reference_errors().into_iter())
            .chain(after.rule_errors())
            .filter(|(subject, _)| *subject == name)
            .map(|(_, error)| error);
        let mut errors: Vec<WorkspaceError> = own.collect();
        for error in after.errors() {
            if !before.contains(&error) && !errors.contains(&error) {
                errors.push(error);
            }
        }
        errors
    }

    /// Everything that keeps the definitions from fitting each other.
    ///
    /// - A reference to a name that no definition of that kind has.
    /// - Selectors that include each other in a circle.
    /// - A rule that names what exists, and could still never work.
    ///
    /// The errors come in that order, and those of one sort in the order of the kinds and names of their definitions.
    pub(crate) fn errors(&self) -> Vec<WorkspaceError> {
        let of_definitions = (self.reference_errors().into_iter()).map(|(_, error)| error);
        let of_rules = self.rule_errors().into_iter().map(|(_, error)| error);
        of_definitions.chain(self.selector_errors()).chain(of_rules).collect()
    }

    /// Every reference to a name that no definition of that kind has, with the definition that refers to it.
    fn reference_errors(&self) -> Vec<(DefinitionName, WorkspaceError)> {
        let names: BTreeSet<DefinitionName> = self.definitions.iter().map(AnyDefinition::name).collect();
        let mut errors = Vec::new();
        for definition in &self.definitions {
            let name = definition.name();
            for reference in definition.references() {
                if !names.contains(&reference) {
                    let error = WorkspaceError::UnknownReference {
                        definition: name.clone(),
                        reference,
                    };
                    errors.push((name.clone(), error));
                }
            }
        }
        errors
    }

    /// The same contents with `definition`, in the place of the definition of its name when there is one.
    fn with(&self, definition: AnyDefinition) -> Self {
        let name = definition.name();
        let mut definitions: Vec<AnyDefinition> = (self.definitions.iter())
            .filter(|held| held.name() != name)
            .cloned()
            .collect();
        definitions.push(definition);
        definitions.sort_by_cached_key(AnyDefinition::name);
        Self { definitions }
    }

    fn selectors(&self) -> impl Iterator<Item = &Selector> {
        self.definitions.iter().filter_map(|definition| match definition {
            AnyDefinition::Selector(selector) => Some(selector),
            _ => None,
        })
    }

    fn presets(&self) -> impl Iterator<Item = &Preset> {
        self.definitions.iter().filter_map(|definition| match definition {
            AnyDefinition::Preset(preset) => Some(preset),
            _ => None,
        })
    }

    fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.definitions.iter().filter_map(|definition| match definition {
            AnyDefinition::Rule(rule) => Some(rule),
            _ => None,
        })
    }

    /// What is wrong with the selectors as a set, other than an include that names nothing:
    /// they include each other in a circle.
    fn selector_errors(&self) -> Option<WorkspaceError> {
        let by_name: BTreeMap<&SelectorName, &Selector> =
            self.selectors().map(|selector| (selector.name(), selector)).collect();
        // An include that names nothing is an unknown reference already. Without it, the set shows the circles.
        let selectors = self.selectors().cloned().map(|selector| match selector.include() {
            Some(include) if !by_name.contains_key(include) => selector.not_including(),
            _ => selector,
        });
        SelectorSet::new(selectors)
            .err()
            .map(|error| WorkspaceError::InvalidSelectors { error })
    }

    /// The rules that name what exists, and could still never work, each with the name of its rule.
    ///
    /// A rule is resolved against the selectors that resolve themselves, and against the presets.
    /// A rule that names something else is left alone: what it names is what is wrong, and that is said already.
    fn rule_errors(&self) -> Vec<(DefinitionName, WorkspaceError)> {
        let by_name: BTreeMap<&SelectorName, &Selector> =
            self.selectors().map(|selector| (selector.name(), selector)).collect();
        let sound = self
            .selectors()
            .filter(|selector| resolves(selector, &by_name))
            .cloned();
        let (Ok(selectors), Ok(presets)) = (SelectorSet::new(sound), PresetSet::new(self.presets().cloned())) else {
            // A name is there twice, which no store allows. Nothing resolves against that.
            return Vec::new();
        };
        self.rules()
            .filter(|rule| rule.selector().name().is_none_or(|name| selectors.contains(name)))
            .filter(|rule| rule.action().preset().is_none_or(|name| presets.contains(name)))
            .filter_map(|rule| {
                let error = rule.resolve(&selectors, &presets).err()?;
                Some((rule.name().clone().into(), WorkspaceError::InvalidRule { error }))
            })
            .collect()
    }
}

/// Whether a selector resolves: everything it includes exists, down to a selector that includes nothing,
/// and no selector on the way includes one that came before.
fn resolves(selector: &Selector, by_name: &BTreeMap<&SelectorName, &Selector>) -> bool {
    let mut seen = BTreeSet::from([selector.name()]);
    let mut last = selector;
    while let Some(include) = last.include() {
        let Some(included) = by_name.get(include) else {
            return false;
        };
        if !seen.insert(included.name()) {
            return false;
        }
        last = included;
    }
    true
}
