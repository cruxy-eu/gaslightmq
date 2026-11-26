//! Everything that the stores hold at one moment, and whether it fits together.
//!
//! The rules of a workspace are here, as plain functions over definitions: nothing in this module waits.

use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_environment::Environment;
use gaslightmq_rule::{Preset, PresetSet, Rule, RuleName};
use gaslightmq_scenario::Scenario;
use gaslightmq_selector::{Selector, SelectorName, SelectorSet};
use gaslightmq_surface::PadLayout;

use crate::kind::AnyDefinition;
use crate::{DefinitionChange, DefinitionName, WorkspaceDefinition, WorkspaceError};

/// The definitions of every kind, as the stores listed them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Contents {
    /// In the order of their kinds, and within a kind in the order of their names.
    definitions: Vec<AnyDefinition>,
}

impl Contents {
    /// The contents of six stores.
    pub(crate) fn new(
        selectors: Vec<Selector>,
        presets: Vec<Preset>,
        rules: Vec<Rule>,
        scenarios: Vec<Scenario>,
        layouts: Vec<PadLayout>,
        environments: Vec<Environment>,
    ) -> Self {
        let mut definitions: Vec<AnyDefinition> = (selectors.into_iter().map(AnyDefinition::Selector))
            .chain(presets.into_iter().map(AnyDefinition::Preset))
            .chain(rules.into_iter().map(AnyDefinition::Rule))
            .chain(scenarios.into_iter().map(AnyDefinition::Scenario))
            .chain(layouts.into_iter().map(AnyDefinition::Layout))
            .chain(environments.into_iter().map(AnyDefinition::Environment))
            .collect();
        definitions.sort_by_cached_key(AnyDefinition::name);
        Self { definitions }
    }

    /// The definition of this kind and name.
    pub(crate) fn get(&self, name: &DefinitionName) -> Option<&AnyDefinition> {
        self.definitions.iter().find(|definition| definition.name() == *name)
    }

    /// What changed from these contents to `after`, by name, in the order of the kinds and the names.
    ///
    /// A name that only `after` has is created, a name that only these contents have is deleted,
    /// and a name whose definition differs is updated.
    pub(crate) fn changes_to(&self, after: &Contents) -> Vec<DefinitionChange> {
        let before: BTreeMap<DefinitionName, &AnyDefinition> = self
            .definitions
            .iter()
            .map(|definition| (definition.name(), definition))
            .collect();
        let now: BTreeMap<DefinitionName, &AnyDefinition> = after
            .definitions
            .iter()
            .map(|definition| (definition.name(), definition))
            .collect();
        let names: BTreeSet<&DefinitionName> = before.keys().chain(now.keys()).collect();
        names
            .into_iter()
            .filter_map(|name| match (before.get(name), now.get(name)) {
                (None, Some(_)) => Some(DefinitionChange::Created(name.clone())),
                (Some(_), None) => Some(DefinitionChange::Deleted(name.clone())),
                (Some(held), Some(read)) if held != read => Some(DefinitionChange::Updated(name.clone())),
                _ => None,
            })
            .collect()
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
    /// - A rule name that two rules have: a rule of the workspace and a rule that a scenario owns,
    ///   or rules that two scenarios own.
    /// - A rule that names what exists, and could still never work, of the workspace or of a scenario.
    ///
    /// The errors come in that order, and those of one sort in the order of the kinds and names of their definitions.
    pub(crate) fn errors(&self) -> Vec<WorkspaceError> {
        let of_definitions = (self.reference_errors().into_iter()).map(|(_, error)| error);
        let of_rules = self.rule_errors().into_iter().map(|(_, error)| error);
        of_definitions
            .chain(self.selector_errors())
            .chain(self.rule_name_errors())
            .chain(of_rules)
            .collect()
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

    /// Every definition of the kind `D`, in the order of their names.
    pub(crate) fn of<D: WorkspaceDefinition>(&self) -> impl Iterator<Item = &D> {
        self.definitions.iter().filter_map(D::of)
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

    fn scenarios(&self) -> impl Iterator<Item = &Scenario> {
        self.definitions.iter().filter_map(|definition| match definition {
            AnyDefinition::Scenario(scenario) => Some(scenario),
            _ => None,
        })
    }

    /// Every rule name that more than one rule has, with the definitions that hold those rules:
    /// the rule of the workspace, and the scenarios that own a rule of that name.
    ///
    /// A store keeps every name once within its kind, and a scenario every name of its rules once,
    /// so each definition holds a rule of one name once.
    fn rule_name_errors(&self) -> Vec<WorkspaceError> {
        let mut holders: BTreeMap<&RuleName, Vec<DefinitionName>> = BTreeMap::new();
        for rule in self.rules() {
            holders.entry(rule.name()).or_default().push(rule.name().clone().into());
        }
        for scenario in self.scenarios() {
            for rule in scenario.owned_rules() {
                holders
                    .entry(rule.name())
                    .or_default()
                    .push(scenario.name().clone().into());
            }
        }
        holders
            .into_iter()
            .filter(|(_, holders)| holders.len() > 1)
            .map(|(rule, holders)| WorkspaceError::SharedRuleName {
                rule: rule.clone(),
                holders,
            })
            .collect()
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

    /// The rules that name what exists, and could still never work, each with the name of the definition that holds
    /// it: the rule itself, or the scenario that owns it.
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
        let resolvable = |rule: &&Rule| {
            rule.selector().name().is_none_or(|name| selectors.contains(name))
                && rule.action().preset().is_none_or(|name| presets.contains(name))
        };
        let of_the_workspace = self.rules().filter(resolvable).filter_map(|rule| {
            let error = rule.resolve(&selectors, &presets).err()?;
            Some((rule.name().clone().into(), WorkspaceError::InvalidRule { error }))
        });
        let of_scenarios = self.scenarios().flat_map(|scenario| {
            scenario.owned_rules().filter(resolvable).filter_map(|rule| {
                let error = rule.resolve(&selectors, &presets).err()?;
                let invalid = WorkspaceError::InvalidOwnedRule {
                    scenario: scenario.name().clone(),
                    error,
                };
                Some((scenario.name().clone().into(), invalid))
            })
        });
        of_the_workspace.chain(of_scenarios).collect()
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
