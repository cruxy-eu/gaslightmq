//! Why a workspace did not do what it was asked: create, update, delete, or validate.

use gaslightmq_rule::{RuleError, RuleName};
use gaslightmq_scenario::ScenarioName;
use gaslightmq_selector::SelectorSetError;

use crate::{DefinitionName, Revision};

/// One thing that keeps the definitions of a workspace from fitting each other.
///
/// A workspace holds together as a whole, so one look can find many of these.
/// Each says which definition it is about, with its kind.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let rule: RuleName = "slow-order-producers".parse()?;
/// let selector: SelectorName = "order-producers".parse().expect("a selector name");
/// let error = WorkspaceError::UnknownReference { definition: rule.into(), reference: selector.into() };
/// assert_eq!(
///     error.to_string(),
///     "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist"
/// );
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkspaceError {
    /// A definition refers to a name that no definition of that kind has:
    /// the selector that a selector includes, the selector or the preset of a rule,
    /// a rule, a selector or a preset that a scenario names,
    /// or the selector of a row or the preset of a column of a pad layout.
    #[error("{definition} refers to {reference}, which does not exist")]
    UnknownReference {
        definition: DefinitionName,
        reference: DefinitionName,
    },
    /// The selectors do not hold together: they include each other in a circle.
    #[error("{error}")]
    InvalidSelectors { error: SelectorSetError },
    /// Two rules have the same name: a rule of the workspace and a rule that a scenario owns, or rules that two
    /// scenarios own. Rule names are unique among all rules, so a name would not say which rule it means.
    /// `holders` names the definitions that hold them, the rule of the workspace first.
    #[error("{} each hold a rule named {:?}", all(.holders), .rule.as_str())]
    SharedRuleName {
        rule: RuleName,
        holders: Vec<DefinitionName>,
    },
    /// A rule of the workspace names what exists, and could still never work. The error names the rule.
    #[error("{error}")]
    InvalidRule { error: RuleError },
    /// A rule that a scenario owns names what exists, and could still never work. The error names the rule.
    #[error("scenario {:?}: {error}", .scenario.as_str())]
    InvalidOwnedRule { scenario: ScenarioName, error: RuleError },
}

/// Why a workspace did not [create](crate::Workspace::create) a definition.
///
/// A create that fails changed nothing.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let name: PresetName = "blackhole".parse()?;
/// let error = CreateError::NameTaken { name: name.into() };
/// assert_eq!(error.to_string(), "preset \"blackhole\" exists already");
/// # Ok::<(), PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CreateError {
    /// A definition of this kind has the name already. Creating never overwrites it.
    #[error("{name} exists already")]
    NameTaken { name: DefinitionName },
    /// The definitions would not fit each other with this one. `errors` holds everything that would be wrong.
    #[error("{name} does not fit the workspace: {}", each(.errors))]
    DoesNotFit {
        name: DefinitionName,
        errors: Vec<WorkspaceError>,
    },
    /// Someone changed where the store would keep the definition, since the store read it, and the store wrote
    /// nothing. A [reload](crate::Workspace::reload) takes the change in. `reason` is what the store said changed.
    #[error("{name} was not created, because {reason}")]
    ChangedOutside { name: DefinitionName, reason: String },
    /// A store could not read its definitions, or could not keep this one.
    /// `reason` is what the store said, after the store that could not read.
    #[error("{name} was not created: {reason}")]
    StoreFailed { name: DefinitionName, reason: String },
}

/// Why a workspace did not [update](crate::Workspace::update) a definition.
///
/// An update that fails changed nothing.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let name: PresetName = "blackhole".parse()?;
/// let error = UpdateError::NotFound { name: name.into() };
/// assert_eq!(error.to_string(), "preset \"blackhole\" does not exist");
/// # Ok::<(), PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpdateError {
    /// No definition of this kind has the name. Updating never creates one.
    #[error("{name} does not exist")]
    NotFound { name: DefinitionName },
    /// The update expects another revision than the definition has: someone changed the definition since.
    /// `revision` is the one it has now.
    #[error("{name} changed since: it has revision {revision} now")]
    Changed { name: DefinitionName, revision: Revision },
    /// The definitions would not fit each other with the change. `errors` holds everything that would be wrong:
    /// with the definition itself, and with the definitions that use it.
    #[error("{name} does not fit the workspace: {}", each(.errors))]
    DoesNotFit {
        name: DefinitionName,
        errors: Vec<WorkspaceError>,
    },
    /// Someone changed where the store keeps the definition, since the store read it, and the store wrote nothing.
    /// A [reload](crate::Workspace::reload) takes the change in. `reason` is what the store said changed.
    #[error("{name} was not updated, because {reason}")]
    ChangedOutside { name: DefinitionName, reason: String },
    /// A store could not read its definitions, or could not keep the change.
    /// `reason` is what the store said, after the store that could not read.
    #[error("{name} was not updated: {reason}")]
    StoreFailed { name: DefinitionName, reason: String },
}

/// Why a workspace did not [delete](crate::Workspace::delete) a definition.
///
/// A delete that fails changed nothing.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let selector: SelectorName = "order-producers".parse()?;
/// let rule: RuleName = "slow-order-producers".parse().expect("a rule name");
/// let error = DeleteError::InUse { name: selector.into(), users: vec![rule.into()] };
/// assert_eq!(error.to_string(), "selector \"order-producers\" is used by rule \"slow-order-producers\"");
/// # Ok::<(), SelectorNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeleteError {
    /// No definition of this kind has the name.
    #[error("{name} does not exist")]
    NotFound { name: DefinitionName },
    /// The delete expects another revision than the definition has: someone changed the definition since.
    /// `revision` is the one it has now.
    #[error("{name} changed since: it has revision {revision} now")]
    Changed { name: DefinitionName, revision: Revision },
    /// Other definitions refer to this one, and would refer to nothing without it. `users` names each of them.
    #[error("{name} is used by {}", all(.users))]
    InUse {
        name: DefinitionName,
        users: Vec<DefinitionName>,
    },
    /// Someone changed where the store keeps the definition, since the store read it, and the store wrote nothing.
    /// A [reload](crate::Workspace::reload) takes the change in. `reason` is what the store said changed.
    #[error("{name} was not deleted, because {reason}")]
    ChangedOutside { name: DefinitionName, reason: String },
    /// A store could not read its definitions, or could not remove this one.
    /// `reason` is what the store said, after the store that could not read.
    #[error("{name} was not deleted: {reason}")]
    StoreFailed { name: DefinitionName, reason: String },
}

/// Why the definitions of a workspace are [not valid](crate::Workspace::validate), or could not be looked at.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let rule: RuleName = "slow-order-producers".parse()?;
/// let preset: PresetName = "delay-1s".parse().expect("a preset name");
/// let error = ValidationError::Invalid {
///     errors: vec![WorkspaceError::UnknownReference { definition: rule.into(), reference: preset.into() }],
/// };
/// assert_eq!(
///     error.to_string(),
///     "the definitions do not fit each other: \
///      rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist"
/// );
/// # Ok::<(), RuleNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// The definitions do not fit each other. `errors` holds everything that is wrong, and there is at least one.
    #[error("the definitions do not fit each other: {}", each(.errors))]
    Invalid { errors: Vec<WorkspaceError> },
    /// A store could not read its definitions, so nobody knows whether they fit.
    /// `reason` names the store, and what it said.
    #[error("the definitions were not validated: {reason}")]
    StoreFailed { reason: String },
}

/// Why a workspace did not [reload](crate::Workspace::reload) what its stores hold.
///
/// A reload that fails changed nothing: the workspace and its stores hold what they held before.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// let error = ReloadError::StoreFailed {
///     reason: "the rule store could not be read again: rules/slow.yaml: [0].trigger.every: \
///              a count is at least one".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the definitions were not reloaded: the rule store could not be read again: \
///      rules/slow.yaml: [0].trigger.every: a count is at least one"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReloadError {
    /// What the stores read again does not fit together. `errors` holds everything that is wrong,
    /// and there is at least one.
    #[error("the definitions that the stores read again do not fit each other: {}", each(.errors))]
    Invalid { errors: Vec<WorkspaceError> },
    /// A store could not read again, or what it read does not read.
    /// `reason` names every store that failed, and what each one said.
    #[error("the definitions were not reloaded: {reason}")]
    StoreFailed { reason: String },
}

/// Why a workspace did not [save](crate::Workspace::save) the changes that its stores hold.
///
/// A save checks every store before any writes, so a change that someone made outside holds up the whole save,
/// and nothing is written.
///
/// ```
/// use gaslightmq_workspace::prelude::*;
///
/// let error = SaveError::ChangedOutside {
///     reason: "selectors.yaml changed since the store read it; rules/slow.yaml is gone since the store read it"
///         .to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the workspace saved nothing, because selectors.yaml changed since the store read it; \
///      rules/slow.yaml is gone since the store read it"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SaveError {
    /// Someone changed a place that the save would write, since the store read it, and no store wrote anything.
    /// A [reload](crate::Workspace::reload) takes the change in. `reason` names every place that changed, and how.
    #[error("the workspace saved nothing, because {reason}")]
    ChangedOutside { reason: String },
    /// A store could not check or write its changes. `reason` names every store that failed, what it said,
    /// and what it wrote before. What a store did not write, it still holds unsaved.
    #[error("the workspace was not saved: {reason}")]
    StoreFailed { reason: String },
}

/// Every error in one line, with `; ` between them.
fn each(errors: &[WorkspaceError]) -> String {
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    messages.join("; ")
}

/// The definitions that use another one, as a sentence lists them: `a`, `a and b`, or `a, b and c`.
fn all(users: &[DefinitionName]) -> String {
    let names: Vec<String> = users.iter().map(ToString::to_string).collect();
    match names.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first @ .., last] => format!("{} and {last}", first.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use gaslightmq_lie::LieKind;
    use gaslightmq_rule::{PresetName, RuleName};
    use gaslightmq_selector::SelectorName;

    use super::*;

    fn rule(name: &str) -> DefinitionName {
        name.parse::<RuleName>().unwrap().into()
    }

    fn selector(name: &str) -> DefinitionName {
        name.parse::<SelectorName>().unwrap().into()
    }

    fn preset(name: &str) -> DefinitionName {
        name.parse::<PresetName>().unwrap().into()
    }

    fn scenario(name: &str) -> DefinitionName {
        name.parse::<ScenarioName>().unwrap().into()
    }

    /// The rule and the selector of one reference that names nothing.
    fn unknown_selector() -> WorkspaceError {
        WorkspaceError::UnknownReference {
            definition: rule("slow-order-producers"),
            reference: selector("order-producers"),
        }
    }

    #[test]
    fn a_workspace_error_says_everything_in_one_message() {
        let circle = SelectorSetError::IncludeCycle {
            path: vec!["fetches".parse().unwrap(), "fetches".parse().unwrap()],
        };
        let never_fires = RuleError::LieBeforeTruth {
            rule: "blackhole-what-worked".parse().unwrap(),
            lie: LieKind::Silence,
        };
        let cases = [
            (
                unknown_selector(),
                "rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist".to_owned(),
            ),
            (
                WorkspaceError::InvalidSelectors { error: circle.clone() },
                circle.to_string(),
            ),
            (
                WorkspaceError::InvalidRule {
                    error: never_fires.clone(),
                },
                never_fires.to_string(),
            ),
            (
                WorkspaceError::SharedRuleName {
                    rule: "blackhole-metadata".parse().unwrap(),
                    holders: vec![rule("blackhole-metadata"), scenario("quiet-metadata")],
                },
                "rule \"blackhole-metadata\" and scenario \"quiet-metadata\" each hold a rule named \"blackhole-metadata\""
                    .to_owned(),
            ),
            (
                WorkspaceError::InvalidOwnedRule {
                    scenario: "black-friday".parse().unwrap(),
                    error: never_fires.clone(),
                },
                format!("scenario \"black-friday\": {never_fires}"),
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }

    #[test]
    fn a_definition_that_does_not_fit_says_everything_that_is_wrong_in_one_line() {
        let unknown_preset = WorkspaceError::UnknownReference {
            definition: rule("slow-order-producers"),
            reference: preset("delay-1s"),
        };
        let message = "rule \"slow-order-producers\" does not fit the workspace: \
                       rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist; \
                       rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist";

        let created = CreateError::DoesNotFit {
            name: rule("slow-order-producers"),
            errors: vec![unknown_selector(), unknown_preset.clone()],
        };
        let updated = UpdateError::DoesNotFit {
            name: rule("slow-order-producers"),
            errors: vec![unknown_selector(), unknown_preset],
        };

        assert_eq!(created.to_string(), message);
        assert_eq!(updated.to_string(), message);
        assert!(created.source().is_none());
        assert!(updated.source().is_none());
    }

    #[test]
    fn a_delete_error_lists_who_uses_the_definition() {
        let in_use = |users: &[DefinitionName]| DeleteError::InUse {
            name: selector("order-producers"),
            users: users.to_vec(),
        };

        assert_eq!(
            in_use(&[rule("slow-order-producers")]).to_string(),
            "selector \"order-producers\" is used by rule \"slow-order-producers\""
        );
        assert_eq!(
            in_use(&[selector("from-ingest"), rule("slow"), rule("flaky")]).to_string(),
            "selector \"order-producers\" is used by selector \"from-ingest\", rule \"slow\" and rule \"flaky\""
        );
    }

    #[test]
    fn an_error_about_a_store_writes_what_the_store_said_once() {
        let reason = "the selector store could not be read: selectors.yaml: permission denied";
        let name = rule("slow-order-producers");

        let created = CreateError::StoreFailed {
            name: name.clone(),
            reason: reason.to_owned(),
        };
        let updated = UpdateError::StoreFailed {
            name: name.clone(),
            reason: reason.to_owned(),
        };
        let deleted = DeleteError::StoreFailed {
            name,
            reason: reason.to_owned(),
        };
        let validated = ValidationError::StoreFailed {
            reason: reason.to_owned(),
        };
        let reloaded = ReloadError::StoreFailed {
            reason: reason.to_owned(),
        };

        assert_eq!(
            created.to_string(),
            format!("rule \"slow-order-producers\" was not created: {reason}")
        );
        assert_eq!(
            updated.to_string(),
            format!("rule \"slow-order-producers\" was not updated: {reason}")
        );
        assert_eq!(
            deleted.to_string(),
            format!("rule \"slow-order-producers\" was not deleted: {reason}")
        );
        assert_eq!(
            validated.to_string(),
            format!("the definitions were not validated: {reason}")
        );
        assert_eq!(
            reloaded.to_string(),
            format!("the definitions were not reloaded: {reason}")
        );
    }

    #[test]
    fn a_change_over_an_edit_that_nobody_read_says_what_changed() {
        let name = selector("order-producers");
        let reason = "selectors.yaml changed since the store read it";

        let created = CreateError::ChangedOutside {
            name: name.clone(),
            reason: reason.to_owned(),
        };
        let updated = UpdateError::ChangedOutside {
            name: name.clone(),
            reason: reason.to_owned(),
        };
        let deleted = DeleteError::ChangedOutside {
            name,
            reason: reason.to_owned(),
        };

        assert_eq!(
            created.to_string(),
            format!("selector \"order-producers\" was not created, because {reason}")
        );
        assert_eq!(
            updated.to_string(),
            format!("selector \"order-producers\" was not updated, because {reason}")
        );
        assert_eq!(
            deleted.to_string(),
            format!("selector \"order-producers\" was not deleted, because {reason}")
        );
    }

    #[test]
    fn a_reload_that_does_not_fit_says_everything_that_is_wrong_in_one_line() {
        let error = ReloadError::Invalid {
            errors: vec![unknown_selector()],
        };

        assert_eq!(
            error.to_string(),
            "the definitions that the stores read again do not fit each other: \
             rule \"slow-order-producers\" refers to selector \"order-producers\", which does not exist"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn a_definition_that_changed_says_the_revision_it_has_now() {
        let name = rule("slow-order-producers");
        let revision = Revision::new(9);
        let message = "rule \"slow-order-producers\" changed since: it has revision 9 now";

        let updated = UpdateError::Changed {
            name: name.clone(),
            revision,
        };
        let deleted = DeleteError::Changed { name, revision };

        assert_eq!(updated.to_string(), message);
        assert_eq!(deleted.to_string(), message);
    }

    #[test]
    fn a_name_that_is_taken_or_missing_names_the_definition_with_its_kind() {
        let name = preset("blackhole");

        assert_eq!(
            CreateError::NameTaken { name: name.clone() }.to_string(),
            "preset \"blackhole\" exists already"
        );
        assert_eq!(
            UpdateError::NotFound { name: name.clone() }.to_string(),
            "preset \"blackhole\" does not exist"
        );
        assert_eq!(
            DeleteError::NotFound { name }.to_string(),
            "preset \"blackhole\" does not exist"
        );
    }
}
