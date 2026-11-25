//! Why a workspace did not do what it was asked: create, update, delete, or validate.

use gaslightmq_rule::RuleError;
use gaslightmq_selector::SelectorSetError;

use crate::DefinitionName;

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
    /// or the selector of a row or the preset of a column of a pad layout.
    #[error("{definition} refers to {reference}, which does not exist")]
    UnknownReference {
        definition: DefinitionName,
        reference: DefinitionName,
    },
    /// A definition has the UUID of another definition, of any kind.
    /// That happens when someone copies a definition with its UUID.
    #[error("{definition} has the UUID of {holder}")]
    SharedUuid {
        definition: DefinitionName,
        holder: DefinitionName,
    },
    /// The selectors do not hold together: they include each other in a circle.
    #[error("{error}")]
    InvalidSelectors { error: SelectorSetError },
    /// A rule names what exists, and could still never work. The error names the rule.
    #[error("{error}")]
    InvalidRule { error: RuleError },
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
    /// The definitions would not fit each other with the change. `errors` holds everything that would be wrong:
    /// with the definition itself, and with the definitions that use it.
    #[error("{name} does not fit the workspace: {}", each(.errors))]
    DoesNotFit {
        name: DefinitionName,
        errors: Vec<WorkspaceError>,
    },
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
    /// Other definitions refer to this one, and would refer to nothing without it. `users` names each of them.
    #[error("{name} is used by {}", all(.users))]
    InUse {
        name: DefinitionName,
        users: Vec<DefinitionName>,
    },
    /// A store could not read its definitions, or could not remove this one.
    /// `reason` is what the store said, after the store that could not read.
    #[error("{name} was not deleted: {reason}")]
    StoreFailed { name: DefinitionName, reason: String },
}

/// Why the definitions of a workspace are [not valid](crate::Workspace::validate), or could not be looked at.
///
/// ```
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_workspace::prelude::*;
///
/// let copy: RuleName = "blackhole-fetches".parse()?;
/// let original: RuleName = "blackhole-metadata".parse()?;
/// let error = ValidationError::Invalid {
///     errors: vec![WorkspaceError::SharedUuid { definition: copy.into(), holder: original.into() }],
/// };
/// assert_eq!(
///     error.to_string(),
///     "the definitions do not fit each other: rule \"blackhole-fetches\" has the UUID of rule \"blackhole-metadata\""
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
                WorkspaceError::SharedUuid {
                    definition: preset("blackhole"),
                    holder: selector("metadata"),
                },
                "preset \"blackhole\" has the UUID of selector \"metadata\"".to_owned(),
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
