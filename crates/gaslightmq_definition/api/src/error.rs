//! Why a store did not do what it was asked: read, create, or change.

/// Why a store has no answer to [`list`](crate::Store::list), to [`get`](crate::Store::get),
/// or to another question about what it holds.
///
/// A store that holds no definition of a name fails nothing: `get` answers `None`.
/// This error is for a store that could not look.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// let error = StoreReadError::Failed {
///     reason: "rules/slow.yaml: trigger.every: a count is at least one".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the store could not read its definitions: rules/slow.yaml: trigger.every: a count is at least one"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreReadError {
    /// The store could not read the definitions that it holds.
    /// `reason` says what stood in the way: what the store reads from did not answer, or what it holds does not read.
    #[error("the store could not read its definitions: {reason}")]
    Failed { reason: String },
}

/// Why a store did not [create](crate::Store::create) a definition.
///
/// A create that fails changed nothing.
/// Whoever asked knows which definition it handed over, so the error does not name it.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// let error = StoreCreateError::Failed { reason: "the directory is read-only".to_owned() };
/// assert_eq!(error.to_string(), "the store could not keep the definition: the directory is read-only");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreCreateError {
    /// A definition of this kind has the name already. Creating never overwrites it.
    #[error("the store holds a definition of that name already")]
    NameTaken,
    /// Someone else changed where the definition would go, since the store read it, so the store wrote nothing.
    /// `reason` says what changed, such as a file that appeared. Reading again takes the change in.
    #[error("the store wrote nothing, because {reason}")]
    ChangedOutside { reason: String },
    /// The store could not keep the definition. `reason` says what stood in the way.
    #[error("the store could not keep the definition: {reason}")]
    Failed { reason: String },
}

/// Why a store did not [update](crate::Store::update) or [delete](crate::Store::delete) a definition.
///
/// Both change a definition that the store holds, and both fail in the same three ways.
/// A change that fails changed nothing.
/// Whoever asked knows which name it handed over, so the error does not name it.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// let error = StoreChangeError::NotFound;
/// assert_eq!(error.to_string(), "the store holds no definition of that name");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreChangeError {
    /// No definition of this kind has the name. Updating never creates one.
    #[error("the store holds no definition of that name")]
    NotFound,
    /// Someone else changed where the definition is, since the store read it, so the store wrote nothing.
    /// `reason` says what changed, such as a file that someone edited. Reading again takes the change in.
    #[error("the store wrote nothing, because {reason}")]
    ChangedOutside { reason: String },
    /// The store could not change or remove the definition. `reason` says what stood in the way.
    #[error("the store could not change the definition: {reason}")]
    Failed { reason: String },
}

/// Why a store did not [check](crate::Store::check_unsaved), [stage](crate::Store::stage_unsaved) or
/// [commit](crate::Store::commit_staged) the changes that it holds.
///
/// A check or a stage that fails changed nothing: the store holds every change that it held, and writes them with
/// the next save. A commit that fails stopped on the way: what it put in its place before is saved.
///
/// ```
/// use gaslightmq_definition_api::prelude::*;
///
/// let error = StoreSaveError::ChangedOutside {
///     reason: "selectors.yaml changed since the store read it".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the store wrote nothing, because selectors.yaml changed since the store read it"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreSaveError {
    /// Someone else changed where a change would go, since the store read it, and the store wrote nothing.
    /// `reason` says every place that changed, and how.
    #[error("the store wrote nothing, because {reason}")]
    ChangedOutside { reason: String },
    /// The store could not look, write or put a change in its place. `reason` says where, and what stood in the way.
    #[error("the store could not save its changes: {reason}")]
    Failed { reason: String },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_read_error_writes_its_reason_once() {
        let error = StoreReadError::Failed {
            reason: "selectors.yaml: permission denied".to_owned(),
        };

        assert_eq!(
            error.to_string(),
            "the store could not read its definitions: selectors.yaml: permission denied"
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn a_create_error_says_everything_in_one_message() {
        let cases = [
            (
                StoreCreateError::NameTaken,
                "the store holds a definition of that name already",
            ),
            (
                StoreCreateError::ChangedOutside {
                    reason: "rules/slow.yaml appeared since the store read the rules".to_owned(),
                },
                "the store wrote nothing, because rules/slow.yaml appeared since the store read the rules",
            ),
            (
                StoreCreateError::Failed {
                    reason: "the directory is read-only".to_owned(),
                },
                "the store could not keep the definition: the directory is read-only",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }

    #[test]
    fn a_change_error_says_everything_in_one_message() {
        let cases = [
            (StoreChangeError::NotFound, "the store holds no definition of that name"),
            (
                StoreChangeError::ChangedOutside {
                    reason: "selectors.yaml changed since the store read it".to_owned(),
                },
                "the store wrote nothing, because selectors.yaml changed since the store read it",
            ),
            (
                StoreChangeError::Failed {
                    reason: "the directory is read-only".to_owned(),
                },
                "the store could not change the definition: the directory is read-only",
            ),
        ];

        for (error, message) in cases {
            assert_eq!(error.to_string(), message);
            assert!(error.source().is_none(), "{error}");
        }
    }
}
