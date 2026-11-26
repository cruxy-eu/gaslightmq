//! Why the files of a workspace do not read.

use std::path::PathBuf;

/// One file of a workspace that does not read, and why.
///
/// `file` is the path of the file below the directory of the workspace.
/// `reason` starts with the path of the key that failed, when there is one, and then says what is wrong with
/// the value there: the type of the value says that, and knows nothing about files.
///
/// ```
/// use gaslightmq_workspace_fs::prelude::*;
///
/// let error = FileError {
///     file: "rules/orders.yaml".into(),
///     reason: "[0].trigger.every: a count is at least one".to_owned(),
/// };
/// assert_eq!(error.to_string(), "rules/orders.yaml: [0].trigger.every: a count is at least one");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{}: {reason}", .file.display())]
pub struct FileError {
    /// The file, below the directory of the workspace.
    pub file: PathBuf,
    /// What is wrong in it, after the path of the key that failed.
    pub reason: String,
}

/// Why the [stores of a workspace](crate::FileStores) did not open: its files do not read.
///
/// A process does not start on a workspace that does not read, so the error holds every file that does not,
/// and says all of them in one message.
///
/// ```
/// use gaslightmq_workspace_fs::prelude::*;
///
/// let error = OpenError {
///     errors: vec![
///         FileError { file: "selectors.yaml".into(), reason: "[1].uuid: unknown field `uuid`".to_owned() },
///         FileError { file: "rules/orders.yaml".into(), reason: "[0].trigger.every: a count is at least one".to_owned() },
///     ],
/// };
/// assert_eq!(
///     error.to_string(),
///     "the files of the workspace do not read: selectors.yaml: [1].uuid: unknown field `uuid`; \
///      rules/orders.yaml: [0].trigger.every: a count is at least one"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the files of the workspace do not read: {}", each(.errors))]
pub struct OpenError {
    /// Every file that does not read, in the order of the kinds and of the files. There is at least one.
    pub errors: Vec<FileError>,
}

/// Every error in one line, with `; ` between them.
pub(crate) fn each(errors: &[FileError]) -> String {
    let messages: Vec<String> = errors.iter().map(ToString::to_string).collect();
    messages.join("; ")
}
