//! The workspace as a whole: when it writes its changes, saving them, and reading its files again.

use std::fmt;

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::RuleName;
use serde::{Deserialize, Serialize};

use crate::{DefinitionRef, Operation, Revision, RuleHolder};

/// Read the files of the workspace again, and take in what someone changed there as one change.
///
/// The process does not watch the files: an edit by hand changes nothing until someone reloads.
/// A reload compares what the files hold with what the process holds, by name.
/// A definition whose content differs is updated, a name that the files no longer hold is deleted,
/// and a new name is created. A definition that did not change keeps its revision.
///
/// - **A reload is one change.** Every definition that it creates or updates has the revision of the reload,
///   which it answers with, and it applies as a whole or not at all. A reload that changes nothing takes a number
///   too.
/// - **An armed rule follows it.** A rule that is armed in an environment and whose definition changed, or what it
///   refers to, stays armed with what the files say, and keeps its counters and its budget.
///   A rule of the workspace whose definition went disarms.
/// - **What changed comes from the files.** The journal of every environment that runs records each change with
///   the surface `file`, whoever asked for the reload, and nobody undoes it.
/// - **A workspace without files has nothing to reload**, and the reload changes nothing.
/// - **A reload does not let go of changes that nobody saved, unless it says so.**
///   A workspace that [writes on save](WriteBack::OnSave) can hold changes that are not in the files yet.
///   A reload refuses with `unsaved` then, and one with `discard-unsaved` lets go of them: each comes back to what
///   the files hold, as a change of the reload.
///
/// With `serde`, it is `{ "operation": "reload-workspace" }`, and `"discard-unsaved": true` next to it for a reload
/// that lets go of what nobody saved.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let request = OperationRequest::new(RequestSurface::Cli, ReloadWorkspace { discard_unsaved: false });
/// assert_eq!(
///     serde_json::to_value(request).expect("a request has a form"),
///     serde_json::json!({ "from": { "surface": "cli" }, "operation": "reload-workspace" }),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReloadWorkspace {
    /// Whether the reload lets go of the changes that the workspace holds and nobody saved.
    /// Without it, a reload refuses while there are any.
    #[serde(rename = "discard-unsaved", default, skip_serializing_if = "std::ops::Not::not")]
    pub discard_unsaved: bool,
}

impl Operation for ReloadWorkspace {
    type Answer = Reloaded;
    type Error = ReloadWorkspaceError;
}

/// What a [reload](ReloadWorkspace) took in from the files of the workspace.
///
/// With `serde`, it is a map of its fields, and a reload that changed nothing leaves `changes` out:
/// `{ "revision": 12, "changes": [{ "updated": { "selector": "order-producers" } }] }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::selector::SelectorName;
///
/// let producers: SelectorName = "order-producers".parse().expect("a selector name");
/// let reloaded = Reloaded { revision: Revision::new(12), changes: vec![DefinitionChange::Updated(producers.into())] };
/// assert_eq!(
///     serde_json::to_value(&reloaded).expect("an answer has a form"),
///     serde_json::json!({ "revision": 12, "changes": [{ "updated": { "selector": "order-producers" } }] }),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reloaded {
    /// The revision of the reload, which every definition that it created or updated has from here on.
    pub revision: Revision,
    /// What the reload changed, in the order of the kinds and the names of the definitions.
    /// A definition that the files hold as the process held it is not among them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<DefinitionChange>,
}

/// What a [reload](ReloadWorkspace) changed in one definition, which it names with its kind.
///
/// With `serde`, it is a map with one entry: what happened, and the name of the definition.
/// `{ "created": { "rule": "slow-order-producers" } }`
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::RuleName;
///
/// let name: RuleName = "slow-order-producers".parse().expect("a rule name");
/// assert_eq!(DefinitionChange::Deleted(name.into()).to_string(), "rule \"slow-order-producers\" is gone");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefinitionChange {
    /// The files hold a definition of a name that the process did not have.
    Created(DefinitionRef),
    /// The files hold another definition under a name that the process had.
    Updated(DefinitionRef),
    /// The files no longer hold a definition of a name that the process had.
    Deleted(DefinitionRef),
}

/// Writes the name of the definition, and what happened to it: `rule "slow-order-producers" changed`.
impl fmt::Display for DefinitionChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created(name) => write!(f, "{name} is new"),
            Self::Updated(name) => write!(f, "{name} changed"),
            Self::Deleted(name) => write!(f, "{name} is gone"),
        }
    }
}

/// Why the control plane did not [reload](ReloadWorkspace) the workspace.
///
/// A reload that fails changed nothing: the process holds what it held, and every armed rule stays as it is.
///
/// | Variant | What to do about it |
/// |---|---|
/// | [`NameTaken`](Self::NameTaken) | Delete the live rule first, or give the rule of the files another name. |
/// | [`Unsaved`](Self::Unsaved) | [Save](SaveWorkspace) first, or reload again with `discard-unsaved`. |
/// | [`LiveRuleDoesNotFit`](Self::LiveRuleDoesNotFit) | Disarm or delete the live rule first, or bring back in the files what it uses. |
/// | [`DoesNotFit`](Self::DoesNotFit), [`StoreFailed`](Self::StoreFailed) | Fix the files, and reload again: `reason` says what is wrong. |
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = ReloadWorkspaceError::DoesNotFit {
///     reason: "rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the files of the workspace do not fit each other: \
///      rule \"slow-order-producers\" refers to preset \"delay-1s\", which does not exist",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ReloadWorkspaceError {
    /// The files hold a rule, of the workspace or of a scenario, with the name of a live rule.
    /// A rule of the workspace is in every environment, so it never stands next to a live rule of its name,
    /// and rule names are unique among all rules. `holder` names the environment of the live rule.
    #[error("the workspace was not reloaded: the files hold a rule {:?}, and {}", .rule.as_str(), holds(.holder))]
    NameTaken { rule: RuleName, holder: RuleHolder },
    /// A live rule that is armed in the environment would not resolve against what the files hold:
    /// they no longer have the selector or the preset that it uses. `reason` says what is wrong with it.
    #[error(
        "the workspace was not reloaded: rule {:?}, which is armed in environment {:?}, would not fit it: {reason}",
        .rule.as_str(),
        .environment.as_str()
    )]
    LiveRuleDoesNotFit {
        environment: EnvironmentName,
        rule: RuleName,
        reason: String,
    },
    /// The workspace writes on save, and holds changes of these definitions that nobody saved.
    /// The reload would let go of them, and does not without `discard-unsaved`. There is at least one.
    #[error("the workspace was not reloaded: {} changed, and nobody saved it", all(.definitions))]
    Unsaved { definitions: Vec<DefinitionRef> },
    /// The files read, and the definitions in them do not fit each other. `reason` says everything that is wrong.
    #[error("the files of the workspace do not fit each other: {reason}")]
    DoesNotFit { reason: String },
    /// A file of the workspace does not read, or a store could not read again.
    /// `reason` names every file that does not read, with the path of its key that failed.
    #[error("the workspace was not reloaded: {reason}")]
    StoreFailed { reason: String },
}

/// What holds the other rule, as the rest of a sentence: `environment "canary-proxy" has a live rule of that name`.
fn holds(holder: &RuleHolder) -> String {
    match holder {
        RuleHolder::Environment(environment) => {
            format!("environment {:?} has a live rule of that name", environment.as_str())
        }
        RuleHolder::Scenario(scenario) => format!("scenario {:?} owns a rule of that name", scenario.as_str()),
    }
}

/// The definitions as a sentence lists them: `a`, `a and b`, or `a, b and c`.
fn all(definitions: &[DefinitionRef]) -> String {
    let names: Vec<String> = definitions.iter().map(ToString::to_string).collect();
    match names.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first @ .., last] => format!("{} and {last}", first.join(", ")),
    }
}

/// When the workspace writes a change of a definition to its files.
///
/// Whoever starts the process picks one, with `write_back` of its configuration.
/// The workspace writes each change until someone says otherwise.
///
/// With `serde`, it is the name in kebab case: `each-change` or `on-save`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WriteBack {
    /// Every create, update and delete writes the file of its definition before it answers.
    #[default]
    EachChange,
    /// A change stays in memory, and one [save](SaveWorkspace) writes every change in one go.
    OnSave,
}

/// The workspace as a whole: when it writes its changes, whether it takes any, and what nobody saved yet.
///
/// The [state of the process](crate::ProcessState) holds it, and a
/// [`workspace-changed`](crate::StreamMessage::WorkspaceChanged) says what it is after a change.
///
/// With `serde`, it is a map of its fields. A workspace that takes changes leaves `read-only` out, and one with
/// nothing unsaved leaves `unsaved` out: `{ "write-back": "on-save", "unsaved": [{ "selector": "order-producers" }] }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::selector::SelectorName;
///
/// let producers: SelectorName = "order-producers".parse().expect("a selector name");
/// let workspace = WorkspaceStatus { write_back: WriteBack::OnSave, read_only: false, unsaved: vec![producers.into()] };
/// assert_eq!(
///     serde_json::to_value(&workspace).expect("a state has a form"),
///     serde_json::json!({ "write-back": "on-save", "unsaved": [{ "selector": "order-producers" }] }),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceStatus {
    /// When the workspace writes a change of a definition to its files.
    #[serde(rename = "write-back")]
    pub write_back: WriteBack,
    /// Whether the workspace is read-only: the process reads its files, and reads them again, and refuses every
    /// change of a definition. Whoever starts the process says so.
    #[serde(rename = "read-only", default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// Every definition that someone created, updated or deleted since the last save, in the order of the kinds and
    /// the names. A workspace that writes each change has none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsaved: Vec<DefinitionRef>,
}

/// Write every change that the workspace holds and nobody saved yet, in one go.
///
/// Only a workspace that [writes on save](WriteBack::OnSave) holds such changes.
///
/// - **The save checks every file that it would write before it writes one.**
///   When someone edited one of them since the process read it, the save fails with `changed-outside`,
///   names every such file, and writes nothing. A [reload](ReloadWorkspace) with `discard-unsaved` takes the edits
///   in, and whoever made the changes makes them again on top of them.
/// - **A save changes no definition.** Every definition keeps its revision, so the save expects nothing and
///   answers with no revision, and no journal records it.
/// - **A workspace with nothing unsaved saves nothing**, and answers so.
///
/// With `serde`, it is `{ "operation": "save-workspace" }`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveWorkspace {}

impl Operation for SaveWorkspace {
    type Answer = Saved;
    type Error = SaveWorkspaceError;
}

/// What a [save](SaveWorkspace) wrote.
///
/// With `serde`, it is a map of its field, and a save that wrote nothing is `{}`:
/// `{ "saved": [{ "selector": "order-producers" }, { "rule": "slow-order-producers" }] }`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Saved {
    /// Every definition whose change the save wrote, in the order of the kinds and the names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub saved: Vec<DefinitionRef>,
}

/// Why the control plane did not [save](SaveWorkspace) the workspace.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = SaveWorkspaceError::ChangedOutside {
///     reason: "selectors.yaml changed since the store read it".to_owned(),
/// };
/// assert_eq!(
///     error.to_string(),
///     "the workspace saved nothing, because selectors.yaml changed since the store read it",
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SaveWorkspaceError {
    /// Someone edited a file that the save would write since the process read it, and the save wrote nothing.
    /// `reason` names every such file, and how it changed: its bytes changed, it is gone, or it exists now.
    #[error("the workspace saved nothing, because {reason}")]
    ChangedOutside { reason: String },
    /// A file could not be written. `reason` says which, what the system said, and what the save wrote before.
    /// What it did not write stays unsaved.
    #[error("the workspace was not saved: {reason}")]
    StoreFailed { reason: String },
}
