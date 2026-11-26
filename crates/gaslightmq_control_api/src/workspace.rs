//! The workspace as a whole: reading its files again.

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
///
/// With `serde`, it is `{ "operation": "reload-workspace" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let request = OperationRequest::new(RequestSurface::Cli, ReloadWorkspace {});
/// assert_eq!(
///     serde_json::to_value(request).expect("a request has a form"),
///     serde_json::json!({ "from": { "surface": "cli" }, "operation": "reload-workspace" }),
/// );
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReloadWorkspace {}

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
