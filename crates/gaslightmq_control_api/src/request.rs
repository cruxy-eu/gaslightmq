//! One request to the control plane: an operation, and where it comes from.

use serde::{Deserialize, Serialize};

use crate::AnyOperation;

/// One request to the control plane: an operation, and where it comes from.
///
/// Every request says which surface sends it, whatever it asks.
/// A request that does not say so holds no request that a server reads: see [`RequestError::Unreadable`].
///
/// With `serde`, it is the map of its operation, with a `from` key next to the keys of the operation:
/// `{ "from": { "surface": "pad" }, "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// // Someone presses ⌘Z in the UI, and the tab takes back the arming of a rule.
/// let disarm = DisarmRule {
///     environment: "orders-sim".parse().expect("an environment name"),
///     rule: "slow-order-producers".parse().expect("a rule name"),
///     expects: None,
/// };
/// let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
/// let request = serde_json::to_value(OperationRequest::new(undo, disarm)).expect("a request has a form");
/// assert_eq!(
///     request,
///     serde_json::json!({
///         "from": { "surface": "ui", "cause": "undo" },
///         "operation": "disarm-rule",
///         "environment": "orders-sim",
///         "rule": "slow-order-producers",
///     }),
/// );
/// ```
///
/// [`RequestError::Unreadable`]: crate::RequestError::Unreadable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRequest {
    /// Where the request comes from.
    pub from: RequestSource,
    /// What the request asks.
    #[serde(flatten)]
    pub operation: AnyOperation,
}

impl OperationRequest {
    /// The request that asks `operation`, and comes from `from`.
    ///
    /// A [`RequestSurface`] is a source without a cause, so most requests name their surface alone.
    ///
    /// ```
    /// use gaslightmq_control_api::prelude::*;
    ///
    /// let stop = StopEnvironment { environment: "orders-sim".parse().expect("an environment name") };
    /// let request = OperationRequest::new(RequestSurface::Cli, stop.clone());
    /// assert_eq!(request.from, RequestSource::new(RequestSurface::Cli));
    /// assert_eq!(request.operation, AnyOperation::StopEnvironment(stop));
    /// ```
    pub fn new(from: impl Into<RequestSource>, operation: impl Into<AnyOperation>) -> Self {
        Self {
            from: from.into(),
            operation: operation.into(),
        }
    }
}

/// Where a request comes from: the surface that sends it, and why, when it takes back or repeats an earlier change.
///
/// The control plane has no users, so this is what the sender says about itself.
/// It never says who a person is.
///
/// With `serde`, it is a map of its fields, and a request without a cause leaves `cause` out:
/// `{ "surface": "quick-inject" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let press = RequestSource::from(RequestSurface::Pad);
/// assert_eq!(press.cause, None);
///
/// let redo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Redo);
/// assert_eq!(redo.surface, RequestSurface::Ui);
/// assert_eq!(redo.cause, Some(RequestCause::Redo));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSource {
    /// The surface that sends the request.
    pub surface: RequestSurface,
    /// Why the surface sends it, when the request takes back or repeats an earlier change of its sender.
    /// `None` for every other request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<RequestCause>,
}

impl RequestSource {
    /// A request of `surface` that neither takes back nor repeats an earlier change.
    pub const fn new(surface: RequestSurface) -> Self {
        Self { surface, cause: None }
    }

    /// The same source, for a request that has `cause`.
    #[must_use]
    pub const fn with_cause(self, cause: RequestCause) -> Self {
        Self {
            surface: self.surface,
            cause: Some(cause),
        }
    }
}

/// A request of a surface, without a cause.
impl From<RequestSurface> for RequestSource {
    fn from(surface: RequestSurface) -> Self {
        Self::new(surface)
    }
}

/// A surface that sends requests to the control plane.
///
/// | Surface | The request comes from |
/// |---|---|
/// | [`Pad`](Self::Pad) | A press or a turn at the pad grid. |
/// | [`QuickInject`](Self::QuickInject) | A ⌘K sentence. |
/// | [`Ui`](Self::Ui) | Any other view of the UI. |
/// | [`Cli`](Self::Cli) | `gmq`. |
/// | [`Test`](Self::Test) | A test library. |
/// | [`Api`](Self::Api) | Any other client of the control plane. |
///
/// A change that no request made has a surface too: a step of a scenario, a lifetime that ended,
/// a file that someone edited. The process names those itself, so no request can claim one,
/// and a request that tries is [unreadable](crate::RequestError::Unreadable).
///
/// With `serde`, it is the name in kebab case: `quick-inject`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let surface: RequestSurface = serde_json::from_str("\"quick-inject\"").expect("a surface of a request");
/// assert_eq!(surface, RequestSurface::QuickInject);
///
/// // A lifetime ends by itself, and no request says that it is one.
/// assert!(serde_json::from_str::<RequestSurface>("\"lifetime\"").is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequestSurface {
    /// A press or a turn at the pad grid.
    Pad,
    /// A ⌘K sentence.
    QuickInject,
    /// Any other view of the UI.
    Ui,
    /// `gmq`.
    Cli,
    /// A test library.
    Test,
    /// Any other client of the control plane.
    Api,
}

/// Why a surface sends a request that takes back or repeats an earlier change of its own.
///
/// The control plane has no operation that undoes: an undo is the inverse of a change, sent as one more request,
/// and this says that it is one.
///
/// With `serde`, it is the name in kebab case: `undo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequestCause {
    /// The request takes back an earlier change of its sender.
    Undo,
    /// The request makes a change again that its sender took back.
    Redo,
}
