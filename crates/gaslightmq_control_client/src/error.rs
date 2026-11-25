//! Why an operation got no answer of its own, and why a live stream ended.

use gaslightmq_control_api::RequestError;

/// Why an operation got no answer of its own: neither its answer nor its error.
///
/// An operation that the control plane answered returns its own error instead, as itself,
/// so a caller decides on it: see [`ControlClient::send`](crate::ControlClient::send).
///
/// ```
/// use gaslightmq_control_client::SendError;
///
/// let error = SendError::Unreachable { reason: "connection refused".to_owned() };
/// assert_eq!(error.to_string(), "the control plane is unreachable: connection refused");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    /// No answer came: the client could not reach the control plane, the connection broke,
    /// or the [request timeout](crate::ControlClientConfiguration::request_timeout) passed.
    /// The control plane may have done the operation. A caller tries again later.
    #[error("the control plane is unreachable: {reason}")]
    Unreachable { reason: String },
    /// The control plane did not take the request, and did nothing: it does not serve the operation yet,
    /// or it could not read the request. `error` says which.
    #[error("the control plane did not take the request: {error}")]
    Rejected { error: RequestError },
    /// Something answered that the client cannot read: no answer of the control plane to the operation.
    /// The other side is no control plane, or one of another version. Trying again gets the same.
    #[error("the control plane answered what the client cannot read: {reason}")]
    Unreadable { reason: String },
}

/// Why a live stream did not open, or why it ended.
///
/// A stream that ended no longer holds what the process holds,
/// so a caller [opens](crate::ControlClient::live_stream) a new one, which starts with a new snapshot.
///
/// ```
/// use gaslightmq_control_client::StreamError;
///
/// let error = StreamError::Unreachable { reason: "the control plane stopped".to_owned() };
/// assert_eq!(error.to_string(), "the live stream is unreachable: the control plane stopped");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    /// The stream did not open within the [request timeout](crate::ControlClientConfiguration::request_timeout),
    /// it broke, or the control plane closed it. A caller opens a new stream later.
    #[error("the live stream is unreachable: {reason}")]
    Unreachable { reason: String },
    /// Something sent what the client cannot read: no message of the live stream.
    /// The other side is no control plane, or one of another version. A new stream gets the same.
    #[error("the live stream sent what the client cannot read: {reason}")]
    Unreadable { reason: String },
}
