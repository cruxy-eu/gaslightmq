//! The sessions of an environment: who is connected, and closing a session.

use gaslightmq_clock::Instant;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_field::{ClientAddress, ClientSoftware, ConnectionNumber, Endpoint};
use gaslightmq_kernel::ClientId;
use serde::{Deserialize, Serialize};

use crate::{EnvironmentNotRunning, Operation};

/// A session that is open, with its identity as far as it is known.
///
/// A session is known by the number of its connection, which lives as long as the connection.
/// Its address and its endpoint are known from the moment it opens.
/// Its first request [identifies](SessionIdentity) it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionStatus {
    /// The number of the connection, such as `#7`.
    pub connection: ConnectionNumber,
    /// The address the client connects from.
    pub address: ClientAddress,
    /// The endpoint the client connected to: `bootstrap`, or a broker.
    pub endpoint: Endpoint,
    /// When the connection opened, on the clock of the environment.
    pub opened: Instant,
    /// Who the client is, once its first request said so. `None` while the client is anonymous.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<SessionIdentity>,
}

/// Who the client of an identified session is.
///
/// The first request identifies a session, with the `client.id` it carries or without one.
/// The client software comes from the first `ApiVersions` request that names it,
/// which is the first request or a later one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionIdentity {
    /// The `client.id` of the first request, when it carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientId>,
    /// The library of the client and its version, once an `ApiVersions` request named them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub software: Option<ClientSoftware>,
}

/// Who closed a session.
///
/// With `serde`, it is the name: `client`, `backend`, `lie` or `proxy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionCloser {
    /// The client closed its connection.
    Client,
    /// The backend closed it: its broker went down, its listener moved, or it had no answer for a call.
    Backend,
    /// A hang-up lie closed it.
    Lie,
    /// The proxy closed it: it could not keep the answers of the connection in order, or someone closed the session.
    Proxy,
}

/// List the sessions that are open in an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSessions {
    /// The environment.
    pub environment: EnvironmentName,
}

impl Operation for ListSessions {
    type Answer = Vec<SessionStatus>;
    type Error = EnvironmentNotRunning;
}

/// Close a session of an environment, as a proxy closes it.
///
/// The client sees its connection close, and may connect again in a new session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseSession {
    /// The environment.
    pub environment: EnvironmentName,
    /// The number of the connection of the session.
    pub connection: ConnectionNumber,
}

impl Operation for CloseSession {
    type Answer = ();
    type Error = CloseSessionError;
}

/// Why an environment did not [close a session](CloseSession).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::field::ConnectionNumber;
///
/// let error = CloseSessionError::NotFound {
///     environment: "orders-sim".parse().expect("an environment name"),
///     connection: ConnectionNumber::new(7),
/// };
/// assert_eq!(error.to_string(), "environment \"orders-sim\" has no open session on connection #7");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CloseSessionError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// No session of the environment is open on that connection: it closed already, or it never opened.
    #[error("environment {:?} has no open session on connection {connection}", .environment.as_str())]
    NotFound {
        environment: EnvironmentName,
        connection: ConnectionNumber,
    },
}
