//! The backend of an environment: its kind, its endpoints and what it can do.

use std::collections::BTreeMap;

use gaslightmq_environment::{EnvironmentName, ListenAddress};
use gaslightmq_field::Endpoint;
use gaslightmq_incident::IncidentSupport;
use serde::{Deserialize, Serialize};

use crate::{EnvironmentNotRunning, Operation};

/// What stands behind the proxy of an environment, or answers alone.
///
/// With `serde`, it is the name: `upstream` or `simulated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    /// A real cluster. Its clock is the wall clock, and nobody edits its state.
    Upstream,
    /// A broker that simulates a cluster from its World, on a virtual clock.
    Simulated,
}

/// The backend of an environment as it is now: its kind, its endpoints and the incidents it can do.
///
/// A surface hides what a backend cannot do, and marks what it only emulates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendStatus {
    /// The kind of backend.
    pub kind: BackendKind,
    /// Every endpoint that accepts connections, with the address of its listener.
    /// An endpoint without a listener has none yet: its address was taken, and the engine tries again.
    pub endpoints: BTreeMap<Endpoint, Option<ListenAddress>>,
    /// What the backend does with each kind of incident: apply it, emulate it, or neither.
    pub incidents: IncidentSupport,
}

/// Get the backend of an environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetBackend {
    /// The environment.
    pub environment: EnvironmentName,
}

impl Operation for GetBackend {
    type Answer = BackendStatus;
    type Error = EnvironmentNotRunning;
}
