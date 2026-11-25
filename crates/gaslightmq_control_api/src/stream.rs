//! The live stream: what the control plane sends while a client is connected, and what the client follows.

use gaslightmq_call::Call;
use gaslightmq_environment::EnvironmentName;
use gaslightmq_field::{ClientSoftware, ConnectionNumber};
use gaslightmq_rule::RuleName;
use gaslightmq_selector::SelectorRef;
use serde::{Deserialize, Serialize};

use crate::{
    AnyDefinition, BackendStatus, DefinitionRef, Definitions, RuleStatus, SessionCloser, SessionIdentity,
    SessionStatus, Stats,
};

/// One message of the live stream, from the control plane to a client.
///
/// The first message is the [`Snapshot`](Self::Snapshot): the whole state of the process.
/// Every message after it is one change to that state, or one call.
/// A message about something live names its environment.
/// A client that applies every change to the snapshot holds what the process holds.
///
/// With `serde`, a message is a map whose `message` key says which one it is:
/// `{ "message": "environment-started", "environment": "orders-sim" }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StreamMessage {
    /// The whole state, sent when a client connects.
    Snapshot(ProcessState),
    /// A call that ended, which the client's [subscription](Subscription) follows.
    /// The engine blanks every credential of the call.
    Call { environment: EnvironmentName, call: Call },
    /// What the environment counted so far, at a fixed interval.
    Stats { environment: EnvironmentName, stats: Stats },
    /// A rule of the environment changed its state, or a live rule changed its definition, or someone made it.
    RuleChanged {
        environment: EnvironmentName,
        rule: RuleStatus,
    },
    /// A live rule left the environment: someone deleted it, or kept it as a rule of the workspace.
    RuleRemoved {
        environment: EnvironmentName,
        rule: RuleName,
    },
    /// Someone created or updated a definition of the workspace.
    DefinitionChanged { definition: AnyDefinition },
    /// Someone deleted a definition of the workspace.
    DefinitionDeleted { name: DefinitionRef },
    /// A client connected.
    SessionOpened {
        environment: EnvironmentName,
        session: SessionStatus,
    },
    /// The first request of a session said who its client is.
    SessionIdentified {
        environment: EnvironmentName,
        connection: ConnectionNumber,
        identity: SessionIdentity,
    },
    /// An `ApiVersions` request after the first request named the client software of a session.
    SessionSoftwareNamed {
        environment: EnvironmentName,
        connection: ConnectionNumber,
        software: ClientSoftware,
    },
    /// A session ended.
    SessionClosed {
        environment: EnvironmentName,
        connection: ConnectionNumber,
        by: SessionCloser,
    },
    /// An environment came up, with what is live in it.
    EnvironmentStarted { environment: RunningEnvironment },
    /// An environment went down, and everything live in it with it.
    EnvironmentStopped { environment: EnvironmentName },
    /// The definitions of the workspace do not fit each other, or a file did not read.
    /// The workspace keeps the last definitions that did. `reason` says what is wrong.
    WorkspaceInvalid { reason: String },
    /// The last [subscription](Subscription) of this client is in force: from this message on, the client gets every
    /// call that it names, and none before. A subscription to [nothing](Subscription::Nothing) gets one too.
    /// Only the client whose subscription it is gets this message.
    Following { subscription: Subscription },
    /// The last [subscription](Subscription) of this client cannot be followed, and the client follows no calls.
    /// `reason` says what was wrong: the environment does not run, the selector does not exist,
    /// or the client sent no subscription. Only the client whose subscription it was gets this message.
    SubscriptionRefused { reason: String },
}

/// The whole state of a process, which a client gets when it connects.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessState {
    /// Every definition of the workspace, which every environment shares.
    pub definitions: Definitions,
    /// Every environment that runs, with what is live in it.
    pub environments: Vec<RunningEnvironment>,
}

/// An environment that runs, with what is live in it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningEnvironment {
    /// The name of the environment. Its definition is one of the [definitions](ProcessState::definitions).
    pub environment: EnvironmentName,
    /// The backend of the environment.
    pub backend: BackendStatus,
    /// The rules of the environment with their state: the rules of the workspace, and the live rules.
    pub rules: Vec<RuleStatus>,
    /// The sessions that are open.
    pub sessions: Vec<SessionStatus>,
}

/// Which calls a client of the live stream follows, from a client to the control plane.
///
/// A client follows the calls of one environment that one selector matches, or no calls.
/// It follows none until it says otherwise, and each subscription replaces the one before it.
/// Every other message reaches every client.
///
/// With `serde`, a subscription is a map whose `follow` key says which one it is:
/// `{ "follow": "calls", "environment": "orders-sim", "selector": "order-producers" }` or `{ "follow": "nothing" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::selector::{Conditions, SelectorRef};
///
/// // Every call of the environment: a selector without conditions matches every call.
/// let every_call = Subscription::Calls {
///     environment: "orders-sim".parse().expect("an environment name"),
///     selector: SelectorRef::Inline(Conditions::new()),
/// };
/// assert_ne!(every_call, Subscription::Nothing);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "follow", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Subscription {
    /// The calls of the environment that the selector matches: a selector of the workspace by its name,
    /// or conditions of the client's own.
    Calls {
        environment: EnvironmentName,
        selector: SelectorRef,
    },
    /// No calls.
    Nothing,
}
