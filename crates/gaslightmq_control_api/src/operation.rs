//! What a client asks of the control plane, one operation at a time, and the one type that carries any of them.

use std::fmt::Debug;

use gaslightmq_rule::{Preset, Rule};
use gaslightmq_selector::Selector;
use gaslightmq_surface::PadLayout;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    ArmRule, CloseSession, CreateDefinition, CreateLiveRule, DeleteDefinition, DeleteLiveRule, DisarmRule, FireRule,
    GetBackend, GetDefinition, GetEnvironment, KeepRule, ListDefinitions, ListEnvironmentRules, ListEnvironments,
    ListSessions, ParseSentence, SendIntent, StartEnvironment, StopEnvironment, UpdateDefinition, UpdateLiveRule,
};

/// One thing that a client asks of the control plane, with what it gets back.
///
/// Each operation is a type of its own. It says what it answers with, and why it can fail:
/// the error holds only what can go wrong with that operation, so a client decides on every variant of it.
/// Operations that fail in the same ways share an error.
///
/// An operation travels as an [`AnyOperation`], in an [`OperationRequest`](crate::OperationRequest) that says where
/// it comes from. Its answer travels as the `serde` form of [`Answer`](Self::Answer),
/// and its failure as the form of [`Error`](Self::Error).
/// How they travel is up to the client and the server: this crate holds what is asked and answered.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::rule::Rule;
///
/// /// What a client sends for an operation, whichever it is.
/// fn ask<O: Operation>(operation: O) -> AnyOperation {
///     operation.into()
/// }
///
/// let request = ask(GetDefinition::<Rule> { name: "slow-order-producers".parse().expect("a rule name") });
/// let json = serde_json::to_value(&request).expect("a request has a form");
/// assert_eq!(json, serde_json::json!({ "operation": "get-rule", "name": "slow-order-producers" }));
/// ```
pub trait Operation: Into<AnyOperation> {
    /// What the control plane answers with when the operation succeeds.
    type Answer: Debug + Serialize + DeserializeOwned;
    /// Why the operation failed. Each variant is a case that a client decides on.
    type Error: std::error::Error + Serialize + DeserializeOwned;
}

/// Any operation, as one value: what a client sends, and what a server reads before it knows which operation came.
///
/// Each variant holds the type of its operation, and converts from it.
/// The definitions of the workspace have the five operations of [`EditableDefinition`](crate::EditableDefinition)
/// for each kind.
///
/// With `serde`, it is the map of its operation with an `operation` key that names it:
/// `{ "operation": "arm-rule", "environment": "orders-sim", "rule": "slow-order-producers" }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AnyOperation {
    /// [List the selectors](ListDefinitions).
    ListSelectors(ListDefinitions<Selector>),
    /// [Get a selector](GetDefinition).
    GetSelector(GetDefinition<Selector>),
    /// [Create a selector](CreateDefinition).
    CreateSelector(CreateDefinition<Selector>),
    /// [Update a selector](UpdateDefinition).
    UpdateSelector(UpdateDefinition<Selector>),
    /// [Delete a selector](DeleteDefinition).
    DeleteSelector(DeleteDefinition<Selector>),
    /// [List the presets](ListDefinitions).
    ListPresets(ListDefinitions<Preset>),
    /// [Get a preset](GetDefinition).
    GetPreset(GetDefinition<Preset>),
    /// [Create a preset](CreateDefinition).
    CreatePreset(CreateDefinition<Preset>),
    /// [Update a preset](UpdateDefinition).
    UpdatePreset(UpdateDefinition<Preset>),
    /// [Delete a preset](DeleteDefinition).
    DeletePreset(DeleteDefinition<Preset>),
    /// [List the rules of the workspace](ListDefinitions).
    ListRules(ListDefinitions<Rule>),
    /// [Get a rule of the workspace](GetDefinition).
    GetRule(GetDefinition<Rule>),
    /// [Create a rule in the workspace](CreateDefinition).
    CreateRule(CreateDefinition<Rule>),
    /// [Update a rule of the workspace](UpdateDefinition).
    UpdateRule(UpdateDefinition<Rule>),
    /// [Delete a rule of the workspace](DeleteDefinition).
    DeleteRule(DeleteDefinition<Rule>),
    /// [List the pad layouts](ListDefinitions).
    ListLayouts(ListDefinitions<PadLayout>),
    /// [Get a pad layout](GetDefinition).
    GetLayout(GetDefinition<PadLayout>),
    /// [Create a pad layout](CreateDefinition).
    CreateLayout(CreateDefinition<PadLayout>),
    /// [Update a pad layout](UpdateDefinition).
    UpdateLayout(UpdateDefinition<PadLayout>),
    /// [Delete a pad layout](DeleteDefinition).
    DeleteLayout(DeleteDefinition<PadLayout>),
    /// [List the environments](ListEnvironments).
    ListEnvironments(ListEnvironments),
    /// [Get an environment](GetEnvironment).
    GetEnvironment(GetEnvironment),
    /// [Start an environment](StartEnvironment).
    StartEnvironment(StartEnvironment),
    /// [Stop an environment](StopEnvironment).
    StopEnvironment(StopEnvironment),
    /// [List the rules of an environment, with their state](ListEnvironmentRules).
    ListEnvironmentRules(ListEnvironmentRules),
    /// [Add a live rule](CreateLiveRule).
    CreateLiveRule(CreateLiveRule),
    /// [Replace a live rule](UpdateLiveRule).
    UpdateLiveRule(UpdateLiveRule),
    /// [Remove a live rule](DeleteLiveRule).
    DeleteLiveRule(DeleteLiveRule),
    /// [Keep a live rule](KeepRule).
    KeepRule(KeepRule),
    /// [Arm a rule](ArmRule).
    ArmRule(ArmRule),
    /// [Disarm a rule](DisarmRule).
    DisarmRule(DisarmRule),
    /// [Fire a rule by hand](FireRule).
    FireRule(FireRule),
    /// [Send what someone did at a pad](SendIntent).
    SendIntent(SendIntent),
    /// [List the open sessions](ListSessions).
    ListSessions(ListSessions),
    /// [Close a session](CloseSession).
    CloseSession(CloseSession),
    /// [Get the backend](GetBackend).
    GetBackend(GetBackend),
    /// [Read a ⌘K sentence](ParseSentence).
    ParseSentence(ParseSentence),
}

impl AnyOperation {
    /// The name of the operation, as the `operation` key of its `serde` form says it: `arm-rule`, `create-selector`.
    ///
    /// ```
    /// use gaslightmq_control_api::prelude::*;
    ///
    /// let stop = StopEnvironment { environment: "orders-sim".parse().expect("an environment name") };
    /// assert_eq!(AnyOperation::from(stop).name(), "stop-environment");
    /// ```
    pub const fn name(&self) -> &'static str {
        match self {
            AnyOperation::ListSelectors(_) => "list-selectors",
            AnyOperation::GetSelector(_) => "get-selector",
            AnyOperation::CreateSelector(_) => "create-selector",
            AnyOperation::UpdateSelector(_) => "update-selector",
            AnyOperation::DeleteSelector(_) => "delete-selector",
            AnyOperation::ListPresets(_) => "list-presets",
            AnyOperation::GetPreset(_) => "get-preset",
            AnyOperation::CreatePreset(_) => "create-preset",
            AnyOperation::UpdatePreset(_) => "update-preset",
            AnyOperation::DeletePreset(_) => "delete-preset",
            AnyOperation::ListRules(_) => "list-rules",
            AnyOperation::GetRule(_) => "get-rule",
            AnyOperation::CreateRule(_) => "create-rule",
            AnyOperation::UpdateRule(_) => "update-rule",
            AnyOperation::DeleteRule(_) => "delete-rule",
            AnyOperation::ListLayouts(_) => "list-layouts",
            AnyOperation::GetLayout(_) => "get-layout",
            AnyOperation::CreateLayout(_) => "create-layout",
            AnyOperation::UpdateLayout(_) => "update-layout",
            AnyOperation::DeleteLayout(_) => "delete-layout",
            AnyOperation::ListEnvironments(_) => "list-environments",
            AnyOperation::GetEnvironment(_) => "get-environment",
            AnyOperation::StartEnvironment(_) => "start-environment",
            AnyOperation::StopEnvironment(_) => "stop-environment",
            AnyOperation::ListEnvironmentRules(_) => "list-environment-rules",
            AnyOperation::CreateLiveRule(_) => "create-live-rule",
            AnyOperation::UpdateLiveRule(_) => "update-live-rule",
            AnyOperation::DeleteLiveRule(_) => "delete-live-rule",
            AnyOperation::KeepRule(_) => "keep-rule",
            AnyOperation::ArmRule(_) => "arm-rule",
            AnyOperation::DisarmRule(_) => "disarm-rule",
            AnyOperation::FireRule(_) => "fire-rule",
            AnyOperation::SendIntent(_) => "send-intent",
            AnyOperation::ListSessions(_) => "list-sessions",
            AnyOperation::CloseSession(_) => "close-session",
            AnyOperation::GetBackend(_) => "get-backend",
            AnyOperation::ParseSentence(_) => "parse-sentence",
        }
    }
}

/// Why the control plane answered a request with neither the answer nor the error of its operation.
///
/// It is the same for every operation: whatever the request asks, the server may not serve it yet,
/// read no operation in it, or not read where it comes from.
///
/// With `serde`, it is a map with an `error` key that names the variant:
/// `{ "error": "not-served", "operation": "keep-rule" }`.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = RequestError::NotServed { operation: "keep-rule".to_owned() };
/// assert_eq!(error.to_string(), "the process does not serve the operation `keep-rule` yet");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RequestError {
    /// The process knows the operation, and does not serve it yet. `operation` is its [name](AnyOperation::name).
    #[error("the process does not serve the operation `{operation}` yet")]
    NotServed { operation: String },
    /// The request holds no operation that the process knows, it does not say
    /// [where it comes from](crate::OperationRequest), it claims a surface that only the process names,
    /// or it is larger than the process takes.
    /// `reason` says what was wrong with it.
    #[error("the request was not read: {reason}")]
    Unreadable { reason: String },
}

/// Converts each operation into the variant of [`AnyOperation`] that holds it.
macro_rules! requests {
    ($($operation:ty => $variant:ident;)*) => {
        $(
            impl From<$operation> for AnyOperation {
                fn from(operation: $operation) -> Self {
                    AnyOperation::$variant(operation)
                }
            }
        )*
    };
}

requests! {
    ListDefinitions<Selector> => ListSelectors;
    GetDefinition<Selector> => GetSelector;
    CreateDefinition<Selector> => CreateSelector;
    UpdateDefinition<Selector> => UpdateSelector;
    DeleteDefinition<Selector> => DeleteSelector;
    ListDefinitions<Preset> => ListPresets;
    GetDefinition<Preset> => GetPreset;
    CreateDefinition<Preset> => CreatePreset;
    UpdateDefinition<Preset> => UpdatePreset;
    DeleteDefinition<Preset> => DeletePreset;
    ListDefinitions<Rule> => ListRules;
    GetDefinition<Rule> => GetRule;
    CreateDefinition<Rule> => CreateRule;
    UpdateDefinition<Rule> => UpdateRule;
    DeleteDefinition<Rule> => DeleteRule;
    ListDefinitions<PadLayout> => ListLayouts;
    GetDefinition<PadLayout> => GetLayout;
    CreateDefinition<PadLayout> => CreateLayout;
    UpdateDefinition<PadLayout> => UpdateLayout;
    DeleteDefinition<PadLayout> => DeleteLayout;
    ListEnvironments => ListEnvironments;
    GetEnvironment => GetEnvironment;
    StartEnvironment => StartEnvironment;
    StopEnvironment => StopEnvironment;
    ListEnvironmentRules => ListEnvironmentRules;
    CreateLiveRule => CreateLiveRule;
    UpdateLiveRule => UpdateLiveRule;
    DeleteLiveRule => DeleteLiveRule;
    KeepRule => KeepRule;
    ArmRule => ArmRule;
    DisarmRule => DisarmRule;
    FireRule => FireRule;
    SendIntent => SendIntent;
    ListSessions => ListSessions;
    CloseSession => CloseSession;
    GetBackend => GetBackend;
    ParseSentence => ParseSentence;
}
