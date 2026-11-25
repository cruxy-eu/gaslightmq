//! What a control surface sends: the intent of a pad press or an encoder turn.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_surface::{Intent, LayoutName};
use serde::{Deserialize, Serialize};

use crate::Operation;

/// Send what someone did at a pad of a layout to an environment.
///
/// A surface sends intent, and never state: *arm the rule at row 1, column 4 on layer order-ingest*,
/// or *change probability by +3*. The environment acts on the rule the pad points at,
/// and makes the live rule of the pad first when the pad has none.
/// What the rule became reaches every client as a [`StreamMessage::RuleChanged`](crate::StreamMessage::RuleChanged).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
/// use gaslightmq_control_api::surface::{Column, Intent, Pad, Row};
///
/// let pad = Pad::new(
///     "order-desk".parse().expect("a layout name"),
///     "order-ingest".parse().expect("a layer"),
///     Row::from_index(0),
///     Column::from_index(3),
/// );
/// let press = SendIntent { environment: "orders-sim".parse().expect("an environment name"), intent: Intent::Arm { pad } };
/// assert_eq!(press.intent.to_string(), "arm the rule at row 1, column 4 on layer order-ingest of layout order-desk");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendIntent {
    /// The environment whose rules the pad plays.
    pub environment: EnvironmentName,
    /// What someone did, at which pad.
    pub intent: Intent,
}

impl Operation for SendIntent {
    type Answer = ();
    type Error = IntentError;
}

/// Why an environment did not act on an [intent](SendIntent).
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let error = IntentError::UnknownLayout { layout: "order-desk".parse().expect("a layout name") };
/// assert_eq!(error.to_string(), "layout \"order-desk\" does not exist");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "kebab-case", deny_unknown_fields)]
pub enum IntentError {
    /// The environment does not run.
    #[error("environment {:?} does not run", .environment.as_str())]
    NotRunning { environment: EnvironmentName },
    /// The pad belongs to a layout that the workspace does not have.
    #[error("layout {:?} does not exist", .layout.as_str())]
    UnknownLayout { layout: LayoutName },
    /// The intent changed nothing: the pad lies outside its layout, its rule cannot be made or armed,
    /// or the rule has nothing for the encoder to change. `reason` says which.
    #[error("the intent changed nothing: {reason}")]
    Refused { reason: String },
}
