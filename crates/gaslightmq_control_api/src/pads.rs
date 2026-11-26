//! What a control surface sends: the intent of a pad press or an encoder turn, and what it did.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_surface::{Hold, Intent, LayoutName};
use serde::{Deserialize, Serialize};

use crate::{Operation, RuleStatus};

/// Send what someone did at a pad of a layout to an environment.
///
/// A press says the state it wants: *arm the rule at row 1, column 4 on layer order-ingest*, and never
/// *the other state*. A turn says how far an encoder moved: *change probability by +3*.
/// The environment acts on the rule the pad points at, and makes the live rule of the pad first when the pad has none.
/// A press that finds the state it wants changes nothing, so two people who press one pad to arm it both find it armed.
/// The answer says what the intent did, and what the rule became reaches every client as a
/// [`StreamMessage::RuleChanged`](crate::StreamMessage::RuleChanged).
/// An intent expects no revision: it acts on the rule as the rule is, and says the state it wants.
///
/// The environment counts the holds of a rule: a press in hold adds one and answers with it, and its release names it.
/// A rule that holds armed disarms when its last hold goes, and a release never disarms a rule that something else
/// armed. A disarm ends every hold, and a release after it changes nothing.
///
/// A hold belongs to the [presence](crate::RequestSource::presence) that its request names, and ends with it:
/// a client that is gone never sends its release. A hold of a request that names none ends by its release or by a
/// disarm alone. A hold that names a presence which has ended is [refused](IntentError::Refused).
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
    type Answer = IntentEffect;
    type Error = IntentError;
}

/// What an [intent](SendIntent) did in its environment.
///
/// A surface reads from it what to take back on an undo, and never works out itself what a press did:
/// a press that made the rule is undone by deleting it, and a press that changed nothing leaves nothing to undo.
///
/// With `serde`, it is a map with `rule`, `made` and `changed`, and with `hold` after a press in hold:
/// `{ "rule": { … }, "made": true, "changed": true, "hold": 7 }`.
/// `rule` is `null` when the pad points at no rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentEffect {
    /// The rule of the pad as the environment holds it now, with its state and its revision there.
    /// An undo of the intent expects that revision.
    /// `None` when the pad points at no rule, which a disarm and a release of such a pad leave as it is.
    pub rule: Option<RuleStatus>,
    /// Whether the intent made the rule: the pad had none, so it got the live rule of its position.
    pub made: bool,
    /// Whether the intent changed the rule: its state, or for a turn its definition.
    pub changed: bool,
    /// The hold that a press in hold added, which its release names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold: Option<Hold>,
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
    /// The environment cannot act on the intent: the pad lies outside its layout, its rule cannot be made or armed,
    /// it has no rule to turn, the rule has nothing for the encoder to change, or the presence that holds the pad
    /// has ended. `reason` says which.
    /// An intent that finds the state it wants is no error: it changes nothing, and its answer says so.
    #[error("the intent was refused: {reason}")]
    Refused { reason: String },
    /// A store of the workspace could not read the layout, or what the rule of the pad is made of.
    /// `reason` says which store, and what it said.
    #[error("the intent was not acted on: {reason}")]
    StoreFailed { reason: String },
}
