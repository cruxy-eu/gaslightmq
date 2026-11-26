//! The journal of an environment: what changed there, and where each change came from.

use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::{ChangeCause, ChangeSource, ChangeSurface, JournalEntry, JournalEntryId};
use serde::{Deserialize, Serialize};

use crate::{EnvironmentNotRunning, Operation, RequestCause, RequestSource, RequestSurface};

/// Query the journal of an environment: what changed there, the oldest entry first.
///
/// Each environment has a journal of its own, which only grows, and its proxy keeps the recent entries in memory.
/// The answer holds the entries that it keeps, each with its ID, its time, the revision that its change left,
/// and where the change came from.
///
/// With `after`, the answer holds only the entries that came after the entry of that ID.
/// A client that follows the [live stream](crate::StreamMessage::JournalEntry) reads the journal once, and gets
/// every later entry as a message. The [snapshot](crate::StreamMessage::Snapshot) holds no journal,
/// so a client that got a new snapshot reads on from the last entry it holds.
/// The IDs count from one without a gap, so a client sees it when the environment let go of entries in between.
///
/// With `serde`, it is a map of its fields, and a query for the whole journal leaves `after` out:
/// `{ "environment": "orders-sim", "after": 41 }`.
///
/// ```
/// use gaslightmq_control_api::journal::{ChangeCause, JournalEntryId};
/// use gaslightmq_control_api::prelude::*;
///
/// let read_on = QueryJournal {
///     environment: "orders-sim".parse().expect("an environment name"),
///     after: Some(JournalEntryId::new(41).expect("an ID that is at least one")),
/// };
/// let request = serde_json::to_value(OperationRequest::new(RequestSurface::Cli, read_on)).expect("a request has a form");
/// assert_eq!(
///     request,
///     serde_json::json!({
///         "from": { "surface": "cli" },
///         "operation": "query-journal",
///         "environment": "orders-sim",
///         "after": 41,
///     }),
/// );
///
/// // The answer holds the entries after it: someone took the arming of a rule back with ⌘Z.
/// let answer = serde_json::json!([{
///     "id": 42,
///     "at": 4_200_000_000_u64,
///     "revision": 12,
///     "from": { "surface": "ui", "cause": "undo" },
///     "change": "rule-disarmed",
///     "rule": "slow-order-producers",
/// }]);
/// let entries: <QueryJournal as Operation>::Answer = serde_json::from_value(answer.clone()).expect("the answer");
/// assert_eq!(entries[0].from.cause, Some(ChangeCause::Undo));
///
/// // An entry that comes later is a message of the live stream, in the same form.
/// let message = serde_json::json!({ "message": "journal-entry", "environment": "orders-sim", "entry": answer[0] });
/// let message: StreamMessage = serde_json::from_value(message).expect("a message of the live stream");
/// let StreamMessage::JournalEntry { entry, .. } = message else {
///     panic!("the message holds an entry of the journal");
/// };
/// assert_eq!(entry, entries[0]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryJournal {
    /// The environment.
    pub environment: EnvironmentName,
    /// The entry that the client read last. `None` for every entry that the environment keeps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<JournalEntryId>,
}

impl Operation for QueryJournal {
    type Answer = Vec<JournalEntry>;
    type Error = EnvironmentNotRunning;
}

/// The surface of a request, as the journal records it with every change that the request makes.
impl From<RequestSurface> for ChangeSurface {
    fn from(surface: RequestSurface) -> Self {
        match surface {
            RequestSurface::Pad => ChangeSurface::Pad,
            RequestSurface::QuickInject => ChangeSurface::QuickInject,
            RequestSurface::Ui => ChangeSurface::Ui,
            RequestSurface::Cli => ChangeSurface::Cli,
            RequestSurface::Test => ChangeSurface::Test,
            RequestSurface::Api => ChangeSurface::Api,
        }
    }
}

/// The cause of a request, as the journal records it with every change that the request makes.
impl From<RequestCause> for ChangeCause {
    fn from(cause: RequestCause) -> Self {
        match cause {
            RequestCause::Undo => ChangeCause::Undo,
            RequestCause::Redo => ChangeCause::Redo,
        }
    }
}

/// Where a request comes from, as the journal records it with every change that the request makes:
/// its surface and its cause. The presence of its sender is no part of an entry.
///
/// ```
/// use gaslightmq_control_api::journal::{ChangeCause, ChangeSource, ChangeSurface};
/// use gaslightmq_control_api::prelude::*;
///
/// let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);
/// assert_eq!(
///     ChangeSource::from(undo),
///     ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo),
/// );
/// ```
impl From<RequestSource> for ChangeSource {
    fn from(source: RequestSource) -> Self {
        Self {
            surface: source.surface.into(),
            cause: source.cause.map(Into::into),
        }
    }
}
