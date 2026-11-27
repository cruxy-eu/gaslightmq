//! What a stretch of a recording holds, so a reader knows whether to read it.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use gaslightmq_call::{Call, CallId};
use gaslightmq_clock::Instant;
use gaslightmq_field::prelude::*;
use gaslightmq_journal::JournalEntryId;

use crate::encoding::{self, EncodingVersion, Framed, Kind, Written, read, v1, write};
use crate::entry::Kept;
use crate::{Entry, MessageError};

/// The values that the calls of a stretch have for one field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummaryValues {
    /// Every value that a call of the stretch has for the field, for the call as a whole or for one of its parts,
    /// each value once.
    Listed(BTreeSet<FieldValue>),
    /// The stretch has more values than a summary lists, so it can hold any value.
    Any,
}

/// What a stretch of a recording holds: its instants, its counts, its IDs, and the values of every field.
///
/// A storage keeps a summary for every stretch it can skip, and asks [`Wanted`] about it.
/// A summary only proves that a stretch cannot hold what a reader wants.
/// The reader decides every entry that it does read itself.
///
/// Two summaries add up to the summary of both stretches, so a layout sums up at every level it has, with one rule.
///
/// ```
/// use std::collections::BTreeSet;
///
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_clock::Instant;
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_recording::prelude::*;
/// use gaslightmq_wire::Request;
/// use gaslightmq_wire::messages::ProduceRequest;
///
/// let partition: TopicPartition = "orders.eu/3".parse().expect("a topic partition");
/// let fields = Fields::new()
///     .with(ConnectionNumber::new(7))
///     .with(ApiKey::PRODUCE)
///     .with_part(PartFields::new(partition))
///     .with(Outcome::Ok);
/// let call = Call::new(
///     CallId::FIRST,
///     fields,
///     Delivered::Nothing,
///     Explanation::none(),
///     CallTiming::arrived(Instant::ZERO),
/// )
/// .expect("a call on connection 7");
/// let api_version = 9;
/// let correlation_id = 1;
/// let request = Request::new(api_version, correlation_id, Some("order-ingest"), &ProduceRequest::default())
///     .expect("a Produce request");
/// let entry = Entry::call(call, CallFrames::new(request.into_frame()), RecordingMode::Full)?;
///
/// // `recordings.summary_values` says how many values of one field a summary lists.
/// let listed = 256;
/// let mut summary = Summary::empty();
/// summary.add(&entry, listed);
///
/// assert_eq!(summary.calls(), 1);
/// assert_eq!(summary.call_ids(), Some((CallId::FIRST, CallId::FIRST)));
/// let topics = BTreeSet::from(["orders.eu".parse::<TopicName>().expect("a topic").into()]);
/// assert_eq!(summary.values(Field::Topic), Some(&SummaryValues::Listed(topics)));
/// // No call of the stretch names a group, so the field is not in the summary.
/// assert_eq!(summary.values(Field::Group), None);
/// # Ok::<(), CallEntryError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    instants: Option<(Instant, Instant)>,
    calls: u64,
    call_ids: Option<(CallId, CallId)>,
    journal: u64,
    journal_ids: Option<(JournalEntryId, JournalEntryId)>,
    sessions: u64,
    fields: BTreeMap<Field, SummaryValues>,
}

impl Summary {
    /// The summary of a stretch without an entry.
    pub const fn empty() -> Summary {
        Summary {
            instants: None,
            calls: 0,
            call_ids: None,
            journal: 0,
            journal_ids: None,
            sessions: 0,
            fields: BTreeMap::new(),
        }
    }

    /// The summary that a recording holds as these parts, read back.
    pub(crate) fn from_parts(
        instants: Option<(Instant, Instant)>,
        calls: u64,
        call_ids: Option<(CallId, CallId)>,
        journal: u64,
        journal_ids: Option<(JournalEntryId, JournalEntryId)>,
        sessions: u64,
        fields: Vec<(Field, SummaryValues)>,
    ) -> Result<Summary, String> {
        let ordered = |range: Option<(u64, u64)>, what: &str| match range {
            Some((from, to)) if from > to => Err(format!("{what} of a summary end before they start")),
            _ => Ok(()),
        };
        ordered(
            instants.map(|(from, to)| (from.as_nanos(), to.as_nanos())),
            "the instants",
        )?;
        ordered(
            call_ids.map(|(lowest, highest)| (lowest.value(), highest.value())),
            "the call IDs",
        )?;
        ordered(
            journal_ids.map(|(first, last)| (first.value(), last.value())),
            "the journal entry IDs",
        )?;
        let mut by_field = BTreeMap::new();
        for (field, values) in fields {
            if let SummaryValues::Listed(listed) = &values
                && let Some(other) = listed.iter().find(|value| value.field() != field)
            {
                return Err(format!(
                    "a summary lists a value of `{}` under `{field}`",
                    other.field()
                ));
            }
            if by_field.insert(field, values).is_some() {
                return Err(format!("a summary holds `{field}` twice"));
            }
        }
        Ok(Summary {
            instants,
            calls,
            call_ids,
            journal,
            journal_ids,
            sessions,
            fields: by_field,
        })
    }

    /// The stretch also holds `entry`.
    ///
    /// `listed` is how many values of one field a summary lists, which is `recordings.summary_values`.
    /// A field with more values says [`SummaryValues::Any`] from then on.
    pub fn add(&mut self, entry: &Entry, listed: usize) {
        match entry.kept() {
            Kept::Session { at } => {
                self.sessions += 1;
                self.cover(*at);
            }
            Kept::Journal { id, at } => {
                self.journal += 1;
                self.journal_ids = Some(widened(self.journal_ids, *id));
                self.cover(*at);
            }
            Kept::Call(call) => self.add_call(call, listed),
        }
    }

    fn add_call(&mut self, call: &Call, listed: usize) {
        self.calls += 1;
        self.call_ids = Some(widened(self.call_ids, call.id()));
        let timing = call.timing();
        let moments = [timing.forwarded_at(), timing.answered_at(), timing.delivered_at()];
        self.cover(timing.arrived_at());
        for moment in moments.into_iter().flatten() {
            self.cover(moment);
        }
        for field in Field::ALL {
            // A selector tests a field on the call as a whole or on one of its parts, so a summary lists both.
            let readings = [call.whole_call(field), call.read(field)];
            for values in readings.into_iter().filter_map(Reading::present) {
                self.list(field, values.iter().cloned(), listed);
            }
        }
    }

    fn cover(&mut self, instant: Instant) {
        self.instants = Some(widened(self.instants, instant));
    }

    fn list(&mut self, field: Field, values: impl Iterator<Item = FieldValue>, listed: usize) {
        let held = self
            .fields
            .entry(field)
            .or_insert_with(|| SummaryValues::Listed(BTreeSet::new()));
        if let SummaryValues::Listed(set) = held {
            set.extend(values);
            if set.len() > listed {
                *held = SummaryValues::Any;
            }
        }
    }

    /// The stretch of this summary and the stretch of `other`, as one:
    /// the wider ranges, the sums of the counts, and the values of both, or [`SummaryValues::Any`].
    ///
    /// `listed` is how many values of one field a summary lists, which is `recordings.summary_values`.
    pub fn merge(&mut self, other: &Summary, listed: usize) {
        self.instants = spanning(self.instants, other.instants);
        self.calls += other.calls;
        self.call_ids = spanning(self.call_ids, other.call_ids);
        self.journal += other.journal;
        self.journal_ids = spanning(self.journal_ids, other.journal_ids);
        self.sessions += other.sessions;
        for (field, values) in &other.fields {
            match values {
                SummaryValues::Listed(values) => self.list(*field, values.iter().cloned(), listed),
                SummaryValues::Any => {
                    self.fields.insert(*field, SummaryValues::Any);
                }
            }
        }
    }

    /// The earliest and the latest instant that an entry of the stretch holds.
    pub const fn instants(&self) -> Option<(Instant, Instant)> {
        self.instants
    }

    /// The number of call records.
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// The lowest and the highest call ID among the calls.
    pub const fn call_ids(&self) -> Option<(CallId, CallId)> {
        self.call_ids
    }

    /// The number of journal entries.
    pub const fn journal_entries(&self) -> u64 {
        self.journal
    }

    /// The first and the last ID of the journal entries.
    pub const fn journal_ids(&self) -> Option<(JournalEntryId, JournalEntryId)> {
        self.journal_ids
    }

    /// The number of session events.
    pub const fn session_events(&self) -> u64 {
        self.sessions
    }

    /// The values that the calls of the stretch have for `field`, or `None` when no call of the stretch has the field.
    ///
    /// Whether a selector can match a set of values is matching, so that check belongs to the selector.
    pub fn values(&self, field: Field) -> Option<&SummaryValues> {
        self.fields.get(&field)
    }

    /// Every field that a call of the stretch has, with its values, in the order of the fields.
    pub fn fields(&self) -> impl Iterator<Item = (Field, &SummaryValues)> {
        self.fields.iter().map(|(field, values)| (*field, values))
    }

    /// The summary as a message behind its head: what a layout keeps of it.
    pub fn encode(&self) -> Bytes {
        Framed::archive(Kind::Summary, &write::summary(self)).into_bytes()
    }

    /// Reads the summary that [`encode`](Self::encode) wrote, in the version that its recording says.
    ///
    /// # Errors
    ///
    /// [`MessageError::Damaged`] when the bytes are no summary.
    pub fn decode(version: EncodingVersion, bytes: &[u8]) -> Result<Summary, MessageError> {
        let damaged = |reason| MessageError::Damaged { reason };
        let message = encoding::aligned_message(Kind::Summary, bytes).map_err(damaged)?;
        match version.written() {
            Written::V1 => {
                let stored = rkyv::access::<v1::ArchivedSummary, rkyv::rancor::Error>(&message)
                    .map_err(|error| damaged(error.to_string()))?;
                read::summary(stored).map_err(damaged)
            }
        }
    }
}

/// The range that holds `range` and `value`.
fn widened<T: Ord + Copy>(range: Option<(T, T)>, value: T) -> (T, T) {
    match range {
        Some((lowest, highest)) => (lowest.min(value), highest.max(value)),
        None => (value, value),
    }
}

/// The range that holds both ranges.
fn spanning<T: Ord + Copy>(one: Option<(T, T)>, other: Option<(T, T)>) -> Option<(T, T)> {
    match (one, other) {
        (Some((lowest, highest)), Some((other_lowest, other_highest))) => {
            Some((lowest.min(other_lowest), highest.max(other_highest)))
        }
        (range, None) | (None, range) => range,
    }
}

/// What a reader looks for, as far as a summary can tell.
///
/// It carries a selector to a storage without the storage knowing one.
/// A storage asks it about every summary it has, at every level it has one,
/// and reads no stretch that cannot hold what is wanted.
/// A storage without summaries asks nothing and hands out everything.
///
/// A function of a summary is a `Wanted`, and [`Everything`] is the one that always says yes.
///
/// ```
/// use gaslightmq_call::CallId;
/// use gaslightmq_recording::prelude::*;
///
/// // A reader that looks for the calls 100 to 200 rules out every stretch that cannot hold one of them.
/// let (first, last) = (CallId::new(100).expect("a call ID"), CallId::new(200).expect("a call ID"));
/// let wanted = move |summary: &Summary| {
///     summary.call_ids().is_some_and(|(lowest, highest)| lowest <= last && first <= highest)
/// };
///
/// assert!(!wanted.may_be_in(&Summary::empty()));
/// assert!(Everything.may_be_in(&Summary::empty()));
/// ```
pub trait Wanted: Send + 'static {
    /// Whether the stretch that `summary` sums up can hold what the reader wants.
    /// `false` promises that it holds nothing of it.
    fn may_be_in(&self, summary: &Summary) -> bool;
}

impl<F: Fn(&Summary) -> bool + Send + 'static> Wanted for F {
    fn may_be_in(&self, summary: &Summary) -> bool {
        self(summary)
    }
}

/// What a reader wants that reads a recording whole: it says yes to every stretch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Everything;

impl Wanted for Everything {
    fn may_be_in(&self, _summary: &Summary) -> bool {
        true
    }
}
