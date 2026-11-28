//! Views: an entry read where it lies in the buffer of its batch.
//!
//! A view is a place in the buffer. Reading a value through it reads the bytes of that value:
//! a number where it stands, and a text as the bytes it is. No view copies a text, and no view allocates.
//!
//! A view hands out values, and the types check them.
//! The check of a batch says that a text is text, and not that it is the name of a topic.
//! So a view gives a topic as text and an error code as a number,
//! and the methods that build a type, such as [`CallView::to_call`], build it through its checks.
//! They are the one place where a value that its type does not take is found.

use std::ops::Range;

use bytes::Bytes;
use gaslightmq_call::{Call, CallId, Explanation, RuleExplanation};
use gaslightmq_clock::Instant;
use gaslightmq_field::{ConnectionNumber, Field, FieldValue, Fields, PartFields};
use gaslightmq_journal::{JournalEntry, JournalEntryId};
use gaslightmq_lie::Priority;
use rkyv::string::ArchivedString;

use crate::encoding::{read, v1};
use crate::{EntryError, FrameSlot, FrameState, RecordedFrame, RecordedSessionEvent};

fn damaged(reason: String) -> EntryError {
    EntryError::Damaged { reason }
}

/// One entry of a batch, where it lies.
#[derive(Debug, Clone, Copy)]
pub enum EntryView<'a> {
    /// An event of a session.
    Session(SessionEventView<'a>),
    /// A call that ended, with its frames.
    Call(CallView<'a>),
    /// A journal entry.
    Journal(JournalEntryView<'a>),
}

impl EntryView<'_> {
    /// When the entry happened, on the clock of the environment. A call happened when its request arrived.
    pub fn at(&self) -> Instant {
        match self {
            EntryView::Session(event) => event.at(),
            EntryView::Call(call) => call.arrived_at(),
            EntryView::Journal(entry) => entry.at(),
        }
    }
}

/// An event of a session, where it lies.
#[derive(Debug, Clone, Copy)]
pub struct SessionEventView<'a> {
    event: &'a v1::ArchivedSessionEvent,
}

impl<'a> SessionEventView<'a> {
    pub(crate) const fn new(event: &'a v1::ArchivedSessionEvent) -> Self {
        Self { event }
    }

    /// The number of the connection of the session.
    pub fn connection(&self) -> ConnectionNumber {
        ConnectionNumber::new(self.event.connection.to_native())
    }

    /// When it happened, on the clock of the environment.
    pub fn at(&self) -> Instant {
        read::instant(self.event.at)
    }

    /// The event as its type, built through its checks. It owns what it holds, and keeps no buffer in memory.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_session_event(&self) -> Result<RecordedSessionEvent, EntryError> {
        read::session_event(self.event).map_err(damaged)
    }
}

/// A journal entry, where it lies.
#[derive(Debug, Clone, Copy)]
pub struct JournalEntryView<'a> {
    entry: &'a v1::ArchivedJournalEntry,
}

impl<'a> JournalEntryView<'a> {
    pub(crate) const fn new(entry: &'a v1::ArchivedJournalEntry) -> Self {
        Self { entry }
    }

    /// Which entry of its journal this is.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the number is no ID of a journal entry.
    pub fn id(&self) -> Result<JournalEntryId, EntryError> {
        read::journal_entry_id(self.entry.id).map_err(damaged)
    }

    /// When the change happened, on the clock of the environment.
    pub fn at(&self) -> Instant {
        read::instant(self.entry.at)
    }

    /// The entry as its type, built through its checks. It owns what it holds, and keeps no buffer in memory.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_journal_entry(&self) -> Result<JournalEntry, EntryError> {
        read::journal_entry(self.entry).map_err(damaged)
    }
}

/// A call record, where it lies, with the slots of its frames.
#[derive(Debug, Clone, Copy)]
pub struct CallView<'a> {
    call: &'a v1::ArchivedCall,
    frames: Option<&'a Bytes>,
    frames_at: u64,
}

impl<'a> CallView<'a> {
    pub(crate) const fn new(call: &'a v1::ArchivedCall, frames: Option<&'a Bytes>, frames_at: u64) -> Self {
        Self {
            call,
            frames,
            frames_at,
        }
    }

    /// Which call of its environment this is.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the number is no call ID.
    pub fn id(&self) -> Result<CallId, EntryError> {
        read::call_id(self.call.id).map_err(damaged)
    }

    /// The connection of the session that the call ran on.
    pub fn connection(&self) -> ConnectionNumber {
        ConnectionNumber::new(self.call.connection.to_native())
    }

    /// When the request arrived, on the clock of the environment.
    pub fn arrived_at(&self) -> Instant {
        read::instant(self.call.timing.arrived)
    }

    /// The fields of the call.
    pub const fn fields(&self) -> FieldsView<'a> {
        FieldsView {
            fields: &self.call.fields,
        }
    }

    /// Why the verdict of the call came out as it did.
    pub const fn explanation(&self) -> ExplanationView<'a> {
        ExplanationView {
            explanation: &self.call.explanation,
        }
    }

    /// Where the frames that the recording holds of this call lie in `frames`, the frames of the batch:
    /// back to back, in the order of the slots.
    fn kept(&self, frames: &Bytes) -> Result<[Range<usize>; 4], EntryError> {
        let outside = || damaged("the frames of the call lie outside the frames of its batch".to_owned());
        let mut at = usize::try_from(self.frames_at).map_err(|_| outside())?;
        let mut ranges = [0..0, 0..0, 0..0, 0..0];
        for (range, slot) in ranges.iter_mut().zip(self.call.frames.iter()) {
            let len = usize::try_from(slot.kept.to_native()).map_err(|_| outside())?;
            let end = at.checked_add(len).filter(|end| *end <= frames.len());
            *range = at..end.ok_or_else(outside)?;
            at = range.end;
        }
        Ok(ranges)
    }

    /// Every frame that the recording holds of this call, as one slice of the frames of the batch.
    /// It is empty for a reader that asked for no frames.
    pub(crate) fn kept_frames(&self) -> Result<Bytes, EntryError> {
        let Some(frames) = self.frames else {
            return Ok(Bytes::new());
        };
        let [first, .., last] = self.kept(frames)?;
        Ok(frames.slice(first.start..last.end))
    }

    /// One of the four frames of the call: its state, the length it had on the wire, and the bytes that the recording
    /// holds of it, when the reader asked for frames.
    ///
    /// The bytes are a slice of the buffer that the frames of the batch were unpacked into.
    /// They copy nothing, and they keep that buffer in memory for as long as someone holds them.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the slot says a frame that lies outside the frames of the batch.
    pub fn frame(&self, slot: FrameSlot) -> Result<RecordedFrame, EntryError> {
        let stored = &self.call.frames[slot.index()];
        let state = read::frame_state(&stored.state);
        let kept = match (self.frames, state) {
            (Some(frames), FrameState::Whole | FrameState::Redacted) => {
                let [request, upstream, truth, delivered] = self.kept(frames)?;
                Some(frames.slice(match slot {
                    FrameSlot::Request => request,
                    FrameSlot::Upstream => upstream,
                    FrameSlot::Truth => truth,
                    FrameSlot::Delivered => delivered,
                }))
            }
            (Some(_), FrameState::None | FrameState::Withheld) | (None, _) => None,
        };
        Ok(RecordedFrame {
            state,
            on_the_wire: stored.on_the_wire.to_native(),
            kept,
        })
    }

    /// The call as its type, built through its checks. It owns what it holds, and keeps no buffer in memory.
    ///
    /// A reader that shows a call, or hands it to someone else, asks for the type.
    /// A reader that replays, matches, counts or seeds does not.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_call(&self) -> Result<Call, EntryError> {
        read::call(self.call).map_err(damaged)
    }
}

/// The fields of a call, where they lie.
///
/// A selector matches a field source, and a field source lends no value yet: its readings own what they hold.
/// Until it does, a selector matches the [`Fields`] that [`to_fields`](Self::to_fields) builds,
/// and no more of the call.
#[derive(Debug, Clone, Copy)]
pub struct FieldsView<'a> {
    fields: &'a v1::ArchivedFields,
}

impl<'a> FieldsView<'a> {
    /// What the call and its session have, in the order of the fields.
    pub fn values(&self) -> impl Iterator<Item = FieldValueView<'a>> + use<'a> {
        self.fields.values.iter().map(|value| FieldValueView { value })
    }

    /// What the client context provides, in the order of the fields.
    pub fn learned(&self) -> impl Iterator<Item = FieldValueView<'a>> + use<'a> {
        self.fields.learned.iter().map(|value| FieldValueView { value })
    }

    /// The topics and partitions that the request names, in the order of the request.
    pub fn parts(&self) -> impl Iterator<Item = PartView<'a>> + use<'a> {
        self.fields.parts.iter().map(|part| PartView { part })
    }

    /// The fields as their type, which a selector matches as it matches a live call.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_fields(&self) -> Result<Fields, EntryError> {
        read::fields(self.fields).map_err(damaged)
    }
}

/// One value of one field of a call, where it lies.
#[derive(Debug, Clone, Copy)]
pub struct FieldValueView<'a> {
    value: &'a v1::ArchivedFieldValue,
}

impl<'a> FieldValueView<'a> {
    /// The field that this is a value of.
    pub const fn field(&self) -> Field {
        use v1::ArchivedFieldValue as Stored;
        match self.value {
            Stored::Connection(_) => Field::Connection,
            Stored::Address { .. } => Field::Address,
            Stored::Host(_) => Field::Host,
            Stored::Endpoint(_) => Field::Endpoint,
            Stored::ClientId(_) => Field::ClientId,
            Stored::ClientSoftware { .. } => Field::ClientSoftware,
            Stored::Api(_) => Field::Api,
            Stored::ApiVersion(_) => Field::ApiVersion,
            Stored::Topic(_) => Field::Topic,
            Stored::Partition(_) => Field::Partition,
            Stored::Acks(_) => Field::Acks,
            Stored::Group(_) => Field::Group,
            Stored::MemberId(_) => Field::MemberId,
            Stored::GroupInstanceId(_) => Field::GroupInstanceId,
            Stored::TransactionalId(_) => Field::TransactionalId,
            Stored::Outcome(_) => Field::Outcome,
            Stored::ErrorCode(_) => Field::ErrorCode,
            Stored::Latency(_) => Field::Latency,
        }
    }

    /// The value as the text it is, for a field whose value is one text:
    /// `client.id`, `topic`, `group`, `member.id`, `group.instance.id` and `transactional.id`.
    pub fn text(&self) -> Option<&'a str> {
        use v1::ArchivedFieldValue as Stored;
        match self.value {
            Stored::ClientId(text)
            | Stored::Topic(text)
            | Stored::Group(text)
            | Stored::MemberId(text)
            | Stored::GroupInstanceId(text)
            | Stored::TransactionalId(text) => Some(text.as_str()),
            _ => None,
        }
    }

    /// The value as the number it is, for a field whose value is one number:
    /// `connection`, `api`, `api.version`, `partition`, `acks`, `error.code`, and `latency` in milliseconds.
    pub fn number(&self) -> Option<i128> {
        use v1::ArchivedFieldValue as Stored;
        Some(match self.value {
            Stored::Connection(number) | Stored::Latency(number) => number.to_native().into(),
            Stored::Api(number) | Stored::Acks(number) | Stored::ErrorCode(number) => number.to_native().into(),
            Stored::ApiVersion(number) => number.to_native().into(),
            Stored::Partition(number) => number.to_native().into(),
            _ => return None,
        })
    }

    /// The value as its type, built through its checks.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_value(&self) -> Result<FieldValue, EntryError> {
        read::field_value(self.value).map_err(damaged)
    }
}

/// One topic or partition that a request names, where it lies.
#[derive(Debug, Clone, Copy)]
pub struct PartView<'a> {
    part: &'a v1::ArchivedPartFields,
}

impl<'a> PartView<'a> {
    /// The name of the topic, as the text it is.
    pub fn topic(&self) -> &'a str {
        match &self.part.part {
            v1::ArchivedPart::Topic(topic) => topic.as_str(),
            v1::ArchivedPart::Partition(partition) => partition.topic.as_str(),
        }
    }

    /// The number of the partition, when the request names one.
    pub fn partition(&self) -> Option<u32> {
        match &self.part.part {
            v1::ArchivedPart::Topic(_) => None,
            v1::ArchivedPart::Partition(partition) => Some(partition.partition.to_native()),
        }
    }

    /// The part and its truth as their type, built through its checks.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_part_fields(&self) -> Result<PartFields, EntryError> {
        read::part_fields(self.part).map_err(damaged)
    }
}

/// The explanation of a call, where it lies.
#[derive(Debug, Clone, Copy)]
pub struct ExplanationView<'a> {
    explanation: &'a v1::ArchivedExplanation,
}

impl<'a> ExplanationView<'a> {
    /// The lines of the explanation: every rule that the call touched, the highest priority first.
    pub fn rules(&self) -> impl Iterator<Item = RuleExplanationView<'a>> + use<'a> {
        self.explanation.rules.iter().map(|line| RuleExplanationView { line })
    }

    /// What went wrong inside GaslightMQ while it handled the call, when something did.
    pub fn failure(&self) -> Option<&'a str> {
        self.explanation.failure.as_ref().map(ArchivedString::as_str)
    }

    /// The explanation as its type, built through its checks. The verdict of the call follows from it.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_explanation(&self) -> Result<Explanation, EntryError> {
        read::explanation(self.explanation).map_err(damaged)
    }
}

/// One line of an explanation, where it lies: one rule that the call touched.
#[derive(Debug, Clone, Copy)]
pub struct RuleExplanationView<'a> {
    line: &'a v1::ArchivedRuleExplanation,
}

impl<'a> RuleExplanationView<'a> {
    /// The name that the rule had when the call met it, as the text it is.
    pub fn rule(&self) -> &'a str {
        self.line.rule.as_str()
    }

    /// The priority that the rule had.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the number is no priority on this machine.
    pub fn priority(&self) -> Result<Priority, EntryError> {
        read::priority(self.line.priority).map_err(damaged)
    }

    /// Whether the rule fired: it told a lie, or it fired incidents.
    pub const fn fired(&self) -> bool {
        matches!(
            self.line.course,
            v1::ArchivedCourse::Lied { .. } | v1::ArchivedCourse::FiredIncidents { .. }
        )
    }

    /// The line as its type, built through its checks.
    ///
    /// # Errors
    ///
    /// [`EntryError::Damaged`] when the entry holds a value that its type does not take.
    pub fn to_rule_explanation(&self) -> Result<RuleExplanation, EntryError> {
        read::rule_explanation(self.line).map_err(damaged)
    }
}
