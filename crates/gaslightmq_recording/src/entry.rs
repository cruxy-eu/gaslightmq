//! What a recording holds after its description: session events, calls with their frames, and journal entries.

use std::fmt;

use bytes::Bytes;
use gaslightmq_call::Call;
use gaslightmq_clock::Instant;
use gaslightmq_environment::RecordingMode;
use gaslightmq_field::{ClientAddress, ClientSoftware, ConnectionNumber, Endpoint};
use gaslightmq_journal::{JournalEntry, JournalEntryId};
use gaslightmq_kernel::ClientId;
use gaslightmq_wire::Frame;

use crate::encoding::{Framed, Kind, v1, write};
use crate::{CallEntryError, redact};

/// Who closed a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Closer {
    /// The client closed the connection, or gave up on it.
    Client,
    /// The backend went away.
    Backend,
    /// A hang-up lie closed the connection.
    Lie,
    /// The proxy closed the connection itself, because it could not keep the answers of the connection in order.
    Proxy,
}

/// What happened to a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionChange {
    /// A client connected to an endpoint.
    ///
    /// A recording starts with one of these for every session that is open at that moment.
    /// Such an event holds what the session has learned by then.
    Opened {
        /// Where the client connected from.
        address: ClientAddress,
        /// The endpoint that the client connected to.
        endpoint: Endpoint,
        /// The `client.id` of the session, when it was identified before the recording started and carries one.
        client_id: Option<ClientId>,
        /// The software of the client, when it was named before the recording started.
        software: Option<ClientSoftware>,
    },
    /// The first request arrived, so the session knows which client it belongs to.
    Identified {
        /// The `client.id`, when the first request carried one.
        client_id: Option<ClientId>,
    },
    /// An `ApiVersions` request named the software of the client.
    SoftwareNamed {
        /// The name and the version of the software.
        software: ClientSoftware,
    },
    /// The session ended.
    Closed {
        /// Who closed it.
        by: Closer,
    },
}

/// One event of one session, where it happened.
///
/// Every connection number in a recording has its session in the same recording:
/// a recording starts with an [`Opened`](SessionChange::Opened) event for every session that is open at that moment,
/// whose instant is when the session really opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedSessionEvent {
    /// The number of the connection, which every call of the session names.
    pub connection: ConnectionNumber,
    /// When it happened, on the clock of the environment.
    pub at: Instant,
    /// What happened.
    pub change: SessionChange,
}

/// Which of the four frames of a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FrameSlot {
    /// The request as the client sent it.
    Request,
    /// The response of the real cluster, when the overlay of the backend changed it into the truth.
    Upstream,
    /// The truth: the response of the backend, when the request reached it.
    Truth,
    /// The response that the client received, when it received one.
    Delivered,
}

impl FrameSlot {
    /// Every slot, in the order in which a recording keeps the frames of a call.
    pub const ALL: [FrameSlot; 4] = [
        FrameSlot::Request,
        FrameSlot::Upstream,
        FrameSlot::Truth,
        FrameSlot::Delivered,
    ];

    pub(crate) const fn index(self) -> usize {
        match self {
            FrameSlot::Request => 0,
            FrameSlot::Upstream => 1,
            FrameSlot::Truth => 2,
            FrameSlot::Delivered => 3,
        }
    }
}

/// What a recording holds of one frame of a call.
///
/// Every frame that existed keeps the length it had on the wire, whatever the recording holds of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FrameState {
    /// The call has no such frame: a lie ended it before forwarding, or the client received nothing.
    None,
    /// The recording holds the frame as it was on the wire.
    Whole,
    /// The recording holds the frame without what it must not keep. It still decodes.
    Redacted,
    /// The frame existed, and the recording holds none of it.
    Withheld,
}

/// The frames of one call as they were on the wire, as the engine hands them to the recorder next to the call.
///
/// A frame is the buffer that the session read from its socket, or wrote to it.
/// Handing it over shares that buffer, and copies nothing.
///
/// ```
/// use bytes::Bytes;
/// use gaslightmq_recording::prelude::*;
/// use gaslightmq_wire::Frame;
///
/// let read_from_the_socket = Bytes::from_static(&[0, 18, 0, 3, 0, 0, 0, 7]);
/// let frames = CallFrames::new(Frame::new(read_from_the_socket.clone()));
///
/// // The frames hold the same buffer, and no copy of it.
/// assert_eq!(frames.get(FrameSlot::Request).map(|frame| frame.payload().as_ptr()), Some(read_from_the_socket.as_ptr()));
/// assert_eq!(frames.get(FrameSlot::Truth), None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallFrames {
    pub(crate) frames: [Option<Frame>; 4],
}

impl CallFrames {
    /// The frames of a call whose request is `request`, and that has no response yet.
    pub const fn new(request: Frame) -> CallFrames {
        CallFrames {
            frames: [Some(request), None, None, None],
        }
    }

    /// The real cluster answered with `upstream`, and the overlay changed it into the truth.
    #[must_use]
    pub fn with_upstream(mut self, upstream: Frame) -> CallFrames {
        self.frames[FrameSlot::Upstream.index()] = Some(upstream);
        self
    }

    /// The backend answered with `truth`.
    #[must_use]
    pub fn with_truth(mut self, truth: Frame) -> CallFrames {
        self.frames[FrameSlot::Truth.index()] = Some(truth);
        self
    }

    /// The client received `delivered`.
    #[must_use]
    pub fn with_delivered(mut self, delivered: Frame) -> CallFrames {
        self.frames[FrameSlot::Delivered.index()] = Some(delivered);
        self
    }

    /// The frame of `slot`, when the call has one.
    pub const fn get(&self, slot: FrameSlot) -> Option<&Frame> {
        self.frames[slot.index()].as_ref()
    }
}

/// One frame of a recorded call: what the recording holds of it.
///
/// The bytes are a slice of the buffer that the frames of the batch were unpacked into.
/// They copy nothing, and they keep that buffer in memory for as long as someone holds them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedFrame {
    pub(crate) state: FrameState,
    pub(crate) on_the_wire: u64,
    pub(crate) kept: Option<Bytes>,
}

impl RecordedFrame {
    /// What the recording holds of the frame.
    pub const fn state(&self) -> FrameState {
        self.state
    }

    /// The bytes that the frame had on the wire, without the four bytes of its length.
    /// A redacted and a withheld frame keep it too.
    pub const fn length_on_the_wire(&self) -> u64 {
        self.on_the_wire
    }

    /// The bytes that the recording holds of the frame, without the four bytes of its length:
    /// the frame as it was on the wire, or the frame without what the recording must not keep.
    ///
    /// `None` when the recording holds none of it, and when the reader asked for no frames.
    pub const fn bytes(&self) -> Option<&Bytes> {
        self.kept.as_ref()
    }

    /// The same bytes as a frame, which goes into the codec of Kafka and into a pipeline as a frame from a socket
    /// does. It shares the buffer, and copies nothing.
    pub fn frame(&self) -> Option<Frame> {
        self.kept.clone().map(Frame::new)
    }
}

/// What an entry holds for the summary of its stretch.
#[derive(Debug, Clone)]
pub(crate) enum Kept {
    Session { at: Instant },
    Call(Box<Call>),
    Journal { id: JournalEntryId, at: Instant },
}

/// One thing that a recording keeps: a session event, a call with its frames, or a journal entry.
///
/// An entry is made where the recorder takes it, and it is made in one way.
/// Making it encodes it, and for a call it applies the mode of the recording and blanks credentials,
/// so a storage takes entries and never sees what a recording must not keep.
///
/// A storage stores the bytes of an entry as they are, and looks into none of them.
/// It hands an entry to a [`BatchEncoder`](crate::BatchEncoder), which gives the bytes and the frames.
pub struct Entry {
    message: Framed,
    frames: Vec<Bytes>,
    kept: Kept,
}

impl Entry {
    /// The entry of an event of a session.
    pub fn session(event: &RecordedSessionEvent) -> Entry {
        Entry {
            message: Framed::archive(Kind::Session, &write::session_event(event)),
            frames: Vec::new(),
            kept: Kept::Session { at: event.at },
        }
    }

    /// The entry of a call that ended: its record, and its frames as they were on the wire.
    ///
    /// `mode` is the mode of the recording. This is where a frame loses what the recording must not keep:
    ///
    /// | The frame | In a full recording | In a metadata-only recording |
    /// |---|---|---|
    /// | A `SaslAuthenticate` request, or a response to one | [Redacted](FrameState::Redacted): its authentication bytes are empty. | The same. |
    /// | A frame that holds records: a Produce request, and a Fetch or a `ShareFetch` response | [Whole](FrameState::Whole). | Redacted: every record is there without its key, its value and its headers. A control batch stays whole. |
    /// | A frame of any other API that the codec reads | Whole. | Whole. |
    /// | A frame that the codec cannot read | [Withheld](FrameState::Withheld). | Withheld. |
    ///
    /// A whole frame is the buffer that `frames` holds, shared and not copied.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use gaslightmq_call::prelude::*;
    /// use gaslightmq_clock::Instant;
    /// use gaslightmq_environment::RecordingMode;
    /// use gaslightmq_field::prelude::*;
    /// use gaslightmq_kernel::prelude::*;
    /// use gaslightmq_recording::prelude::*;
    /// use gaslightmq_wire::messages::ProduceRequest;
    /// use gaslightmq_wire::messages::produce_request::{PartitionProduceData, TopicProduceData};
    /// use gaslightmq_wire::protocol::StrBytes;
    /// use gaslightmq_wire::{Frame, Record, RecordBatch, Request};
    ///
    /// // A producer writes a card number to `orders.eu/3`.
    /// let card = b"4111 1111 1111 1111";
    /// let records = RecordBatch::new(vec![Record::new(Bytes::from_static(card)).with_key("customer-42")]);
    /// let partition = PartitionProduceData::default()
    ///     .with_index(3)
    ///     .with_records(Some(records.encode().expect("a batch of one record")));
    /// let topic = TopicProduceData::default()
    ///     .with_name(StrBytes::from_static_str("orders.eu").into())
    ///     .with_partition_data(vec![partition]);
    /// let body = ProduceRequest::default().with_acks(-1).with_topic_data(vec![topic]);
    /// let (api_version, correlation_id) = (9, 1);
    /// let produce = Request::new(api_version, correlation_id, Some("order-ingest"), &body).expect("a Produce request");
    /// let fields = Fields::new()
    ///     .with(ConnectionNumber::new(7))
    ///     .with(ApiKey::PRODUCE)
    ///     .with_part("orders.eu/3".parse::<TopicPartition>().expect("a topic partition"))
    ///     .with(Outcome::Ok);
    /// let call = Call::new(
    ///     CallId::FIRST,
    ///     fields,
    ///     ResponseSummary::new(Answer::Ok).into(),
    ///     Explanation::none(),
    ///     CallTiming::arrived(Instant::ZERO),
    /// )
    /// .expect("a call on connection 7");
    ///
    /// // A metadata-only recording keeps the Produce, and not what its record holds.
    /// let frames = CallFrames::new(produce.frame().clone());
    /// let entry = Entry::call(call, frames, RecordingMode::MetadataOnly)?;
    /// let encoded = BatchEncoder::new().encode(entry);
    /// let [kept] = encoded.frames() else { panic!("the recording keeps the request") };
    /// assert!(!kept.windows(card.len()).any(|bytes| bytes == card));
    ///
    /// // What it keeps is still a Produce for `orders.eu/3`, with one record.
    /// let kept: ProduceRequest = Request::decode(Frame::new(kept.clone()))
    ///     .and_then(|request| request.body())
    ///     .expect("a redacted frame decodes");
    /// let records = kept.topic_data[0].partition_data[0].records.as_ref().expect("the records stay");
    /// let batches = RecordBatch::decode_all(records).expect("a redacted batch decodes");
    /// assert_eq!((kept.acks, batches[0].records.len()), (-1, 1));
    /// assert_eq!((&batches[0].records[0].key, &batches[0].records[0].value), (&None, &None));
    /// # Ok::<(), CallEntryError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`CallEntryError::LieNotStored`] when the call tells a lie that the encoding has no stored type for.
    pub fn call(call: Call, frames: CallFrames, mode: RecordingMode) -> Result<Entry, CallEntryError> {
        let kept = redact::frames(frames, mode);
        let slots = kept.each_ref().map(|frame| v1::FrameSlot {
            state: write::frame_state(frame.state),
            on_the_wire: frame.on_the_wire,
            kept: frame.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64),
        });
        let message = Framed::archive(Kind::Call, &write::call(&call, slots)?);
        Ok(Entry {
            message,
            frames: kept.into_iter().filter_map(|frame| frame.bytes).collect(),
            kept: Kept::Call(Box::new(call)),
        })
    }

    /// The entry of a journal entry, which stands where its change happened.
    pub fn journal(entry: &JournalEntry) -> Entry {
        Entry {
            message: Framed::archive(Kind::Journal, &write::journal_entry(entry)),
            frames: Vec::new(),
            kept: Kept::Journal {
                id: entry.id,
                at: entry.at,
            },
        }
    }

    /// Whether the entry is a call.
    pub const fn is_call(&self) -> bool {
        matches!(self.kept, Kept::Call(_))
    }

    /// The bytes that the entry takes among the entries of its batch: its head, its message,
    /// and the zeros up to the next multiple of 16.
    pub fn size(&self) -> usize {
        self.message.len()
    }

    /// The bytes that the entry takes among the frames of its batch: the frames that the recording holds of it.
    pub fn frames_size(&self) -> usize {
        self.frames.iter().map(Bytes::len).sum()
    }

    pub(crate) const fn kept(&self) -> &Kept {
        &self.kept
    }

    pub(crate) fn into_parts(self) -> (Framed, Vec<Bytes>) {
        (self.message, self.frames)
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("kept", &self.kept)
            .field("size", &self.size())
            .field("frames_size", &self.frames_size())
            .finish_non_exhaustive()
    }
}
