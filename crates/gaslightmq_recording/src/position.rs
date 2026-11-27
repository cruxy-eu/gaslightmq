//! Where an entry is in its recording, where a reader starts, and how far a recording goes.

use std::fmt;

use bytes::Bytes;
use gaslightmq_clock::Instant;

use crate::MessageError;
use crate::encoding::{self, EncodingVersion, Framed, Kind, Written, read, v1, write};

/// Which entry of its recording this is: a number that counts from 0.
///
/// A storage gives every entry the next position, so a higher position is a later entry.
///
/// ```
/// use gaslightmq_recording::prelude::*;
///
/// let third = Position::FIRST.next().next();
/// assert_eq!(third, Position::new(2));
/// assert_eq!(third.to_string(), "2");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(u64);

impl Position {
    /// The position of the first entry of a recording.
    pub const FIRST: Position = Position(0);

    /// The position with that number.
    pub const fn new(number: u64) -> Position {
        Position(number)
    }

    /// The number of the position: how many entries stand before it.
    pub const fn number(self) -> u64 {
        self.0
    }

    /// The position of the entry after this one. It stops at the highest position.
    #[must_use]
    pub const fn next(self) -> Position {
        Position(self.0.saturating_add(1))
    }

    /// The position `entries` entries after this one. It stops at the highest position.
    #[must_use]
    pub const fn after(self, entries: u64) -> Position {
        Position(self.0.saturating_add(entries))
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Where a stream of entries starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Place {
    /// At the first entry of the recording.
    Start,
    /// At the last entry that a commit covers, at the moment the stream starts.
    End,
    /// At the entry with this position.
    At(Position),
}

/// The direction of a stream of entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Towards later entries: the positions rise.
    Forwards,
    /// Towards earlier entries: the positions fall.
    Backwards,
}

/// Whether a reader wants the frames of the calls it reads.
///
/// In a full recording the frames are most of the bytes.
/// A reader that asks for none gets every call with the states and the lengths of its frames,
/// and no storage reads a frame for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Frames {
    /// The reader takes the frames that the recording holds.
    With,
    /// The reader takes no frame.
    Without,
}

/// How a recording ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Ending {
    /// Someone stopped the recording.
    Stopped,
    /// The environment of the recording stopped.
    EnvironmentStopped,
    /// The recorder fell behind the traffic, and stopped the recording itself.
    /// The last entry of the recording is the journal entry that says so.
    FellBehind,
}

impl Ending {
    /// How the recording ended, in words.
    pub const fn description(self) -> &'static str {
        match self {
            Ending::Stopped => "someone stopped the recording",
            Ending::EnvironmentStopped => "the environment stopped",
            Ending::FellBehind => "the recorder could not keep up",
        }
    }
}

impl fmt::Display for Ending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.description())
    }
}

/// The end of a recording that ended in order.
///
/// A recording without an end still runs, or it was cut short: by a crash, or by a write that failed.
/// A recording cannot say that it could not be written, so such a recording has no end.
///
/// ```
/// use gaslightmq_clock::{Duration, Instant};
/// use gaslightmq_recording::prelude::*;
///
/// let end = End {
///     ending: Ending::Stopped,
///     at: Instant::ZERO + Duration::from_secs(90),
///     entries: 1204,
///     calls: 1180,
/// };
///
/// // A layout keeps the end as a message, and reads it back.
/// assert_eq!(End::decode(EncodingVersion::CURRENT, &end.encode()), Ok(end));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct End {
    /// How the recording ended.
    pub ending: Ending,
    /// When it ended, on the clock of the environment.
    pub at: Instant,
    /// How many entries the recording holds.
    pub entries: u64,
    /// How many of them are calls.
    pub calls: u64,
}

impl End {
    /// The end as a message behind its head: what a layout keeps of it.
    pub fn encode(&self) -> Bytes {
        Framed::archive(Kind::End, &write::end(self)).into_bytes()
    }

    /// Reads the end that [`encode`](Self::encode) wrote, in the version that its recording says.
    ///
    /// # Errors
    ///
    /// [`MessageError::Damaged`] when the bytes are no end.
    pub fn decode(version: EncodingVersion, bytes: &[u8]) -> Result<End, MessageError> {
        let damaged = |reason| MessageError::Damaged { reason };
        let message = encoding::aligned_message(Kind::End, bytes).map_err(damaged)?;
        match version.written() {
            Written::V1 => {
                let stored = rkyv::access::<v1::ArchivedEnd, rkyv::rancor::Error>(&message)
                    .map_err(|error| damaged(error.to_string()))?;
                Ok(read::end(stored))
            }
        }
    }
}

/// How far a recording goes, and whether it ended in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Extent {
    /// How many entries a commit covers. The position of the last of them is one less.
    pub entries: u64,
    /// The end of the recording, when it ended in order.
    /// Without one, the recording still runs, or it was cut short.
    pub end: Option<End>,
}
