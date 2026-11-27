//! The encoding of a recording: what a message is in bytes, for every version that was ever written.
//!
//! A message is the bytes of one thing that a recording holds: an entry, a description, a summary or an end.
//! It is the archive of a stored type: the type as it lies in memory, made free of the place it lies at.
//! Reading a value of it is reading memory, so nothing is decoded, and a message cannot grow.
//! Every change of a stored type is therefore a new version of the encoding.
//!
//! | Module | Holds |
//! |---|---|
//! | `v1` | The stored types of version 1. A release freezes them. |
//! | `write` | The way from the types of a call, a journal and a recording to the stored types of the current version. |
//! | `read` | The way back, through the checks of every type. |
//!
//! # A new version
//!
//! The specification says the steps, in "When `Call` or a journal entry changes".
//! In this crate they are:
//!
//! 1. Add the module of the next version, with the stored types that change and every stored type that holds one,
//!    and take every other stored type from the version before, under its name.
//! 2. Write the conversion from the version before in the new module, for every stored type that was written anew.
//! 3. Move `write`, `read` and the views to the stored types of the new version, and raise
//!    [`EncodingVersion::CURRENT`].
//! 4. In [`Batch::checked`](crate::Batch::checked) and in the readers of the other messages, give the version before
//!    its arm: check the buffer with its stored types, convert each value, and write it again.
//! 5. Add the samples of the new version and the file of their bytes, next to those of version 1.

use bytes::Bytes;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Portable, Serialize};

pub(crate) mod read;
pub(crate) mod v1;
#[cfg(test)]
mod v1_samples;
pub(crate) mod write;

/// The address that a message asks for: it starts at a multiple of this many bytes, in the buffer it is read from.
///
/// 16 is the most that a value of a stored type can ask for, which a number of 128 bits does,
/// so no later stored type changes the rule.
pub(crate) const ALIGNMENT: usize = 16;

/// A buffer that starts at a multiple of [`ALIGNMENT`].
pub(crate) type Aligned = AlignedVec<ALIGNMENT>;

/// The version of the encoding of a recording, for a number that this build reads.
///
/// A recording says the version that it was written in, and a build reads every version that a release wrote.
/// So a value of this type is a version whose stored types this build has.
///
/// ```
/// use gaslightmq_recording::prelude::*;
///
/// assert_eq!(EncodingVersion::read_by_this_build(1), Ok(EncodingVersion::CURRENT));
///
/// let newer = EncodingVersion::CURRENT.number() + 1;
/// let refused = EncodingVersion::read_by_this_build(newer).unwrap_err();
/// assert_eq!(refused, EncodingVersionError::Newer { written: newer, known: EncodingVersion::CURRENT.number() });
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodingVersion(Written);

/// The versions that a build ever wrote, each with its stored types in this build.
///
/// Whatever reads a message decides on this type, without an arm for the rest,
/// so a new version does not compile until every reader has the way from the version before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Written {
    V1 = 1,
}

impl EncodingVersion {
    /// The version that this build writes.
    pub const CURRENT: EncodingVersion = EncodingVersion(Written::V1);

    /// The version with that number, as a recording says it.
    ///
    /// # Errors
    ///
    /// - [`EncodingVersionError::Newer`] for a number above [`CURRENT`](Self::CURRENT):
    ///   a later build wrote the recording.
    /// - [`EncodingVersionError::Zero`] for zero, which no build writes.
    pub const fn read_by_this_build(number: u16) -> Result<EncodingVersion, EncodingVersionError> {
        match number {
            0 => Err(EncodingVersionError::Zero),
            1 => Ok(EncodingVersion(Written::V1)),
            newer => Err(EncodingVersionError::Newer {
                written: newer,
                known: Self::CURRENT.number(),
            }),
        }
    }

    /// The number of the version, as a recording says it.
    pub const fn number(self) -> u16 {
        self.0 as u16
    }

    /// Which of the versions that a build ever wrote this is.
    pub(crate) const fn written(self) -> Written {
        self.0
    }
}

/// Why a number is no [`EncodingVersion`] that this build reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodingVersionError {
    /// The versions of the encoding count from one, so what says zero is no recording.
    #[error("the versions of the encoding of a recording count from 1")]
    Zero,
    /// A later build wrote the recording, in a version that this build has no stored types for.
    #[error("the recording has version {written} of the encoding, and this build reads up to version {known}")]
    Newer {
        /// The version that the recording has.
        written: u16,
        /// The newest version that this build reads.
        known: u16,
    },
}

/// The kind of a message, as its head says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Session = 1,
    Call = 2,
    Journal = 3,
    Description = 4,
    Summary = 5,
    End = 6,
}

impl Kind {
    const fn of(byte: u8) -> Option<Kind> {
        Some(match byte {
            1 => Kind::Session,
            2 => Kind::Call,
            3 => Kind::Journal,
            4 => Kind::Description,
            5 => Kind::Summary,
            6 => Kind::End,
            _ => return None,
        })
    }

    pub(crate) const fn what(self) -> &'static str {
        match self {
            Kind::Session => "a session event",
            Kind::Call => "a call",
            Kind::Journal => "a journal entry",
            Kind::Description => "a description",
            Kind::Summary => "a summary",
            Kind::End => "an end",
        }
    }
}

/// The head of a message: 16 bytes before it.
///
/// | Offset | Size | Holds |
/// |---|---|---|
/// | 0 | 4 | The length of the message. |
/// | 4 | 1 | The kind of the message. |
/// | 5 | 3 | Zero. |
/// | 8 | 8 | For a call that the recording holds frames of: where they start in the frames of its batch. Zero for every other message. |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Head {
    pub(crate) len: usize,
    pub(crate) kind: Kind,
    pub(crate) frames_at: u64,
}

impl Head {
    /// The bytes of a head.
    pub(crate) const LEN: usize = 16;

    /// Reads the head at the start of `bytes`, and checks that its message lies inside them.
    pub(crate) fn read(bytes: &[u8]) -> Result<Head, String> {
        let Some((&[l0, l1, l2, l3, kind, z0, z1, z2, f0, f1, f2, f3, f4, f5, f6, f7], rest)) =
            bytes.split_first_chunk::<{ Head::LEN }>()
        else {
            return Err(format!(
                "{} bytes are left, and a head is {} bytes",
                bytes.len(),
                Head::LEN
            ));
        };
        let len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
        let Some(kind) = Kind::of(kind) else {
            return Err(format!("no message has the kind {kind}"));
        };
        if [z0, z1, z2] != [0, 0, 0] {
            return Err("the bytes 5 to 7 of a head are zero".to_owned());
        }
        if len > rest.len() {
            return Err(format!(
                "the head says a message of {len} bytes, and {} bytes are left",
                rest.len()
            ));
        }
        Ok(Head {
            len,
            kind,
            frames_at: u64::from_le_bytes([f0, f1, f2, f3, f4, f5, f6, f7]),
        })
    }

    pub(crate) fn write(self, out: &mut [u8]) {
        let len = u32::try_from(self.len).expect("a message of less than 4 GiB");
        out[0..4].copy_from_slice(&len.to_le_bytes());
        out[4] = self.kind as u8;
        out[5..8].fill(0);
        out[8..16].copy_from_slice(&self.frames_at.to_le_bytes());
    }

    /// The bytes from the start of this head to the start of the next one: the head, the message,
    /// and the zeros up to the next multiple of 16.
    pub(crate) const fn stride(self) -> usize {
        Head::LEN + self.len.next_multiple_of(ALIGNMENT)
    }
}

/// A message behind its head, in a buffer of its own that starts at a multiple of 16:
/// the bytes as they stand in a batch, or in an object of a layout.
#[derive(Debug, Clone)]
pub(crate) struct Framed {
    bytes: Aligned,
    kind: Kind,
    len: usize,
}

impl Framed {
    /// Archives `value` as a message of `kind`.
    pub(crate) fn archive<T>(kind: Kind, value: &T) -> Framed
    where
        T: for<'a> Serialize<HighSerializer<Aligned, ArenaHandle<'a>, rancor::Error>>,
    {
        let mut bytes = Aligned::new();
        bytes.resize(Head::LEN, 0);
        let mut bytes =
            rkyv::api::high::to_bytes_in(value, bytes).expect("a stored type archives into a buffer in memory");
        let len = bytes.len() - Head::LEN;
        bytes.resize(Head::LEN + len.next_multiple_of(ALIGNMENT), 0);
        let mut framed = Framed { bytes, kind, len };
        framed.set_frames_at(0);
        framed
    }

    /// Says where the frames of the message start in the frames of its batch.
    pub(crate) fn set_frames_at(&mut self, frames_at: u64) {
        let head = Head {
            len: self.len,
            kind: self.kind,
            frames_at,
        };
        head.write(&mut self.bytes[..Head::LEN]);
    }

    /// The head, the message and the zeros behind it: a multiple of 16 bytes.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// The bytes as one shared buffer, without a copy.
    pub(crate) fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self.bytes)
    }
}

/// Checks that `message` is the archive of a `T`: every reference, every length, every text and every choice.
///
/// `message` starts at a multiple of 16.
pub(crate) fn check<T>(message: &[u8]) -> Result<(), String>
where
    T: Portable + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>,
{
    rkyv::access::<T, rancor::Error>(message)
        .map(drop)
        .map_err(|error| error.to_string())
}

/// The archive in `message`, read where it lies.
///
/// # Safety
///
/// [`check`] passed for the same `T` on the same bytes, at the same address.
pub(crate) unsafe fn view<T: Portable>(message: &[u8]) -> &T {
    // SAFETY: the caller says that `check::<T>` passed on these bytes where they lie,
    // which is all that an unchecked access asks.
    unsafe { rkyv::access_unchecked::<T>(message) }
}

/// The message of `kind` at the start of `bytes`, behind its head, at a multiple of 16.
///
/// A message that a layout hands over can stand anywhere, so it is copied to a buffer of its own first.
/// These messages are small: a description, a summary or an end.
pub(crate) fn aligned_message(kind: Kind, bytes: &[u8]) -> Result<Aligned, String> {
    let head = Head::read(bytes)?;
    if head.kind != kind {
        return Err(format!("it is {}, and not {}", head.kind.what(), kind.what()));
    }
    let mut message = Aligned::with_capacity(head.len);
    message.extend_from_slice(&bytes[Head::LEN..Head::LEN + head.len]);
    Ok(message)
}
