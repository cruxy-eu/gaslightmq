use std::fmt;

/// Why the bytes of a connection are not the start of a frame.
///
/// [`FrameDecoder::next_frame`](crate::FrameDecoder::next_frame) returns it.
/// The length that a frame announces is a value that the decoder checks,
/// and each variant says what is wrong with that length.
///
/// ```
/// use gaslightmq_wire::FrameError;
///
/// let error = FrameError::TooLarge { len: 9, max: 8 };
/// assert_eq!(error.to_string(), "frame of 9 bytes exceeds the limit of 8");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// A frame announced a length of zero.
    #[error("frame of zero bytes")]
    Empty,
    /// A frame announced more bytes than the decoder accepts.
    #[error("frame of {len} bytes exceeds the limit of {max}")]
    TooLarge { len: usize, max: usize },
}

/// Why bytes are not a header, a body or a record batch.
///
/// Everything that reads bytes returns it:
/// [`Request::decode`](crate::Request::decode), [`Response::decode`](crate::Response::decode)
/// and [`Response::peek_correlation_id`](crate::Response::peek_correlation_id) for a header,
/// [`Request::body`](crate::Request::body) and [`Response::body`](crate::Response::body) for a body,
/// and [`RecordBatch::decode_all`](crate::RecordBatch::decode_all) and [`RecordBatches::decode`](crate::RecordBatches::decode)
/// for records.
///
/// Whoever asked knows what it asked for, and does the same whatever is wrong with the bytes.
/// So the error has no variants: its message names what did not decode, and says why.
///
/// ```
/// use gaslightmq_wire::messages::{FetchRequest, MetadataRequest};
/// use gaslightmq_wire::{EncodeError, Request};
///
/// let request = Request::new(12, 7, Some("orders"), &MetadataRequest::default())?;
/// let error = request.body::<FetchRequest>().expect_err("a Metadata request has no Fetch body");
/// assert_eq!(
///     error.to_string(),
///     "cannot decode request body: it is of api key 3, and it was read as api key 1"
/// );
/// # Ok::<(), EncodeError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot decode {what}: {reason}")]
pub struct DecodeError {
    /// What the bytes were read as, such as a request header.
    what: &'static str,
    /// What is wrong with the bytes.
    reason: String,
}

impl DecodeError {
    pub(crate) fn new(what: &'static str, reason: impl fmt::Display) -> Self {
        Self {
            what,
            reason: reason.to_string(),
        }
    }

    /// There are `len` bytes, and the fixed part of `what` needs more.
    pub(crate) fn cut_short(what: &'static str, len: usize) -> Self {
        Self::new(what, format_args!("it is cut short at {len} bytes"))
    }
}

/// Why a header, a body or a record batch cannot be written, for example at a version it does not exist in.
///
/// Everything that writes bytes returns it:
/// [`Request::new`](crate::Request::new), [`Response::new`](crate::Response::new),
/// [`Request::response`](crate::Request::response), [`RecordBatch::encode`](crate::RecordBatch::encode)
/// and [`RecordBatches::encode`](crate::RecordBatches::encode).
///
/// Whoever asked does the same whatever stood in the way.
/// So the error has no variants: its message names what did not encode, and says why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot encode {what}: {reason}")]
pub struct EncodeError {
    /// What was written, such as a response body.
    what: &'static str,
    /// What stood in the way.
    reason: String,
}

impl EncodeError {
    pub(crate) fn new(what: &'static str, reason: impl fmt::Display) -> Self {
        Self {
            what,
            reason: reason.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn an_error_says_everything_in_one_message() {
        let empty = FrameError::Empty;
        assert_eq!(empty.to_string(), "frame of zero bytes");
        assert!(empty.source().is_none());

        let too_large = FrameError::TooLarge { len: 9, max: 8 };
        assert_eq!(too_large.to_string(), "frame of 9 bytes exceeds the limit of 8");
        assert!(too_large.source().is_none());

        let cut_short = DecodeError::cut_short("request header", 3);
        assert_eq!(
            cut_short.to_string(),
            "cannot decode request header: it is cut short at 3 bytes"
        );
        assert!(cut_short.source().is_none());

        let unwritable = EncodeError::new("record batch", "a record has too many bytes");
        assert_eq!(
            unwritable.to_string(),
            "cannot encode record batch: a record has too many bytes"
        );
        assert!(unwritable.source().is_none());
    }
}
