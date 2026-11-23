use std::fmt;

/// Why bytes could not become a frame, a request, a response or a record
/// batch, or the other way around.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// A frame announced a length of zero.
    #[error("frame of zero bytes")]
    EmptyFrame,
    /// A frame announced more bytes than the decoder accepts.
    #[error("frame of {len} bytes exceeds the limit of {max}")]
    FrameTooLarge { len: usize, max: usize },
    /// There are fewer bytes than the fixed part of `what` needs.
    #[error("{what} is cut short at {len} bytes")]
    Truncated { what: &'static str, len: usize },
    /// The body belongs to an API this crate has no messages for.
    #[error("unknown api key {api_key}")]
    UnknownApi { api_key: i16 },
    /// The body was asked for as a message of another API.
    #[error("body of api key {actual} read as api key {expected}")]
    WrongApi { expected: i16, actual: i16 },
    /// The bytes of `what` are not valid.
    #[error("cannot decode {what}: {reason}")]
    Decode { what: &'static str, reason: String },
    /// `what` cannot be written, for example at a version it does not exist in.
    #[error("cannot encode {what}: {reason}")]
    Encode { what: &'static str, reason: String },
}

impl WireError {
    pub(crate) fn decode(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::Decode {
            what,
            reason: reason.to_string(),
        }
    }

    pub(crate) fn encode(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::Encode {
            what,
            reason: reason.to_string(),
        }
    }
}
