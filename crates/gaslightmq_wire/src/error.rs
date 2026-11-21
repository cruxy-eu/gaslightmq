use std::fmt;

/// Why bytes could not become a frame, a request, a response or a record
/// batch, or the other way around.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// A frame announced a length of zero.
    EmptyFrame,
    /// A frame announced more bytes than the decoder accepts.
    FrameTooLarge { len: usize, max: usize },
    /// There are fewer bytes than the fixed part of `what` needs.
    Truncated { what: &'static str, len: usize },
    /// The body belongs to an API this crate has no messages for.
    UnknownApi { api_key: i16 },
    /// The body was asked for as a message of another API.
    WrongApi { expected: i16, actual: i16 },
    /// The bytes of `what` are not valid.
    Decode { what: &'static str, reason: String },
    /// `what` cannot be written, for example at a version it does not exist in.
    Encode { what: &'static str, reason: String },
}

impl WireError {
    pub(crate) fn decode(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::Decode { what, reason: reason.to_string() }
    }

    pub(crate) fn encode(what: &'static str, reason: impl fmt::Display) -> Self {
        Self::Encode { what, reason: reason.to_string() }
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFrame => f.write_str("frame of zero bytes"),
            Self::FrameTooLarge { len, max } => write!(f, "frame of {len} bytes exceeds the limit of {max}"),
            Self::Truncated { what, len } => write!(f, "{what} is cut short at {len} bytes"),
            Self::UnknownApi { api_key } => write!(f, "unknown api key {api_key}"),
            Self::WrongApi { expected, actual } => write!(f, "body of api key {actual} read as api key {expected}"),
            Self::Decode { what, reason } => write!(f, "cannot decode {what}: {reason}"),
            Self::Encode { what, reason } => write!(f, "cannot encode {what}: {reason}"),
        }
    }
}

impl std::error::Error for WireError {}
