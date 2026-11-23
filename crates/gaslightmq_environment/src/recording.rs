use std::fmt;
use std::str::FromStr;

use crate::text::text_form;

/// How much of a call a recording keeps.
///
/// | Mode | Keeps | Text |
/// |---|---|---|
/// | [`Full`](Self::Full) | Everything. | `full` |
/// | [`MetadataOnly`](Self::MetadataOnly) | Everything but the keys, values and headers of records, which can hold personal or secret data. | `metadata-only` |
///
/// Whatever the mode, no recording keeps a client's credentials.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let mode: RecordingMode = "metadata-only".parse()?;
/// assert_eq!(mode, RecordingMode::MetadataOnly);
/// assert!(!mode.keeps_records());
/// assert_eq!(RecordingMode::Full.to_string(), "full");
/// # Ok::<(), RecordingModeError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordingMode {
    /// The recording keeps every call whole.
    Full,
    /// The recording drops record keys, values and headers, and keeps everything else.
    MetadataOnly,
}

impl RecordingMode {
    /// Every mode.
    pub const ALL: [Self; 2] = [Self::Full, Self::MetadataOnly];

    /// The text of the mode in a file: `full` or `metadata-only`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::MetadataOnly => "metadata-only",
        }
    }

    /// Whether a recording keeps the keys, values and headers of records.
    pub const fn keeps_records(self) -> bool {
        matches!(self, Self::Full)
    }
}

impl FromStr for RecordingMode {
    type Err = RecordingModeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.name() == text)
            .ok_or_else(|| RecordingModeError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for RecordingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(RecordingMode, RecordingModeError);

/// Why a text is not a [`RecordingMode`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecordingModeError {
    /// No mode has this name.
    Unknown { name: String },
}

impl fmt::Display for RecordingModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { name } => write!(
                f,
                "no recording mode is named {name:?}: there are full and metadata-only"
            ),
        }
    }
}

impl std::error::Error for RecordingModeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mode_has_a_name() {
        for (mode, name) in [
            (RecordingMode::Full, "full"),
            (RecordingMode::MetadataOnly, "metadata-only"),
        ] {
            assert_eq!(mode.name(), name);
            assert_eq!(mode.to_string(), name);
            assert_eq!(name.parse::<RecordingMode>(), Ok(mode));
            assert_eq!(String::from(mode), name);
        }
        assert!(RecordingMode::Full.keeps_records());
        assert!(!RecordingMode::MetadataOnly.keeps_records());
    }

    #[test]
    fn a_name_of_no_mode_is_an_error() {
        for name in ["", "Full", "metadata_only", "metadata"] {
            assert_eq!(
                name.parse::<RecordingMode>(),
                Err(RecordingModeError::Unknown { name: name.to_owned() })
            );
        }
        assert_eq!(
            "metadata".parse::<RecordingMode>().unwrap_err().to_string(),
            "no recording mode is named \"metadata\": there are full and metadata-only"
        );
    }
}
