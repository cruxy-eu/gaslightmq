use std::fmt;
use std::str::FromStr;

/// A mode of play: what a press and a release of a pad do.
///
/// | Mode | Name | A press | A release |
/// |---|---|---|---|
/// | [`Latch`](Self::Latch) | `latch` | Toggles the pad: it arms a rule that is not armed, and disarms an armed one. | Nothing. |
/// | [`Hold`](Self::Hold) | `hold` | Arms the pad. | Disarms it. |
///
/// A surface starts in latch.
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// assert_eq!(PlayMode::default(), PlayMode::Latch);
/// assert_eq!("hold".parse(), Ok(PlayMode::Hold));
/// assert_eq!(PlayMode::Hold.to_string(), "hold");
/// ```
///
/// With the `serde` feature, a mode serializes as its name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum PlayMode {
    /// A press toggles the pad.
    #[default]
    Latch,
    /// The pad is armed while pressed, and disarms on release.
    Hold,
}

impl PlayMode {
    /// Both modes, the one a surface starts in first.
    pub const ALL: [PlayMode; 2] = [PlayMode::Latch, PlayMode::Hold];

    /// The name of the mode in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            PlayMode::Latch => "latch",
            PlayMode::Hold => "hold",
        }
    }
}

/// Writes the name of the mode.
impl fmt::Display for PlayMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Reads the name of a mode.
impl FromStr for PlayMode {
    type Err = PlayModeError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        PlayMode::ALL
            .into_iter()
            .find(|mode| mode.name() == name)
            .ok_or_else(|| PlayModeError::Unknown { name: name.to_owned() })
    }
}

/// Why a text is not a [`PlayMode`].
///
/// ```
/// use gaslightmq_surface::prelude::*;
///
/// let error = "toggle".parse::<PlayMode>().unwrap_err();
/// assert_eq!(error.to_string(), "no mode of play is called \"toggle\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlayModeError {
    /// No mode of play has this name.
    #[error("no mode of play is called {name:?}")]
    Unknown { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mode_reads_and_writes_its_name() {
        assert_eq!(PlayMode::ALL.map(PlayMode::name), ["latch", "hold"]);
        for mode in PlayMode::ALL {
            assert_eq!(mode.to_string(), mode.name());
            assert_eq!(mode.name().parse(), Ok(mode));
        }
        assert_eq!(PlayMode::default(), PlayMode::Latch);
    }

    #[test]
    fn a_name_of_no_mode_is_an_error() {
        for name in ["", "Latch", "toggle"] {
            assert_eq!(
                name.parse::<PlayMode>(),
                Err(PlayModeError::Unknown { name: name.to_owned() })
            );
        }
    }
}
