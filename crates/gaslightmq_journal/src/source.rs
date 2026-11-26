//! Where a change came from: the surface, and the cause of a change that takes back or repeats an earlier one.

use std::fmt;

/// Where a change came from.
///
/// The journal records the surface, and not a person: the control plane has no users.
///
/// | Surface | The change came from |
/// |---|---|
/// | [`Pad`](Self::Pad) | A press or a turn at the pad grid. |
/// | [`QuickInject`](Self::QuickInject) | A ⌘K sentence. |
/// | [`Ui`](Self::Ui) | Any other view of the UI. |
/// | [`Cli`](Self::Cli) | `gmq`. |
/// | [`Test`](Self::Test) | A test library. |
/// | [`Api`](Self::Api) | Any other client of the control plane. |
/// | [`Step`](Self::Step) | A step of the active scenario. |
/// | [`Lifetime`](Self::Lifetime) | A lifetime that ended, or a firing that spent it. |
/// | [`File`](Self::File) | A file of the workspace that someone edited, taken in by a reload. |
///
/// Whoever asks for a change names one of the first six.
/// Whoever runs the environment names the last three itself, for a change that nobody asked for,
/// so no request names one.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// assert_eq!(ChangeSurface::QuickInject.to_string(), "quick-inject");
/// assert_eq!(ChangeSurface::Lifetime.name(), "lifetime");
/// ```
///
/// With the `serde` feature, a surface serializes as its name: `quick-inject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub enum ChangeSurface {
    /// A press or a turn at the pad grid.
    Pad,
    /// A ⌘K sentence.
    QuickInject,
    /// Any other view of the UI.
    Ui,
    /// `gmq`.
    Cli,
    /// A test library.
    Test,
    /// Any other client of the control plane.
    Api,
    /// A step of the active scenario.
    Step,
    /// A lifetime that ended, or a firing that spent it.
    Lifetime,
    /// A file of the workspace that someone edited, taken in by a reload.
    File,
}

impl ChangeSurface {
    /// The name of the surface in lower case, with a hyphen between two words.
    pub const fn name(self) -> &'static str {
        match self {
            ChangeSurface::Pad => "pad",
            ChangeSurface::QuickInject => "quick-inject",
            ChangeSurface::Ui => "ui",
            ChangeSurface::Cli => "cli",
            ChangeSurface::Test => "test",
            ChangeSurface::Api => "api",
            ChangeSurface::Step => "step",
            ChangeSurface::Lifetime => "lifetime",
            ChangeSurface::File => "file",
        }
    }
}

/// Writes the name of the surface.
impl fmt::Display for ChangeSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Why a change came, when it takes back or repeats an earlier change of whoever asked for it.
///
/// Nothing takes an entry out of a journal.
/// An undo is a change like any other: it adds an entry, with the cause `undo`.
/// The journal then shows both: that someone armed a rule, and that they took it back.
///
/// With the `serde` feature, a cause serializes as its name: `undo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "kebab-case")
)]
pub enum ChangeCause {
    /// The change takes back an earlier change.
    Undo,
    /// The change makes a change again that someone took back.
    Redo,
}

impl ChangeCause {
    /// The name of the cause in lower case.
    pub const fn name(self) -> &'static str {
        match self {
            ChangeCause::Undo => "undo",
            ChangeCause::Redo => "redo",
        }
    }
}

/// Writes the name of the cause.
impl fmt::Display for ChangeCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

/// Where a change comes from: its surface, and its cause when it takes back or repeats an earlier change.
///
/// Whoever asks for a change says where it comes from, and the journal records that with the change.
/// A [`ChangeSurface`] is a source without a cause, so most changes name their surface alone.
///
/// ```
/// use gaslightmq_journal::prelude::*;
///
/// let press = ChangeSource::from(ChangeSurface::Pad);
/// assert_eq!(press.surface, ChangeSurface::Pad);
/// assert_eq!(press.cause, None);
///
/// // Someone presses ⌘Z, and the tab takes its last change back.
/// let undo = ChangeSource::new(ChangeSurface::Ui).with_cause(ChangeCause::Undo);
/// assert_eq!(undo.surface, ChangeSurface::Ui);
/// assert_eq!(undo.cause, Some(ChangeCause::Undo));
/// ```
///
/// With the `serde` feature, it is a map of its fields, and it leaves out the `cause` it does not have:
/// `{ "surface": "ui", "cause": "undo" }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct ChangeSource {
    /// The surface that the change comes from.
    pub surface: ChangeSurface,
    /// Why the change comes, when it takes back or repeats an earlier one. `None` for every other change.
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    pub cause: Option<ChangeCause>,
}

impl ChangeSource {
    /// A change of `surface` that neither takes back nor repeats an earlier one.
    pub const fn new(surface: ChangeSurface) -> Self {
        Self { surface, cause: None }
    }

    /// The same source, for a change that has `cause`.
    #[must_use]
    pub const fn with_cause(self, cause: ChangeCause) -> Self {
        Self {
            surface: self.surface,
            cause: Some(cause),
        }
    }
}

/// A change of a surface, without a cause.
impl From<ChangeSurface> for ChangeSource {
    fn from(surface: ChangeSurface) -> Self {
        Self::new(surface)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_surface_and_a_cause_write_their_names() {
        let surfaces = [
            (ChangeSurface::Pad, "pad"),
            (ChangeSurface::QuickInject, "quick-inject"),
            (ChangeSurface::Ui, "ui"),
            (ChangeSurface::Cli, "cli"),
            (ChangeSurface::Test, "test"),
            (ChangeSurface::Api, "api"),
            (ChangeSurface::Step, "step"),
            (ChangeSurface::Lifetime, "lifetime"),
            (ChangeSurface::File, "file"),
        ];

        for (surface, name) in surfaces {
            assert_eq!(surface.to_string(), name);
        }
        assert_eq!(ChangeCause::Undo.to_string(), "undo");
        assert_eq!(ChangeCause::Redo.to_string(), "redo");
    }

    #[test]
    fn a_surface_is_a_source_without_a_cause() {
        let source = ChangeSource::from(ChangeSurface::Cli);

        assert_eq!(source, ChangeSource::new(ChangeSurface::Cli));
        assert_eq!(source.cause, None);
        assert_eq!(source.with_cause(ChangeCause::Redo).cause, Some(ChangeCause::Redo));
    }
}
