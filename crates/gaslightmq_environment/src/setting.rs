use std::fmt;
use std::str::FromStr;

use crate::text::text_form;

/// A place that can set a setting.
///
/// A setting can come from several places, and the first one that sets it wins.
/// The variants stand in that order, and they compare in it: a flag is the least, and it wins.
///
/// | Source | Is | Text |
/// |---|---|---|
/// | [`Flag`](Self::Flag) | A command-line flag. | `flag` |
/// | [`Variable`](Self::Variable) | An environment variable. | `variable` |
/// | [`Environment`](Self::Environment) | The [environment](crate::Environment) the setting belongs to. | `environment` |
/// | [`Workspace`](Self::Workspace) | The settings of the workspace. | `workspace` |
/// | [`Default`](Self::Default) | The built-in default. | `default` |
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// assert!(SettingSource::Flag < SettingSource::Environment);
/// assert_eq!(SettingSource::ORDER[0], SettingSource::Flag);
/// assert_eq!(SettingSource::Variable.to_string(), "variable");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SettingSource {
    /// A command-line flag.
    Flag,
    /// An environment variable.
    Variable,
    /// The environment the setting belongs to.
    Environment,
    /// The settings of the workspace.
    Workspace,
    /// The built-in default.
    Default,
}

impl SettingSource {
    /// Every source, the one that wins first.
    pub const ORDER: [Self; 5] = [
        Self::Flag,
        Self::Variable,
        Self::Environment,
        Self::Workspace,
        Self::Default,
    ];

    /// The text of the source.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Flag => "flag",
            Self::Variable => "variable",
            Self::Environment => "environment",
            Self::Workspace => "workspace",
            Self::Default => "default",
        }
    }

    /// Where the source stands in the [order](Self::ORDER).
    const fn position(self) -> usize {
        self as usize
    }
}

impl FromStr for SettingSource {
    type Err = SettingSourceError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ORDER
            .into_iter()
            .find(|source| source.name() == text)
            .ok_or_else(|| SettingSourceError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for SettingSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(SettingSource, SettingSourceError);

/// Why a text is not a [`SettingSource`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SettingSourceError {
    /// No source has this name.
    #[error("no source of a setting is named {name:?}: there are flag, variable, environment, workspace and default")]
    Unknown { name: String },
}

/// One setting, with what each [source](SettingSource) gave for it.
///
/// The caller hands in the value of each source that set the setting.
/// Nothing here reads a flag, a variable or a file.
/// [`effective`](Self::effective) then gives the value of the first source that set it, in the
/// [order](SettingSource::ORDER) of the sources.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// // The file of the environment names a host, and the command names another one with a flag.
/// let in_the_file: HostName = "gaslight.staging".parse()?;
/// let flag: Option<HostName> = Some("localhost".parse()?);
/// let variable: Option<HostName> = None;
///
/// let advertise = Setting::new()
///     .set(SettingSource::Environment, in_the_file)
///     .set(SettingSource::Flag, flag)
///     .set(SettingSource::Variable, variable);
///
/// let effective = advertise.effective().expect("the file sets it");
/// assert_eq!(*effective.value(), "localhost");
/// assert_eq!(effective.source(), SettingSource::Flag);
/// # Ok::<(), HostNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setting<T> {
    /// The value of each source, in the order of [`SettingSource::ORDER`].
    values: [Option<T>; 5],
}

impl<T> Setting<T> {
    /// A setting that no source sets.
    pub fn new() -> Self {
        Self {
            values: [None, None, None, None, None],
        }
    }

    /// A setting with a built-in default, which holds until another source sets it.
    pub fn with_default(default: T) -> Self {
        Self::new().set(SettingSource::Default, default)
    }

    /// The same setting with what `source` gave for it: a value, or `None` when the source does not set it.
    ///
    /// What the source gave before is gone.
    #[must_use]
    pub fn set(mut self, source: SettingSource, value: impl Into<Option<T>>) -> Self {
        self.values[source.position()] = value.into();
        self
    }

    /// What `source` gave, or `None` when it does not set the setting.
    pub fn given_by(&self, source: SettingSource) -> Option<&T> {
        self.values[source.position()].as_ref()
    }

    /// The value of the first source that sets the setting, or `None` when no source does.
    pub fn effective(&self) -> Option<EffectiveSetting<&T>> {
        SettingSource::ORDER
            .into_iter()
            .find_map(|source| self.given_by(source).map(|value| EffectiveSetting { value, source }))
    }

    /// The value of the first source that sets the setting, without the other values.
    pub fn into_effective(self) -> Option<EffectiveSetting<T>> {
        SettingSource::ORDER
            .into_iter()
            .zip(self.values)
            .find_map(|(source, value)| value.map(|value| EffectiveSetting { value, source }))
    }
}

/// A setting that no source sets.
impl<T> Default for Setting<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// The value of a setting, and the [source](SettingSource) that set it.
///
/// Whoever shows the value can say where it comes from.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let recording = Setting::with_default(RecordingMode::MetadataOnly)
///     .set(SettingSource::Workspace, RecordingMode::Full)
///     .into_effective()
///     .expect("the default sets it");
///
/// assert_eq!(recording.source(), SettingSource::Workspace);
/// assert_eq!(recording.into_value(), RecordingMode::Full);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EffectiveSetting<T> {
    value: T,
    source: SettingSource,
}

impl<T> EffectiveSetting<T> {
    /// The value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// The source that set the value.
    pub fn source(&self) -> SettingSource {
        self.source
    }

    /// The value, without its source.
    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T: Clone> EffectiveSetting<&T> {
    /// The same setting with a value of its own, and not a borrowed one.
    pub fn cloned(&self) -> EffectiveSetting<T> {
        EffectiveSetting {
            value: self.value.clone(),
            source: self.source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sources_stand_in_the_order_that_wins() {
        use SettingSource::{Default, Environment, Flag, Variable, Workspace};

        assert_eq!(SettingSource::ORDER, [Flag, Variable, Environment, Workspace, Default]);
        assert!(SettingSource::ORDER.is_sorted());
        for (position, source) in SettingSource::ORDER.into_iter().enumerate() {
            assert_eq!(source.position(), position);
        }
    }

    #[test]
    fn a_source_has_a_name() {
        for (source, name) in
            SettingSource::ORDER
                .into_iter()
                .zip(["flag", "variable", "environment", "workspace", "default"])
        {
            assert_eq!(source.to_string(), name);
            assert_eq!(name.parse::<SettingSource>(), Ok(source));
        }
        let error = "file".parse::<SettingSource>().unwrap_err();
        assert_eq!(
            error,
            SettingSourceError::Unknown {
                name: "file".to_owned()
            }
        );
        assert_eq!(
            error.to_string(),
            "no source of a setting is named \"file\": there are flag, variable, environment, workspace and default"
        );
    }

    #[test]
    fn a_setting_that_no_source_sets_has_no_value() {
        let setting: Setting<u16> = Setting::new();

        assert_eq!(setting.effective(), None);
        assert_eq!(setting, Setting::default());
        assert_eq!(setting.into_effective(), None);
    }

    #[test]
    fn the_first_source_that_sets_a_setting_wins() {
        // Each source gives its own position, so the value says who won.
        let all = SettingSource::ORDER
            .into_iter()
            .fold(Setting::new(), |setting, source| setting.set(source, source.position()));

        for (position, winner) in SettingSource::ORDER.into_iter().enumerate() {
            // The sources before the winner do not set the setting.
            let setting = SettingSource::ORDER[..position]
                .iter()
                .fold(all.clone(), |setting, source| setting.set(*source, None));

            let effective = setting.effective().unwrap();
            assert_eq!((*effective.value(), effective.source()), (&position, winner));
            assert_eq!(effective.cloned(), setting.clone().into_effective().unwrap());
            assert_eq!(setting.into_effective().unwrap().into_value(), position);
        }
    }

    #[test]
    fn the_order_does_not_depend_on_the_order_of_the_calls() {
        let setting = Setting::with_default("default")
            .set(SettingSource::Workspace, "workspace")
            .set(SettingSource::Flag, "flag")
            .set(SettingSource::Environment, "environment");

        assert_eq!(setting.effective().unwrap().source(), SettingSource::Flag);
        assert_eq!(setting.given_by(SettingSource::Workspace), Some(&"workspace"));
        assert_eq!(setting.given_by(SettingSource::Variable), None);
    }

    #[test]
    fn a_default_holds_until_another_source_sets_the_setting() {
        let setting = Setting::with_default(30);
        let effective = setting.effective().unwrap();
        assert_eq!((*effective.value(), effective.source()), (&30, SettingSource::Default));

        let setting = setting.set(SettingSource::Variable, 10);
        let effective = setting.into_effective().unwrap();
        assert_eq!(
            (effective.into_value(), effective.source()),
            (10, SettingSource::Variable)
        );
    }

    #[test]
    fn what_a_source_gave_before_is_gone() {
        let setting = Setting::new()
            .set(SettingSource::Flag, 1)
            .set(SettingSource::Flag, 2)
            .set(SettingSource::Workspace, 3);
        assert_eq!(setting.effective().unwrap().into_value(), &2);

        let setting = setting.set(SettingSource::Flag, None);
        assert_eq!(setting.effective().unwrap().source(), SettingSource::Workspace);
    }
}
