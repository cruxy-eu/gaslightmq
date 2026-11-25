//! Which theme the UI shows: the one of the system, or one that someone picked.

/// The theme someone picked in the shell. The browser keeps the choice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ThemeChoice {
    /// Light or dark, as the system prefers, and again when the system changes.
    #[default]
    System,
    /// Light, whatever the system prefers.
    Light,
    /// Dark, whatever the system prefers.
    Dark,
}

impl ThemeChoice {
    /// Every choice, in the order the shell offers them.
    pub const ALL: [ThemeChoice; 3] = [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark];

    /// The name the browser keeps.
    pub const fn name(self) -> &'static str {
        match self {
            ThemeChoice::System => "system",
            ThemeChoice::Light => "light",
            ThemeChoice::Dark => "dark",
        }
    }

    /// The choice the browser keeps under `name`. A name that it does not know follows the system.
    ///
    /// ```
    /// use gaslightmq_ui::ThemeChoice;
    ///
    /// assert_eq!(ThemeChoice::named("light"), ThemeChoice::Light);
    /// assert_eq!(ThemeChoice::named("sepia"), ThemeChoice::System);
    /// ```
    pub fn named(name: &str) -> Self {
        ThemeChoice::ALL
            .into_iter()
            .find(|choice| choice.name() == name)
            .unwrap_or_default()
    }

    /// The choice as the shell writes it.
    pub const fn label(self) -> &'static str {
        match self {
            ThemeChoice::System => "System",
            ThemeChoice::Light => "Light",
            ThemeChoice::Dark => "Dark",
        }
    }
}
