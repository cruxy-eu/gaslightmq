use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::{Action, ActionRef, PresetName};

/// A named, reusable lie or incident with its parameters.
///
/// ```yaml
/// name: Delay 1 s
/// lie: timing
/// delay: 1s
/// jitter: 200ms
/// ```
///
/// ```yaml
/// name: Leader election
/// incident: leader-moves
/// ```
///
/// Rules reference a preset by its name, and pad grid columns, ⌘K suggestions and rule editors offer presets.
///
/// ```
/// use gaslightmq_clock::TimeSpan;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// let delay = TimingLie::new()
///     .with_delay(TimeSpan::from_secs(1))
///     .with_jitter(TimeSpan::from_millis(200));
/// let preset = Preset::new("Delay 1 s".parse()?, delay);
///
/// assert_eq!(preset.name(), "Delay 1 s");
/// assert_eq!(preset.action().to_string(), "+1 s ±200 ms");
/// # Ok::<(), PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Preset {
    name: PresetName,
    #[cfg_attr(feature = "serde", serde(flatten))]
    action: Action,
}

impl Preset {
    /// The preset that gives `action` a name.
    pub fn new(name: PresetName, action: impl Into<Action>) -> Self {
        Self {
            name,
            action: action.into(),
        }
    }

    /// The same preset under another name.
    /// The rules that reference the preset by its old name do not follow: whoever holds them updates them.
    #[must_use]
    pub fn renamed(self, name: PresetName) -> Self {
        Self { name, ..self }
    }

    /// The same preset with another action.
    #[must_use]
    pub fn with_action(self, action: impl Into<Action>) -> Self {
        Self {
            action: action.into(),
            ..self
        }
    }

    /// The name of the preset, unique among presets.
    pub fn name(&self) -> &PresetName {
        &self.name
    }

    /// The lie or the incident the preset names.
    pub fn action(&self) -> &Action {
        &self.action
    }
}

/// The presets of a workspace by name, which resolves the action of a rule.
///
/// Every name is there once, and [`new`](Self::new) checks that.
/// A set does not change: build a new one from the presets after an edit.
///
/// Whether a rule or a pad layout names a preset that exists is for whoever holds those to check,
/// with [`contains`](Self::contains) or with [`Rule::resolve`](crate::Rule::resolve).
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// let name = "Leader flap".parse().expect("a preset name");
/// let flap = Preset::new(name, RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));
/// let presets = PresetSet::new([flap])?;
///
/// // A rule writes `action: Leader flap`, or an action of its own.
/// let named = ActionRef::from("Leader flap".parse::<PresetName>().expect("a preset name"));
/// let inline = ActionRef::from(LieDefinition::Silence);
///
/// assert_eq!(presets.resolve(&named).map(ToString::to_string), Some("NOT_LEADER_OR_FOLLOWER".into()));
/// assert_eq!(presets.resolve(&inline).map(ToString::to_string), Some("silence".into()));
/// assert_eq!(presets.resolve(&"Blackhole".parse::<PresetName>().expect("a preset name").into()), None);
/// # Ok::<(), PresetSetError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PresetSet {
    by_name: BTreeMap<PresetName, Preset>,
}

impl PresetSet {
    /// The set of those presets.
    ///
    /// # Errors
    ///
    /// [`PresetSetError::DuplicateName`] when two presets have one name.
    pub fn new(presets: impl IntoIterator<Item = Preset>) -> Result<Self, PresetSetError> {
        let mut by_name = BTreeMap::new();
        for preset in presets {
            if let Some(twice) = by_name.insert(preset.name().clone(), preset) {
                return Err(PresetSetError::DuplicateName {
                    name: twice.name().clone(),
                });
            }
        }
        Ok(Self { by_name })
    }

    /// The preset of this name.
    pub fn get(&self, name: &PresetName) -> Option<&Preset> {
        self.by_name.get(name)
    }

    /// Whether a preset has this name.
    pub fn contains(&self, name: &PresetName) -> bool {
        self.by_name.contains_key(name)
    }

    /// The presets in the order of their names.
    pub fn iter(&self) -> btree_map::Values<'_, PresetName, Preset> {
        self.by_name.values()
    }

    /// How many presets there are.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Whether there is no preset.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// The action that `action` references: the one of a preset of the set, or the inline one.
    ///
    /// The result is `None` when the reference names a preset that the set does not have.
    pub fn resolve<'a>(&'a self, action: &'a ActionRef) -> Option<&'a Action> {
        match action {
            ActionRef::Preset(name) => self.get(name).map(Preset::action),
            ActionRef::Inline(action) => Some(action),
        }
    }
}

impl<'a> IntoIterator for &'a PresetSet {
    type Item = &'a Preset;
    type IntoIter = btree_map::Values<'a, PresetName, Preset>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for PresetSet {
    type Item = Preset;
    type IntoIter = btree_map::IntoValues<PresetName, Preset>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_name.into_values()
    }
}

/// Why presets are not a [`PresetSet`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PresetSetError {
    /// Two presets have the same name.
    #[error("two presets are named {:?}", .name.as_str())]
    DuplicateName { name: PresetName },
}

#[cfg(test)]
mod tests {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_incident::IncidentDefinition;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    use super::*;

    fn name(text: &str) -> PresetName {
        text.parse().unwrap()
    }

    fn delay() -> Preset {
        Preset::new(name("Delay 1 s"), TimingLie::new().with_delay(TimeSpan::from_secs(1)))
    }

    fn flap() -> Preset {
        Preset::new(name("Leader flap"), RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER))
    }

    fn election() -> Preset {
        Preset::new(
            name("Leader election"),
            IncidentDefinition::LeaderMoves {
                partition: None,
                to: None,
            },
        )
    }

    #[test]
    fn a_preset_names_a_lie_or_an_incident() {
        assert_eq!(delay().name(), "Delay 1 s");
        assert!(delay().action().is_lie());
        assert!(election().action().is_incident());
    }

    #[test]
    fn a_preset_changes_one_part_at_a_time() {
        let renamed = delay().renamed(name("Slow"));
        assert_eq!(renamed.name(), "Slow");
        assert_eq!(renamed.action(), delay().action());

        let silenced = delay().with_action(LieDefinition::Silence);
        assert_eq!(silenced.name(), delay().name());
        assert_eq!(silenced.action(), &Action::Lie(LieDefinition::Silence));
    }

    #[test]
    fn a_set_holds_its_presets_by_name() {
        let set = PresetSet::new([flap(), delay(), election()]).unwrap();

        assert_eq!(set.len(), 3);
        assert!(!set.is_empty());
        assert!(set.contains(&name("Leader flap")));
        assert!(!set.contains(&name("leader flap")));
        assert_eq!(set.get(&name("Delay 1 s")), Some(&delay()));
        assert_eq!(set.get(&name("Blackhole")), None);
        assert_eq!(
            set.iter().map(Preset::name).collect::<Vec<_>>(),
            [&name("Delay 1 s"), &name("Leader election"), &name("Leader flap")]
        );
        assert_eq!((&set).into_iter().count(), 3);
        assert_eq!(set.into_iter().collect::<Vec<_>>(), [delay(), election(), flap()]);
    }

    #[test]
    fn an_empty_set_is_a_set() {
        let set = PresetSet::new([]).unwrap();

        assert!(set.is_empty());
        assert_eq!(set, PresetSet::default());
    }

    #[test]
    fn two_presets_with_one_name_are_an_error() {
        assert_eq!(
            PresetSet::new([delay(), flap(), delay().with_action(LieDefinition::Silence)]),
            Err(PresetSetError::DuplicateName {
                name: name("Delay 1 s")
            })
        );
    }

    #[test]
    fn a_reference_resolves_to_a_preset_of_the_set_or_to_its_own_action() {
        let set = PresetSet::new([delay(), flap()]).unwrap();
        let inline = ActionRef::from(LieDefinition::Silence);

        assert_eq!(set.resolve(&name("Leader flap").into()), Some(flap().action()));
        assert_eq!(set.resolve(&inline), Some(&Action::Lie(LieDefinition::Silence)));
        assert_eq!(set.resolve(&name("Blackhole").into()), None);
    }
}
