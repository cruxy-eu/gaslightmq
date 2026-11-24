use std::collections::{BTreeMap, BTreeSet};

use gaslightmq_kernel::{ClientId, GroupId, TopicName};
use gaslightmq_rule::{PresetName, PresetTitle};
use gaslightmq_selector::{SelectorName, SelectorTitle};

use crate::CompletionKind;

/// The names the system knows, which a sentence can say without saying what they are.
///
/// Whoever reads a sentence hands them over: the topics, groups and clients that the traffic showed,
/// and the presets and selectors of the workspace, each with its title when it has one.
/// A sentence reads a known name by itself, and it offers the known names as completions.
/// A name that is not known still reads after the word for its kind: `topic orders.eu`.
///
/// A sentence says a preset and a selector by its name, and never by its title.
/// The title only helps to find the name: a word that starts a word of the title completes to the name.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_quick_inject::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let known = KnownNames::new()
///     .with_topics(["orders.eu".parse()?, "orders.us".parse()?])
///     .with_clients([ClientId::new("order-ingest").expect("a client ID")])
///     .with_selectors([(
///         "order-producers".parse().expect("a selector name"),
///         Some("Order producers".parse().expect("a selector title")),
///     )]);
///
/// assert_eq!(known.topics().count(), 2);
/// assert!(known.groups().next().is_none());
/// let titles: Vec<Option<&str>> = known.selectors().map(|(_, title)| title.map(SelectorTitle::as_str)).collect();
/// assert_eq!(titles, [Some("Order producers")]);
/// # Ok::<(), TopicNameError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KnownNames {
    presets: BTreeMap<PresetName, Option<PresetTitle>>,
    selectors: BTreeMap<SelectorName, Option<SelectorTitle>>,
    topics: BTreeSet<TopicName>,
    groups: BTreeSet<GroupId>,
    clients: BTreeSet<ClientId>,
}

impl KnownNames {
    /// No names: a sentence then reads a name only after the word for its kind.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same names, and those topics.
    #[must_use]
    pub fn with_topics(mut self, topics: impl IntoIterator<Item = TopicName>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// The same names, and those consumer groups.
    #[must_use]
    pub fn with_groups(mut self, groups: impl IntoIterator<Item = GroupId>) -> Self {
        self.groups.extend(groups);
        self
    }

    /// The same names, and the IDs of those clients.
    #[must_use]
    pub fn with_clients(mut self, clients: impl IntoIterator<Item = ClientId>) -> Self {
        self.clients.extend(clients);
        self
    }

    /// The same names, and those presets, each with its title when it has one.
    /// A preset that is there already takes the new title.
    #[must_use]
    pub fn with_presets(mut self, presets: impl IntoIterator<Item = (PresetName, Option<PresetTitle>)>) -> Self {
        self.presets.extend(presets);
        self
    }

    /// The same names, and those selectors, each with its title when it has one.
    /// A selector that is there already takes the new title.
    #[must_use]
    pub fn with_selectors(
        mut self,
        selectors: impl IntoIterator<Item = (SelectorName, Option<SelectorTitle>)>,
    ) -> Self {
        self.selectors.extend(selectors);
        self
    }

    /// The known topics, in the order of their names.
    pub fn topics(&self) -> impl Iterator<Item = &TopicName> {
        self.topics.iter()
    }

    /// The known consumer groups, in the order of their names.
    pub fn groups(&self) -> impl Iterator<Item = &GroupId> {
        self.groups.iter()
    }

    /// The IDs of the known clients, in their order.
    pub fn clients(&self) -> impl Iterator<Item = &ClientId> {
        self.clients.iter()
    }

    /// The known presets with their titles, in the order of their names.
    pub fn presets(&self) -> impl Iterator<Item = (&PresetName, Option<&PresetTitle>)> {
        self.presets.iter().map(|(name, title)| (name, title.as_ref()))
    }

    /// The known selectors with their titles, in the order of their names.
    pub fn selectors(&self) -> impl Iterator<Item = (&SelectorName, Option<&SelectorTitle>)> {
        self.selectors.iter().map(|(name, title)| (name, title.as_ref()))
    }

    /// Whether `name` is known as a name of `kind`.
    pub(crate) fn knows(&self, kind: NameKind, name: &str) -> bool {
        match kind {
            NameKind::Preset => self.presets.contains_key(name),
            NameKind::Selector => self.selectors.contains_key(name),
            NameKind::Topic => self.topics.contains(name),
            NameKind::Group => self.groups.contains(name),
            NameKind::Client => self.clients.contains(name),
            NameKind::Broker => false,
        }
    }

    /// What `name` is known as: the kind that `hinted` names when it is one of them,
    /// and else the first kind in the order of [`NameKind::KNOWN`].
    pub(crate) fn kind_of(&self, name: &str, hinted: Option<NameKind>) -> Option<NameKind> {
        hinted
            .filter(|kind| self.knows(*kind, name))
            .or_else(|| NameKind::KNOWN.into_iter().find(|kind| self.knows(*kind, name)))
    }

    /// Every known name of `kind`, in the order of the names.
    pub(crate) fn names(&self, kind: NameKind) -> Vec<Known<'_>> {
        let untitled = |name| Known { name, title: None };
        match kind {
            NameKind::Preset => self
                .presets()
                .map(|(name, title)| Known {
                    name: name.as_str(),
                    title: title.map(PresetTitle::as_str),
                })
                .collect(),
            NameKind::Selector => self
                .selectors()
                .map(|(name, title)| Known {
                    name: name.as_str(),
                    title: title.map(SelectorTitle::as_str),
                })
                .collect(),
            NameKind::Topic => self.topics.iter().map(TopicName::as_str).map(untitled).collect(),
            NameKind::Group => self.groups.iter().map(GroupId::as_str).map(untitled).collect(),
            NameKind::Client => self.clients.iter().map(ClientId::as_str).map(untitled).collect(),
            NameKind::Broker => Vec::new(),
        }
    }
}

/// One known name, with the title of what it names. Only a preset and a selector can have one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Known<'a> {
    pub name: &'a str,
    pub title: Option<&'a str>,
}

/// What a name in a sentence names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameKind {
    Preset,
    Selector,
    Topic,
    Group,
    Client,
    /// The number of a broker. No broker is a known name: its word or its form says what it is.
    Broker,
}

impl NameKind {
    /// The kinds a name can be known as.
    /// A name that is known as several reads as the first of them, unless the word before it says otherwise.
    pub(crate) const KNOWN: [Self; 5] = [Self::Preset, Self::Selector, Self::Topic, Self::Group, Self::Client];

    /// Every kind.
    const ALL: [Self; 6] = [
        Self::Preset,
        Self::Selector,
        Self::Topic,
        Self::Group,
        Self::Client,
        Self::Broker,
    ];

    /// The word that says the next word is a name of this kind: `client billing`.
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Self::Preset => "preset",
            Self::Selector => "selector",
            Self::Topic => "topic",
            Self::Group => "group",
            Self::Client => "client",
            Self::Broker => "broker",
        }
    }

    /// The kind that `word` announces, in lower case.
    pub(crate) fn announced_by(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.word() == word)
    }

    /// What a sentence that ends on the word for this kind still needs.
    pub(crate) const fn needed(self) -> &'static str {
        match self {
            Self::Preset => "the name of a preset",
            Self::Selector => "the name of a selector",
            Self::Topic => "a topic",
            Self::Group => "a group",
            Self::Client => "the ID of a client",
            Self::Broker => "the number of a broker",
        }
    }

    /// The kind a completion of such a name shows. No broker completes.
    pub(crate) const fn completion(self) -> Option<CompletionKind> {
        match self {
            Self::Preset => Some(CompletionKind::Preset),
            Self::Selector => Some(CompletionKind::Selector),
            Self::Topic => Some(CompletionKind::Topic),
            Self::Group => Some(CompletionKind::Group),
            Self::Client => Some(CompletionKind::Client),
            Self::Broker => None,
        }
    }
}
