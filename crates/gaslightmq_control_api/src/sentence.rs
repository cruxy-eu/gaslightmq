//! A ⌘K sentence that someone is still typing, and the rule it gives.

use std::ops::Range;

use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::Rule;
use serde::{Deserialize, Serialize};

use crate::{EnvironmentNotRunning, Operation};

/// Read the ⌘K sentence typed so far, against the names an environment knows.
///
/// The environment knows the topics, groups and clients of its traffic, and the presets and selectors of the
/// workspace. A browser only holds a window of both, so it asks.
/// Parsing changes nothing: it creates no rule. Whoever wants the rule [creates it](crate::CreateLiveRule).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParseSentence {
    /// The environment whose names the sentence may use.
    pub environment: EnvironmentName,
    /// The text typed so far. The word at its end is the word that is being typed.
    pub text: String,
}

impl Operation for ParseSentence {
    type Answer = ParsedSentence;
    type Error = EnvironmentNotRunning;
}

/// What a sentence gives: the rule, the completions for the word at its end, and the words that were not read.
///
/// Reading a sentence does not fail, because a sentence that is being typed is rarely whole.
/// The words that were not read change nothing about the rule, so whoever creates the rule looks at them first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedSentence {
    /// The rule the sentence gives, once it says what happens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<Rule>,
    /// What the word at the end can become, in the order to offer them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completions: Vec<WordCompletion>,
    /// The words that were not read, in the order they stand.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unread: Vec<UnreadWord>,
}

/// One thing the word at the end of a sentence can become.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WordCompletion {
    /// What the word becomes.
    pub kind: WordKind,
    /// The word as the sentence writes it. A preset and a selector are their name, and never their title.
    pub word: String,
    /// The title of the preset or the selector that the word names, for a surface to show next to the name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The whole sentence with the word in place, and a space after it for the next word.
    pub sentence: String,
}

/// What a completion is, so a surface shows it next to the word.
///
/// With `serde`, it is the name in kebab case: `lie`, `preset`, `api`, `selector`, `topic`, `group`, `client`
/// or `error-code`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WordKind {
    /// A word that names a lie, such as `delay`.
    Lie,
    /// The name of a preset.
    Preset,
    /// The name of an API, as Kafka spells it.
    Api,
    /// The name of a selector.
    Selector,
    /// A topic.
    Topic,
    /// A consumer group.
    Group,
    /// The ID of a client.
    Client,
    /// The name of an error, as Kafka spells it.
    ErrorCode,
}

/// A word of the sentence that was not read: where it stands, and why.
///
/// ```
/// use gaslightmq_control_api::prelude::*;
///
/// let typed = "delay prodcue 2s";
/// let unread = UnreadWord {
///     word: "prodcue".to_owned(),
///     start: 6,
///     reason: "`prodcue` is no word of a sentence, and no name that is known".to_owned(),
/// };
/// assert_eq!(&typed[unread.span()], "prodcue");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnreadWord {
    /// The text that was not read.
    pub word: String,
    /// The byte of the sentence that the word starts at.
    pub start: usize,
    /// Why the word was not read, in one line.
    pub reason: String,
}

impl UnreadWord {
    /// The bytes of the sentence that the word stands at, for a surface to mark.
    pub fn span(&self) -> Range<usize> {
        self.start..self.start.saturating_add(self.word.len())
    }
}
