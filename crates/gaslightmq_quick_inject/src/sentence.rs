use gaslightmq_rule::Rule;

use crate::{Completion, Span};

/// A sentence as it was read: the rule it gives, what the word at its end can become, and what was not read.
///
/// A sentence is what someone typed so far, so it is rarely whole.
/// Reading it never fails: every word that says something is read, and every other word is [unread](Self::unread).
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
///
/// let sentence = QuickInject::default().read("delay prod", &KnownNames::new());
///
/// // `delay` is a rule already: every call waits for the default delay.
/// assert_eq!(sentence.rule().map(|rule| rule.name().as_str()), Some("delay-500ms-on-all-calls"));
/// // `prod` is no word yet, and it can become one.
/// assert_eq!(sentence.unread()[0].word(), "prod");
/// assert_eq!(sentence.completions()[0].sentence(), "delay Produce ");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sentence {
    text: String,
    rule: Option<Rule>,
    completions: Vec<Completion>,
    unread: Vec<Unread>,
}

impl Sentence {
    /// The sentence that reading `text` gave.
    pub(crate) fn new(text: &str, rule: Option<Rule>, completions: Vec<Completion>, unread: Vec<Unread>) -> Self {
        Self {
            text: text.to_owned(),
            rule,
            completions,
            unread,
        }
    }

    /// The text that was read.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The rule the sentence gives, or `None` while it does not say what happens.
    ///
    /// It is a definition and nothing else: nobody created it, and nobody armed it.
    /// The words that were not read changed nothing about it,
    /// so whoever creates the rule looks at [`unread`](Self::unread) first.
    pub fn rule(&self) -> Option<&Rule> {
        self.rule.as_ref()
    }

    /// The rule the sentence gives, to keep.
    pub fn into_rule(self) -> Option<Rule> {
        self.rule
    }

    /// What the word at the end of the sentence can become, best first.
    /// There are at most as many as the [limit](crate::QuickInjectConfiguration::completion_limit) allows.
    pub fn completions(&self) -> &[Completion] {
        &self.completions
    }

    /// The parts of the sentence that were not read, in the order they stand in.
    pub fn unread(&self) -> &[Unread] {
        &self.unread
    }
}

/// A part of a sentence that was not read: where it stands, and why.
///
/// A surface shows the message at the [`span`](Self::span).
/// The variant says what someone does about it.
///
/// | Variant | Someone |
/// |---|---|
/// | `Unknown` | Types another word, or takes a [completion](crate::Sentence::completions). |
/// | `Invalid` | Corrects the value. |
/// | `Conflict` | Takes the word out, or the word it does not go with. |
/// | `Unfinished` | Types on. |
///
/// The message is complete, and it has no [`source`](std::error::Error::source).
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
///
/// let sentence = QuickInject::default().read("blackhole metadata 200%", &KnownNames::new());
///
/// let [unread] = sentence.unread() else { panic!("one word is not a chance") };
/// assert!(matches!(unread, Unread::Invalid { .. }));
/// assert_eq!(unread.to_string(), "`200%`: chance `200%` is not between 0% and 100%");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unread {
    /// The word is no word of the grammar, no value and no name the system knows.
    #[error("`{word}` is no word of a sentence, and no name that is known")]
    Unknown { word: String, span: Span },
    /// The word has the form of a value, and it is not one.
    /// `reason` says what is wrong with the value.
    #[error("`{word}`: {reason}")]
    Invalid { word: String, span: Span, reason: String },
    /// The word was understood, and the sentence has no place for it.
    /// `conflict` is the rule of the grammar that it breaks.
    #[error("`{word}`: {conflict}")]
    Conflict {
        word: String,
        span: Span,
        conflict: &'static str,
    },
    /// The word needs something that the sentence does not say yet, such as the name after `client`.
    /// `needs` says what.
    #[error("`{word}` needs {needs}")]
    Unfinished {
        word: String,
        span: Span,
        needs: &'static str,
    },
}

impl Unread {
    /// The text that was not read.
    pub fn word(&self) -> &str {
        match self {
            Unread::Unknown { word, .. }
            | Unread::Invalid { word, .. }
            | Unread::Conflict { word, .. }
            | Unread::Unfinished { word, .. } => word,
        }
    }

    /// Where the text stands in the sentence.
    pub fn span(&self) -> Span {
        match self {
            Unread::Unknown { span, .. }
            | Unread::Invalid { span, .. }
            | Unread::Conflict { span, .. }
            | Unread::Unfinished { span, .. } => *span,
        }
    }
}
