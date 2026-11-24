use std::fmt;

use gaslightmq_kernel::{ApiKey, ErrorCode};

use crate::KnownNames;
use crate::draft::Draft;
use crate::known::NameKind;
use crate::words::{LieWord, Meaning, Word};

/// What a completion is, so a surface can show it next to the word.
///
/// Names are unique per kind only: a selector and a preset can share one.
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
///
/// assert_eq!(CompletionKind::ErrorCode.to_string(), "error code");
/// assert_eq!(CompletionKind::ALL.len(), 8);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompletionKind {
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

impl CompletionKind {
    /// Every kind, in the order a sentence offers them.
    pub const ALL: [Self; 8] = [
        Self::Lie,
        Self::Preset,
        Self::Api,
        Self::Selector,
        Self::Topic,
        Self::Group,
        Self::Client,
        Self::ErrorCode,
    ];

    /// The kind in words, as a surface shows it next to a completion.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Lie => "lie",
            Self::Preset => "preset",
            Self::Api => "API",
            Self::Selector => "selector",
            Self::Topic => "topic",
            Self::Group => "group",
            Self::Client => "client",
            Self::ErrorCode => "error code",
        }
    }

    /// The kind of name a completion of this kind is, or `None` for a word that means the same in every sentence.
    const fn names(self) -> Option<NameKind> {
        match self {
            Self::Lie | Self::Api | Self::ErrorCode => None,
            Self::Preset => Some(NameKind::Preset),
            Self::Selector => Some(NameKind::Selector),
            Self::Topic => Some(NameKind::Topic),
            Self::Group => Some(NameKind::Group),
            Self::Client => Some(NameKind::Client),
        }
    }

    /// The words of this kind, which mean the same in every sentence. A kind of name has none.
    fn vocabulary(self) -> Vec<&'static str> {
        match self {
            Self::Lie => LieWord::ALL.into_iter().map(LieWord::word).collect(),
            Self::Api => ApiKey::known().filter_map(ApiKey::name).collect(),
            Self::ErrorCode => ErrorCode::known().filter_map(ErrorCode::name).collect(),
            Self::Preset | Self::Selector | Self::Topic | Self::Group | Self::Client => Vec::new(),
        }
    }
}

/// Writes the [name](Self::name).
impl fmt::Display for CompletionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One thing the word at the end of a sentence can become.
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
///
/// let known = KnownNames::new().with_presets(["leader-flap".parse()?, "blackhole".parse()?]);
/// let quick_inject = QuickInject::default();
///
/// let sentence = quick_inject.read("produce lead", &known);
/// let completion = &sentence.completions()[0];
/// assert_eq!(completion.kind(), CompletionKind::Preset);
/// assert_eq!(completion.word(), "leader-flap");
/// assert_eq!(completion.sentence(), "produce leader-flap ");
///
/// // `blackhole` alone is the lie, so the preset of that name has the word for its kind in front.
/// let sentence = quick_inject.read("bla", &known);
/// let words: Vec<&str> = sentence.completions().iter().map(Completion::word).collect();
/// assert_eq!(words, ["blackhole", "preset blackhole"]);
/// # Ok::<(), gaslightmq_rule::PresetNameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Completion {
    kind: CompletionKind,
    word: String,
    sentence: String,
}

impl Completion {
    /// What the word becomes: a lie, an API, an error code, or a name of some kind.
    pub fn kind(&self) -> CompletionKind {
        self.kind
    }

    /// The word as the sentence writes it.
    ///
    /// A name that would read as something else has the word for its kind in front: `client payments`.
    pub fn word(&self) -> &str {
        &self.word
    }

    /// The whole sentence with the word in place, and a space after it for the next word.
    /// A surface puts it where the typed text was.
    pub fn sentence(&self) -> &str {
        &self.sentence
    }
}

/// The word that someone is typing at the end of a sentence, and what stands before it.
pub(crate) struct Typing<'a> {
    /// The word so far. It is empty when the sentence ends between two words.
    typed: &'a str,
    /// The sentence before the word, up to where a completion starts.
    before: &'a str,
    /// What the word before says about this one.
    after: Option<Meaning<'a>>,
    /// How many words stand before this one.
    words_before: usize,
}

impl<'a> Typing<'a> {
    /// The word at the end of `sentence`, of which `words` are the words.
    pub(crate) fn at_the_end_of(sentence: &'a str, words: &[Word<'a>]) -> Self {
        let (typed, earlier) = match words.split_last() {
            Some((last, earlier)) if last.span.end() == sentence.len() => (Some(last), earlier),
            _ => (None, words),
        };
        Self {
            typed: typed.map_or("", |typed| typed.text),
            before: typed.map_or(sentence, |typed| sentence.get(..typed.span.start()).unwrap_or_default()),
            after: earlier.last().and_then(|word| Meaning::of(word.text)),
            words_before: earlier.len(),
        }
    }

    /// The words of the sentence that stand before this one, of which `words` are all the words.
    ///
    /// What they say decides what this word can still become.
    /// The word itself does not: half a word often reads as something already, such as a topic after `on`.
    pub(crate) fn words_before<'w>(&self, words: &'w [Word<'a>]) -> &'w [Word<'a>] {
        words.get(..self.words_before).unwrap_or(words)
    }

    /// The kinds this word can be, best first.
    ///
    /// After the word for a kind of name, it is a name of that kind.
    /// After a preposition, the kind the preposition hints at comes first.
    fn kinds(&self) -> Vec<CompletionKind> {
        let hinted = match &self.after {
            Some(Meaning::Announces(kind)) => return kind.completion().into_iter().collect(),
            Some(Meaning::Preposition(preposition)) => preposition.hints().and_then(NameKind::completion),
            _ => None,
        };
        hinted
            .into_iter()
            .chain(CompletionKind::ALL.into_iter().filter(|kind| Some(*kind) != hinted))
            .collect()
    }

    /// Whether the word can still become `candidate`: it starts like it, in any case, and it is not there yet.
    fn starts(&self, candidate: &str) -> bool {
        candidate
            .get(..self.typed.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(self.typed))
            && !candidate.eq_ignore_ascii_case(self.typed)
    }

    /// How a sentence writes `name` here, so that it reads back as a name of `kind`.
    /// `None` for a name that no sentence can write: one without a character, or one with white space.
    fn written(&self, kind: NameKind, name: &str, known: &KnownNames) -> Option<String> {
        if name.is_empty() || name.contains(char::is_whitespace) {
            return None;
        }
        let hinted = match &self.after {
            // The word for the kind stands before the name already.
            Some(Meaning::Announces(_)) => return Some(name.to_owned()),
            Some(Meaning::Preposition(preposition)) => preposition.hints(),
            _ => None,
        };
        // A name can be a word of the grammar too, and the system can know it as several kinds.
        // It then reads as something else, unless the word for its kind stands before it.
        if Meaning::of(name).is_none() && known.kind_of(name, hinted) == Some(kind) {
            Some(name.to_owned())
        } else {
            Some(format!("{} {name}", kind.word()))
        }
    }

    /// Every word of `kind` that this word can become, as the sentence writes it.
    fn candidates(&self, kind: CompletionKind, known: &KnownNames) -> Vec<String> {
        match kind.names() {
            None => kind
                .vocabulary()
                .into_iter()
                .filter(|word| self.starts(word))
                .map(str::to_owned)
                .collect(),
            Some(names) => known
                .names(names)
                .into_iter()
                .filter(|name| self.starts(name))
                .filter_map(|name| self.written(names, name, known))
                .collect(),
        }
    }

    /// What the word can become, when the words before it said `draft`: at most `limit` completions.
    pub(crate) fn completions(&self, draft: &Draft<'_>, known: &KnownNames, limit: usize) -> Vec<Completion> {
        self.kinds()
            .into_iter()
            .filter(|kind| draft.still_takes(*kind))
            .flat_map(|kind| {
                self.candidates(kind, known).into_iter().map(move |word| Completion {
                    kind,
                    sentence: format!("{}{word} ", self.before),
                    word,
                })
            })
            .take(limit)
            .collect()
    }
}
