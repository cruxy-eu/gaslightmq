//! The words of a sentence, and the words that mean the same in every sentence.

use gaslightmq_kernel::{ApiKey, ErrorCode};

use crate::Span;
use crate::known::NameKind;

/// One word of a sentence, with where it stands. White space separates words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Word<'a> {
    pub text: &'a str,
    pub span: Span,
}

impl<'a> Word<'a> {
    /// The words of `sentence`, in their order.
    pub(crate) fn all(sentence: &'a str) -> Vec<Word<'a>> {
        let mut words = Vec::new();
        let mut rest = sentence.trim_start();
        while !rest.is_empty() {
            let start = sentence.len() - rest.len();
            let (text, after) = rest.split_at(rest.find(char::is_whitespace).unwrap_or(rest.len()));
            words.push(Word {
                text,
                span: Span::new(start, start + text.len()),
            });
            rest = after.trim_start();
        }
        words
    }
}

/// A lie that a sentence names with one word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LieWord {
    Delay,
    Throttle,
    Error,
    Blackhole,
    Disconnect,
}

impl LieWord {
    /// Every lie word, in the order a sentence offers them.
    pub(crate) const ALL: [Self; 5] = [
        Self::Delay,
        Self::Throttle,
        Self::Error,
        Self::Blackhole,
        Self::Disconnect,
    ];

    /// The word, as a sentence offers it.
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Self::Delay => "delay",
            Self::Throttle => "throttle",
            Self::Error => "error",
            Self::Blackhole => "blackhole",
            Self::Disconnect => "disconnect",
        }
    }

    /// The other word for the same lie, which is the word of the glossary.
    const fn other_word(self) -> Option<&'static str> {
        match self {
            Self::Delay | Self::Throttle => None,
            Self::Error => Some("refuse"),
            Self::Blackhole => Some("silence"),
            Self::Disconnect => Some("hang-up"),
        }
    }

    /// The lie that `word` names, in lower case.
    fn named_by(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|lie| lie.word() == word || lie.other_word() == Some(word))
    }
}

/// A small word that stands before another and says nothing itself.
///
/// It can say what the next word is, when nothing else does: `to orders` is a topic, and `for billing` a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Preposition {
    /// `on`, `to` and `from`: before a topic.
    Topic,
    /// `for`: before a client, or before a lifetime.
    For,
    /// `by`: before the time span of a delay or a throttle.
    By,
    /// `with`: before the error code of a refusal.
    With,
}

impl Preposition {
    /// The preposition that `word` is, in lower case.
    fn read(word: &str) -> Option<Self> {
        match word {
            "on" | "to" | "from" => Some(Self::Topic),
            "for" => Some(Self::For),
            "by" => Some(Self::By),
            "with" => Some(Self::With),
            _ => None,
        }
    }

    /// What the word after it is, when that word is a name that nothing else explains.
    pub(crate) const fn hints(self) -> Option<NameKind> {
        match self {
            Self::Topic => Some(NameKind::Topic),
            Self::For => Some(NameKind::Client),
            Self::By | Self::With => None,
        }
    }

    /// What a sentence that ends on the preposition still needs.
    pub(crate) const fn needed(self) -> &'static str {
        match self {
            Self::Topic => "a topic",
            Self::For => "a client or a lifetime",
            Self::By => "a time span",
            Self::With => "an error code",
        }
    }
}

/// What a word means by itself, whatever the names the system knows.
///
/// The words of the grammar and the names Kafka gives read in any case: `Produce`, `produce` and `PRODUCE` are one API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Meaning<'a> {
    Lie(LieWord),
    /// The word for a kind of name: the next word is such a name.
    Announces(NameKind),
    Preposition(Preposition),
    Api(ApiKey),
    ErrorCode(ErrorCode),
    /// The word ends in a percent sign: a chance, or a text that is none.
    Chance(&'a str),
    /// The word is `b` and digits: the number of a broker, or digits that are none.
    Broker(&'a str),
    /// The word starts with a digit: a time span, or a text that is none.
    TimeSpan(&'a str),
}

impl<'a> Meaning<'a> {
    /// What `word` means by itself, or `None` for a word that can only be a name.
    pub(crate) fn of(word: &'a str) -> Option<Self> {
        let lower = word.to_ascii_lowercase();
        if let Some(lie) = LieWord::named_by(&lower) {
            return Some(Self::Lie(lie));
        }
        if let Some(kind) = NameKind::announced_by(&lower) {
            return Some(Self::Announces(kind));
        }
        if let Some(preposition) = Preposition::read(&lower) {
            return Some(Self::Preposition(preposition));
        }
        if let Some(api) = ApiKey::known().find(|api| api.name().is_some_and(|name| name.eq_ignore_ascii_case(word))) {
            return Some(Self::Api(api));
        }
        if let Some(code) =
            ErrorCode::known().find(|code| code.name().is_some_and(|name| name.eq_ignore_ascii_case(word)))
        {
            return Some(Self::ErrorCode(code));
        }
        if word.ends_with('%') {
            return Some(Self::Chance(word));
        }
        if let Some(digits) = word.strip_prefix(['b', 'B'])
            && !digits.is_empty()
            && digits.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Some(Self::Broker(digits));
        }
        word.starts_with(|first: char| first.is_ascii_digit())
            .then_some(Self::TimeSpan(word))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every word of `sentence`: its text, and the text its span cuts out.
    fn words(sentence: &str) -> Vec<(&str, &str)> {
        Word::all(sentence)
            .into_iter()
            .map(|word| (word.text, &sentence[word.span.range()]))
            .collect()
    }

    #[test]
    fn white_space_separates_words() {
        assert_eq!(
            words("  delay   produce\torders.*\u{a0}750ms "),
            [
                ("delay", "delay"),
                ("produce", "produce"),
                ("orders.*", "orders.*"),
                ("750ms", "750ms"),
            ]
        );
        assert_eq!(words(""), []);
        assert_eq!(words(" \t "), []);
    }

    #[test]
    fn a_word_stands_at_the_bytes_of_its_text() {
        let spans: Vec<(usize, usize)> = Word::all("délai  produce")
            .into_iter()
            .map(|word| (word.span.start(), word.span.end()))
            .collect();

        assert_eq!(spans, [(0, 6), (8, 15)]);
    }

    #[test]
    fn a_word_of_the_grammar_reads_in_any_case() {
        assert_eq!(Meaning::of("Delay"), Some(Meaning::Lie(LieWord::Delay)));
        assert_eq!(Meaning::of("HANG-UP"), Some(Meaning::Lie(LieWord::Disconnect)));
        assert_eq!(Meaning::of("Client"), Some(Meaning::Announces(NameKind::Client)));
        assert_eq!(Meaning::of("FROM"), Some(Meaning::Preposition(Preposition::Topic)));
        assert_eq!(Meaning::of("offsetcommit"), Some(Meaning::Api(ApiKey::OFFSET_COMMIT)));
        assert_eq!(
            Meaning::of("not_coordinator"),
            Some(Meaning::ErrorCode(ErrorCode::NOT_COORDINATOR))
        );
    }

    #[test]
    fn a_word_with_the_form_of_a_value_is_that_value_or_none() {
        assert_eq!(Meaning::of("40%"), Some(Meaning::Chance("40%")));
        assert_eq!(Meaning::of("x%"), Some(Meaning::Chance("x%")));
        assert_eq!(Meaning::of("b2"), Some(Meaning::Broker("2")));
        assert_eq!(Meaning::of("B12"), Some(Meaning::Broker("12")));
        assert_eq!(Meaning::of("750ms"), Some(Meaning::TimeSpan("750ms")));
        assert_eq!(Meaning::of("3scale"), Some(Meaning::TimeSpan("3scale")));
    }

    #[test]
    fn every_other_word_can_only_be_a_name() {
        for word in [
            "orders.eu",
            "b",
            "b2b",
            "billing",
            "order-ingest",
            "delayed",
            "leader-flap",
            "",
            "é",
        ] {
            assert_eq!(Meaning::of(word), None, "{word}");
        }
    }
}
