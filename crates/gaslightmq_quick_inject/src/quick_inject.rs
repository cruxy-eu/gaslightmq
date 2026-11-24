use gaslightmq_clock::TimeSpan;
use gaslightmq_field::{Endpoint, Field, FieldValue};
use gaslightmq_kernel::BrokerId;
use gaslightmq_rule::PresetName;
use gaslightmq_selector::{Glob, SelectorName};
use gaslightmq_trigger::{Chance, Lifetime};

use crate::completion::Typing;
use crate::draft::{ActionWord, Draft};
use crate::known::NameKind;
use crate::words::{Meaning, Preposition, Word};
use crate::{KnownNames, QuickInjectConfiguration, Sentence};

/// The ⌘K parser: it reads the sentence someone types, and gives the rule the sentence means.
///
/// It holds a [configuration](QuickInjectConfiguration) and nothing else, so one value reads every sentence.
/// See the [grammar](crate#the-grammar).
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_quick_inject::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
/// use gaslightmq_trigger::prelude::*;
///
/// let known = KnownNames::new().with_clients([ClientId::new("order-ingest")?]);
/// let sentence = QuickInject::default().read("delay produce orders.* 750ms 40% order-ingest", &known);
///
/// let rule = sentence.rule().expect("the sentence names a lie");
/// assert_eq!(rule.name(), "delay-750ms-on-produce-orders-for-order-ingest");
/// assert_eq!(rule.title().map(RuleTitle::as_str), Some("Delay 750ms on Produce orders.* for order-ingest"));
///
/// let SelectorRef::Inline(calls) = rule.selector() else { panic!("the sentence names no selector") };
/// assert_eq!(calls.to_string(), "api is Produce and topic matches orders.*");
/// assert_eq!(rule.scope().to_string(), "client.id is order-ingest");
///
/// let ActionRef::Inline(action) = rule.action() else { panic!("the sentence names no preset") };
/// assert_eq!(action.to_string(), "+750 ms");
///
/// let chance = rule.trigger().parts().and_then(TriggerParts::chance);
/// assert_eq!(chance.map(|chance| chance.to_string()), Some("40%".to_owned()));
/// assert_eq!(rule.lifetime(), Lifetime::UntilDisarmed);
/// assert!(sentence.unread().is_empty());
/// # Ok::<(), ClientIdError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuickInject {
    configuration: QuickInjectConfiguration,
}

impl QuickInject {
    /// A parser with that configuration.
    pub fn new(configuration: QuickInjectConfiguration) -> Self {
        Self { configuration }
    }

    /// What the parser is configured with.
    pub fn configuration(&self) -> &QuickInjectConfiguration {
        &self.configuration
    }

    /// Reads the text someone typed so far.
    ///
    /// `known` holds the names the system knows at this moment.
    /// The sentence reads them without the word for their kind, and it offers them as completions.
    ///
    /// Reading changes nothing and creates no rule: the [`Sentence`] only says what the text means.
    pub fn read(&self, text: &str, known: &KnownNames) -> Sentence {
        let words = Word::all(text);
        let mut reader = Reader {
            words: words.iter(),
            known,
            draft: Draft::of(text),
        };
        reader.read_every_word();

        let completions =
            Typing::at_the_end_of(text, &words).completions(&reader.draft, known, self.configuration.completion_limit);
        let (rule, unread) = reader.draft.finish(&self.configuration);
        Sentence::new(text, rule, completions, unread)
    }
}

/// The units of a lifetime that counts, in lower case.
const COUNTED: [&str; 4] = ["match", "matches", "firing", "firings"];

/// Reads the words of one sentence in their order, and notes in a draft what each one says.
struct Reader<'a> {
    words: std::slice::Iter<'a, Word<'a>>,
    known: &'a KnownNames,
    draft: Draft<'a>,
}

impl<'a> Reader<'a> {
    /// Reads the sentence to its end.
    fn read_every_word(&mut self) {
        while let Some(word) = self.words.next() {
            self.read(word, None);
        }
    }

    /// The next word, without taking it.
    fn peek(&self) -> Option<&'a Word<'a>> {
        self.words.as_slice().first()
    }

    /// Reads one word. `hinted` is what the preposition before it says it is, when nothing else does.
    fn read(&mut self, word: &Word<'a>, hinted: Option<NameKind>) {
        match Meaning::of(word.text) {
            Some(Meaning::Lie(lie)) => self.draft.say_action(word.span, ActionWord::Lie(lie)),
            Some(Meaning::Announces(kind)) => match self.words.next() {
                Some(name) => self.read_name(kind, name),
                None => self.draft.unfinished(word.span, kind.needed()),
            },
            Some(Meaning::Preposition(preposition)) => self.read_after(preposition, word),
            Some(Meaning::Api(api)) => self.draft.say_name(word.span, FieldValue::from(api)),
            Some(Meaning::ErrorCode(error_code)) => self.draft.say_error_code(word.span, error_code),
            Some(Meaning::Chance(text)) => match text.parse::<Chance>() {
                Ok(chance) => self.draft.say_chance(word.span, chance),
                Err(error) => self.draft.invalid(word.span, error.to_string()),
            },
            Some(Meaning::Broker(digits)) => match digits.parse::<BrokerId>() {
                Ok(broker) => self.draft.say_name(word.span, Endpoint::Broker(broker).into()),
                Err(error) => self.draft.invalid(word.span, error.to_string()),
            },
            Some(Meaning::TimeSpan(text)) => match text.parse::<TimeSpan>() {
                Ok(time_span) => self.draft.say_time_span(word.span, time_span),
                Err(error) => self.draft.invalid(word.span, error.to_string()),
            },
            None => {
                // A name. What the system knows it as comes first, then what the preposition before it says.
                // A pattern that nothing explains is a pattern for topics.
                let pattern = Glob::is_pattern(word.text);
                let kind = self
                    .known
                    .kind_of(word.text, hinted)
                    .or(hinted)
                    .or(pattern.then_some(NameKind::Topic));
                match kind {
                    Some(kind) => self.read_name(kind, word),
                    None => self.draft.unknown(word.span),
                }
            }
        }
    }

    /// Reads what stands after a preposition.
    ///
    /// `for` and a time span or a count is a lifetime: `for 20s`, `for 3 firings`.
    /// Every other word reads as it would without the preposition, with the hint of the preposition.
    fn read_after(&mut self, preposition: Preposition, word: &Word<'a>) {
        let Some(next) = self.peek() else {
            return self.draft.unfinished(word.span, preposition.needed());
        };
        self.words.next();
        let counts = next.text.starts_with(|first: char| first.is_ascii_digit());
        if preposition == Preposition::For && counts {
            self.read_lifetime(next);
        } else {
            self.read(next, preposition.hints());
        }
    }

    /// Reads the lifetime that starts at `first`: a time span, or a number and what it counts.
    fn read_lifetime(&mut self, first: &Word<'a>) {
        let counts = first.text.bytes().all(|byte| byte.is_ascii_digit());
        let unit = self
            .peek()
            .filter(|_| counts)
            .map(|unit| (unit.span, unit.text.to_ascii_lowercase()))
            .filter(|(_, unit)| COUNTED.contains(&unit.as_str()));
        let (text, span) = match unit {
            Some((end, unit)) => {
                self.words.next();
                (format!("{} {unit}", first.text), first.span.up_to(end))
            }
            None => (first.text.to_owned(), first.span),
        };
        match text.parse::<Lifetime>() {
            Ok(lifetime) => self.draft.say_lifetime(span, lifetime),
            Err(error) => self.draft.invalid(span, error.to_string()),
        }
    }

    /// Reads `word` as a name of `kind`.
    ///
    /// A word with a wildcard is a pattern for the names of a topic, a group or a client.
    fn read_name(&mut self, kind: NameKind, word: &Word<'a>) {
        let (span, text) = (word.span, word.text);
        let read = match kind {
            NameKind::Preset => PresetName::new(text)
                .map(|preset| self.draft.say_action(span, ActionWord::Preset(preset)))
                .map_err(|error| error.to_string()),
            NameKind::Selector => SelectorName::new(text)
                .map(|selector| self.draft.say_selector(span, selector))
                .map_err(|error| error.to_string()),
            NameKind::Broker => text
                .parse::<BrokerId>()
                .map(|broker| self.draft.say_name(span, Endpoint::Broker(broker).into()))
                .map_err(|error| error.to_string()),
            NameKind::Topic => self.read_value(Field::Topic, word),
            NameKind::Group => self.read_value(Field::Group, word),
            NameKind::Client => self.read_value(Field::ClientId, word),
        };
        if let Err(reason) = read {
            self.draft.invalid(span, reason);
        }
    }

    /// Reads `word` as a pattern for the values of `field`, or as one value of it.
    /// The error is what is wrong with the value, as text.
    fn read_value(&mut self, field: Field, word: &Word<'a>) -> Result<(), String> {
        if Glob::is_pattern(word.text) {
            Glob::new(word.text)
                .map(|pattern| self.draft.say_pattern(word.span, field, pattern))
                .map_err(|error| error.to_string())
        } else {
            field
                .parse_value(word.text)
                .map(|name| self.draft.say_name(word.span, name))
                .map_err(|error| error.to_string())
        }
    }
}
