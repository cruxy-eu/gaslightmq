//! ⌘K: a sentence that someone types, what the control plane reads in it, and the live rule it gives.

use gaslightmq_control_api::environment::EnvironmentName;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::rule::Rule;

/// A view that ⌘K jumps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Jump {
    /// The live traffic.
    Record,
    /// The rules of the environment.
    Rules,
    /// The pads.
    Pads,
}

impl Jump {
    /// Every view, in the order ⌘K offers them.
    pub const ALL: [Jump; 3] = [Jump::Record, Jump::Rules, Jump::Pads];

    /// The name of the view.
    pub const fn name(self) -> &'static str {
        match self {
            Jump::Record => "Live traffic",
            Jump::Rules => "Rules",
            Jump::Pads => "Pads",
        }
    }
}

/// One line of the list under the sentence.
#[derive(Debug, Clone, PartialEq)]
pub enum PaletteItem {
    /// Make the rule of the sentence a live rule of the environment, and arm it.
    Create(Rule),
    /// Put a completion in place of the word at the end of the sentence.
    Complete(WordCompletion),
    /// Show another view.
    Jump(Jump),
}

/// What someone does with the rule of the sentence: add it as a live rule, then arm it.
#[derive(Debug, Clone, PartialEq)]
pub struct Creation {
    /// Add the rule to the environment as a live rule, disarmed.
    pub create: CreateLiveRule,
    /// Arm it once it is there.
    pub arm: ArmRule,
}

/// A sentence that the palette asks the control plane to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    /// Which question this is, for its answer.
    pub number: u64,
    /// The text typed so far.
    pub text: String,
}

impl Question {
    /// The operation that reads the sentence against the names of `environment`.
    pub fn operation(&self, environment: EnvironmentName) -> ParseSentence {
        ParseSentence {
            environment,
            text: self.text.clone(),
        }
    }
}

/// ⌘K: the sentence typed so far, what the control plane read in it, and the line of the list that is highlighted.
///
/// Every change of the sentence makes a new question. Whoever asks waits a moment first, and asks only when the
/// question is still the [latest](Self::is_latest), so a fast typist sends one question and not one per key.
/// Only the answer to the latest question counts: an answer to an older sentence that arrives late changes nothing.
///
/// ```
/// use gaslightmq_ui::palette::{Palette, PaletteItem};
///
/// let mut palette = Palette::default();
/// let first = palette.type_text("delay prod".to_owned());
/// let second = palette.type_text("delay produce".to_owned());
/// assert!(!palette.is_latest(first.number));
/// assert!(palette.is_latest(second.number));
///
/// // Until the answer comes, the list only jumps to other views.
/// assert!(palette.items().iter().all(|item| matches!(item, PaletteItem::Jump(_))));
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Palette {
    text: String,
    /// How many questions the palette made, so an answer says which sentence it reads.
    asked: u64,
    /// The answer to the latest question.
    parsed: Option<ParsedSentence>,
    highlighted: usize,
}

impl Palette {
    /// The sentence typed so far.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Someone typed: the sentence is `text` now, and the question that reads it.
    pub fn type_text(&mut self, text: String) -> Question {
        self.text = text;
        self.asked += 1;
        self.parsed = None;
        self.highlighted = 0;
        Question {
            number: self.asked,
            text: self.text.clone(),
        }
    }

    /// Whether question `number` reads the sentence typed now.
    pub fn is_latest(&self, number: u64) -> bool {
        number == self.asked
    }

    /// The control plane read the sentence of question `number`. An answer to an older question changes nothing.
    pub fn answered(&mut self, number: u64, parsed: ParsedSentence) {
        if self.is_latest(number) {
            self.parsed = Some(parsed);
            self.highlighted = 0;
        }
    }

    /// What the control plane read in the sentence typed now, once it answered.
    pub fn parsed(&self) -> Option<&ParsedSentence> {
        self.parsed.as_ref()
    }

    /// The rule of the sentence, while the sentence is whole: it says what happens, and every word was read.
    pub fn rule(&self) -> Option<&Rule> {
        let parsed = self.parsed()?;
        if parsed.unread.is_empty() {
            parsed.rule.as_ref()
        } else {
            None
        }
    }

    /// The lines of the list: the rule of a whole sentence first, then the completions of the word at its end,
    /// then the views.
    pub fn items(&self) -> Vec<PaletteItem> {
        let mut items = Vec::new();
        items.extend(self.rule().cloned().map(PaletteItem::Create));
        if let Some(parsed) = self.parsed() {
            items.extend(parsed.completions.iter().cloned().map(PaletteItem::Complete));
        }
        items.extend(Jump::ALL.map(PaletteItem::Jump));
        items
    }

    /// Which line is highlighted.
    pub fn highlighted(&self) -> usize {
        self.highlighted.min(self.items().len().saturating_sub(1))
    }

    /// The highlighted line, which Enter runs.
    pub fn chosen(&self) -> Option<PaletteItem> {
        self.items().into_iter().nth(self.highlighted())
    }

    /// The highlight moves `by` lines, and wraps around at either end.
    pub fn highlight(&mut self, by: isize) {
        let count = isize::try_from(self.items().len()).unwrap_or(isize::MAX);
        if count == 0 {
            return;
        }
        let at = isize::try_from(self.highlighted()).unwrap_or(0);
        self.highlighted = usize::try_from((at + by).rem_euclid(count)).unwrap_or(0);
    }

    /// The highlight moves to line `index`.
    pub fn highlight_at(&mut self, index: usize) {
        self.highlighted = index;
    }

    /// The completion that Tab takes: the highlighted one, or the first one.
    pub fn completion(&self) -> Option<WordCompletion> {
        match self.chosen() {
            Some(PaletteItem::Complete(completion)) => Some(completion),
            _ => self.parsed()?.completions.first().cloned(),
        }
    }

    /// The rule of a whole sentence as a live rule of `environment`, and the command that arms it.
    /// `None` while the sentence is not whole.
    pub fn creation(&self, environment: &EnvironmentName) -> Option<Creation> {
        let rule = self.rule()?.clone();
        Some(Creation {
            arm: ArmRule {
                environment: environment.clone(),
                rule: rule.name().clone(),
            },
            create: CreateLiveRule {
                environment: environment.clone(),
                rule,
            },
        })
    }
}
