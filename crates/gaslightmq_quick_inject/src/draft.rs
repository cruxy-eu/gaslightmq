//! What the words of a sentence said so far, and the rule they give together.

use std::collections::BTreeMap;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::{Field, FieldGroup, FieldValue};
use gaslightmq_kernel::ErrorCode;
use gaslightmq_lie::{HangUpLie, LieDefinition, RefusalLie, TimingLie};
use gaslightmq_rule::{ActionRef, PresetName, Rule, RulePhrase, RuleTitle};
use gaslightmq_selector::{Condition, Conditions, Glob, Scope, SelectorName, SelectorRef, ValueSet};
use gaslightmq_trigger::{Chance, Lifetime, TriggerParts};

use crate::words::LieWord;
use crate::{CompletionKind, QuickInjectConfiguration, Span, Unread};

/// What a sentence takes once.
mod once {
    pub(super) const ACTION: &str = "a sentence names one lie or one preset";
    pub(super) const TIME_SPAN: &str = "a sentence names one time span, next to the one of its lifetime";
    pub(super) const ERROR_CODE: &str = "a refusal has one error code";
    pub(super) const CHANCE: &str = "a sentence names one chance";
    pub(super) const LIFETIME: &str = "a sentence names one lifetime";
    pub(super) const SELECTOR: &str = "a sentence names one selector";
}

/// What a sentence takes only next to something else.
mod only {
    pub(super) const SELECTOR_OR_CALLS: &str =
        "a sentence names a selector, or it says the calls itself with an API, a topic or a group";
    pub(super) const PATTERN_ALONE: &str = "a field takes one pattern, or one or more names";
    pub(super) const TIMING_TAKES_TIME: &str = "only a delay and a throttle take a time span";
    pub(super) const REFUSAL_TAKES_CODE: &str = "only a refusal takes an error code";
}

/// What happens, as a word of the sentence names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActionWord {
    Lie(LieWord),
    Preset(PresetName),
}

/// Something a word said, with where the word stands, so it can still be marked when the sentence ends.
#[derive(Debug, Clone)]
struct Said<T> {
    value: T,
    span: Span,
}

/// What a sentence says about one field: one pattern, or names of which any one will do.
#[derive(Debug, Default)]
struct FieldSaid {
    names: Vec<FieldValue>,
    pattern: Option<Glob>,
}

impl FieldSaid {
    /// The condition on `field`. There is one for everything a draft keeps.
    fn condition(&self, field: Field) -> Option<Condition> {
        match (&self.pattern, self.names.as_slice()) {
            (Some(pattern), _) => Some(Condition::matches(field, pattern.clone())),
            (None, [name]) => Some(Condition::is(name.clone())),
            (None, names) => ValueSet::new(names.iter().cloned()).ok().map(Condition::IsAnyOf),
        }
    }
}

/// An action with the words the title of its rule starts with.
struct Told {
    action: ActionRef,
}

/// What the words of one sentence said so far.
///
/// Every word says one thing. A word that the sentence has no place for is kept as unread, with the reason.
#[derive(Debug)]
pub(crate) struct Draft<'a> {
    sentence: &'a str,
    action: Option<Said<ActionWord>>,
    time_span: Option<Said<TimeSpan>>,
    error_code: Option<Said<ErrorCode>>,
    chance: Option<Chance>,
    lifetime: Option<Lifetime>,
    selector: Option<SelectorName>,
    fields: BTreeMap<Field, FieldSaid>,
    unread: Vec<Unread>,
}

impl<'a> Draft<'a> {
    /// The draft of a sentence that no word of was read yet.
    pub(crate) fn of(sentence: &'a str) -> Self {
        Self {
            sentence,
            action: None,
            time_span: None,
            error_code: None,
            chance: None,
            lifetime: None,
            selector: None,
            fields: BTreeMap::new(),
            unread: Vec::new(),
        }
    }

    /// The text at `span`.
    fn word(&self, span: Span) -> String {
        self.sentence.get(span.range()).unwrap_or_default().to_owned()
    }

    /// No rule of the grammar reads the word at `span`.
    pub(crate) fn unknown(&mut self, span: Span) {
        let word = self.word(span);
        self.unread.push(Unread::Unknown { word, span });
    }

    /// The word at `span` has the form of a value and is not one, for that `reason`.
    pub(crate) fn invalid(&mut self, span: Span, reason: String) {
        let word = self.word(span);
        self.unread.push(Unread::Invalid { word, span, reason });
    }

    /// The word at `span` needs something that the sentence does not say.
    pub(crate) fn unfinished(&mut self, span: Span, needs: &'static str) {
        let word = self.word(span);
        self.unread.push(Unread::Unfinished { word, span, needs });
    }

    /// The sentence has no place for the word at `span`.
    fn conflict(&mut self, span: Span, conflict: &'static str) {
        let word = self.word(span);
        self.unread.push(Unread::Conflict { word, span, conflict });
    }

    /// Whether the sentence says which calls it is about, with an API, a topic or a group.
    fn says_calls(&self) -> bool {
        self.fields.keys().any(|field| field.group() != FieldGroup::Connection)
    }

    /// Whether another word of `kind` would still say something, so that it is worth offering.
    pub(crate) fn still_takes(&self, kind: CompletionKind) -> bool {
        let action = self.action.as_ref().map(|said| &said.value);
        match kind {
            CompletionKind::Lie | CompletionKind::Preset => action.is_none(),
            CompletionKind::ErrorCode => {
                self.error_code.is_none() && matches!(action, None | Some(ActionWord::Lie(LieWord::Error)))
            }
            CompletionKind::Selector => self.selector.is_none() && !self.says_calls(),
            CompletionKind::Api | CompletionKind::Topic | CompletionKind::Group => self.selector.is_none(),
            CompletionKind::Client => true,
        }
    }

    /// The word at `span` names what happens.
    pub(crate) fn say_action(&mut self, span: Span, action: ActionWord) {
        if self.action.is_some() {
            self.conflict(span, once::ACTION);
        } else {
            self.action = Some(Said { value: action, span });
        }
    }

    /// The word at `span` is a time span: how long a delay or a throttle is.
    pub(crate) fn say_time_span(&mut self, span: Span, time_span: TimeSpan) {
        if self.time_span.is_some() {
            self.conflict(span, once::TIME_SPAN);
        } else {
            self.time_span = Some(Said { value: time_span, span });
        }
    }

    /// The word at `span` is the error code of a refusal.
    pub(crate) fn say_error_code(&mut self, span: Span, error_code: ErrorCode) {
        if self.error_code.is_some() {
            self.conflict(span, once::ERROR_CODE);
        } else {
            self.error_code = Some(Said {
                value: error_code,
                span,
            });
        }
    }

    /// The word at `span` is the chance that the rule fires.
    pub(crate) fn say_chance(&mut self, span: Span, chance: Chance) {
        if self.chance.is_some() {
            self.conflict(span, once::CHANCE);
        } else {
            self.chance = Some(chance);
        }
    }

    /// The words at `span` say how long the rule stays armed.
    pub(crate) fn say_lifetime(&mut self, span: Span, lifetime: Lifetime) {
        if self.lifetime.is_some() {
            self.conflict(span, once::LIFETIME);
        } else {
            self.lifetime = Some(lifetime);
        }
    }

    /// The word at `span` names the selector of the rule.
    pub(crate) fn say_selector(&mut self, span: Span, selector: SelectorName) {
        if self.selector.is_some() {
            self.conflict(span, once::SELECTOR);
        } else if self.says_calls() {
            self.conflict(span, only::SELECTOR_OR_CALLS);
        } else {
            self.selector = Some(selector);
        }
    }

    /// The word at `span` names a value of a field. Several names of one field mean any of them.
    pub(crate) fn say_name(&mut self, span: Span, name: FieldValue) {
        let Some(said) = self.field(span, name.field()) else {
            return;
        };
        if said.pattern.is_some() {
            self.conflict(span, only::PATTERN_ALONE);
        } else if !said.names.contains(&name) {
            said.names.push(name);
        }
    }

    /// The word at `span` is a pattern for the values of `field`.
    pub(crate) fn say_pattern(&mut self, span: Span, field: Field, pattern: Glob) {
        let Some(said) = self.field(span, field) else {
            return;
        };
        if said.pattern.is_some() || !said.names.is_empty() {
            self.conflict(span, only::PATTERN_ALONE);
        } else {
            said.pattern = Some(pattern);
        }
    }

    /// What the sentence says about `field` so far, unless it names a selector and the field is about the calls.
    fn field(&mut self, span: Span, field: Field) -> Option<&mut FieldSaid> {
        if self.selector.is_some() && field.group() != FieldGroup::Connection {
            self.conflict(span, only::SELECTOR_OR_CALLS);
            return None;
        }
        Some(self.fields.entry(field).or_default())
    }

    /// The rule the sentence gives, if it says what happens, and everything the words left unread, in their order.
    pub(crate) fn finish(mut self, configuration: &QuickInjectConfiguration) -> (Option<Rule>, Vec<Unread>) {
        let rule = self.told(configuration).and_then(|told| self.rule(told));
        self.unread.sort_by_key(Unread::span);
        (rule, self.unread)
    }

    /// What happens when the rule fires.
    ///
    /// A sentence without a lie word still names a lie: an error code is a refusal, and a time span is a delay.
    /// A time span or an error code that the action does not take is left unread.
    fn told(&mut self, configuration: &QuickInjectConfiguration) -> Option<Told> {
        let said = self.action.take();
        let action = match &said {
            Some(said) => said.value.clone(),
            None if self.error_code.is_some() => ActionWord::Lie(LieWord::Error),
            None if self.time_span.is_some() => ActionWord::Lie(LieWord::Delay),
            None => return None,
        };
        let time_span = self.time_span.take();
        if let Some(time_span) = &time_span
            && !matches!(action, ActionWord::Lie(LieWord::Delay | LieWord::Throttle))
        {
            self.conflict(time_span.span, only::TIMING_TAKES_TIME);
        }
        let error_code = self.error_code.take();
        if let Some(error_code) = &error_code
            && action != ActionWord::Lie(LieWord::Error)
        {
            self.conflict(error_code.span, only::REFUSAL_TAKES_CODE);
        }
        let time_span = time_span.map(|said| said.value);

        match action {
            ActionWord::Preset(preset) => Some(Told { action: preset.into() }),
            ActionWord::Lie(LieWord::Delay) => {
                let lie = TimingLie::new().with_delay(time_span.unwrap_or(configuration.default_delay));
                Some(Told { action: lie.into() })
            }
            ActionWord::Lie(LieWord::Throttle) => {
                let lie = TimingLie::new().with_throttle(time_span.unwrap_or(configuration.default_throttle));
                Some(Told { action: lie.into() })
            }
            ActionWord::Lie(LieWord::Error) => {
                let Some(error_code) = error_code else {
                    // Only the word `error` names a refusal without its code.
                    if let Some(said) = said {
                        self.unfinished(said.span, "an error code");
                    }
                    return None;
                };
                Some(Told {
                    action: RefusalLie::new(error_code.value).into(),
                })
            }
            ActionWord::Lie(LieWord::Blackhole) => Some(Told {
                action: LieDefinition::Silence.into(),
            }),
            ActionWord::Lie(LieWord::Disconnect) => Some(Told {
                action: HangUpLie::new().into(),
            }),
        }
    }

    /// The rule that does what the sentence told, to the calls and for the clients it names.
    fn rule(&self, told: Told) -> Option<Rule> {
        let mut calls = Conditions::new();
        let mut scope = Conditions::new();
        for (field, said) in &self.fields {
            let conditions = match field.group() {
                FieldGroup::Connection => &mut scope,
                _ => &mut calls,
            };
            conditions.insert(said.condition(*field)?);
        }
        let selector = match &self.selector {
            Some(selector) => SelectorRef::from(selector.clone()),
            None => SelectorRef::from(calls),
        };
        let trigger = match self.chance {
            Some(chance) => TriggerParts::new().with_chance(chance),
            None => TriggerParts::new(),
        };

        let scope = Scope::new(scope).ok()?;
        let phrase = RulePhrase::of(&selector, &scope, &told.action);
        let rule = Rule::new(phrase.name(), selector, told.action)
            .scoped(scope)
            .with_trigger(trigger)
            .with_lifetime(self.lifetime.unwrap_or_default());
        // A client or a group can have an ID that no title holds, such as one with a character nobody sees.
        // The rule then has no title, and it shows its name.
        Some(match RuleTitle::new(phrase.as_str()) {
            Ok(title) => rule.with_title(title),
            Err(_) => rule,
        })
    }
}
