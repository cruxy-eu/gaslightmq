//! What a rule says, in words, and the name it has when nobody named it.

use std::fmt;

use gaslightmq_clock::TimeSpan;
use gaslightmq_field::Field;
use gaslightmq_lie::{LieDefinition, LieKind, Stage};
use gaslightmq_selector::{Condition, Conditions, Scope, SelectorRef};

use crate::{Action, ActionRef, RuleName};

/// What a rule says, in words: what happens, on which calls, for which clients and at which broker.
///
/// `Refuse with NOT_LEADER_OR_FOLLOWER on Produce orders`.
///
/// A rule that nobody named has the [name](Self::name) of its phrase, and a rule that ⌘K made has it as its title too.
/// The trigger, the lifetime and the dice key are no part of it: two rules that differ in those alone say the same.
///
/// | Part | Says |
/// |---|---|
/// | The action | A preset by its name. A refusal with its error code, a delay or a throttle with its time span, a blackhole, a disconnect, an incident by its kind. A lie at another stage than the default of its kind says it. |
/// | The calls | A selector by its name, or the APIs, the topics, the groups and every other condition of the inline selector, in that order. Several values of one field are any of them: `Produce + Fetch`. |
/// | The scope | The clients after `for`, the brokers after `at`, and every other condition of the scope. |
///
/// ```
/// use gaslightmq_kernel::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_rule::prelude::*;
/// use gaslightmq_selector::prelude::*;
///
/// let phrase = RulePhrase::of(
///     &Conditions::from(Condition::is(ApiKey::PRODUCE)).into(),
///     &Scope::client(ClientId::new("order-ingest").expect("a client ID")),
///     &RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER).at(Stage::AfterResponse).into(),
/// );
///
/// assert_eq!(
///     phrase.as_str(),
///     "Refuse with NOT_LEADER_OR_FOLLOWER after the response on Produce for order-ingest"
/// );
/// // A name holds 63 characters at most, so the name of a long phrase is cut there.
/// assert_eq!(phrase.name(), "refuse-with-not-leader-or-follower-after-the-response-on-produc");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RulePhrase(String);

impl RulePhrase {
    /// What a rule of this selector, scope and action says.
    pub fn of(selector: &SelectorRef, scope: &Scope, action: &ActionRef) -> Self {
        let calls = match selector {
            SelectorRef::Named(name) => name.to_string(),
            SelectorRef::Inline(conditions) if conditions.is_empty() => "all calls".to_owned(),
            SelectorRef::Inline(conditions) => calls(conditions),
        };
        Self(format!("{} on {calls}{}", said(action), clients(scope.conditions())))
    }

    /// The phrase as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name of a rule that says this: the phrase in the characters a name holds.
    ///
    /// A letter is in lower case, and whatever stands between letters and digits is one hyphen.
    /// A name that would be longer than [`RuleName::MAX_LENGTH`] is cut there.
    pub fn name(&self) -> RuleName {
        let mut name = String::new();
        for character in self.0.chars() {
            if name.len() == RuleName::MAX_LENGTH {
                break;
            }
            if character.is_ascii_alphanumeric() {
                name.push(character.to_ascii_lowercase());
            } else if !name.is_empty() && !name.ends_with('-') {
                name.push('-');
            }
        }
        match RuleName::new(name.trim_end_matches('-')) {
            Ok(name) => name,
            // A phrase starts with what happens: the name of a preset, or a word.
            Err(error) => unreachable!("a phrase starts with a letter or a digit: {error}"),
        }
    }
}

impl fmt::Display for RulePhrase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl RuleName {
    /// The same name with `-` and `number` at its end: `slow-order-producers-2`.
    ///
    /// The name is cut first where the end would not fit, so the numbered name holds at most
    /// [`MAX_LENGTH`](Self::MAX_LENGTH) characters.
    /// Whoever creates a rule that nobody named numbers its name, until no other rule has it.
    #[must_use]
    pub fn numbered(&self, number: u32) -> RuleName {
        let end = format!("-{number}");
        let kept = self.as_str().len().min(Self::MAX_LENGTH - end.len());
        let name = format!("{}{end}", self.as_str()[..kept].trim_end_matches('-'));
        match RuleName::new(&name) {
            Ok(name) => name,
            Err(error) => unreachable!("a name with a number at its end is a name: {error}"),
        }
    }
}

/// What the action does, as the phrase starts: `Delay 750ms`.
fn said(action: &ActionRef) -> String {
    match action {
        ActionRef::Preset(preset) => preset.to_string(),
        ActionRef::Inline(Action::Lie(lie)) => lied(lie),
        ActionRef::Inline(Action::Incident(incident)) => incident.kind().to_string(),
    }
}

/// What a lie does, with its stage when it is not the default of its kind.
fn lied(lie: &LieDefinition) -> String {
    let words = match lie {
        LieDefinition::Refusal(refusal) => format!("Refuse with {}", refusal.error),
        LieDefinition::Timing(timing) => {
            let throttles = timing.throttle() != TimeSpan::ZERO;
            match (timing.delay() == TimeSpan::ZERO, throttles) {
                (false, true) => format!("Delay {} and throttle {}", timing.delay(), timing.throttle()),
                (true, true) => format!("Throttle {}", timing.throttle()),
                (_, false) => format!("Delay {}", timing.delay()),
            }
        }
        LieDefinition::Silence => "Blackhole".to_owned(),
        LieDefinition::HangUp(_) => "Disconnect".to_owned(),
        // A lie that a later build adds says its kind.
        other => other.kind().title().to_owned(),
    };
    format!("{words}{}", at_stage(lie.kind(), lie.stage()))
}

/// The stage as the end of a phrase, or nothing when it is the default of the kind.
fn at_stage(kind: LieKind, stage: Stage) -> String {
    if stage == kind.default_stage() {
        String::new()
    } else {
        format!(" {}", stage.phrase())
    }
}

/// The calls of inline conditions: the APIs, the topics, the groups, and every other condition by its field.
fn calls(conditions: &Conditions) -> String {
    let first = [Field::Api, Field::Topic];
    let mut said: Vec<String> = first
        .iter()
        .filter_map(|field| conditions.get(*field))
        .map(values)
        .collect();
    if let Some(group) = conditions.get(Field::Group) {
        said.push(format!("group {}", values(group)));
    }
    let others = conditions
        .iter()
        .filter(|condition| !matches!(condition.field(), Field::Api | Field::Topic | Field::Group));
    said.extend(others.map(|condition| format!("{} {}", condition.field(), values(condition))));
    said.join(" ")
}

/// The clients and the brokers of a scope, each after its word, and every other condition by its field.
fn clients(scope: &Conditions) -> String {
    let clients = scope
        .get(Field::ClientId)
        .map(|clients| format!(" for {}", values(clients)));
    let endpoints = scope
        .get(Field::Endpoint)
        .map(|endpoints| format!(" at {}", values(endpoints)));
    let others = scope
        .iter()
        .filter(|condition| !matches!(condition.field(), Field::ClientId | Field::Endpoint))
        .map(|condition| format!(" {} {}", condition.field(), values(condition)));
    clients.into_iter().chain(endpoints).chain(others).collect()
}

/// The values of a condition, as a phrase writes them: `Produce + Fetch`, `orders.*` or `not Metadata`.
fn values(condition: &Condition) -> String {
    let any_of = |values: &gaslightmq_selector::ValueSet| {
        let values: Vec<String> = values.iter().map(ToString::to_string).collect();
        values.join(" + ")
    };
    match condition {
        Condition::Is(value) => value.to_string(),
        Condition::IsAnyOf(values) => any_of(values),
        Condition::Matches(_, pattern) => pattern.to_string(),
        Condition::IsNot(values) => format!("not {}", any_of(values)),
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    use super::*;
    use crate::PresetName;

    #[test]
    fn a_phrase_says_what_happens_and_to_which_calls() {
        let produce = SelectorRef::from(Conditions::from(Condition::is(ApiKey::PRODUCE)));
        let delay = ActionRef::from(TimingLie::new().with_delay(TimeSpan::from_millis(750)));
        let blackhole = ActionRef::from(LieDefinition::Silence);
        let preset = ActionRef::from("leader-flap".parse::<PresetName>().unwrap());

        assert_eq!(
            RulePhrase::of(&produce, &Scope::all(), &delay).as_str(),
            "Delay 750ms on Produce"
        );
        assert_eq!(
            RulePhrase::of(&Conditions::new().into(), &Scope::all(), &blackhole).as_str(),
            "Blackhole on all calls"
        );
        assert_eq!(
            RulePhrase::of(&produce, &Scope::all(), &preset).as_str(),
            "leader-flap on Produce"
        );
    }

    #[test]
    fn a_name_is_the_phrase_in_the_characters_of_a_name() {
        let produce = SelectorRef::from(Conditions::from(Condition::is(ApiKey::PRODUCE)));
        let refusal = ActionRef::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER));

        assert_eq!(
            RulePhrase::of(&produce, &Scope::all(), &refusal).name(),
            "refuse-with-not-leader-or-follower-on-produce"
        );
    }

    #[test]
    fn a_numbered_name_fits_the_length_of_a_name() {
        let long = RuleName::new("a".repeat(RuleName::MAX_LENGTH)).unwrap();
        let name: RuleName = "slow".parse().unwrap();

        assert_eq!(name.numbered(2), "slow-2");
        assert_eq!(long.numbered(12).as_str().len(), RuleName::MAX_LENGTH);
        assert!(long.numbered(12).as_str().ends_with("a-12"));
    }
}
