//! Quick inject: the sentence someone types in the ⌘K palette, and the rule it means.
//!
//! `delay produce orders.* 750ms 40% order-ingest` is a rule:
//! it delays 40% of the Produce calls of `order-ingest` to the `orders.*` topics by 750 ms.
//! This crate reads such a sentence while it is typed.
//!
//! | Type | Is |
//! |---|---|
//! | [`QuickInject`] | The parser. It [reads](QuickInject::read) the text typed so far. |
//! | [`QuickInjectConfiguration`] | What a sentence means where it leaves a value out, and how many completions it offers. |
//! | [`KnownNames`] | The topics, groups, clients, presets and selectors the system knows, and the titles of the last two. Whoever reads a sentence hands them over. |
//! | [`Sentence`] | A sentence as it was read: the rule, the completions and what was not read. |
//! | [`Completion`], [`CompletionKind`] | One thing the word at the end can become, and what it is. |
//! | [`Unread`] | A part of the sentence that was not read: where it stands, and why. |
//! | [`Span`] | Where a part of a sentence stands. |
//!
//! The [`prelude`] holds all of them.
//!
//! ```
//! use gaslightmq_kernel::prelude::*;
//! use gaslightmq_quick_inject::prelude::*;
//! use gaslightmq_rule::prelude::*;
//!
//! // The host knows these names from the traffic and from the workspace.
//! let known = KnownNames::new()
//!     .with_topics(["orders.eu".parse().expect("a topic"), "orders.us".parse().expect("a topic")])
//!     .with_presets([("leader-flap".parse()?, None)]);
//! let quick_inject = QuickInject::default();
//!
//! // Someone types. The word at the end of the text is no word yet, and it can become a topic.
//! let typing = quick_inject.read("leader-flap on produce ord", &known);
//! assert_eq!(typing.unread()[0].word(), "ord");
//! assert_eq!(typing.completions()[0].kind(), CompletionKind::Topic);
//! assert_eq!(typing.completions()[0].sentence(), "leader-flap on produce orders.eu ");
//!
//! // They take the completion, and say how long the rule lives.
//! let sentence = quick_inject.read("leader-flap on produce orders.eu for 3 firings", &known);
//! let rule = sentence.rule().expect("the sentence names a preset");
//! assert_eq!(rule.name(), "leader-flap-on-produce-orders-eu");
//! assert_eq!(rule.title().map(RuleTitle::as_str), Some("leader-flap on Produce orders.eu"));
//! assert_eq!(rule.action().preset().map(PresetName::as_str), Some("leader-flap"));
//! assert_eq!(rule.lifetime().to_string(), "3 firings");
//! assert!(sentence.unread().is_empty());
//! # Ok::<(), PresetNameError>(())
//! ```
//!
//! A sentence only gives a definition.
//! Whoever read it creates the rule, as a live rule, and arms it.
//!
//! # The grammar
//!
//! A sentence is words with white space between them.
//! Their order is free: each word says by itself what it is, except after a word that announces the next one.
//!
//! | A word | Says | Example |
//! |---|---|---|
//! | `delay`, `throttle`, `error`, `blackhole`, `disconnect` | What happens: a lie. `refuse`, `silence` and `hang-up` are other words for the last three. | `delay` |
//! | A time span | How long the delay or the throttle is. | `750ms` |
//! | The name of an error | The error code of the refusal. | `NOT_LEADER_OR_FOLLOWER` |
//! | The name of an API | The calls of that API. Several APIs are any of them. | `produce fetch` |
//! | A text with `*` or `?` | The topics that match the pattern. After `group`, `client` or `for`, the groups or the clients that do. | `orders.*` |
//! | `b` and a number | The calls to that broker. | `b2` |
//! | A percentage | The chance that a matching call fires the rule. | `40%` |
//! | `for` and a time span or a count | The lifetime. | `for 20s`, `for 3 firings`, `for 100 matches` |
//! | A [known name](KnownNames) | That preset, selector, topic, group or client. | `order-ingest` |
//! | `preset`, `selector`, `topic`, `group`, `client`, `broker` | The next word is a name of that kind, known or not, whatever else it looks like. | `client billing`, `preset blackhole` |
//! | `on`, `to`, `from`, `for`, `by`, `with` | Nothing. The first three say that an unknown name after them is a topic, and `for` says that it is a client. | `to orders for billing` |
//!
//! - **Words read in any case.**
//!   `Produce`, `produce` and `PRODUCE` are one API. A name is exact: `orders.eu` is not `Orders.eu`.
//! - **A word has no white space**, and nothing quotes one.
//!   A preset and a selector have a slug for a name, so one word names them: `leader-flap`.
//!   A sentence cannot name a client or a group whose ID holds white space.
//! - **The grammar comes first.**
//!   A topic that is called `fetch` and a preset that is called `blackhole` need the word for their kind:
//!   `topic fetch`, `preset blackhole`.
//! - **A name of several kinds is the first of them**, in this order: preset, selector, topic, group, client.
//!   The word for the kind, or a preposition, says otherwise: `for payments` is the client, when `payments` is a topic too.
//! - **Several names of one field are any of them**: `produce fetch`, `orders.eu orders.us`.
//!   A pattern stands alone on its field.
//! - **A client and a broker are the scope** of the rule. An API, a topic and a group are its selector.
//!   A sentence that names a selector says no API, topic or group next to it.
//!
//! # What a sentence leaves out
//!
//! A sentence is a rule as soon as it says what happens.
//!
//! | Left out | The rule |
//! |---|---|
//! | The lie word | Refuses, when the sentence names an error code. Delays, when it names a time span. Otherwise there is no rule yet. |
//! | The time span | Delays by the [default delay](QuickInjectConfiguration::default_delay), or throttles by the [default throttle](QuickInjectConfiguration::default_throttle). |
//! | The error code | Does not exist yet: `error` needs its code. |
//! | APIs, topics and groups | Considers every call. |
//! | Clients and brokers | Applies to all clients. |
//! | The chance | Fires on every matching call. |
//! | The lifetime | Stays armed until disarmed. |
//!
//! A lie acts at the default stage of its kind.
//! A sentence says no stage, no jitter, no trigger other than a chance, and no incident.
//! A preset says all of those, and the rule can be edited once it exists.
//!
//! The rule gets the title that says what it does, its [phrase](gaslightmq_rule::RulePhrase):
//! `Delay 750ms on Produce orders.* for order-ingest`.
//! Its name is the name of that phrase, as every rule that nobody named has: `delay-750ms-on-produce-orders-for-order-ingest`.
//! Names are unique among rules, and the sentence does not know the other rules: whoever creates the rule checks that.
//!
//! # Completions
//!
//! The word at the end of the text is the word that is being typed.
//! A sentence [offers](Sentence::completions) the words that start like it, in any case:
//! lie words, presets, APIs, selectors, topics, groups, clients and error codes, in that order.
//!
//! - A preset and a selector also complete from their title: a word that starts any word of the title offers the name.
//!   `prod` offers `order-producers`, when its title is `Order producers`.
//!   The sentence takes the name and never the title, and the [completion](Completion::title) carries the title to show.
//! - After the word for a kind of name, it offers names of that kind only.
//! - After a preposition, the kind the preposition hints at comes first.
//! - It offers nothing the sentence has no place for: no second lie, and no selector next to an API.
//!   The words before the one that is being typed decide that. The word itself does not, whatever it reads as so far.
//! - A name that would read as something else is offered with the word for its kind: `client payments`, `preset blackhole`.
//! - A text that ends in white space offers every next word, up to the [limit](QuickInjectConfiguration::completion_limit).
//!
//! # Errors
//!
//! Reading a sentence does not fail, because a sentence that is being typed is rarely whole.
//! What the parser could not read is a list of [`Unread`], each with the [`Span`] it stands at.
//! The message of an [`Unread`] is complete, so it has no [`source`](std::error::Error::source).
//! What another crate said about a value is text in its `reason`.
//!
//! The words that were not read change nothing about the rule.
//! `delay prodcue` delays every call, so whoever creates the rule looks at the unread words first.
//!
//! # No IO
//!
//! The crate depends on the definitions it produces, in `gaslightmq_rule`, `gaslightmq_selector` and `gaslightmq_lie`,
//! on `gaslightmq_trigger` for the chance and the lifetime,
//! and on `gaslightmq_field`, `gaslightmq_kernel` and `gaslightmq_clock` for fields, Kafka's names and time spans.
//! It does not depend on the Kafka codec.
//! Reading is a pure function of the text, the known names and the configuration:
//! the crate has no async code, no IO, no clock and no randomness.
//! The host runs it behind the control plane, and the UI asks the control plane.

mod completion;
mod configuration;
mod draft;
mod known;
mod quick_inject;
mod sentence;
mod span;
mod words;

pub use completion::{Completion, CompletionKind};
pub use configuration::QuickInjectConfiguration;
pub use known::KnownNames;
pub use quick_inject::QuickInject;
pub use sentence::{Sentence, Unread};
pub use span::Span;

/// Every item of this crate.
///
/// Import it whole wherever code reads sentences, next to the preludes of the crates a rule is made of.
/// No name in it is a name of the standard prelude or of those preludes, so the import shadows nothing.
///
/// ```
/// use gaslightmq_quick_inject::prelude::*;
/// use gaslightmq_rule::prelude::*;
///
/// let sentence: Sentence = QuickInject::default().read("blackhole metadata", &KnownNames::new());
/// assert_eq!(sentence.rule().map(Rule::name).map(RuleName::as_str), Some("blackhole-on-metadata"));
/// ```
pub mod prelude {
    #[doc(no_inline)]
    pub use crate::{
        Completion, CompletionKind, KnownNames, QuickInject, QuickInjectConfiguration, Sentence, Span, Unread,
    };
}
