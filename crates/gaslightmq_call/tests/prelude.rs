//! The prelude holds every type, and its names clash with none of `std`, of the clock,
//! or of the preludes that code imports next to it.

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most, the clock,
/// and the prelude of every crate whose words a call record meets:
/// the vocabulary it is made of, and the crates that evaluate what it explains.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_and_every_neighbour {
    #![expect(unused_imports, reason = "the imports only have to compile next to each other")]

    use std::borrow::*;
    use std::collections::*;
    use std::convert::*;
    use std::error::*;
    use std::fmt::*;
    use std::hash::*;
    use std::io::*;
    use std::net::*;
    use std::num::*;
    use std::ops::*;
    use std::path::*;
    use std::prelude::rust_2024::*;
    use std::str::*;
    use std::string::*;
    use std::sync::*;
    use std::time::*;

    use gaslightmq_call::prelude::*;
    use gaslightmq_clock::*;
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    fn fired() -> TriggerExplanation {
        TriggerExplanation::new(TriggerDecision::Fired, Some(NonZeroU64::MIN), TriggerChecks::new())
            .expect("a trigger without parts fires")
    }

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let silence = gaslightmq_lie::LieDefinition::Silence.on(gaslightmq_field::Target::Call);
        let lost = LostTarget::new(
            gaslightmq_field::Target::Call,
            RecordedRuleName::new("Blackhole").expect("a name"),
        );
        let fate = LieFate::all_applied(silence.clone());
        let line = RuleExplanation::lied(
            RecordedRuleName::new("Blackhole").expect("a name"),
            gaslightmq_lie::Priority::HIGHEST,
            gaslightmq_field::Target::Call,
            fired(),
            fate.clone(),
        )
        .expect("a rule that fired its lie");
        let explanation = Explanation::new([line.clone()])
            .expect("one line")
            .with_failure(Failure::new("the frame ends early"));
        let timing = CallTiming::arrived(gaslightmq_clock::Instant::ZERO);
        let fields = gaslightmq_field::Fields::new()
            .with(gaslightmq_field::ConnectionNumber::new(7))
            .without_truth();
        let call = Call::new(
            CallNumber::FIRST,
            fields,
            Delivered::Nothing,
            explanation.clone(),
            timing,
        )
        .expect("a call that ended");
        let id: CallId = call.id();
        let verdict: Verdict = call.verdict();
        let response = ResponseSummary::new(Answer::Ok);

        vec![
            id.to_string(),
            response.to_string(),
            Delivered::ConnectionClosed.to_string(),
            verdict.to_string(),
            ExplanationStatus::PreEmpted.to_string(),
            DieCheck::new(1, 2).expect("a die").to_string(),
            WindowCheck::new(gaslightmq_clock::TimeSpan::ZERO, None, gaslightmq_clock::TimeSpan::ZERO)
                .expect("a window")
                .to_string(),
            BudgetCheck::new(1, NonZeroU64::MIN).expect("a budget").to_string(),
            lost.to_string(),
            fate.to_string(),
            line.to_string(),
            Failure::new("the frame ends early").to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        /// What an error says. A message says everything, so the error has no source.
        fn message_of<E: std::error::Error>(error: E) -> String {
            assert!(error.source().is_none(), "{error}");
            error.to_string()
        }

        let name = || RecordedRuleName::new("x").expect("a name");
        vec![
            message_of(CallNumberError::Zero),
            message_of(CallIdError::InvalidNumber {
                input: "#7.0".to_owned(),
                error: CallNumberError::Zero,
            }),
            message_of(RecordedRuleNameError::Empty),
            message_of(TriggerDecisionError::Unknown { name: "x".to_owned() }),
            message_of(DieCheckError::RollOutOfRange { ppm: 1_000_000 }),
            message_of(WindowCheckError::Empty {
                opens_after: gaslightmq_clock::TimeSpan::from_secs(2),
                closes_after: gaslightmq_clock::TimeSpan::from_secs(1),
            }),
            message_of(BudgetCheckError::LeftAboveLimit {
                left: 2,
                limit: NonZeroU64::MIN,
            }),
            message_of(TriggerExplanationError::FiredAgainstCheck),
            message_of(LieFateError::Nowhere),
            message_of(RuleExplanationError::NoIncident { rule: name() }),
            message_of(ExplanationError::RuleTwice { rule: name() }),
            message_of(CallTimingError::AnsweredWithoutForwarding),
            message_of(CallError::NoConnection),
        ]
    }
}

#[test]
fn the_prelude_names_every_type_next_to_std_and_every_neighbour() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_item(),
        [
            "#7.1",
            "ok",
            "connection closed",
            "silence",
            "pre-empted",
            "rolled 0.0001% under 0.0002%",
            "window open 0s after arming",
            "1 of 1 left",
            "by Blackhole",
            "silence applied",
            "Blackhole: matched the call; match 1: fired; silence applied",
            "the frame ends early",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_std_and_every_neighbour::every_error(),
        [
            "a call number is at least one",
            "call ID `#7.0`: a call number is at least one",
            "recorded rule name \"\" is empty",
            "no trigger decision is called \"x\"",
            "a roll of 1000000 parts per million is not below one million",
            "a time window from 2s to 1s after arming is never open",
            "a budget of 1 does not have 2 left",
            "a trigger does not fire while one of its parts does not agree",
            "a lie that fired applied or lost somewhere, and this one did neither",
            "rule \"x\" fired incidents, and the explanation lists none",
            "rule \"x\" has two lines in one explanation",
            "a backend does not answer a request that never left for it",
            "a call record needs the one `connection` its call ran on",
        ]
    );
}
