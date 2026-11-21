//! The prelude holds every type, and its names clash with none of `std`, of the clock or of the other preludes.

/// Imports the prelude next to the standard prelude, the clock, the preludes of the kernel, the fields and the lies,
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_the_others {
    #![allow(unused_imports)]

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

    use gaslightmq_clock::*;
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let partition = Target::of_parts(["orders.eu/3".parse().unwrap()]).unwrap();
        let priority: Priority = Priority::new(1);
        let claim: Claim = Claim::Refusal {
            stage: Stage::AfterResponse,
            target: partition,
        };
        let refusal: FiredLie<&str> = FiredLie::new("Lost acknowledgement", priority, claim.clone());
        let hang_up: FiredLie<&str> = FiredLie::new(
            "Late disconnect",
            Priority::HIGHEST,
            Claim::Terminal {
                stage: Stage::AfterResponse,
            },
        );
        let slow = FiredLie::new("Slow", Priority::new(2), TimingLie::new().at(Stage::BeforeForwarding));

        let request: RequestPass<&str> = RequestPass::of([refusal, hang_up, slow]);
        let call: Combination<&str> = request.without_response_pass();
        let fate: &Fate<&str> = call.fate_of(&"Lost acknowledgement").unwrap();
        let loss: &Loss<&str> = &fate.shadowed()[0];
        let effect: &Effect<&str> = call.after_response();
        let winners: &Winners<&str> = effect.refusals();
        let delay: Delay = effect.delay();

        vec![
            priority.position().to_string(),
            claim.class().to_string(),
            fate.id().to_string(),
            loss.to().to_string(),
            effect.terminal().unwrap().to_string(),
            winners.is_empty().to_string(),
            delay.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(ClaimError::NoAspect { kind: LieKind::Data }),
            Box::new(ClaimError::NoTiming { kind: LieKind::Timing }),
        ];

        errors
            .iter()
            .map(|error| {
                assert!(error.source().is_none(), "{error}");
                error.to_string()
            })
            .collect()
    }
}

#[test]
fn the_prelude_names_every_type_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(
        items,
        [
            "1",
            "refusal",
            "Lost acknowledgement",
            "Late disconnect",
            "Late disconnect",
            "true",
            "+0 s",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "a data lie rewrites, and its definition names no aspect",
            "a timing lie is a modifier, and its definition holds no timing",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_combine_the_lies_of_a_call() {
    use gaslightmq_clock::TimeSpan;
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    let matched = Target::of_parts(["orders.eu/3".parse().unwrap()]).unwrap();
    let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_secs(1)))
        .on(matched.clone())
        .unwrap();
    let refusal = LieDefinition::from(RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER))
        .on(matched)
        .unwrap();

    let call = RequestPass::of([
        FiredLie::new("Delay 1 s", Priority::new(0), Claim::try_from(&delay).unwrap()),
        FiredLie::new(
            "Refuse the 3rd Produce",
            Priority::new(1),
            Claim::try_from(&refusal).unwrap(),
        ),
    ])
    .without_response_pass();

    assert!(call.fates().iter().all(Fate::counts));
}
