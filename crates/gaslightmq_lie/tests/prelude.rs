//! The prelude holds every type, and its names clash with none of `std`, of the clock, of the kernel's prelude or of the field prelude.

/// Imports the prelude next to the standard prelude, the clock, the preludes of the kernel and of the fields,
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_the_kernel_and_the_fields {
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

    use gaslightmq_clock::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let refusal = RefusalLie::new(ErrorCode::NOT_LEADER_OR_FOLLOWER);
        let timing = TimingLie::new();
        let hang_up = HangUpLie::new().at(Stage::AfterResponse);
        let definition = LieDefinition::from(refusal);
        let class: CombinationClass = definition.class();
        let targeted: TargetedLie = definition.on(Target::Call);

        vec![
            LieKind::HangUp.to_string(),
            Stage::BeforeForwarding.to_string(),
            class.to_string(),
            Aspect::HighWatermark.to_string(),
            targeted.to_string(),
            timing.to_string(),
            hang_up.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        let name = || "x".to_owned();
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(LieKindError::Unknown { name: name() }),
            Box::new(StageError::Unknown { name: name() }),
            Box::new(StageError::NotAllowed {
                kind: LieKind::Silence,
                stage: Stage::AfterResponse,
            }),
            Box::new(CombinationClassError::Unknown { name: name() }),
            Box::new(AspectError::Unknown { name: name() }),
            Box::new(TimingLieError::JitterExceedsDelay {
                jitter: TimeSpan::from_millis(200),
                delay: TimeSpan::ZERO,
            }),
            Box::new(TimingLieError::ThrottleTooLong {
                throttle: TimeSpan::from_secs(3_000_000),
            }),
            Box::new(TargetedLieError::PartsNotAllowed { kind: LieKind::Timing }),
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
fn the_prelude_names_every_type_next_to_std_the_kernel_and_the_fields() {
    let items = next_to_std_the_kernel_and_the_fields::every_item();
    assert_eq!(
        items,
        [
            "hang-up",
            "before",
            "refusal",
            "high-watermark",
            "NOT_LEADER_OR_FOLLOWER",
            "+0 s",
            "hang-up after the response",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_std_the_kernel_and_the_fields::every_error(),
        [
            "no lie is called \"x\"",
            "no stage is called \"x\"",
            "silence cannot act after the response",
            "no combination class is called \"x\"",
            "no aspect is called \"x\"",
            "a jitter of 200ms is longer than the delay of 0s",
            "a throttle of 50000m is longer than 2147483647ms, the longest that Kafka can tell a client",
            "timing always targets the whole call",
        ]
    );
}
