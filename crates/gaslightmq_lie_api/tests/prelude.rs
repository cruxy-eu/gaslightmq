//! The prelude holds every item, and its names clash with none of `std`, of the clock or of the other preludes.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the clock, the preludes of the kernel, the fields and the lies,
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
///
/// `gaslightmq_wire` has no prelude, and its `messages` hold a `TopicName` and a `BrokerId` of their own.
/// The request and the response therefore come by name.
mod next_to_the_others {
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

    use gaslightmq_clock::*;
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;

    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
    use gaslightmq_wire::{Api, Request, Response};

    /// One value of every item of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let request: Request = Request::new(4, 7, Some("order-ingest"), &HeartbeatRequest::default()).unwrap();
        let truth: Response = request.response(&HeartbeatResponse::default()).unwrap();

        let lies: Lies = Lies::new()
            .with(Timing)
            .unwrap()
            .with(Silence)
            .unwrap()
            .with(HangUp)
            .unwrap();
        let behaviour: &dyn Lie = lies.of(LieKind::HangUp).unwrap();
        let delay = LieDefinition::from(TimingLie::new().with_delay(TimeSpan::from_millis(750))).on(Target::Call);
        let hold: Effect = lies.after_response(&delay, &request, &truth).unwrap();
        let instruction: Instruction = hold.instruction();
        let dropped: Effect = Silence
            .before_forwarding(&LieDefinition::Silence.on(Target::Call), &request)
            .unwrap();
        let closed: Effect = HangUp
            .before_forwarding(&LieDefinition::from(HangUpLie::new()).on(Target::Call), &request)
            .unwrap();

        vec![
            format!("{lies:?}"),
            behaviour.kind().to_string(),
            Timing.kind().to_string(),
            hold.to_string(),
            instruction.to_string(),
            dropped.to_string(),
            closed.to_string(),
        ]
    }

    /// One error of every type of the prelude, each named without a path.
    ///
    /// A message says everything, so no error has a source.
    pub fn every_error() -> Vec<String> {
        let errors: Vec<Box<dyn std::error::Error>> = vec![
            Box::new(LieError::NotRegistered { kind: LieKind::Refusal }),
            Box::new(LieError::ApiNotSupported {
                kind: LieKind::Topology,
                api: Api::from_wire(0, 9),
            }),
            Box::new(LiesError::KindTwice { kind: LieKind::Timing }),
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
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(
        items,
        [
            "{Timing, Silence, HangUp}",
            "hang-up",
            "timing",
            "hold +750 ms",
            "hold",
            "drop",
            "close",
        ]
    );
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "no behaviour is registered for refusal",
            "topology cannot be told on Produce v9",
            "timing is registered twice",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_tell_a_lie() {
    use gaslightmq_field::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_wire::Request;
    use gaslightmq_wire::messages::HeartbeatRequest;

    let request = Request::new(4, 7, None, &HeartbeatRequest::default()).unwrap();
    let silence = LieDefinition::Silence.on(Target::Call);

    let effect = Lies::of_every_build().before_forwarding(&silence, &request).unwrap();

    assert!(effect.ends_call());
}
