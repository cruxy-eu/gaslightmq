//! The prelude holds every item, and its names clash with none of `std` or of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the kernel, the fields, the lies, the contract, the surgery and the call record.
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
    use std::time::*;

    use gaslightmq_call::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_lie_refusal::prelude::*;
    use gaslightmq_wire_edit::prelude::*;

    use gaslightmq_wire::messages::{HeartbeatRequest, HeartbeatResponse};
    use gaslightmq_wire::{Api, Request, Response};

    /// One value of every item of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let request: Request = Request::new(4, 7, Some("analytics-etl"), &HeartbeatRequest::default()).unwrap();
        let truth: Response = request.response(&HeartbeatResponse::default()).unwrap();
        let rebalance = LieDefinition::from(RefusalLie::new(ErrorCode::REBALANCE_IN_PROGRESS)).on(Target::Call);

        let behaviour: Refusal = Refusal;
        let asked = EditableRequest::decode(&request, &TopicNames::new()).unwrap();
        let before = behaviour.before_forwarding(&rebalance, &asked).unwrap();
        let refused: RefusedRequest =
            RefusedRequest::new(asked, &Target::Call, ErrorCode::REBALANCE_IN_PROGRESS).unwrap();
        let after: HeartbeatResponse = refused.after_response(&truth).unwrap().body().unwrap();

        vec![
            behaviour.kind().to_string(),
            refused.failures().to_string(),
            before.to_string(),
            after.error_code.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    assert_eq!(
        next_to_the_others::every_item(),
        ["refusal", "call: REBALANCE_IN_PROGRESS", "answer Heartbeat v4", "27",]
    );
}

#[test]
fn the_prelude_is_enough_to_register_the_refusal() {
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_lie_refusal::prelude::*;

    let lies = Lies::of_every_build().with(Refusal).unwrap();

    assert!(lies.tells(LieKind::Refusal));
    assert_eq!(
        lies.with(Refusal).unwrap_err(),
        LieError::KindTwice { kind: LieKind::Refusal }
    );
}
