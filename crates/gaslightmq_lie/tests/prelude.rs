//! The prelude holds every type, and its names clash with none of `std`, of the kernel's prelude or of the field prelude.

/// Imports the prelude next to the standard prelude, the preludes of the kernel and of the fields,
/// and the modules of `std` that code imports most.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
mod next_to_std_the_kernel_and_the_fields {
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
    use std::time::*;

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
        let unknown: LieError = "hangup".parse::<LieKind>().unwrap_err();

        vec![
            LieKind::HangUp.to_string(),
            Stage::BeforeForwarding.to_string(),
            class.to_string(),
            Aspect::HighWatermark.to_string(),
            targeted.to_string(),
            timing.to_string(),
            hang_up.to_string(),
            unknown.to_string(),
        ]
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
            "no lie is called \"hangup\"",
        ]
    );
}
