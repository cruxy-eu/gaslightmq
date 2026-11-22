//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that code imports next to a pipeline.
///
/// All of them are glob imports, so they rank the same.
/// A name that two of them hold is ambiguous, and using it does not compile.
/// This module names every item of the prelude, so it only compiles while no name clashes.
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
    use gaslightmq_combine::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_lie_refusal::prelude::*;
    use gaslightmq_pipeline::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_session::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use gaslightmq_wire_edit::prelude::*;

    /// One value of every item of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let now = gaslightmq_clock::Instant::ZERO;
        let request = gaslightmq_wire::Request::new(
            4,
            41,
            Some("order-ingest"),
            &gaslightmq_wire::messages::HeartbeatRequest::default(),
        )
        .unwrap();

        let topics: KnownTopics = KnownTopics::new();
        let shared: SharedBySessions = SharedBySessions::new(
            ClientContexts::new(core::time::Duration::from_secs(60)),
            topics.clone(),
            Arc::new(Lies::of_every_build()),
        );
        let connection = Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Bootstrap,
        );
        let snapshot = Arc::new(Snapshot::default());
        let mut pipeline: Pipeline =
            Pipeline::open(connection, ConnectionOrdinal::FIRST, shared.clone(), snapshot, now);

        let bytes = request.frame().to_bytes();
        let input: Input<'_> = Input::ClientBytes(&bytes);
        let outputs: Outputs = pipeline.handle(input, now);
        let output: Output = outputs.iter().next().unwrap().clone();
        let Output::Forward { call, .. } = &output else {
            unreachable!("a call that no rule touches goes to the backend");
        };
        let timer: TimerId = TimerId::new(*call, Stage::AfterResponse);

        vec![
            topics.names().len().to_string(),
            format!("{shared:?}"),
            pipeline.key().to_string(),
            outputs.len().to_string(),
            format!("{output:?}"),
            timer.to_string(),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 6);
    assert!(items.iter().all(|item| !item.is_empty()));
    assert_eq!(items[5], "#7.1 after the response");
}

#[test]
fn the_prelude_is_enough_to_drive_a_pipeline() {
    use std::sync::Arc;

    use gaslightmq_pipeline::prelude::*;

    let shared = SharedBySessions::new(
        gaslightmq_session::ClientContexts::new(core::time::Duration::from_secs(60)),
        KnownTopics::new(),
        Arc::new(gaslightmq_lie_api::Lies::of_every_build()),
    );
    let connection = gaslightmq_session::Connection::new(
        gaslightmq_field::ConnectionNumber::new(7),
        "10.0.4.27:51459".parse().unwrap(),
        gaslightmq_field::Endpoint::Bootstrap,
    );
    let now = gaslightmq_clock::Instant::ZERO;
    let mut pipeline = Pipeline::open(
        connection,
        gaslightmq_session::ConnectionOrdinal::FIRST,
        shared,
        Arc::new(gaslightmq_rule::Snapshot::default()),
        now,
    );

    // The client closes before it sent a request, and the session ends with nothing to report.
    let outputs: Outputs = pipeline.handle(Input::ClientClosed, now);

    assert!(outputs.is_empty());
    assert!(pipeline.closed().is_some());
    assert!(
        pipeline
            .handle(
                Input::TimerFired(TimerId::new(
                    "#7.1".parse().unwrap(),
                    gaslightmq_lie::Stage::AfterResponse
                )),
                now
            )
            .is_empty()
    );
}

#[test]
fn a_pipeline_and_what_it_takes_and_returns_move_to_the_task_that_drives_them() {
    use gaslightmq_pipeline::prelude::*;

    fn moves_between_threads<T: Send>() {}

    moves_between_threads::<Pipeline>();
    moves_between_threads::<Input<'_>>();
    moves_between_threads::<Outputs>();
    moves_between_threads::<SharedBySessions>();
    moves_between_threads::<KnownTopics>();
}
