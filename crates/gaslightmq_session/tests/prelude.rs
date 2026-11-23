//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the kernel, of the fields, of the calls and of the triggers.
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
    use gaslightmq_field::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_session::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let now = gaslightmq_clock::Instant::ZERO;
        let request = gaslightmq_wire::Request::new(
            12,
            1,
            Some("order-ingest"),
            &gaslightmq_wire::messages::MetadataRequest::default(),
        )
        .unwrap();

        let connection: Connection = Connection::new(
            ConnectionNumber::new(7),
            "10.0.4.27:51459".parse().unwrap(),
            Endpoint::Broker(BrokerId::from(2)),
        );
        let mut ordinals: ConnectionOrdinals = ConnectionOrdinals::new();
        let ordinal: ConnectionOrdinal = ordinals.assign(connection.host(), connection.endpoint());
        let key: SessionKey = SessionKey::of(&connection, ordinal);
        let contexts: ClientContexts = ClientContexts::new(core::time::Duration::from_secs(60));
        let mut session: Session<&str> = Session::open(connection, ordinal, contexts.clone(), now);
        let opened: Lifecycle = session.lifecycle();
        let identification: Identification = session.identify(&request);
        let change: IdentityChange = identification.changes()[0].clone();
        let identity: Identity = session.identity().clone();
        let client: ClientInstance = session.client().unwrap().clone();
        let context: ClientContext = contexts.join(&client);
        let learned: LearnedFields = LearnedFields::new().with(GroupId::new("analytics-etl").unwrap());
        let mut calls: InFlightCalls<&str> = InFlightCalls::new();
        let call: CallNumber = calls.arrive("Metadata", now);
        let phase: CallPhase = calls.get(call).unwrap().phase();
        let in_flight: InFlightCall<&str> = calls.get(call).unwrap().clone();
        let stalled: Stalled = calls.silence(call, now).unwrap();
        session.close(ClosedBy::Lie, now);
        let closed: Closed = session.closed().unwrap();
        let by: ClosedBy = closed.by();

        vec![
            connection.to_string(),
            ordinals.assigned(connection.host(), connection.endpoint()).to_string(),
            ordinal.to_string(),
            key.to_string(),
            contexts.len().to_string(),
            session.key().to_string(),
            opened.to_string(),
            format!("{identification:?}"),
            change.to_string(),
            format!("{identity:?}"),
            client.to_string(),
            context.sessions().to_string(),
            format!("{learned:?}"),
            calls.len().to_string(),
            call.to_string(),
            phase.to_string(),
            in_flight.content().to_string(),
            stalled.by().to_string(),
            format!("{closed:?}"),
            by.to_string(),
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

        vec![
            message_of(ConnectionOrdinalError::Zero),
            message_of(SessionKeyError::Malformed {
                input: "10.0.4.27".to_owned(),
            }),
            message_of(SessionError::InvalidClientSoftware {
                error: "librdkafka".parse::<ClientSoftware>().unwrap_err(),
            }),
            message_of(SessionError::CallNotInFlight {
                call: CallNumber::FIRST,
            }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 20);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "a connection ordinal is at least one",
            "session key \"10.0.4.27\" does not have the form a host, an endpoint and an ordinal, such as 10.0.4.27 broker 2 #1",
            "client.software: client software \"librdkafka\" does not have the form a name and a version, such as librdkafka 2.6.1",
            "call 1 is not in flight",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_open_a_session() {
    use gaslightmq_field::prelude::*;
    use gaslightmq_session::prelude::*;

    let connection = Connection::new(
        ConnectionNumber::new(7),
        "10.0.4.27:51459".parse().unwrap(),
        Endpoint::Bootstrap,
    );
    let contexts = ClientContexts::new(core::time::Duration::from_secs(60));
    let session: Session<()> = Session::open(
        connection,
        ConnectionOrdinal::FIRST,
        contexts,
        gaslightmq_clock::Instant::ZERO,
    );

    assert_eq!(session.lifecycle(), Lifecycle::Opened);
    assert_eq!(session.key().to_string(), "10.0.4.27 bootstrap #1");
}
