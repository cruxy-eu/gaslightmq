//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates that code imports next to an engine.
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
    use std::future::*;
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

    use gaslightmq_backend::prelude::*;
    use gaslightmq_call::prelude::*;
    use gaslightmq_clock_waitable::SteppedClock;
    use gaslightmq_combine::prelude::*;
    use gaslightmq_engine::prelude::*;
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_journal::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_lie_api::prelude::*;
    use gaslightmq_lie_refusal::prelude::*;
    use gaslightmq_listener::prelude::*;
    use gaslightmq_pipeline::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_session::prelude::*;
    use gaslightmq_trigger::prelude::*;
    use gaslightmq_wire_edit::prelude::*;

    /// A cluster of the bootstrap endpoint alone, which never answers.
    pub struct Mute;

    impl Backend for Mute {
        async fn call(&self, _request: BackendRequest) -> BackendAnswer {
            pending().await
        }

        async fn close(&self, _connection: ConnectionNumber) {}

        async fn endpoints(&self, known: &Endpoints) -> Endpoints {
            let bootstrap = Endpoints::none().with(Endpoint::Bootstrap);
            if *known == bootstrap {
                pending::<()>().await;
            }
            bootstrap
        }

        fn listens(
            &self,
            _endpoint: Endpoint,
            _address: SocketAddr,
        ) -> impl Future<Output = std::result::Result<(), ListenerRejection>> + Send {
            ready(Ok(()))
        }

        fn incident_support(&self) -> IncidentSupport {
            IncidentSupport::none()
        }

        fn apply(
            &self,
            incident: Incident,
        ) -> impl Future<Output = std::result::Result<ExpandedIncident, IncidentRejection>> + Send {
            let unsupported = self.incident_support().check(&Requirements::from(&incident));
            ready(Err(unsupported.expect_err("the cluster supports no incident").into()))
        }
    }

    /// One value of every item of the prelude, each named without a path.
    pub async fn every_item() -> Vec<String> {
        let any: ListenAddress = "127.0.0.1:0".parse().unwrap();
        let listeners = Listeners::new(
            any,
            "127.0.0.1:19092-19191".parse().unwrap(),
            "localhost".parse().unwrap(),
        );
        let mut configuration: EngineConfiguration = EngineConfiguration::new(listeners.unwrap());
        let backlog: EventBacklog = EventBacklog::new(16).unwrap();
        configuration.event_backlog = backlog;

        let engine: Engine<Mute, SteppedClock> =
            Engine::start(configuration.clone(), Mute, SteppedClock::new(), Lies::of_every_build())
                .await
                .unwrap();
        let rules: &ArmedRules = engine.rules();
        let silence = Rule::new("blackhole".parse().unwrap(), Conditions::new(), LieDefinition::Silence);
        let resolved = silence.resolve(&SelectorSet::default(), &PresetSet::default()).unwrap();
        let armed = rules.arm(resolved).unwrap();
        let watching: LiveEvents = engine.live_events();
        let keeping: EveryEvent = engine.every_event();
        let fired: EngineEvent = EngineEvent::IncidentFired(Incident::Rebalance {
            group: "payments".parse().unwrap(),
        });
        let journal: &Journal = engine.journal();
        let size: RecentJournalSize = configuration.recent_journal;
        let items = vec![
            format!("{configuration:?}"),
            backlog.to_string(),
            format!("{engine:?}"),
            format!("{rules:?}"),
            armed.name().to_string(),
            format!("{watching:?} {keeping:?}"),
            format!("{fired:?}"),
            format!("{journal:?} {size}"),
        ];
        engine.stop().await;
        items
    }

    /// One error of every type of the prelude, each named without a path.
    pub fn every_error() -> Vec<String> {
        let backlog: EventBacklogError = EventBacklogError::Zero;
        let not_opened: EndpointOpenError = EndpointOpenError::AddressUnavailable {
            endpoint: Endpoint::Bootstrap,
            reason: "cannot listen on 127.0.0.1:9092: address in use".to_owned(),
        };
        let not_started: EngineStartError = EngineStartError::EndpointNotOpened {
            error: not_opened.clone(),
        };
        let armed: ArmError = ArmError::AlreadyArmed {
            rule: "blackhole".parse().unwrap(),
        };
        let not_armed: DisarmError = DisarmError::NotArmed {
            rule: "blackhole".parse().unwrap(),
        };
        let stopped: LiveEventsError = LiveEventsError::EngineStopped;
        // A message says everything, so no error has a source.
        assert!(std::error::Error::source(&backlog).is_none());
        assert!(std::error::Error::source(&not_opened).is_none());
        assert!(std::error::Error::source(&not_started).is_none());
        assert!(std::error::Error::source(&armed).is_none());
        assert!(std::error::Error::source(&not_armed).is_none());
        assert!(std::error::Error::source(&stopped).is_none());
        vec![
            backlog.to_string(),
            not_opened.to_string(),
            not_started.to_string(),
            armed.to_string(),
            not_armed.to_string(),
            stopped.to_string(),
        ]
    }
}

#[tokio::test]
async fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item().await;

    assert_eq!(items.len(), 8);
    assert!(items.iter().all(|item| !item.is_empty()));
    assert_eq!(items[1], "16 events");
    assert_eq!(items[4], "blackhole");
    assert_eq!(items[7], "Journal { entries: 0, .. } 1000 entries");
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "an event backlog holds at least one event",
            "endpoint `bootstrap` did not open: cannot listen on 127.0.0.1:9092: address in use",
            "endpoint `bootstrap` did not open: cannot listen on 127.0.0.1:9092: address in use",
            "rule \"blackhole\" is armed already",
            "rule \"blackhole\" is not armed",
            "the engine stopped",
        ]
    );
}

#[tokio::test]
async fn the_prelude_is_enough_to_configure_an_engine() {
    use gaslightmq_engine::prelude::*;

    let listeners = gaslightmq_environment::Listeners::new(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:19092-19191".parse().unwrap(),
        "localhost".parse().unwrap(),
    );
    let mut configuration = EngineConfiguration::new(listeners.unwrap());
    configuration.event_backlog = EventBacklog::new(64).unwrap();

    assert_eq!(configuration.event_backlog.events(), 64);
}
