//! The prelude holds every item of the crate, and its names clash with none of the preludes next to it.

#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

/// Imports the prelude next to the standard prelude, the modules of `std` that code imports most,
/// and the preludes of the crates whose types travel with an environment: to the engine, and to the UI.
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
    use gaslightmq_environment::prelude::*;
    use gaslightmq_field::prelude::*;
    use gaslightmq_incident::prelude::*;
    use gaslightmq_kernel::prelude::*;
    use gaslightmq_lie::prelude::*;
    use gaslightmq_rule::prelude::*;
    use gaslightmq_selector::prelude::*;
    use gaslightmq_trigger::prelude::*;

    /// One value of every type of the prelude, each named without a path.
    pub fn every_item() -> Vec<String> {
        let name: EnvironmentName = EnvironmentName::new("staging-proxy").unwrap();
        let world: WorldName = WorldName::new("orders-sim").unwrap();
        let host: HostName = HostName::new("gaslight.staging").unwrap();
        let bootstrap: ServerAddress = ServerAddress::new(HostName::new("kafka-1.staging").unwrap(), 9092).unwrap();
        let address: ListenAddress = "0.0.0.0:9092".parse().unwrap();
        let ports: PortRange = PortRange::new(19092, 19191).unwrap();
        let brokers: BrokerListeners = BrokerListeners::range(address.ip(), ports);
        let listen: Listeners = Listeners::new(address, brokers.clone(), host.clone()).unwrap();
        let path: FilePath = FilePath::new("/run/secrets/kafka-password").unwrap();
        let variable: VariableName = VariableName::new("KAFKA_PASSWORD").unwrap();
        let password: SecretRef = SecretRef::File(path.clone());
        let username: SaslUsername = SaslUsername::new("gaslight").unwrap();
        let mechanism: SaslMechanism = SaslMechanism::ScramSha512;
        let sasl: Sasl = Sasl::new(mechanism, username.clone(), password.clone());
        let tls: Tls = Tls::new().with_ca(path.clone());
        let upstream: Upstream = Upstream::new(bootstrap.clone())
            .with_tls(tls.clone())
            .with_sasl(sasl.clone());
        let recording: RecordingMode = RecordingMode::MetadataOnly;
        let proxy: ProxyDefinition = ProxyDefinition::new(listen.clone()).with_recording(recording);
        let broker: BrokerDefinition = BrokerDefinition::new(world.clone());
        let instances: Instances = Instances::ProxyInFrontOfCluster {
            proxy: proxy.clone(),
            upstream: upstream.clone(),
        };
        let url: SchemaRegistryUrl = "https://schema-registry.staging.internal:8081".parse().unwrap();
        let registry: SchemaRegistry = SchemaRegistry::Real { url: url.clone() };
        let environment: Environment = Environment::new(name.clone(), instances.clone())
            .with_schema_registry(registry.clone())
            .unwrap();
        let set: EnvironmentSet = EnvironmentSet::new([environment.clone()]).unwrap();
        let source: SettingSource = SettingSource::Flag;
        let setting: Setting<HostName> = Setting::new().set(source, host.clone());
        let effective: EffectiveSetting<HostName> = setting.clone().into_effective().unwrap();
        let assigned = brokers.assign([BrokerId::from(1)]).unwrap();

        vec![
            name.to_string(),
            world.to_string(),
            host.to_string(),
            bootstrap.to_string(),
            address.to_string(),
            ports.to_string(),
            brokers.to_string(),
            listen.advertised_host().to_string(),
            path.to_string(),
            variable.to_string(),
            password.to_string(),
            username.to_string(),
            mechanism.to_string(),
            sasl.password().to_string(),
            format!("{tls:?}"),
            upstream.bootstrap().to_string(),
            recording.to_string(),
            format!("{proxy:?}"),
            broker.world().to_string(),
            instances.to_string(),
            url.to_string(),
            format!("{registry:?}"),
            environment.name().to_string(),
            set.len().to_string(),
            source.to_string(),
            format!("{setting:?}"),
            effective.value().to_string(),
            assigned[&BrokerId::from(1)].to_string(),
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

        let name: EnvironmentName = "shop".parse().unwrap();
        vec![
            message_of(EnvironmentNameError::Empty),
            message_of(WorldNameError::Empty),
            message_of(HostNameError::Empty),
            message_of(ServerAddressError::InvalidHost {
                input: ":9092".to_owned(),
                error: HostNameError::Empty,
            }),
            message_of(ListenAddressError::NotAnAddress {
                input: "localhost".to_owned(),
            }),
            message_of(PortRangeError::Backwards { first: 2, last: 1 }),
            message_of(BrokerListenersError::InvalidPorts {
                input: "0.0.0.0:2-1".to_owned(),
                error: PortRangeError::Backwards { first: 2, last: 1 },
            }),
            message_of(ListenersError::BootstrapAmongBrokers {
                address: "0.0.0.0:9092".parse().unwrap(),
            }),
            message_of(SchemaRegistryUrlError::InvalidHost {
                input: "http://".to_owned(),
                error: HostNameError::Empty,
            }),
            message_of(FilePathError::Empty),
            message_of(VariableNameError::Empty),
            message_of(SecretRefError::Literal),
            message_of(SaslMechanismError::Unknown {
                name: "GSSAPI".to_owned(),
            }),
            message_of(SaslUsernameError::Empty),
            message_of(RecordingModeError::Unknown {
                name: "some".to_owned(),
            }),
            message_of(SchemaRegistryError::NoPlace),
            message_of(EnvironmentError::NoInstance { name: name.clone() }),
            message_of(EnvironmentSetError::UnknownEnvironment { name }),
            message_of(SettingSourceError::Unknown {
                name: "file".to_owned(),
            }),
        ]
    }
}

#[test]
fn the_prelude_names_every_item_next_to_the_other_preludes() {
    let items = next_to_the_others::every_item();

    assert_eq!(items.len(), 28);
    assert!(items.iter().all(|item| !item.is_empty()));
}

#[test]
fn the_prelude_names_the_error_of_every_type() {
    assert_eq!(
        next_to_the_others::every_error(),
        [
            "environment name \"\" is empty",
            "world name \"\" is empty",
            "host name \"\" is empty",
            "server address \":9092\": host name \"\" is empty",
            "listen address \"localhost\" is not an IP address and a port, such as 0.0.0.0:9092",
            "port range 2-1 ends before it starts",
            "broker listeners \"0.0.0.0:2-1\": port range 2-1 ends before it starts",
            "the bootstrap listener and a broker listener both listen on 0.0.0.0:9092",
            "schema registry URL \"http://\": host name \"\" is empty",
            "file path \"\" is empty",
            "the name of an environment variable is empty",
            "a secret is a reference and never the value: write { file: … } or { env: … }",
            "no SASL mechanism is named \"GSSAPI\": there are PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512",
            "SASL user name \"\" is empty",
            "no recording mode is named \"some\": there are full and metadata-only",
            "a schema registry has `listen` for the built-in one, or `url` for the real one",
            "environment \"shop\" has neither a proxy nor a broker",
            "no environment is named \"shop\"",
            "no source of a setting is named \"file\": there are flag, variable, environment, workspace and default",
        ]
    );
}

#[test]
fn the_prelude_is_enough_to_define_an_environment() {
    use gaslightmq_environment::prelude::*;

    let listen = Listeners::new(
        "0.0.0.0:9092".parse().unwrap(),
        "0.0.0.0:19092-19094".parse().unwrap(),
        "localhost".parse().unwrap(),
    )
    .unwrap();
    let shop = Environment::new(
        "shop".parse().unwrap(),
        Instances::ProxyInFrontOfBroker {
            proxy: ProxyDefinition::new(listen),
            broker: BrokerDefinition::new("shop".parse().unwrap()),
        },
    );

    assert_eq!(shop.instances().to_string(), "a proxy in front of a broker");
}
