//! The `serde` form of every definition type, and the rules that reading applies.
//!
//! The forms are those of the YAML in `docs/specs/architecture/storage.md` and `backends.md`, written here as JSON.

#![cfg(feature = "serde")]
#![expect(
    clippy::unwrap_used,
    reason = "a panic is how a test fails, also in a helper outside a `#[test]` function"
)]

use std::fmt::Debug;

use gaslightmq_environment::prelude::*;
use gaslightmq_kernel::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// `json` is what the value writes, and what reads back as the same value.
fn writes<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T, json: &str) {
    assert_eq!(serde_json::to_string(value).unwrap(), json);
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value);
}

/// `json` reads as the value, although the value writes something else.
fn reads<T: DeserializeOwned + PartialEq + Debug>(json: &str, value: &T) {
    assert_eq!(serde_json::from_str::<T>(json).unwrap(), *value, "{json}");
}

/// Reading `json` fails with a message that holds `reason`. Returns the whole message.
fn rejects<T: DeserializeOwned + Debug>(json: &str, reason: &str) -> String {
    let error = serde_json::from_str::<T>(json).expect_err(json).to_string();
    assert!(error.contains(reason), "{json}: {error:?} does not hold {reason:?}");
    error
}

/// A UUID, which an environment does not hold.
const PLAIN_KAFKA_UUID: &str = "0193b6c2-5f3e-7a41-9c2d-8e4b1f6a7d10";

fn name(text: &str) -> EnvironmentName {
    text.parse().unwrap()
}

fn listen(bootstrap: &str, brokers: &str, advertise: &str) -> Listeners {
    Listeners::new(
        bootstrap.parse().unwrap(),
        brokers.parse().unwrap(),
        advertise.parse().unwrap(),
    )
    .unwrap()
}

fn broker(world: &str) -> BrokerDefinition {
    BrokerDefinition::new(world.parse().unwrap())
}

fn file(path: &str) -> SecretRef {
    SecretRef::File(path.parse().unwrap())
}

// The environments of the spec

#[test]
fn a_proxy_in_front_of_a_real_cluster_writes_its_upstream_and_its_listeners() {
    // name: staging-proxy
    // proxy:
    //   upstream:
    //     bootstrap: kafka-1.staging:9092
    //     tls:
    //       ca: /run/secrets/kafka-ca.pem
    //     sasl:
    //       mechanism: SCRAM-SHA-512
    //       username: gaslight
    //       password: { file: /run/secrets/kafka-password }
    //   listen:
    //     bootstrap: 0.0.0.0:9092
    //     brokers: 0.0.0.0:19092-19191
    //     advertise: gaslight.staging
    //   recording: metadata-only
    let upstream = Upstream::new("kafka-1.staging:9092".parse().unwrap())
        .with_tls(Tls::new().with_ca("/run/secrets/kafka-ca.pem".parse().unwrap()))
        .with_sasl(Sasl::new(
            SaslMechanism::ScramSha512,
            "gaslight".parse().unwrap(),
            file("/run/secrets/kafka-password"),
        ));
    let proxy = ProxyDefinition::new(listen("0.0.0.0:9092", "0.0.0.0:19092-19191", "gaslight.staging"))
        .with_recording(RecordingMode::MetadataOnly);
    let staging = Environment::new(
        name("staging-proxy"),
        Instances::ProxyInFrontOfCluster { proxy, upstream },
    );

    writes(
        &staging,
        concat!(
            r#"{"name":"staging-proxy","proxy":{"upstream":{"bootstrap":"kafka-1.staging:9092","#,
            r#""tls":{"ca":"/run/secrets/kafka-ca.pem"},"#,
            r#""sasl":{"mechanism":"SCRAM-SHA-512","username":"gaslight","#,
            r#""password":{"file":"/run/secrets/kafka-password"}}},"#,
            r#""listen":{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191","advertise":"gaslight.staging"},"#,
            r#""recording":"metadata-only"}}"#,
        ),
    );
}

#[test]
fn a_proxy_in_front_of_a_broker_writes_the_broker_and_the_proxy() {
    // name: orders-sim
    // broker:
    //   world: orders-sim
    // proxy:
    //   listen:
    //     bootstrap: 0.0.0.0:29092
    //     brokers: 0.0.0.0:29093-29192
    //     advertise: localhost
    let fake = Environment::new(
        name("orders-sim"),
        Instances::ProxyInFrontOfBroker {
            proxy: ProxyDefinition::new(listen("0.0.0.0:29092", "0.0.0.0:29093-29192", "localhost")),
            broker: broker("orders-sim"),
        },
    );

    writes(
        &fake,
        concat!(
            r#"{"name":"orders-sim","broker":{"world":"orders-sim"},"#,
            r#""proxy":{"listen":{"bootstrap":"0.0.0.0:29092","brokers":"0.0.0.0:29093-29192","advertise":"localhost"}}}"#,
        ),
    );
    // The blocks stand in any order.
    reads(
        concat!(
            r#"{"proxy":{"listen":{"advertise":"localhost","brokers":"0.0.0.0:29093-29192","bootstrap":"0.0.0.0:29092"}},"#,
            r#""broker":{"world":"orders-sim"},"name":"orders-sim"}"#,
        ),
        &fake,
    );
}

#[test]
fn a_broker_alone_writes_its_own_listeners() {
    // name: plain-kafka
    // broker:
    //   world: orders-sim
    //   listen:
    //     bootstrap: 0.0.0.0:39092
    //     brokers: 0.0.0.0:39093-39192
    //     advertise: localhost
    let plain = Environment::new(
        name("plain-kafka"),
        Instances::BrokerAlone {
            broker: broker("orders-sim"),
            listen: listen("0.0.0.0:39092", "0.0.0.0:39093-39192", "localhost"),
        },
    );

    writes(
        &plain,
        concat!(
            r#"{"name":"plain-kafka","broker":{"world":"orders-sim","#,
            r#""listen":{"bootstrap":"0.0.0.0:39092","brokers":"0.0.0.0:39093-39192","advertise":"localhost"}}}"#,
        ),
    );
}

#[test]
fn an_environment_writes_its_title_when_it_has_one() {
    // name: plain-kafka
    // title: A broker that nothing lies about
    // broker:
    //   world: orders-sim
    //   listen:
    //     bootstrap: 0.0.0.0:39092
    //     brokers: 0.0.0.0:39093-39192
    //     advertise: localhost
    let plain = Environment::new(
        name("plain-kafka"),
        Instances::BrokerAlone {
            broker: broker("orders-sim"),
            listen: listen("0.0.0.0:39092", "0.0.0.0:39093-39192", "localhost"),
        },
    )
    .with_title("A broker that nothing lies about".parse().unwrap());

    writes(
        &plain,
        concat!(
            r#"{"name":"plain-kafka","title":"A broker that nothing lies about","broker":{"world":"orders-sim","#,
            r#""listen":{"bootstrap":"0.0.0.0:39092","brokers":"0.0.0.0:39093-39192","advertise":"localhost"}}}"#,
        ),
    );
    writes(
        &"A broker that nothing lies about".parse::<EnvironmentTitle>().unwrap(),
        r#""A broker that nothing lies about""#,
    );
    rejects::<EnvironmentTitle>(r#""""#, "environment title \"\" is empty");
}

#[test]
fn an_environment_holds_no_uuid() {
    let blocks = concat!(
        r#""broker":{"world":"orders-sim","#,
        r#""listen":{"bootstrap":"0.0.0.0:39092","brokers":"0.0.0.0:39093-39192","advertise":"localhost"}}}"#,
    );

    // No file holds the UUID of an environment: `uuid` is a key that an environment does not have.
    rejects::<Environment>(
        &format!(r#"{{"name":"plain-kafka","uuid":"{PLAIN_KAFKA_UUID}",{blocks}"#),
        "unknown field `uuid`",
    );
}

#[test]
fn an_environment_writes_its_schema_registry() {
    // schema_registry:
    //   listen: 0.0.0.0:8081
    let shop = Environment::new(
        name("shop"),
        Instances::BrokerAlone {
            broker: broker("shop"),
            listen: listen("0.0.0.0:9092", "0.0.0.0:19092", "localhost"),
        },
    )
    .with_schema_registry(SchemaRegistry::BuiltIn {
        listen: "0.0.0.0:8081".parse().unwrap(),
    })
    .unwrap();
    writes(
        &shop,
        concat!(
            r#"{"name":"shop","broker":{"world":"shop","#,
            r#""listen":{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092","advertise":"localhost"}},"#,
            r#""schema_registry":{"listen":"0.0.0.0:8081"}}"#,
        ),
    );

    // schema_registry:
    //   url: https://schema-registry.staging.internal:8081
    let staging = Environment::new(
        name("staging"),
        Instances::ProxyInFrontOfCluster {
            proxy: ProxyDefinition::new(listen("0.0.0.0:9092", "0.0.0.0:19092-19191", "localhost")),
            upstream: Upstream::new("kafka-1.staging.internal:9092".parse().unwrap()),
        },
    )
    .with_schema_registry(SchemaRegistry::Real {
        url: "https://schema-registry.staging.internal:8081".parse().unwrap(),
    })
    .unwrap();
    writes(
        &staging,
        concat!(
            r#"{"name":"staging","proxy":{"upstream":{"bootstrap":"kafka-1.staging.internal:9092"},"#,
            r#""listen":{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191","advertise":"localhost"}},"#,
            r#""schema_registry":{"url":"https://schema-registry.staging.internal:8081"}}"#,
        ),
    );
}

// The three shapes

const LISTEN: &str = r#"{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191","advertise":"localhost"}"#;

#[test]
fn blocks_that_make_none_of_the_three_shapes_are_an_error() {
    let rejects = |json: String, reason: &str| rejects::<Environment>(&json, reason);

    rejects(
        r#"{"name":"shop"}"#.to_owned(),
        "environment \"shop\" has neither a proxy nor a broker",
    );
    rejects(
        format!(r#"{{"name":"shop","proxy":{{"listen":{LISTEN}}}}}"#),
        "environment \"shop\" has a proxy without an upstream, and no broker for it to stand in front of",
    );
    rejects(
        format!(
            r#"{{"name":"shop","proxy":{{"upstream":{{"bootstrap":"kafka:9092"}},"listen":{LISTEN}}},"broker":{{"world":"shop"}}}}"#
        ),
        "environment \"shop\" has a proxy with an upstream, and a broker too",
    );
    rejects(
        r#"{"name":"shop","broker":{"world":"shop"}}"#.to_owned(),
        "environment \"shop\" has a broker alone, and a broker alone needs listeners",
    );
    rejects(
        format!(r#"{{"name":"shop","proxy":{{"listen":{LISTEN}}},"broker":{{"world":"shop","listen":{LISTEN}}}}}"#),
        "environment \"shop\" has a broker behind a proxy, so only the proxy has listeners",
    );
}

#[test]
fn a_schema_registry_that_does_not_fit_the_shape_is_an_error() {
    let rejects = |json: String, reason: &str| rejects::<Environment>(&json, reason);

    rejects(
        format!(
            r#"{{"name":"staging","proxy":{{"upstream":{{"bootstrap":"kafka:9092"}},"listen":{LISTEN}}},"schema_registry":{{"listen":"0.0.0.0:8081"}}}}"#
        ),
        "environment \"staging\" has no broker, so its schema registry is the real one, with a URL",
    );
    rejects(
        format!(
            r#"{{"name":"shop","broker":{{"world":"shop","listen":{LISTEN}}},"schema_registry":{{"url":"http://registry:8081"}}}}"#
        ),
        "environment \"shop\" has a broker, so its schema registry is the built-in one",
    );
    rejects(
        format!(
            r#"{{"name":"shop","broker":{{"world":"shop","listen":{LISTEN}}},"schema_registry":{{"listen":"127.0.0.1:9092"}}}}"#
        ),
        "environment \"shop\" has its schema registry and a Kafka listener both on 127.0.0.1:9092",
    );
}

#[test]
fn a_key_that_no_block_has_is_an_error() {
    let rejects = |json: String, reason: &str| rejects::<Environment>(&json, reason);

    rejects(
        format!(r#"{{"name":"shop","brokr":{{"world":"shop","listen":{LISTEN}}}}}"#),
        "unknown field `brokr`",
    );
    rejects(
        format!(r#"{{"name":"shop","broker":{{"wrld":"shop","listen":{LISTEN}}}}}"#),
        "unknown field `wrld`",
    );
    rejects(
        format!(r#"{{"name":"shop","proxy":{{"listen":{LISTEN},"recordings":"full"}},"broker":{{"world":"shop"}}}}"#),
        "unknown field `recordings`",
    );
    rejects(
        format!(r#"{{"broker":{{"world":"shop","listen":{LISTEN}}}}}"#),
        "missing field `name`",
    );
    rejects(
        r#"{"name":"shop","broker":{"listen":null}}"#.to_owned(),
        "missing field `world`",
    );
}

#[test]
fn a_value_that_breaks_the_rule_of_its_type_is_an_error() {
    let rejects = |json: String, reason: &str| rejects::<Environment>(&json, reason);

    rejects(
        format!(r#"{{"name":"shop-","broker":{{"world":"shop","listen":{LISTEN}}}}}"#),
        "environment name \"shop-\" starts or ends with a hyphen",
    );
    rejects(
        format!(r#"{{"name":"shop","broker":{{"world":"","listen":{LISTEN}}}}}"#),
        "world name \"\" is empty",
    );
    rejects(
        format!(
            r#"{{"name":"shop","proxy":{{"listen":{LISTEN},"recording":"metadata"}},"broker":{{"world":"shop"}}}}"#
        ),
        "no recording mode is named \"metadata\"",
    );
    rejects(
        format!(r#"{{"name":"shop","proxy":{{"upstream":{{"bootstrap":"kafka"}},"listen":{LISTEN}}}}}"#),
        "server address \"kafka\" has no port",
    );
}

// Listeners

#[test]
fn listeners_write_their_addresses_and_the_advertised_host() {
    writes(&listen("0.0.0.0:9092", "0.0.0.0:19092-19191", "localhost"), LISTEN);
    writes(
        &listen("[::]:9092", "[::]:19092", "2001:db8::7"),
        r#"{"bootstrap":"[::]:9092","brokers":"[::]:19092","advertise":"2001:db8::7"}"#,
    );
}

#[test]
fn listeners_that_do_not_hold_together_are_an_error() {
    rejects::<Listeners>(
        r#"{"bootstrap":"0.0.0.0:19092","brokers":"0.0.0.0:19092-19191","advertise":"localhost"}"#,
        "the bootstrap listener and a broker listener both listen on 0.0.0.0:19092",
    );
    rejects::<Listeners>(
        r#"{"bootstrap":"localhost:9092","brokers":"0.0.0.0:19092-19191","advertise":"localhost"}"#,
        "listen address \"localhost:9092\" is not an IP address and a port",
    );
    rejects::<Listeners>(
        r#"{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191","advertise":"local host"}"#,
        "host name \"local host\" holds the character ' '",
    );
    rejects::<Listeners>(
        r#"{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191"}"#,
        "missing field `advertise`",
    );
    rejects::<Listeners>(
        r#"{"bootstrap":"0.0.0.0:9092","brokers":"0.0.0.0:19092-19191","advertise":"localhost","advertize":"x"}"#,
        "unknown field `advertize`",
    );
}

#[test]
fn broker_listeners_write_a_range_as_text() {
    // brokers: 0.0.0.0:19092-19191
    writes(
        &"0.0.0.0:19092-19191".parse::<BrokerListeners>().unwrap(),
        r#""0.0.0.0:19092-19191""#,
    );
    writes(
        &"0.0.0.0:19092".parse::<BrokerListeners>().unwrap(),
        r#""0.0.0.0:19092""#,
    );
    rejects::<BrokerListeners>(
        r#""0.0.0.0:19191-19092""#,
        "broker listeners \"0.0.0.0:19191-19092\": port range 19191-19092 ends before it starts",
    );
    rejects::<BrokerListeners>(
        r#""19092-19191""#,
        "broker listeners \"19092-19191\" are not an IP address and a range of ports",
    );
    rejects::<BrokerListeners>("19092", "an IP address and a range of ports, or a map");
}

#[test]
fn broker_listeners_write_pinned_brokers_as_a_map() {
    // brokers:
    //   1: 0.0.0.0:19092
    //   2: 0.0.0.0:19093
    //   3: 0.0.0.0:19094
    let address = |text: &str| text.parse::<ListenAddress>().unwrap();
    let pinned = BrokerListeners::pinned([
        (BrokerId::from(3), address("0.0.0.0:19094")),
        (BrokerId::from(1), address("0.0.0.0:19092")),
        (BrokerId::from(2), address("0.0.0.0:19093")),
    ])
    .unwrap();

    writes(
        &pinned,
        r#"{"1":"0.0.0.0:19092","2":"0.0.0.0:19093","3":"0.0.0.0:19094"}"#,
    );
    rejects::<BrokerListeners>("{}", "broker listeners pin no broker to an address");
    rejects::<BrokerListeners>(
        r#"{"1":"0.0.0.0:19092","1":"0.0.0.0:19093"}"#,
        "broker 1 is pinned to two addresses",
    );
    rejects::<BrokerListeners>(
        r#"{"1":"0.0.0.0:19092","2":"0.0.0.0:19092"}"#,
        "brokers 1 and 2 are both pinned to 0.0.0.0:19092",
    );
    rejects::<BrokerListeners>(
        r#"{"-1":"0.0.0.0:19092"}"#,
        r#"invalid value: string "-1", expected a broker ID"#,
    );
    rejects::<BrokerListeners>(
        r#"{"one":"0.0.0.0:19092"}"#,
        r#"invalid value: string "one", expected a broker ID"#,
    );
    // A value that holds the map is read in two steps inside a message of the control plane,
    // and every key is text by then.
    let value = serde_json::to_value(&pinned).unwrap();
    assert_eq!(serde_json::from_value::<BrokerListeners>(value).unwrap(), pinned);
}

// The upstream cluster

#[test]
fn an_upstream_writes_only_what_it_has() {
    let bootstrap = || "kafka-1.staging:9092".parse::<ServerAddress>().unwrap();

    writes(&Upstream::new(bootstrap()), r#"{"bootstrap":"kafka-1.staging:9092"}"#);
    // TLS with the authorities that the machine trusts.
    writes(
        &Upstream::new(bootstrap()).with_tls(Tls::new()),
        r#"{"bootstrap":"kafka-1.staging:9092","tls":{}}"#,
    );
    writes(
        &Tls::new().with_ca("secrets/kafka-ca.pem".parse().unwrap()),
        r#"{"ca":"secrets/kafka-ca.pem"}"#,
    );
    rejects::<Upstream>(
        r#"{"bootstrap":"kafka-1.staging:9092","ssl":{}}"#,
        "unknown field `ssl`",
    );
    rejects::<Tls>(r#"{"ca":""}"#, "file path \"\" is empty");
    rejects::<Tls>(r#"{"key":"secrets/key.pem"}"#, "unknown field `key`");
}

#[test]
fn a_login_writes_its_mechanism_its_user_and_where_the_password_lives() {
    let sasl = Sasl::new(
        SaslMechanism::Plain,
        "gaslight".parse().unwrap(),
        SecretRef::Variable("KAFKA_PASSWORD".parse().unwrap()),
    );

    writes(
        &sasl,
        r#"{"mechanism":"PLAIN","username":"gaslight","password":{"env":"KAFKA_PASSWORD"}}"#,
    );
    rejects::<Sasl>(
        r#"{"mechanism":"GSSAPI","username":"gaslight","password":{"env":"KAFKA_PASSWORD"}}"#,
        "no SASL mechanism is named \"GSSAPI\"",
    );
    rejects::<Sasl>(
        r#"{"mechanism":"PLAIN","username":"","password":{"env":"KAFKA_PASSWORD"}}"#,
        "SASL user name \"\" is empty",
    );
    rejects::<Sasl>(
        r#"{"mechanism":"PLAIN","username":"gaslight"}"#,
        "missing field `password`",
    );
}

// Secrets

#[test]
fn a_secret_reference_writes_the_place_it_names() {
    // password: { file: /run/secrets/kafka-password }
    writes(
        &file("/run/secrets/kafka-password"),
        r#"{"file":"/run/secrets/kafka-password"}"#,
    );
    // password: { env: KAFKA_PASSWORD }
    writes(
        &SecretRef::Variable("KAFKA_PASSWORD".parse().unwrap()),
        r#"{"env":"KAFKA_PASSWORD"}"#,
    );
}

#[test]
fn a_secret_field_takes_a_reference_and_never_a_value() {
    let literal = "a secret is a reference and never the value";

    for json in [
        r#""hunter2""#,
        "1234567",
        "-1234567",
        "1234567.5",
        "true",
        "123456789012345678901234567890",
    ] {
        let error = rejects::<SecretRef>(json, literal);
        // The message names the place of the value in the text, and never the value.
        assert!(!error.contains("hunter2") && !error.contains("1234567"), "{error}");
    }
}

#[test]
fn the_error_of_a_login_does_not_repeat_a_password() {
    for password in [
        r#""hunter2""#,
        r#"{"env":"KAFKA_PASSWORD=hunter2"}"#,
        r#"["hunter2"]"#,
        "[1234567]",
        "null",
    ] {
        let json = format!(r#"{{"mechanism":"PLAIN","username":"gaslight","password":{password}}}"#);
        let error = rejects::<Sasl>(&json, "");
        assert!(!error.contains("hunter2") && !error.contains("1234567"), "{error}");
        assert!(error.contains("a secret is a reference and never the value") || password.starts_with('{'));
    }
}

#[test]
fn no_error_of_a_secret_reference_repeats_what_it_read() {
    // Each of these holds `hunter2` or `1234567` where a reference has a key, a path or the name of a variable.
    let not_text = "a secret reference names its file or its environment variable as text";
    let unknown_key = "a secret reference has the key `file` or the key `env`";
    let cases = [
        (r#"{"env":12345678}"#, not_text),
        (r#"{"file":12345678}"#, not_text),
        (r#"{"env":-1234567}"#, not_text),
        (r#"{"file":1234567.5}"#, not_text),
        (r#"{"env":123456789012345678901234567890}"#, not_text),
        (r#"{"env":["hunter2"]}"#, not_text),
        (r#"{"file":{"hunter2":1234567}}"#, not_text),
        (r#"{"file":true}"#, not_text),
        (r#"{"env":null}"#, not_text),
        (
            r#"{"file":"hunter2\n"}"#,
            "a file path holds the control character '\\n'",
        ),
        (
            r#"{"env":"hunter2 1234567"}"#,
            "the name of an environment variable holds the character ' '",
        ),
        (r#"{"hunter2":"1234567"}"#, unknown_key),
        (r#"{"hunter2":null}"#, unknown_key),
        (r#"{"file":"secrets/kafka-password","hunter2":"1234567"}"#, unknown_key),
    ];

    for (json, reason) in cases {
        let error = rejects::<SecretRef>(json, reason);
        assert!(
            !error.contains("hunter2") && !error.contains("1234567"),
            "{json}: {error}"
        );

        // The same reference as the password of a login.
        let login = format!(r#"{{"mechanism":"PLAIN","username":"gaslight","password":{json}}}"#);
        let error = rejects::<Sasl>(&login, reason);
        assert!(
            !error.contains("hunter2") && !error.contains("1234567"),
            "{login}: {error}"
        );
    }
}

#[test]
fn a_secret_reference_names_one_place() {
    rejects::<SecretRef>("{}", "a secret reference names a file or an environment variable");
    rejects::<SecretRef>(
        r#"{"file":"secrets/kafka-password","env":"KAFKA_PASSWORD"}"#,
        "a secret reference names one place",
    );
    rejects::<SecretRef>(
        r#"{"file":"secrets/a","file":"secrets/b"}"#,
        "a secret reference names one place",
    );
    rejects::<SecretRef>(
        r#"{"path":"secrets/kafka-password"}"#,
        "a secret reference has the key `file` or the key `env`",
    );
    rejects::<SecretRef>(r#"{"file":""}"#, "file path \"\" is empty");
    rejects::<SecretRef>(r#"{"env":""}"#, "the name of an environment variable is empty");
}

// The schema registry

#[test]
fn a_schema_registry_writes_where_it_listens_or_its_url() {
    writes(
        &SchemaRegistry::BuiltIn {
            listen: "0.0.0.0:8081".parse().unwrap(),
        },
        r#"{"listen":"0.0.0.0:8081"}"#,
    );
    writes(
        &SchemaRegistry::Real {
            url: "https://schema-registry.staging.internal:8081".parse().unwrap(),
        },
        r#"{"url":"https://schema-registry.staging.internal:8081"}"#,
    );
    rejects::<SchemaRegistry>("{}", "a schema registry has `listen` for the built-in one");
    rejects::<SchemaRegistry>(
        r#"{"listen":"0.0.0.0:8081","url":"http://registry:8081"}"#,
        "it has `listen` or `url`, and not both",
    );
    rejects::<SchemaRegistry>(r#"{"uri":"http://registry:8081"}"#, "unknown field `uri`");
    let error = rejects::<SchemaRegistry>(
        r#"{"url":"https://admin:hunter2@registry:8081"}"#,
        "a schema registry URL holds no user name and no password",
    );
    assert!(!error.contains("hunter2"), "{error}");
}

// Text

#[test]
fn the_types_with_a_text_form_write_their_text() {
    writes(&name("staging-proxy"), r#""staging-proxy""#);
    writes(&"orders-sim".parse::<WorldName>().unwrap(), r#""orders-sim""#);
    writes(
        &"gaslight.staging".parse::<HostName>().unwrap(),
        r#""gaslight.staging""#,
    );
    writes(
        &"kafka-1.staging:9092".parse::<ServerAddress>().unwrap(),
        r#""kafka-1.staging:9092""#,
    );
    writes(&"0.0.0.0:9092".parse::<ListenAddress>().unwrap(), r#""0.0.0.0:9092""#);
    writes(&"19092-19191".parse::<PortRange>().unwrap(), r#""19092-19191""#);
    writes(
        &"http://localhost:8081/".parse::<SchemaRegistryUrl>().unwrap(),
        r#""http://localhost:8081/""#,
    );
    writes(
        &"secrets/kafka-ca.pem".parse::<FilePath>().unwrap(),
        r#""secrets/kafka-ca.pem""#,
    );
    writes(
        &"KAFKA_PASSWORD".parse::<VariableName>().unwrap(),
        r#""KAFKA_PASSWORD""#,
    );
    writes(&"gaslight".parse::<SaslUsername>().unwrap(), r#""gaslight""#);
    writes(&SaslMechanism::ScramSha256, r#""SCRAM-SHA-256""#);
    writes(&RecordingMode::Full, r#""full""#);
    writes(&RecordingMode::MetadataOnly, r#""metadata-only""#);
    writes(&SettingSource::Flag, r#""flag""#);
}

#[test]
fn a_text_that_breaks_the_rule_of_its_type_is_an_error() {
    rejects::<EnvironmentName>(r#""""#, "environment name \"\" is empty");
    rejects::<HostName>(r#""kafka:9092""#, "host name \"kafka:9092\" holds the character ':'");
    rejects::<ServerAddress>(r#""kafka:0""#, "has a port that is no number between 1 and 65535");
    rejects::<ListenAddress>(r#""0.0.0.0""#, "is not an IP address and a port");
    rejects::<PortRange>(r#""0""#, "port range \"0\" has a port that is no number");
    rejects::<SchemaRegistryUrl>(
        r#""registry:8081""#,
        "schema registry URL \"registry:8081\" does not start with http:// or https://",
    );
    rejects::<RecordingMode>(r#""Full""#, "no recording mode is named \"Full\"");
    rejects::<SettingSource>(r#""file""#, "no source of a setting is named \"file\"");
    rejects::<PortRange>("19092", "invalid type: integer");
}

// Settings

#[test]
fn an_effective_setting_writes_its_value_and_its_source() {
    let advertise = Setting::new()
        .set(
            SettingSource::Environment,
            "gaslight.staging".parse::<HostName>().unwrap(),
        )
        .set(SettingSource::Flag, "localhost".parse::<HostName>().unwrap())
        .into_effective()
        .unwrap();

    writes(&advertise, r#"{"value":"localhost","source":"flag"}"#);
    writes(
        &Setting::with_default(RecordingMode::MetadataOnly)
            .into_effective()
            .unwrap(),
        r#"{"value":"metadata-only","source":"default"}"#,
    );
}
