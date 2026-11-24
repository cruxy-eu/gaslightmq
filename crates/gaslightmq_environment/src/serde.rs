//! The `serde` form of the types that are not text, and that do not derive their form.
//!
//! An environment is one of three shapes, and its form is the one of a file: a `proxy` block and a `broker` block.
//! Reading takes the blocks as they are, and then checks that they make one of the shapes.
//!
//! Broker listeners and secret references look at what is there, so they need a format that describes itself,
//! such as YAML or JSON.

use std::fmt;

use gaslightmq_kernel::BrokerId;
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    BrokerDefinition, BrokerListeners, Environment, EnvironmentError, EnvironmentName, EnvironmentTitle,
    EnvironmentUuid, FilePath, HostName, Instances, ListenAddress, Listeners, ListenersError, ProxyDefinition,
    RecordingMode, SchemaRegistry, SchemaRegistryError, SchemaRegistryUrl, SecretRef, SecretRefError, Upstream,
    VariableName, WorldName,
};

/// The key of a secret reference that names a file.
const FILE: &str = "file";
/// The key of a secret reference that names an environment variable.
const ENV: &str = "env";

/// Refuses everything that the listed methods of a visitor read, with one error that repeats nothing of it.
///
/// What stands in a secret field can be a secret, whatever its type is.
/// The message of `serde` for a value of the wrong type quotes the value, so none of these visitors lets it speak.
macro_rules! refuse {
    ($value:ty, $error:expr; $($visit:ident($type:ty)),* $(,)?) => {
        $(
            fn $visit<E: Error>(self, _: $type) -> Result<$value, E> {
                Err(E::custom($error))
            }
        )*
    };
}

/// Refuses what holds other values, as [`refuse`] does for one value: a list, a map, or a value that is not there.
macro_rules! refuse_the_rest {
    ($value:ty, $error:expr) => {
        fn visit_unit<E: Error>(self) -> Result<$value, E> {
            Err(E::custom($error))
        }

        fn visit_none<E: Error>(self) -> Result<$value, E> {
            Err(E::custom($error))
        }

        fn visit_some<D: Deserializer<'de>>(self, _: D) -> Result<$value, D::Error> {
            Err(D::Error::custom($error))
        }

        fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<$value, A::Error> {
            Err(A::Error::custom($error))
        }
    };
}

/// Writes a map with one entry: `file` and the path, or `env` and the name of the variable.
impl Serialize for SecretRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            SecretRef::File(path) => map.serialize_entry(FILE, path)?,
            SecretRef::Variable(name) => map.serialize_entry(ENV, name)?,
        }
        map.end()
    }
}

/// Reads a map with one entry, and refuses a value.
///
/// Whatever stands there can be a secret: the value instead of a reference, a key, or what a key holds.
/// No error of this form repeats what it read.
impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reference;

        impl<'de> Visitor<'de> for Reference {
            type Value = SecretRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map with the key `file` or the key `env`")
            }

            refuse! {
                SecretRef, SecretRefError::Literal;
                visit_bool(bool), visit_i64(i64), visit_i128(i128), visit_u64(u64), visit_u128(u128), visit_f64(f64),
                visit_str(&str), visit_bytes(&[u8]),
            }

            refuse_the_rest!(SecretRef, SecretRefError::Literal);

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SecretRef, A::Error> {
                let reference = match map.next_key::<Place>()? {
                    None => return Err(A::Error::custom(SecretRefError::NoPlace)),
                    Some(Place::File) => {
                        let PlaceText(path) = map.next_value()?;
                        SecretRef::File(FilePath::new(path).map_err(A::Error::custom)?)
                    }
                    Some(Place::Variable) => {
                        let PlaceText(name) = map.next_value()?;
                        SecretRef::Variable(VariableName::new(name).map_err(A::Error::custom)?)
                    }
                };
                match map.next_key::<Place>()? {
                    Some(_) => Err(A::Error::custom(SecretRefError::TwoPlaces)),
                    None => Ok(reference),
                }
            }
        }

        deserializer.deserialize_any(Reference)
    }
}

/// The key of a secret reference: the kind of place that holds the secret.
enum Place {
    File,
    Variable,
}

/// Reads `file` or `env`. A key that is neither is an error that does not repeat the key.
impl<'de> Deserialize<'de> for Place {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Key;

        impl<'de> Visitor<'de> for Key {
            type Value = Place;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the key `file` or the key `env`")
            }

            fn visit_str<E: Error>(self, key: &str) -> Result<Place, E> {
                match key {
                    FILE => Ok(Place::File),
                    ENV => Ok(Place::Variable),
                    _ => Err(E::custom(SecretRefError::UnknownKey)),
                }
            }

            refuse! {
                Place, SecretRefError::UnknownKey;
                visit_bool(bool), visit_i64(i64), visit_i128(i128), visit_u64(u64), visit_u128(u128), visit_f64(f64),
                visit_bytes(&[u8]),
            }

            refuse_the_rest!(Place, SecretRefError::UnknownKey);

            fn visit_map<A: MapAccess<'de>>(self, _: A) -> Result<Place, A::Error> {
                Err(A::Error::custom(SecretRefError::UnknownKey))
            }
        }

        deserializer.deserialize_any(Key)
    }
}

/// What stands under the key of a secret reference: the path of the file, or the name of the variable, as text.
///
/// The type that the text becomes checks it afterwards, with an error of its own that does not repeat the text.
struct PlaceText(String);

/// Reads text. Anything else is an error that does not repeat what stands there.
impl<'de> Deserialize<'de> for PlaceText {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Text;

        impl<'de> Visitor<'de> for Text {
            type Value = PlaceText;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the path of a file or the name of an environment variable")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<PlaceText, E> {
                Ok(PlaceText(text.to_owned()))
            }

            refuse! {
                PlaceText, SecretRefError::NotText;
                visit_bool(bool), visit_i64(i64), visit_i128(i128), visit_u64(u64), visit_u128(u128), visit_f64(f64),
                visit_bytes(&[u8]),
            }

            refuse_the_rest!(PlaceText, SecretRefError::NotText);

            fn visit_map<A: MapAccess<'de>>(self, _: A) -> Result<PlaceText, A::Error> {
                Err(A::Error::custom(SecretRefError::NotText))
            }
        }

        // Not `deserialize_str`: a format then writes its own message for a number, and quotes the number.
        deserializer.deserialize_any(Text)
    }
}

/// Writes a range as its text, and pinned brokers as a map from the broker ID to the address.
impl Serialize for BrokerListeners {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.as_pinned() {
            Some(pinned) => serializer.collect_map(pinned),
            None => serializer.collect_str(self),
        }
    }
}

/// Reads text as a range, and a map as pinned brokers.
impl<'de> Deserialize<'de> for BrokerListeners {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RangeOrPinned;

        impl<'de> Visitor<'de> for RangeOrPinned {
            type Value = BrokerListeners;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an IP address and a range of ports, or a map from broker IDs to addresses")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<BrokerListeners, E> {
                text.parse().map_err(E::custom)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<BrokerListeners, A::Error> {
                let mut pinned = Vec::new();
                while let Some(entry) = map.next_entry::<BrokerId, ListenAddress>()? {
                    pinned.push(entry);
                }
                BrokerListeners::pinned(pinned).map_err(A::Error::custom)
            }
        }

        deserializer.deserialize_any(RangeOrPinned)
    }
}

/// The `listen` block: where an instance listens, and the host its clients see.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListenersForm {
    bootstrap: ListenAddress,
    brokers: BrokerListeners,
    advertise: HostName,
}

impl TryFrom<ListenersForm> for Listeners {
    type Error = ListenersError;

    fn try_from(form: ListenersForm) -> Result<Self, Self::Error> {
        Listeners::new(form.bootstrap, form.brokers, form.advertise)
    }
}

impl From<Listeners> for ListenersForm {
    fn from(listeners: Listeners) -> Self {
        Self {
            bootstrap: listeners.bootstrap(),
            brokers: listeners.brokers().clone(),
            advertise: listeners.advertised_host().clone(),
        }
    }
}

/// The `schema_registry` block: `listen` for the built-in registry, or `url` for the real one.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchemaRegistryForm {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    listen: Option<ListenAddress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<SchemaRegistryUrl>,
}

impl TryFrom<SchemaRegistryForm> for SchemaRegistry {
    type Error = SchemaRegistryError;

    fn try_from(form: SchemaRegistryForm) -> Result<Self, Self::Error> {
        match (form.listen, form.url) {
            (Some(listen), None) => Ok(SchemaRegistry::BuiltIn { listen }),
            (None, Some(url)) => Ok(SchemaRegistry::Real { url }),
            (None, None) => Err(SchemaRegistryError::NoPlace),
            (Some(_), Some(_)) => Err(SchemaRegistryError::TwoPlaces),
        }
    }
}

impl From<SchemaRegistry> for SchemaRegistryForm {
    fn from(registry: SchemaRegistry) -> Self {
        match registry {
            SchemaRegistry::BuiltIn { listen } => Self {
                listen: Some(listen),
                url: None,
            },
            SchemaRegistry::Real { url } => Self {
                listen: None,
                url: Some(url),
            },
        }
    }
}

/// The form of an environment in a file.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvironmentForm {
    name: EnvironmentName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uuid: Option<EnvironmentUuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<EnvironmentTitle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    broker: Option<BrokerForm>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proxy: Option<ProxyForm>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema_registry: Option<SchemaRegistry>,
}

/// The `proxy` block. Without an `upstream`, the proxy stands in front of the broker of the environment.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyForm {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream: Option<Upstream>,
    listen: Listeners,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recording: Option<RecordingMode>,
}

/// The `broker` block. A broker alone has a `listen` block, and a broker behind a proxy has none.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerForm {
    world: WorldName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    listen: Option<Listeners>,
}

impl ProxyForm {
    fn new(proxy: &ProxyDefinition, upstream: Option<&Upstream>) -> Self {
        Self {
            upstream: upstream.cloned(),
            listen: proxy.listen().clone(),
            recording: proxy.recording(),
        }
    }

    fn definition(self) -> ProxyDefinition {
        let proxy = ProxyDefinition::new(self.listen);
        match self.recording {
            Some(recording) => proxy.with_recording(recording),
            None => proxy,
        }
    }
}

impl BrokerForm {
    fn new(broker: &BrokerDefinition, listen: Option<&Listeners>) -> Self {
        Self {
            world: broker.world().clone(),
            listen: listen.cloned(),
        }
    }
}

impl From<&Environment> for EnvironmentForm {
    fn from(environment: &Environment) -> Self {
        let (proxy, broker) = match environment.instances() {
            Instances::ProxyInFrontOfCluster { proxy, upstream } => (Some(ProxyForm::new(proxy, Some(upstream))), None),
            Instances::ProxyInFrontOfBroker { proxy, broker } => {
                (Some(ProxyForm::new(proxy, None)), Some(BrokerForm::new(broker, None)))
            }
            Instances::BrokerAlone { broker, listen } => (None, Some(BrokerForm::new(broker, Some(listen)))),
        };
        Self {
            name: environment.name().clone(),
            uuid: environment.uuid(),
            title: environment.title().cloned(),
            broker,
            proxy,
            schema_registry: environment.schema_registry().cloned(),
        }
    }
}

impl TryFrom<EnvironmentForm> for Environment {
    type Error = EnvironmentError;

    fn try_from(form: EnvironmentForm) -> Result<Self, Self::Error> {
        let name = form.name;
        let instances = match (form.proxy, form.broker) {
            (None, None) => return Err(EnvironmentError::NoInstance { name }),
            (Some(mut proxy), None) => match proxy.upstream.take() {
                Some(upstream) => Instances::ProxyInFrontOfCluster {
                    proxy: proxy.definition(),
                    upstream,
                },
                None => return Err(EnvironmentError::ProxyWithoutBackend { name }),
            },
            (Some(proxy), Some(broker)) => {
                if proxy.upstream.is_some() {
                    return Err(EnvironmentError::TwoBackends { name });
                }
                if broker.listen.is_some() {
                    return Err(EnvironmentError::BrokerListensBehindProxy { name });
                }
                Instances::ProxyInFrontOfBroker {
                    proxy: proxy.definition(),
                    broker: BrokerDefinition::new(broker.world),
                }
            }
            (None, Some(broker)) => match broker.listen {
                Some(listen) => Instances::BrokerAlone {
                    broker: BrokerDefinition::new(broker.world),
                    listen,
                },
                None => return Err(EnvironmentError::BrokerWithoutListeners { name }),
            },
        };
        let environment = Environment::new(name, instances);
        let environment = match form.uuid {
            Some(uuid) => environment.with_uuid(uuid),
            None => environment,
        };
        let environment = match form.title {
            Some(title) => environment.with_title(title),
            None => environment,
        };
        match form.schema_registry {
            Some(registry) => environment.with_schema_registry(registry),
            None => Ok(environment),
        }
    }
}

/// Writes a map with `name`, with `uuid` and `title` when the environment has them,
/// and with the `broker`, `proxy` and `schema_registry` blocks that the environment has.
impl Serialize for Environment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        EnvironmentForm::from(self).serialize(serializer)
    }
}

/// Reads the blocks, and checks that they make one of the three shapes of an environment.
impl<'de> Deserialize<'de> for Environment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        EnvironmentForm::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}
