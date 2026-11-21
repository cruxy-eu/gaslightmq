use std::fmt;
use std::str::FromStr;

use crate::KernelError;
use crate::code::{self, code_serde};
use crate::text::text_conversions;

/// The key of a Kafka API: the number that says which request a client sends.
///
/// Kafka names its APIs, such as `Produce` for key 0 and `Fetch` for key 1.
/// Every name is a constant of this type, such as [`ApiKey::PRODUCE`].
///
/// An API key holds every number a request can carry.
/// A request can carry a key that Kafka gives no name, from a newer client or a broken one.
/// Such a key has no [`name`](Self::name).
///
/// The text form is the name, such as `Produce`.
/// A key without a name prints as its number, such as `92`.
/// Reading takes a name as Kafka spells it, and the number of any key.
///
/// ```
/// use gaslightmq_kernel::prelude::*;
///
/// let produce: ApiKey = "Produce".parse()?;
/// assert_eq!(produce, ApiKey::PRODUCE);
/// assert_eq!(produce, ApiKey::from(0));
/// assert_eq!(produce.to_string(), "Produce");
///
/// let unknown = ApiKey::from(4711);
/// assert_eq!(unknown.name(), None);
/// assert_eq!(unknown.to_string(), "4711");
/// assert_eq!("4711".parse::<ApiKey>()?, unknown);
///
/// assert!("Prodcue".parse::<ApiKey>().is_err());
/// # Ok::<(), KernelError>(())
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiKey(i16);

const WHAT: &str = "API key";

/// Define a constant for every API that Kafka names, and the table of those names.
///
/// `names.rs` calls it with one line per API: the key, the name of the constant, and the name of the API.
macro_rules! api_keys {
    ($($key:literal $constant:ident $name:literal)*) => {
        impl $crate::ApiKey {
            $(
                #[doc = concat!("`", $name, "`, the API with key ", stringify!($key), ".")]
                pub const $constant: Self = Self::new($key);
            )*

            /// Every key that Kafka names, with its name, in the order of the keys.
            pub(crate) const NAMES: $crate::code::Names = &[$(($key, $name)),*];
        }
    };
}

pub(crate) use api_keys;

impl ApiKey {
    /// Keep `key` as an API key.
    /// Every number is one, with or without a name.
    pub const fn new(key: i16) -> Self {
        Self(key)
    }

    /// The API key as a plain number, as Kafka writes it.
    pub const fn value(self) -> i16 {
        self.0
    }

    /// The name Kafka gives the API, or `None` for a key without a name.
    pub fn name(self) -> Option<&'static str> {
        code::name(Self::NAMES, self.0)
    }

    /// Whether Kafka gives the key a name.
    pub fn is_known(self) -> bool {
        self.name().is_some()
    }

    /// Every key that Kafka names, in the order of the keys.
    pub fn known() -> impl Iterator<Item = Self> {
        Self::NAMES.iter().map(|(key, _)| Self(*key))
    }
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        Self::new(key)
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        key.0
    }
}

/// Reads the name of an API as Kafka spells it, or the number of any key.
impl FromStr for ApiKey {
    type Err = KernelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        code::parse(WHAT, Self::NAMES, text).map(Self)
    }
}

/// Writes the name of the API, or the number of a key without a name.
impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => fmt::Display::fmt(name, f),
            None => fmt::Display::fmt(&self.0, f),
        }
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiKey({self})")
    }
}

text_conversions!(ApiKey);
code_serde!(ApiKey, "the name or the number of a Kafka API");

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn every_number_is_an_api_key() {
        for key in [i16::MIN, -1, 0, 1, 92, 4711, i16::MAX] {
            assert_eq!(ApiKey::new(key).value(), key);
            assert_eq!(ApiKey::from(key).value(), key);
            assert_eq!(i16::from(ApiKey::new(key)), key);
        }
    }

    #[test]
    fn an_api_has_the_name_kafka_gives_it() {
        assert_eq!(ApiKey::PRODUCE.value(), 0);
        assert_eq!(ApiKey::PRODUCE.name(), Some("Produce"));
        assert_eq!(ApiKey::from(1).name(), Some("Fetch"));
        assert_eq!(ApiKey::LIST_OFFSETS.name(), Some("ListOffsets"));
        assert_eq!(ApiKey::FIND_COORDINATOR.value(), 10);
        assert_eq!(ApiKey::API_VERSIONS.name(), Some("ApiVersions"));
        assert!(ApiKey::PRODUCE.is_known());
    }

    #[test]
    fn a_key_can_have_no_name() {
        for key in [i16::MIN, -1, 4, 4711, i16::MAX] {
            assert_eq!(ApiKey::from(key).name(), None, "{key}");
            assert!(!ApiKey::from(key).is_known(), "{key}");
        }
    }

    #[test]
    fn an_api_key_prints_its_name_or_its_number() {
        assert_eq!(ApiKey::PRODUCE.to_string(), "Produce");
        assert_eq!(ApiKey::from(4711).to_string(), "4711");
        assert_eq!(ApiKey::from(-1).to_string(), "-1");
        assert_eq!(format!("{:>9}|{:>6}|", ApiKey::PRODUCE, ApiKey::from(4711)), "  Produce|  4711|");
        assert_eq!(format!("{:?} {:?}", ApiKey::PRODUCE, ApiKey::from(4711)), "ApiKey(Produce) ApiKey(4711)");
    }

    #[test]
    fn an_api_key_reads_a_name_or_a_number() {
        assert_eq!("Produce".parse::<ApiKey>(), Ok(ApiKey::PRODUCE));
        assert_eq!("0".parse::<ApiKey>(), Ok(ApiKey::PRODUCE));
        assert_eq!("4711".parse::<ApiKey>(), Ok(ApiKey::from(4711)));
        assert_eq!("-1".parse::<ApiKey>(), Ok(ApiKey::from(-1)));
        assert_eq!(ApiKey::try_from("JoinGroup"), Ok(ApiKey::JOIN_GROUP));
        assert_eq!(ApiKey::try_from(String::from("JoinGroup")), Ok(ApiKey::JOIN_GROUP));
        assert_eq!(String::from(ApiKey::JOIN_GROUP), "JoinGroup");
    }

    #[test]
    fn an_api_key_does_not_read_a_name_kafka_does_not_have() {
        for text in ["Prodcue", "produce", "PRODUCE", "Produce ", "ProduceRequest"] {
            assert_eq!(
                text.parse::<ApiKey>(),
                Err(KernelError::UnknownName { what: "API key", input: text.to_owned() }),
                "{text:?}"
            );
        }
        assert_eq!("".parse::<ApiKey>(), Err(KernelError::Empty { what: "API key" }));
        assert_eq!(
            "32768".parse::<ApiKey>(),
            Err(KernelError::TooLarge { what: "API key", value: 32_768, max: 32_767 })
        );
        assert_eq!(
            "-32769".parse::<ApiKey>(),
            Err(KernelError::TooSmall { what: "API key", value: -32_769, min: -32_768 })
        );
    }

    #[test]
    fn every_known_key_reads_what_it_prints() {
        assert!(ApiKey::known().count() > 80);
        for key in ApiKey::known() {
            assert!(key.is_known());
            assert_eq!(key.to_string().parse::<ApiKey>(), Ok(key));
            assert_eq!(key.value().to_string().parse::<ApiKey>(), Ok(key));
        }
    }

    #[test]
    fn every_key_without_a_name_reads_what_it_prints() {
        for key in (i16::MIN..=i16::MAX).map(ApiKey::from).filter(|key| !key.is_known()) {
            assert_eq!(key.to_string(), key.value().to_string());
            assert_eq!(key.to_string().parse::<ApiKey>(), Ok(key));
        }
    }

    #[test]
    fn the_names_are_in_the_order_of_the_keys_and_differ() {
        let keys: Vec<i16> = ApiKey::known().map(ApiKey::value).collect();
        let names: BTreeSet<&str> = ApiKey::known().filter_map(ApiKey::name).collect();

        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(names.len(), keys.len());
    }

    #[test]
    fn api_keys_order_as_numbers() {
        assert!(ApiKey::PRODUCE < ApiKey::FETCH);
        assert!(ApiKey::from(-1) < ApiKey::PRODUCE);
    }
}
