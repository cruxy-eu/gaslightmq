use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

use crate::text::kept_text;

/// The name of a host on the network, or its IP address: `kafka-1.staging`, `localhost` or `10.0.4.27`.
///
/// It is the host that clients see in Metadata, and the host of a server that the proxy connects to.
/// Nothing here looks a name up: a host name is text, and whoever connects resolves it.
///
/// # The rule for host names
///
/// A host name is an IP address, or a name of at most 253 characters.
/// A name has parts between dots.
/// Each part has between 1 and 63 characters: ASCII letters, digits, `-` and `_`.
///
/// A name stays as it was written, so `Kafka-1` and `kafka-1` are two values, although a resolver finds one host.
/// An IP address has one text, however it was written: `2001:0DB8:0::7` is `2001:db8::7`.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let host: HostName = "kafka-1.staging".parse()?;
/// assert_eq!(host, "kafka-1.staging");
/// assert_eq!(host.ip(), None);
///
/// let host = HostName::new("10.0.4.27")?;
/// assert_eq!(host.ip(), Some([10, 0, 4, 27].into()));
///
/// assert!(HostName::new("kafka 1").is_err());
/// assert!(HostName::new("kafka-1.staging:9092").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostName(Arc<str>);

impl HostName {
    /// The most characters a name has.
    pub const MAX_LENGTH: usize = 253;

    /// The most characters one part of a name has.
    pub const MAX_PART_LENGTH: usize = 63;

    /// Checks the [rule for host names](Self#the-rule-for-host-names).
    ///
    /// # Errors
    ///
    /// The [`HostNameError`] that says which part of the rule the text breaks.
    pub fn new(host: impl AsRef<str>) -> Result<Self, HostNameError> {
        let host = host.as_ref();
        let input = || host.to_owned();
        if host.is_empty() {
            return Err(HostNameError::Empty);
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(Self::from(ip));
        }
        if let Some(character) = host.chars().find(|character| !is_name_character(*character)) {
            return Err(HostNameError::InvalidCharacter {
                input: input(),
                character,
            });
        }
        if host.len() > Self::MAX_LENGTH {
            return Err(HostNameError::TooLong { input: input() });
        }
        for part in host.split('.') {
            if part.is_empty() {
                return Err(HostNameError::EmptyPart { input: input() });
            }
            if part.len() > Self::MAX_PART_LENGTH {
                return Err(HostNameError::PartTooLong {
                    input: input(),
                    part: part.to_owned(),
                });
            }
        }
        Ok(Self(host.into()))
    }

    /// The IP address, when the host is one and not a name.
    pub fn ip(&self) -> Option<IpAddr> {
        self.0.parse().ok()
    }
}

/// Whether a name can hold the character: a letter, a digit, `-`, `_`, or the dot between two parts.
const fn is_name_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
}

/// An IP address is a host. Its text is the one that the address writes, so one address is one value.
impl From<IpAddr> for HostName {
    fn from(ip: IpAddr) -> Self {
        Self(ip.to_string().into())
    }
}

kept_text!(HostName, HostNameError);

/// Why a text is not a [`HostName`]: the part of the [rule for host names](HostName#the-rule-for-host-names) it breaks.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let error = HostName::new("kafka 1").unwrap_err();
/// assert_eq!(error.to_string(), "host name \"kafka 1\" holds the character ' '");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostNameError {
    /// The text has no character.
    Empty,
    /// The name has more than [`HostName::MAX_LENGTH`] characters.
    TooLong { input: String },
    /// Two dots follow each other, or the name starts or ends with a dot.
    EmptyPart { input: String },
    /// A part between two dots has more than [`HostName::MAX_PART_LENGTH`] characters.
    PartTooLong { input: String, part: String },
    /// The text holds a character that no name holds, and it is no IP address.
    InvalidCharacter { input: String, character: char },
}

impl fmt::Display for HostNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("host name \"\" is empty"),
            Self::TooLong { input } => write!(
                f,
                "host name {input:?} has more than {} characters",
                HostName::MAX_LENGTH
            ),
            Self::EmptyPart { input } => write!(f, "host name {input:?} has an empty part between its dots"),
            Self::PartTooLong { input, part } => write!(
                f,
                "host name {input:?} has the part {part:?} of more than {} characters",
                HostName::MAX_PART_LENGTH
            ),
            Self::InvalidCharacter { input, character } => {
                write!(f, "host name {input:?} holds the character {character:?}")
            }
        }
    }
}

impl std::error::Error for HostNameError {}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn a_host_is_a_name_or_an_ip_address() {
        for text in [
            "localhost",
            "kafka-1.staging",
            "kafka_1",
            "gaslightmq.staging.svc.cluster.local",
            "Kafka-1",
            "10.0.4.27",
            "::1",
            "2001:db8::7",
        ] {
            let host = HostName::new(text).unwrap();
            assert_eq!(host.as_str(), text);
            assert_eq!(host.to_string(), text);
            assert_eq!(text.parse::<HostName>(), Ok(host));
        }
    }

    #[test]
    fn a_host_knows_whether_it_is_an_ip_address() {
        assert_eq!(HostName::new("localhost").unwrap().ip(), None);
        assert_eq!(
            HostName::new("127.0.0.1").unwrap().ip(),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
        assert_eq!(
            HostName::new("::1").unwrap().ip(),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(HostName::from(IpAddr::V6(Ipv6Addr::LOCALHOST)), "::1");
    }

    #[test]
    fn an_ip_address_is_one_value_however_it_is_written() {
        let short = HostName::new("2001:db8::7").unwrap();

        for text in [
            "2001:0db8::7",
            "2001:DB8::7",
            "2001:db8:0:0:0:0:0:7",
            "2001:0db8:0000::0007",
        ] {
            let host = HostName::new(text).unwrap();
            assert_eq!(host, short, "{text}");
            assert_eq!(host.as_str(), "2001:db8::7");
            assert_eq!(Some(host), short.ip().map(HostName::from));
        }
        // A name stays as it was written.
        assert_ne!(HostName::new("Kafka-1").unwrap(), HostName::new("kafka-1").unwrap());
    }

    #[test]
    fn a_text_that_breaks_the_rule_is_an_error() {
        let input = |text: &str| text.to_owned();
        let part = "a".repeat(64);
        let long = [part.as_str(), "internal"].join(".");
        let longest = vec!["a".repeat(63); 4].join(".");

        assert_eq!(HostName::new(""), Err(HostNameError::Empty));
        assert_eq!(
            HostName::new("kafka 1"),
            Err(HostNameError::InvalidCharacter {
                input: input("kafka 1"),
                character: ' '
            })
        );
        assert_eq!(
            HostName::new("kafka-1:9092"),
            Err(HostNameError::InvalidCharacter {
                input: input("kafka-1:9092"),
                character: ':'
            })
        );
        assert_eq!(
            HostName::new("[::1]"),
            Err(HostNameError::InvalidCharacter {
                input: input("[::1]"),
                character: '['
            })
        );
        assert_eq!(
            HostName::new("kafka..internal"),
            Err(HostNameError::EmptyPart {
                input: input("kafka..internal")
            })
        );
        assert_eq!(
            HostName::new("kafka.internal."),
            Err(HostNameError::EmptyPart {
                input: input("kafka.internal.")
            })
        );
        assert_eq!(
            HostName::new(&long),
            Err(HostNameError::PartTooLong {
                input: long.clone(),
                part
            })
        );
        assert_eq!(longest.len(), 255);
        assert_eq!(
            HostName::new(&longest),
            Err(HostNameError::TooLong { input: longest.clone() })
        );
        assert!(HostName::new(&longest[2..]).is_ok());
    }

    #[test]
    fn the_error_says_which_part_of_the_rule_a_text_breaks() {
        let message = |text: &str| HostName::new(text).unwrap_err().to_string();

        assert_eq!(message(""), "host name \"\" is empty");
        assert_eq!(message("kafka/1"), "host name \"kafka/1\" holds the character '/'");
        assert_eq!(
            message(".kafka"),
            "host name \".kafka\" has an empty part between its dots"
        );
        assert!(message(&"a".repeat(64)).ends_with("of more than 63 characters"));
        assert!(message(&vec!["a".repeat(63); 5].join(".")).ends_with("has more than 253 characters"));
    }
}
