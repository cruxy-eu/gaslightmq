use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::address::ServerAddress;
use crate::secret::{FilePath, SecretRef};
use crate::text::{kept_text, text_form};

/// The real cluster that stands behind a proxy, and how the proxy connects to it.
///
/// | Part | Is |
/// |---|---|
/// | [`bootstrap`](Self::bootstrap) | The server the proxy asks for the brokers of the cluster. |
/// | [`tls`](Self::tls) | That the connections are encrypted, and which certificate authority signs the cluster's certificates. |
/// | [`sasl`](Self::sasl) | How the proxy logs in. |
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let upstream = Upstream::new("kafka-1.staging:9092".parse().expect("a server address"))
///     .with_tls(Tls::new().with_ca("/run/secrets/kafka-ca.pem".parse()?))
///     .with_sasl(Sasl::new(
///         SaslMechanism::ScramSha512,
///         "gaslight".parse().expect("a SASL username"),
///         SecretRef::File("/run/secrets/kafka-password".parse()?),
///     ));
///
/// assert_eq!(upstream.bootstrap().to_string(), "kafka-1.staging:9092");
/// assert!(upstream.tls().is_some());
/// # Ok::<(), FilePathError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Upstream {
    bootstrap: ServerAddress,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    tls: Option<Tls>,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    sasl: Option<Sasl>,
}

impl Upstream {
    /// The cluster behind this bootstrap server, over plain connections and without a login.
    pub fn new(bootstrap: ServerAddress) -> Self {
        Self {
            bootstrap,
            tls: None,
            sasl: None,
        }
    }

    /// The same cluster, over encrypted connections.
    #[must_use]
    pub fn with_tls(mut self, tls: Tls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// The same cluster, with a login.
    #[must_use]
    pub fn with_sasl(mut self, sasl: Sasl) -> Self {
        self.sasl = Some(sasl);
        self
    }

    /// The server the proxy asks for the brokers of the cluster.
    pub fn bootstrap(&self) -> &ServerAddress {
        &self.bootstrap
    }

    /// How the connections are encrypted, or `None` for plain connections.
    pub fn tls(&self) -> Option<&Tls> {
        self.tls.as_ref()
    }

    /// How the proxy logs in, or `None` for a cluster without a login.
    pub fn sasl(&self) -> Option<&Sasl> {
        self.sasl.as_ref()
    }
}

/// Encrypted connections to a cluster.
///
/// Without a certificate authority of its own, the cluster's certificates are checked against the authorities
/// that the machine trusts.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let tls = Tls::new().with_ca("/run/secrets/kafka-ca.pem".parse()?);
/// assert_eq!(tls.ca(), Some(&FilePath::new("/run/secrets/kafka-ca.pem")?));
/// assert_eq!(Tls::new().ca(), None);
/// # Ok::<(), FilePathError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Tls {
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    ca: Option<FilePath>,
}

impl Tls {
    /// Encrypted connections to a cluster whose certificates an authority signs that the machine trusts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Encrypted connections to a cluster whose certificates the authority in this file signs.
    #[must_use]
    pub fn with_ca(mut self, ca: FilePath) -> Self {
        self.ca = Some(ca);
        self
    }

    /// The file with the certificate of the authority that signs the cluster's certificates.
    pub fn ca(&self) -> Option<&FilePath> {
        self.ca.as_ref()
    }
}

/// How a proxy logs in to a cluster: a mechanism, a user name and a password.
///
/// The password is a [`SecretRef`]: this type names where the password lives, and never holds it.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let sasl = Sasl::new(
///     SaslMechanism::ScramSha512,
///     "gaslight".parse()?,
///     SecretRef::Variable("KAFKA_PASSWORD".parse().expect("a variable name")),
/// );
/// assert_eq!(sasl.username(), &SaslUsername::new("gaslight")?);
/// assert_eq!(sasl.password().to_string(), "the environment variable KAFKA_PASSWORD");
/// # Ok::<(), SaslUsernameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct Sasl {
    mechanism: SaslMechanism,
    username: SaslUsername,
    password: SecretRef,
}

impl Sasl {
    /// A login as `username`, with the password that `password` names.
    pub fn new(mechanism: SaslMechanism, username: SaslUsername, password: SecretRef) -> Self {
        Self {
            mechanism,
            username,
            password,
        }
    }

    /// The mechanism of the login.
    pub fn mechanism(&self) -> SaslMechanism {
        self.mechanism
    }

    /// The user the proxy logs in as.
    pub fn username(&self) -> &SaslUsername {
        &self.username
    }

    /// Where the password lives.
    pub fn password(&self) -> &SecretRef {
        &self.password
    }
}

/// A SASL mechanism that logs in with a user name and a password.
///
/// The text of a mechanism is the name Kafka gives it.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let mechanism: SaslMechanism = "SCRAM-SHA-512".parse()?;
/// assert_eq!(mechanism, SaslMechanism::ScramSha512);
/// assert_eq!(SaslMechanism::Plain.to_string(), "PLAIN");
/// # Ok::<(), SaslMechanismError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SaslMechanism {
    /// `PLAIN`: the password travels as it is, so the connection should be encrypted.
    Plain,
    /// `SCRAM-SHA-256`.
    ScramSha256,
    /// `SCRAM-SHA-512`.
    ScramSha512,
}

impl SaslMechanism {
    /// Every mechanism.
    pub const ALL: [Self; 3] = [Self::Plain, Self::ScramSha256, Self::ScramSha512];

    /// The name Kafka gives the mechanism.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

impl FromStr for SaslMechanism {
    type Err = SaslMechanismError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mechanism| mechanism.name() == text)
            .ok_or_else(|| SaslMechanismError::Unknown { name: text.to_owned() })
    }
}

impl fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

text_form!(SaslMechanism, SaslMechanismError);

/// Why a text is not a [`SaslMechanism`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SaslMechanismError {
    /// No mechanism that logs in with a password has this name.
    #[error("no SASL mechanism is named {name:?}: there are PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512")]
    Unknown { name: String },
}

/// The user a proxy logs in to a cluster as, such as `gaslight`.
///
/// A user name has at least one character.
/// It is not a secret, so it stands in the definition as it is.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let username: SaslUsername = "gaslight".parse()?;
/// assert_eq!(username, "gaslight");
/// assert_eq!(SaslUsername::new(""), Err(SaslUsernameError::Empty));
/// # Ok::<(), gaslightmq_environment::SaslUsernameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SaslUsername(Arc<str>);

impl SaslUsername {
    /// Checks that the user name has a character.
    ///
    /// # Errors
    ///
    /// [`SaslUsernameError::Empty`] for no text.
    pub fn new(username: impl AsRef<str>) -> Result<Self, SaslUsernameError> {
        let username = username.as_ref();
        if username.is_empty() {
            return Err(SaslUsernameError::Empty);
        }
        Ok(Self(username.into()))
    }
}

kept_text!(SaslUsername, SaslUsernameError);

/// Why a text is not a [`SaslUsername`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SaslUsernameError {
    /// The text has no character.
    #[error("SASL user name \"\" is empty")]
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_upstream_starts_plain_and_without_a_login() {
        let bootstrap: ServerAddress = "kafka:9092".parse().unwrap();
        let upstream = Upstream::new(bootstrap.clone());

        assert_eq!(upstream.bootstrap(), &bootstrap);
        assert_eq!(upstream.tls(), None);
        assert_eq!(upstream.sasl(), None);
    }

    #[test]
    fn an_upstream_takes_tls_and_a_login() {
        let ca = FilePath::new("secrets/kafka-ca.pem").unwrap();
        let password = SecretRef::File(FilePath::new("secrets/kafka-password").unwrap());
        let sasl = Sasl::new(SaslMechanism::Plain, "gaslight".parse().unwrap(), password.clone());
        let upstream = Upstream::new("kafka:9092".parse().unwrap())
            .with_tls(Tls::new().with_ca(ca.clone()))
            .with_sasl(sasl.clone());

        assert_eq!(upstream.tls().and_then(Tls::ca), Some(&ca));
        assert_eq!(upstream.sasl(), Some(&sasl));
        assert_eq!(sasl.mechanism(), SaslMechanism::Plain);
        assert_eq!(sasl.username(), "gaslight");
        assert_eq!(sasl.password(), &password);
    }

    #[test]
    fn a_mechanism_has_the_name_kafka_gives_it() {
        for (mechanism, name) in [
            (SaslMechanism::Plain, "PLAIN"),
            (SaslMechanism::ScramSha256, "SCRAM-SHA-256"),
            (SaslMechanism::ScramSha512, "SCRAM-SHA-512"),
        ] {
            assert_eq!(mechanism.to_string(), name);
            assert_eq!(name.parse::<SaslMechanism>(), Ok(mechanism));
        }
        let error = "scram-sha-512".parse::<SaslMechanism>().unwrap_err();
        assert_eq!(
            error,
            SaslMechanismError::Unknown {
                name: "scram-sha-512".to_owned()
            }
        );
        assert_eq!(
            error.to_string(),
            "no SASL mechanism is named \"scram-sha-512\": there are PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512"
        );
    }

    #[test]
    fn a_user_name_has_a_character() {
        assert_eq!(SaslUsername::new("gaslight").unwrap().as_str(), "gaslight");
        assert_eq!(SaslUsername::new(""), Err(SaslUsernameError::Empty));
        assert_eq!(SaslUsernameError::Empty.to_string(), "SASL user name \"\" is empty");
    }
}
