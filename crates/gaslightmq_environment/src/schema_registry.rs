use std::fmt;
use std::str::FromStr;

use crate::address::{ListenAddress, connect_port, host_of_address, split_port, write_host};
use crate::host::{HostName, HostNameError};
use crate::text::text_form;

/// The schema registry of an environment: the built-in one, or the real one of a real cluster.
///
/// | Registry | Belongs to | Form |
/// |---|---|---|
/// | [`BuiltIn`](Self::BuiltIn) | An environment with a broker. It serves the schemas of the workspace. | `{ listen: 0.0.0.0:8081 }` |
/// | [`Real`](Self::Real) | A proxy in front of a real cluster. The proxy reads schemas from it. | `{ url: https://schema-registry.staging.internal:8081 }` |
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let built_in = SchemaRegistry::BuiltIn { listen: "0.0.0.0:8081".parse()? };
/// let url = "https://schema-registry.staging.internal:8081".parse().expect("a schema registry URL");
/// let real = SchemaRegistry::Real { url };
///
/// assert_eq!(built_in.listen(), Some("0.0.0.0:8081".parse()?));
/// assert_eq!(real.listen(), None);
/// # Ok::<(), ListenAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(
        try_from = "crate::serde::SchemaRegistryForm",
        into = "crate::serde::SchemaRegistryForm"
    )
)]
pub enum SchemaRegistry {
    /// The registry that the process serves itself, next to a broker.
    BuiltIn {
        /// Where the registry listens.
        listen: ListenAddress,
    },
    /// The registry of a real cluster.
    Real {
        /// Where the registry answers.
        url: SchemaRegistryUrl,
    },
}

impl SchemaRegistry {
    /// Where the built-in registry listens, or `None` for the real one.
    pub fn listen(&self) -> Option<ListenAddress> {
        match self {
            Self::BuiltIn { listen } => Some(*listen),
            Self::Real { .. } => None,
        }
    }

    /// Where the real registry answers, or `None` for the built-in one.
    pub fn url(&self) -> Option<&SchemaRegistryUrl> {
        match self {
            Self::BuiltIn { .. } => None,
            Self::Real { url } => Some(url),
        }
    }
}

/// Why what a file holds is not a [`SchemaRegistry`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaRegistryError {
    /// The registry has neither an address to listen on nor a URL.
    #[error("a schema registry has `listen` for the built-in one, or `url` for the real one")]
    NoPlace,
    /// The registry has an address to listen on and a URL.
    #[error("a schema registry is the built-in one or the real one: it has `listen` or `url`, and not both")]
    TwoPlaces,
}

/// Where a real schema registry answers, such as `https://schema-registry.staging.internal:8081`.
///
/// # The rule for URLs
///
/// A URL starts with `http://` or `https://`.
/// A host follows, then a port when the service does not answer on the usual one, and then a path when it has one.
///
/// A URL holds no user name and no password before its host, because a definition never holds a secret.
/// It holds no query and no fragment either.
/// Its path can hold an `@`.
///
/// ```
/// use gaslightmq_environment::prelude::*;
///
/// let url: SchemaRegistryUrl = "https://schema-registry.staging.internal:8081".parse()?;
/// assert!(url.uses_tls());
/// assert_eq!(url.host(), &HostName::new("schema-registry.staging.internal").expect("a host name"));
/// assert_eq!(url.port(), 8081);
///
/// // Without a port, the service answers on the usual one.
/// assert_eq!("http://registry/schemas".parse::<SchemaRegistryUrl>()?.port(), 80);
///
/// assert_eq!("https://admin:hunter2@registry".parse::<SchemaRegistryUrl>(), Err(SchemaRegistryUrlError::Credentials));
/// # Ok::<(), gaslightmq_environment::SchemaRegistryUrlError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaRegistryUrl {
    encrypted: bool,
    host: HostName,
    port: Option<u16>,
    path: String,
}

impl SchemaRegistryUrl {
    /// Whether the URL starts with `https`, so the connection is encrypted.
    pub fn uses_tls(&self) -> bool {
        self.encrypted
    }

    /// The host of the service.
    pub fn host(&self) -> &HostName {
        &self.host
    }

    /// The port the service answers on: the one the URL names, or else 443 with TLS and 80 without.
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(if self.encrypted { 443 } else { 80 })
    }

    /// The path after the host and the port, which starts with `/`, or no text when the URL has none.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl FromStr for SchemaRegistryUrl {
    type Err = SchemaRegistryUrlError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // A text with a token in a query holds a secret, so this comes first, and repeats nothing.
        if text.contains(['?', '#']) {
            return Err(SchemaRegistryUrlError::QueryOrFragment);
        }
        // A password with a `/` in it ends what reads as the host and the port, and the `@` then stands in what
        // reads as the path. Such a text is no URL, and the error that says why would repeat the password.
        Self::without_query(text).map_err(|error| {
            if text.contains('@') {
                SchemaRegistryUrlError::Credentials
            } else {
                error
            }
        })
    }
}

impl SchemaRegistryUrl {
    /// Reads a text that holds no query and no fragment.
    fn without_query(text: &str) -> Result<Self, SchemaRegistryUrlError> {
        let input = || text.to_owned();
        let (scheme, rest) = text.split_once("://").unwrap_or(("", text));
        let (authority, path) = rest.find('/').map_or((rest, ""), |at| rest.split_at(at));
        // Credentials stand before the host. An `@` in the path names nobody.
        if authority.contains('@') {
            return Err(SchemaRegistryUrlError::Credentials);
        }
        let encrypted = if scheme.eq_ignore_ascii_case("https") {
            true
        } else if scheme.eq_ignore_ascii_case("http") {
            false
        } else {
            return Err(SchemaRegistryUrlError::UnknownScheme { input: input() });
        };
        let (host, port) = split_port(authority);
        let host =
            host_of_address(host).map_err(|error| SchemaRegistryUrlError::InvalidHost { input: input(), error })?;
        let port = match port {
            Some(port) => {
                Some(connect_port(port).ok_or_else(|| SchemaRegistryUrlError::InvalidPort { input: input() })?)
            }
            None => None,
        };
        let invalid = |character: &char| character.is_whitespace() || character.is_control();
        if let Some(character) = path.chars().find(invalid) {
            return Err(SchemaRegistryUrlError::InvalidPath {
                input: input(),
                character,
            });
        }
        Ok(Self {
            encrypted,
            host,
            port,
            path: path.to_owned(),
        })
    }
}

impl fmt::Display for SchemaRegistryUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.encrypted { "https://" } else { "http://" })?;
        write_host(f, &self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{port}")?;
        }
        f.write_str(&self.path)
    }
}

text_form!(SchemaRegistryUrl, SchemaRegistryUrlError);

/// Why a text is not a [`SchemaRegistryUrl`]: the part of the [rule for URLs](SchemaRegistryUrl#the-rule-for-urls) it breaks.
///
/// [`Credentials`](Self::Credentials) and [`QueryOrFragment`](Self::QueryOrFragment) do not repeat the text,
/// because it can hold a password or a token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaRegistryUrlError {
    /// The text holds an `@` before its path, so it names a user, and maybe a password.
    /// Or it holds an `@` and is no URL, as when a password holds a `/`.
    #[error("a schema registry URL holds no user name and no password: a secret is a reference and never the value")]
    Credentials,
    /// The text holds a `?` or a `#`.
    #[error("a schema registry URL holds no query and no fragment")]
    QueryOrFragment,
    /// The text does not start with `http://` or `https://`.
    #[error("schema registry URL {input:?} does not start with http:// or https://")]
    UnknownScheme { input: String },
    /// What follows the scheme is not a host.
    #[error("schema registry URL {input:?}: {error}")]
    InvalidHost { input: String, error: HostNameError },
    /// The port is not a number between 1 and 65535.
    #[error("schema registry URL {input:?} has a port that is no number between 1 and 65535")]
    InvalidPort { input: String },
    /// The path holds white space or a control character.
    #[error("schema registry URL {input:?} holds the character {character:?} in its path")]
    InvalidPath { input: String, character: char },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(text: &str) -> SchemaRegistryUrl {
        text.parse().unwrap()
    }

    #[test]
    fn a_url_has_a_scheme_a_host_a_port_and_a_path() {
        let registry = url("https://schema-registry.staging.internal:8081/apis/ccompat/v7");

        assert!(registry.uses_tls());
        assert_eq!(registry.host(), "schema-registry.staging.internal");
        assert_eq!(registry.port(), 8081);
        assert_eq!(registry.path(), "/apis/ccompat/v7");
    }

    #[test]
    fn a_url_writes_what_it_read() {
        for text in [
            "https://schema-registry.staging.internal:8081",
            "http://localhost:8081/",
            "http://registry",
            "https://10.0.4.27/schemas",
            "http://[::1]:8081/schemas",
        ] {
            assert_eq!(url(text).to_string(), text);
        }
        assert_eq!(url("HTTPS://registry").to_string(), "https://registry");
    }

    #[test]
    fn a_url_without_a_port_answers_on_the_usual_one() {
        assert_eq!(url("http://registry").port(), 80);
        assert_eq!(url("https://registry").port(), 443);
        assert!(!url("http://registry").uses_tls());
        assert_eq!(url("http://registry").path(), "");
    }

    #[test]
    fn a_path_holds_an_at_sign() {
        let registry = url("https://registry:8081/apis/@v7/ccompat");

        assert_eq!(registry.path(), "/apis/@v7/ccompat");
        assert_eq!(registry.host(), "registry");
        assert_eq!(registry.to_string(), "https://registry:8081/apis/@v7/ccompat");
    }

    #[test]
    fn a_password_with_a_slash_is_not_repeated_either() {
        // The `/` ends what reads as the host and the port, and the `@` comes after it.
        for text in [
            "https://admin:hunter/2@registry:8081",
            "https://hunter 2/@registry",
            "ftp://admin:hunter/2@registry",
        ] {
            let error = text.parse::<SchemaRegistryUrl>().unwrap_err();
            assert_eq!(error, SchemaRegistryUrlError::Credentials, "{text}");
            assert!(!error.to_string().contains("hunter"));
        }
    }

    #[test]
    fn a_url_with_a_secret_is_an_error_that_does_not_repeat_it() {
        for text in [
            "https://admin:hunter2@registry:8081",
            "https://hunter2@registry",
            "ftp://admin:hunter2@registry",
            "admin:hunter2@registry",
        ] {
            let error = text.parse::<SchemaRegistryUrl>().unwrap_err();
            assert_eq!(error, SchemaRegistryUrlError::Credentials, "{text}");
            assert!(!error.to_string().contains("hunter2"));
        }
        for text in [
            "https://registry/schemas?token=hunter2",
            "https://registry#hunter2",
            "registry?hunter2",
        ] {
            let error = text.parse::<SchemaRegistryUrl>().unwrap_err();
            assert_eq!(error, SchemaRegistryUrlError::QueryOrFragment, "{text}");
            assert!(!error.to_string().contains("hunter2"));
        }
    }

    #[test]
    fn a_text_that_breaks_the_rule_is_an_error() {
        let input = |text: &str| text.to_owned();
        let parse = |text: &str| text.parse::<SchemaRegistryUrl>();

        for text in ["", "registry:8081", "ftp://registry", "://registry", "https:/registry"] {
            assert_eq!(
                parse(text),
                Err(SchemaRegistryUrlError::UnknownScheme { input: input(text) }),
                "{text}"
            );
        }
        assert_eq!(
            parse("https://"),
            Err(SchemaRegistryUrlError::InvalidHost {
                input: input("https://"),
                error: HostNameError::Empty
            })
        );
        assert_eq!(
            parse("https://schema registry"),
            Err(SchemaRegistryUrlError::InvalidHost {
                input: input("https://schema registry"),
                error: HostNameError::InvalidCharacter {
                    input: input("schema registry"),
                    character: ' '
                }
            })
        );
        for text in [
            "https://registry:0",
            "https://registry:",
            "https://registry:http",
            "https://registry:65536/",
        ] {
            assert_eq!(
                parse(text),
                Err(SchemaRegistryUrlError::InvalidPort { input: input(text) }),
                "{text}"
            );
        }
        assert_eq!(
            parse("https://registry/my schemas"),
            Err(SchemaRegistryUrlError::InvalidPath {
                input: input("https://registry/my schemas"),
                character: ' '
            })
        );
    }

    #[test]
    fn the_error_names_the_whole_text() {
        let message = |text: &str| text.parse::<SchemaRegistryUrl>().unwrap_err().to_string();

        assert_eq!(
            message("registry:8081"),
            "schema registry URL \"registry:8081\" does not start with http:// or https://"
        );
        assert_eq!(
            message("https://schema_registry!"),
            "schema registry URL \"https://schema_registry!\": host name \"schema_registry!\" holds the character '!'"
        );
        assert_eq!(
            message("https://registry:0"),
            "schema registry URL \"https://registry:0\" has a port that is no number between 1 and 65535"
        );
        assert_eq!(
            message("https://registry/a b"),
            "schema registry URL \"https://registry/a b\" holds the character ' ' in its path"
        );
        assert_eq!(
            message("https://a:b@registry"),
            "a schema registry URL holds no user name and no password: a secret is a reference and never the value"
        );
        assert_eq!(
            message("https://registry?x"),
            "a schema registry URL holds no query and no fragment"
        );
    }

    #[test]
    fn a_registry_is_built_in_or_real() {
        let listen: ListenAddress = "0.0.0.0:8081".parse().unwrap();
        let built_in = SchemaRegistry::BuiltIn { listen };
        let real = SchemaRegistry::Real {
            url: url("https://registry:8081"),
        };

        assert_eq!(built_in.listen(), Some(listen));
        assert_eq!(built_in.url(), None);
        assert_eq!(real.url(), Some(&url("https://registry:8081")));
        assert_eq!(real.listen(), None);
    }
}
