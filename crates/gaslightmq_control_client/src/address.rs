//! Where a control plane listens.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use url::Url;

/// Where a control plane listens: an `http` URL with a host, and with a port and a path if it needs them.
///
/// The client sends its operations and opens its live stream below that URL,
/// so a control plane behind a path, such as `http://tools.internal/gaslightmq`, works as one at the root.
/// The address names where the control plane is, and nothing else: no user, no query and no fragment.
///
/// ```
/// use gaslightmq_control_client::ControlPlaneAddress;
///
/// let address: ControlPlaneAddress = "http://localhost:9080".parse()?;
/// assert_eq!(address.to_string(), "http://localhost:9080/");
///
/// let behind_a_path: ControlPlaneAddress = "http://tools.internal/gaslightmq".parse()?;
/// assert_eq!(behind_a_path.to_string(), "http://tools.internal/gaslightmq/");
/// # Ok::<(), gaslightmq_control_client::ControlPlaneAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlPlaneAddress {
    /// The URL, with a path that ends in `/`, so that a path below it joins without losing a segment.
    url: Url,
}

impl ControlPlaneAddress {
    /// The scheme of every address: the client speaks plain HTTP.
    const SCHEME: &str = "http";

    /// Reads `text` as the address of a control plane.
    ///
    /// # Errors
    ///
    /// [`ControlPlaneAddressError::Malformed`] when `text` is no URL with a host,
    /// [`ControlPlaneAddressError::NotHttp`] when its scheme is not `http`,
    /// and [`ControlPlaneAddressError::MoreThanAPlace`] when it holds a user, a password, a query or a fragment.
    pub fn new(text: &str) -> Result<Self, ControlPlaneAddressError> {
        // An `http` URL always has a host: the parser refuses one without.
        let mut url = Url::parse(text).map_err(|error| ControlPlaneAddressError::Malformed {
            input: text.to_owned(),
            reason: error.to_string(),
        })?;
        if url.scheme() != Self::SCHEME {
            return Err(ControlPlaneAddressError::NotHttp { input: text.to_owned() });
        }
        let more_than_a_place =
            !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some();
        if more_than_a_place {
            return Err(ControlPlaneAddressError::MoreThanAPlace { input: text.to_owned() });
        }
        if !url.path().ends_with('/') {
            let path = format!("{}/", url.path());
            url.set_path(&path);
        }
        Ok(Self { url })
    }

    /// The URL of `path` below the address. `path` is relative: `operations`, and not `/operations`.
    pub(crate) fn http(&self, path: &str) -> String {
        self.below(path).to_string()
    }

    /// The `ws` URL of `path` below the address, for a WebSocket. `path` may end in a query.
    pub(crate) fn websocket(&self, path: &str) -> String {
        let below = self.below(path);
        // The scheme of a WebSocket that the browser and the native client open over plain HTTP.
        format!("ws{}", &below.as_str()[Self::SCHEME.len()..])
    }

    fn below(&self, path: &str) -> Url {
        self.url.join(path).expect("a relative path joins below an http URL")
    }
}

/// The address of a control plane on a socket address: `127.0.0.1:9080` is `http://127.0.0.1:9080/`.
///
/// ```
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// use gaslightmq_control_client::ControlPlaneAddress;
///
/// let address = ControlPlaneAddress::from(SocketAddr::from((Ipv4Addr::LOCALHOST, 9080)));
/// assert_eq!(address.to_string(), "http://127.0.0.1:9080/");
/// ```
impl From<SocketAddr> for ControlPlaneAddress {
    fn from(address: SocketAddr) -> Self {
        Self::new(&format!("{}://{address}", Self::SCHEME)).expect("a socket address is the host and port of a URL")
    }
}

impl FromStr for ControlPlaneAddress {
    type Err = ControlPlaneAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

impl fmt::Display for ControlPlaneAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.url.as_str())
    }
}

/// Why a text is not the [address of a control plane](ControlPlaneAddress).
///
/// ```
/// use gaslightmq_control_client::{ControlPlaneAddress, ControlPlaneAddressError};
///
/// let error = "https://localhost:9080".parse::<ControlPlaneAddress>().expect_err("the client speaks plain HTTP");
/// assert_eq!(error, ControlPlaneAddressError::NotHttp { input: "https://localhost:9080".to_owned() });
/// assert_eq!(error.to_string(), "control plane address \"https://localhost:9080\": the scheme is not http");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlPlaneAddressError {
    /// The text is no URL with a host. `reason` says what is wrong with it.
    #[error("control plane address {input:?}: {reason}")]
    Malformed { input: String, reason: String },
    /// The URL has a scheme other than `http`.
    #[error("control plane address {input:?}: the scheme is not http")]
    NotHttp { input: String },
    /// The URL holds a user, a password, a query or a fragment, and an address only says where the control plane is.
    #[error("control plane address {input:?}: it holds a user, a password, a query or a fragment")]
    MoreThanAPlace { input: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_below_the_address_keeps_the_path_of_the_address() {
        let address = ControlPlaneAddress::new("http://tools.internal/gaslightmq").expect("an address");

        assert_eq!(
            address.http("operations"),
            "http://tools.internal/gaslightmq/operations"
        );
        assert_eq!(address.websocket("stream"), "ws://tools.internal/gaslightmq/stream");
        assert_eq!(
            address.websocket("stream?presence=9f86"),
            "ws://tools.internal/gaslightmq/stream?presence=9f86"
        );
    }

    #[test]
    fn an_address_without_a_host_is_malformed() {
        let error = ControlPlaneAddress::new("http://:9080").expect_err("no host");

        assert!(matches!(error, ControlPlaneAddressError::Malformed { .. }), "{error}");
    }

    #[test]
    fn an_address_with_a_query_is_more_than_a_place() {
        let error = ControlPlaneAddress::new("http://localhost:9080/?environment=orders-sim").expect_err("a query");

        assert_eq!(
            error,
            ControlPlaneAddressError::MoreThanAPlace {
                input: "http://localhost:9080/?environment=orders-sim".to_owned()
            }
        );
    }
}
