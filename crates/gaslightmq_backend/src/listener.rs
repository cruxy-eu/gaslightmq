//! Why a backend does not send clients to a listener.

use std::net::SocketAddr;

use gaslightmq_field::Endpoint;

/// Why a backend rejects the listener of an endpoint.
///
/// The engine opens a listener for every endpoint, and tells its backend where each one
/// [listens](crate::Backend::listens).
/// A backend that rejects a listener tells no client its address, so no client finds it.
/// A rejected listener changed nothing: clients are told what they were told before.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let rejection = ListenerRejection::Unadvertisable {
///     endpoint: Endpoint::Broker(BrokerId::from(2)),
///     address: "10.0.4.2:19092".parse().expect("an address"),
///     reason: "broker 1 is advertised at gaslight.staging:19092".to_owned(),
/// };
/// assert_eq!(
///     rejection.to_string(),
///     "clients cannot be told that endpoint `broker 2` listens on 10.0.4.2:19092: \
///      broker 1 is advertised at gaslight.staging:19092"
/// );
/// ```
///
/// The message is complete, so a rejection has no source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ListenerRejection {
    /// The backend cannot tell clients that `endpoint` listens on `address`.
    /// `reason` says why, such as another broker that clients are sent to the same address for.
    #[error("clients cannot be told that endpoint `{endpoint}` listens on {address}: {reason}")]
    Unadvertisable {
        endpoint: Endpoint,
        address: SocketAddr,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_rejection_says_everything_in_one_message() {
        let rejection = ListenerRejection::Unadvertisable {
            endpoint: Endpoint::Bootstrap,
            address: "127.0.0.1:0".parse().unwrap(),
            reason: "a listener that takes any free port has to say which one it got".to_owned(),
        };

        assert_eq!(
            rejection.to_string(),
            "clients cannot be told that endpoint `bootstrap` listens on 127.0.0.1:0: \
             a listener that takes any free port has to say which one it got"
        );
        assert!(rejection.source().is_none());
    }
}
