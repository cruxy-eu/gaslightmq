//! What a client of the control plane is configured with.

use gaslightmq_control_api::clock::TimeSpan;

use crate::address::ControlPlaneAddress;

/// What a [`ControlClient`](crate::ControlClient) is configured with.
///
/// A property is a public field with a default. The control plane is no Kafka API,
/// so no property is one of Kafka's: each is ours, and says so.
///
/// ```
/// use gaslightmq_control_client::ControlClientConfiguration;
/// use gaslightmq_control_api::clock::TimeSpan;
///
/// let mut configuration = ControlClientConfiguration::new("http://localhost:9080".parse()?);
/// assert_eq!(configuration.request_timeout, TimeSpan::from_secs(30));
///
/// configuration.request_timeout = TimeSpan::from_secs(5);
/// # Ok::<(), gaslightmq_control_client::ControlPlaneAddressError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ControlClientConfiguration {
    /// Ours. Required.
    ///
    /// Where the control plane listens.
    pub address: ControlPlaneAddress,
    /// Ours. Default: 30 seconds.
    ///
    /// How long the client waits for the control plane: for the answer to an operation,
    /// and for the control plane to take a new live stream.
    /// A stream that is open waits for its next message as long as it takes.
    pub request_timeout: TimeSpan,
}

impl ControlClientConfiguration {
    /// The defaults, for a client of the control plane at `address`.
    pub fn new(address: ControlPlaneAddress) -> Self {
        Self {
            address,
            request_timeout: TimeSpan::from_secs(30),
        }
    }
}
