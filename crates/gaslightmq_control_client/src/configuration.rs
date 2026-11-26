//! What a client of the control plane is configured with.

use gaslightmq_control_api::RequestSurface;
use gaslightmq_control_api::clock::TimeSpan;

use crate::address::ControlPlaneAddress;

/// What a [`ControlClient`](crate::ControlClient) is configured with.
///
/// A property is a public field with a default. The control plane is no Kafka API,
/// so no property is one of Kafka's: each is ours, and says so.
///
/// ```
/// use gaslightmq_control_api::RequestSurface;
/// use gaslightmq_control_api::clock::TimeSpan;
/// use gaslightmq_control_client::ControlClientConfiguration;
///
/// let mut configuration = ControlClientConfiguration::new("http://localhost:9080".parse()?);
/// assert_eq!(configuration.request_timeout, TimeSpan::from_secs(30));
/// // A program that says nothing about itself is any other client of the control plane.
/// assert_eq!(configuration.surface, RequestSurface::Api);
///
/// // `gmq` says that it is `gmq`.
/// configuration.surface = RequestSurface::Cli;
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
    /// Ours. Default: [`RequestSurface::Api`], any other client of the control plane.
    ///
    /// The surface that the client is: every request says that it comes from there,
    /// unless the request [names its source](crate::ControlClient::send_from) itself.
    /// The UI, `gmq` and a test library each say which they are.
    pub surface: RequestSurface,
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
            surface: RequestSurface::Api,
            request_timeout: TimeSpan::from_secs(30),
        }
    }
}
