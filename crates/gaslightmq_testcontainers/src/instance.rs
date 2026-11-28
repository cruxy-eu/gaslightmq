//! GaslightMQ that runs in a container next to a test, and how it starts and stops.

use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::{EnvironmentName, ServerAddress};
use gaslightmq_control_api::{CheckHealth, RequestSurface, RunId};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, ControlPlaneAddress};
use testcontainers::core::client::ClientError;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Healthcheck, ImageExt, TestcontainersError};
use tokio::io::AsyncBufReadExt;

use crate::announcement::{Announced, Announcement};
use crate::configuration::WORKSPACE_IN_THE_CONTAINER;
use crate::{GaslightMqContainerConfiguration, GaslightMqContainerStartError, GaslightMqImage};

/// The network of the container: the one of the machine of the test, so the process reaches whatever the test
/// reaches under the same address, and the test reaches every listener of the process on a free port of its own.
const NETWORK_OF_THE_MACHINE: &str = "host";

/// The user that the process runs as: the files of the workspace that the crate copies in belong to it, so the
/// process writes its definitions and its recordings there.
const OWNER_OF_THE_WORKSPACE: &str = "0:0";

/// The label of every container this crate starts, so the containers of the tests of a machine are found by it.
const LABEL: &str = "gaslightmq.testcontainers";

/// GaslightMQ in a container next to a test: `gmq proxy` in front of a real cluster, with its control plane.
///
/// It runs from [`start`](GaslightMqContainerConfiguration::start) until the test [stops](Self::stop) it.
/// It runs the [image](GaslightMqContainerConfiguration::image) of GaslightMQ, and starts once the process said
/// that it is ready and the control plane says that its environment runs.
/// Every listener takes a free port, so tests run side by side.
///
/// The test reaches the proxy through the [control plane](Self::control), as it reaches any other process of
/// GaslightMQ, and its clients connect to the [bootstrap address](Self::bootstrap) of the proxy.
pub struct GaslightMqContainer {
    container: ContainerAsync<GenericImage>,
    control: ControlClient,
    environment: EnvironmentName,
    bootstrap: SocketAddr,
    control_plane: SocketAddr,
    run: RunId,
    stop_timeout: TimeSpan,
}

impl GaslightMqContainer {
    /// A proxy in front of the real cluster whose bootstrap server is `cluster`, with every default.
    ///
    /// [`start`](GaslightMqContainerConfiguration::start) starts it, and the test changes what it wants to first.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> GaslightMqContainerConfiguration {
        GaslightMqContainerConfiguration::proxy_in_front_of(cluster)
    }

    /// The address that a client of the test bootstraps from: the bootstrap listener of the proxy.
    ///
    /// The proxy names its brokers to the client under its
    /// [advertised host](GaslightMqContainerConfiguration::advertised_host), each on a free port of its own.
    pub fn bootstrap(&self) -> SocketAddr {
        self.bootstrap
    }

    /// The bootstrap address as a client takes it in `bootstrap.servers`: `127.0.0.1:54321`.
    pub fn bootstrap_servers(&self) -> String {
        self.bootstrap.to_string()
    }

    /// The name of the environment that the proxy runs in, which every request about it names.
    pub fn environment(&self) -> &EnvironmentName {
        &self.environment
    }

    /// A client of the control plane, whose every request says that it comes from a test.
    ///
    /// It names no presence, so a hold that it adds ends by its release or by a disarm alone.
    /// A test that has the process to itself leaves `expects` out: the last change wins.
    pub fn control(&self) -> &ControlClient {
        &self.control
    }

    /// Where the control plane listens, for another client: the UI in a browser, or one with a configuration of its
    /// own.
    pub fn control_plane(&self) -> ControlPlaneAddress {
        self.control_plane.into()
    }

    /// The ID of the run of the process, which it made when it started.
    pub fn run(&self) -> RunId {
        self.run
    }

    /// Asks the process to stop, kills it once the [stop timeout](GaslightMqContainerConfiguration::stop_timeout) is
    /// over, and waits until the container is gone.
    ///
    /// Every address is free once the call returns.
    /// A test that lets go of the value without a stop removes the container at once, and waits for that.
    pub async fn stop(self) {
        let seconds = self.stop_timeout.as_millis().div_ceil(1000);
        // A container that is gone already, or a machine that no longer answers, leaves nothing to stop.
        let _ = self
            .container
            .stop_with_timeout(Some(i32::try_from(seconds).unwrap_or(i32::MAX)))
            .await;
        let _ = self.container.rm().await;
    }
}

impl fmt::Debug for GaslightMqContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GaslightMqContainer")
            .field("environment", &self.environment)
            .field("bootstrap", &self.bootstrap)
            .field("control_plane", &self.control_plane)
            .field("run", &self.run)
            .finish_non_exhaustive()
    }
}

impl GaslightMqContainerConfiguration {
    /// Starts GaslightMQ in a container next to the test, and returns once it is ready.
    ///
    /// The container runs the image on the network of the machine of the test, so the process reaches the cluster
    /// under the address that the test hands over, and every broker of it under the address that it advertises.
    /// The files of the workspace are copied into the container before the process starts.
    /// The process is ready once it said where it listens, and its control plane says that its environment runs.
    /// The image is pulled first when the machine does not hold it.
    ///
    /// # Errors
    ///
    /// - [`GaslightMqContainerStartError::InvalidImage`] when the environment names no image.
    /// - [`GaslightMqContainerStartError::WorkspaceUnreadable`] when the workspace of the test cannot be read.
    /// - [`GaslightMqContainerStartError::ImageMissing`] when the machine does not hold the image, and cannot pull it.
    /// - [`GaslightMqContainerStartError::NotStarted`] when no container starts.
    /// - [`GaslightMqContainerStartError::NotReady`] when the process does not get ready within the start timeout.
    pub async fn start(self) -> Result<GaslightMqContainer, GaslightMqContainerStartError> {
        let image = match &self.image {
            Some(image) => image.clone(),
            None => image_of_the_environment()?,
        };
        let mut request = GenericImage::new(image.repository(), image.tag())
            .with_cmd(self.arguments())
            .with_network(NETWORK_OF_THE_MACHINE)
            .with_user(OWNER_OF_THE_WORKSPACE)
            .with_label(LABEL, "")
            // The crate asks the control plane itself, wherever it listens.
            .with_health_check(Healthcheck::none());
        if let Some(directory) = &self.workspace {
            require_directory(directory).await?;
            request = request.with_copy_to(WORKSPACE_IN_THE_CONTAINER, directory.clone());
        }
        let container = request.start().await.map_err(|error| self.not_started(&image, error))?;

        match self.ready(&container).await {
            Ok((announced, control)) => Ok(GaslightMqContainer {
                container,
                control,
                environment: self.environment,
                bootstrap: announced.bootstrap,
                control_plane: announced.control_plane,
                run: announced.run,
                stop_timeout: self.stop_timeout,
            }),
            Err(error) => {
                let _ = container.rm().await;
                Err(error)
            }
        }
    }

    /// Wait until the process in `container` said where it listens and that it is ready, and its control plane says
    /// that its environment runs.
    async fn ready(
        &self,
        container: &ContainerAsync<GenericImage>,
    ) -> Result<(Announced, ControlClient), GaslightMqContainerStartError> {
        let mut printed = Printed::keeping(self.log_lines_in_errors);
        let mut announcement = Announcement::default();
        let mut lines = container.stdout(true).lines();
        let followed = tokio::time::timeout(self.start_timeout.duration(), async {
            // An output that cannot be read any further is the end of the process too.
            while let Ok(Some(line)) = lines.next_line().await {
                announcement.take(&line);
                printed.take(line);
                if announcement.is_ready() {
                    return true;
                }
            }
            false
        })
        .await;
        let not_ready = |reason: String, printed: Printed| async move {
            GaslightMqContainerStartError::NotReady {
                reason,
                log: printed.with_errors_of(container).await,
            }
        };
        match followed {
            Ok(true) => {}
            Ok(false) => return Err(not_ready("the process ended before it was ready".to_owned(), printed).await),
            Err(_) => {
                let reason = format!(
                    "the process did not say that it is ready within {} ms",
                    self.start_timeout.as_millis()
                );
                return Err(not_ready(reason, printed).await);
            }
        }
        let announced = match announcement.announced() {
            Ok(announced) => announced,
            Err(reason) => return Err(not_ready(reason, printed).await),
        };

        let mut configuration = ControlClientConfiguration::new(announced.control_plane.into());
        configuration.surface = RequestSurface::Test;
        let control = ControlClient::new(configuration);
        let reason = match control.send(CheckHealth {}).await {
            Ok(Ok(_)) => return Ok((announced, control)),
            Ok(Err(unhealthy)) => unhealthy.to_string(),
            Err(unanswered) => unanswered.to_string(),
        };
        Err(not_ready(reason, printed).await)
    }

    /// The error of a container of `image` that did not start, for what the machine said.
    fn not_started(&self, image: &GaslightMqImage, error: TestcontainersError) -> GaslightMqContainerStartError {
        match error {
            TestcontainersError::Client(ClientError::PullImage { .. }) => GaslightMqContainerStartError::ImageMissing {
                image: image.clone(),
                reason: error.to_string(),
            },
            TestcontainersError::Client(ClientError::CopyToContainerError(_)) => {
                GaslightMqContainerStartError::WorkspaceUnreadable {
                    directory: self.workspace.clone().unwrap_or_default(),
                    reason: error.to_string(),
                }
            }
            other => GaslightMqContainerStartError::NotStarted {
                reason: other.to_string(),
            },
        }
    }
}

/// The last lines that the process printed.
struct Printed {
    lines: VecDeque<String>,
    keep: usize,
}

impl Printed {
    fn keeping(keep: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            keep,
        }
    }

    fn take(&mut self, line: String) {
        if self.keep == 0 {
            return;
        }
        if self.lines.len() >= self.keep {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }

    /// The last lines of what the process printed, and of what it wrote about its errors after them.
    async fn with_errors_of(mut self, container: &ContainerAsync<GenericImage>) -> String {
        if let Ok(errors) = container.stderr_to_vec().await {
            for line in String::from_utf8_lossy(&errors).lines() {
                self.take(line.to_owned());
            }
        }
        Vec::from(self.lines).join("\n")
    }
}

/// Check that `directory` is a directory that the crate can copy into a container.
async fn require_directory(directory: &Path) -> Result<(), GaslightMqContainerStartError> {
    let unreadable = |reason: String| GaslightMqContainerStartError::WorkspaceUnreadable {
        directory: directory.to_owned(),
        reason,
    };
    match tokio::fs::metadata(directory).await {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(unreadable("it is no directory".to_owned())),
        Err(error) => Err(unreadable(error.to_string())),
    }
}

/// The image that the environment names for every container of the test run, or the default image.
fn image_of_the_environment() -> Result<GaslightMqImage, GaslightMqContainerStartError> {
    match std::env::var_os(GaslightMqImage::VARIABLE) {
        Some(text) if !text.is_empty() => text
            .to_string_lossy()
            .parse()
            .map_err(|error| GaslightMqContainerStartError::InvalidImage { error }),
        _ => Ok(GaslightMqImage::default()),
    }
}
