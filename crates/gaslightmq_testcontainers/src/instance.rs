//! GaslightMQ that runs in a container next to a test, and how it starts and stops.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::environment::{EnvironmentName, ServerAddress};
use gaslightmq_control_api::{CheckHealth, RequestSurface, RunId};
use gaslightmq_control_client::{ControlClient, ControlClientConfiguration, ControlPlaneAddress};
use testcontainers::bollard::Docker;
use testcontainers::bollard::query_parameters::AttachContainerOptions;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::client::ClientError;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::announcement::Announcement;
use crate::configuration::{BOOTSTRAP_PORT, CONTROL_PLANE_PORT, WORKSPACE_IN_THE_CONTAINER, advertised_listeners};
use crate::{GaslightMqContainerConfiguration, GaslightMqContainerStartError, GaslightMqImage};

/// The label of every container this crate starts, so the containers of the tests of a machine are found by it.
const LABEL: &str = "gaslightmq.testcontainers";

/// A host port of 0: the container runtime publishes the port on a free port of the machine.
const ANY_HOST_PORT: u16 = 0;

/// The user that the process runs as: the files of the workspace that the crate copies in belong to it, so the
/// process writes its definitions and its recordings there. The image itself runs as nobody.
const OWNER_OF_THE_WORKSPACE: &str = "0:0";

/// GaslightMQ in a container next to a test: `gmq proxy` in front of a real cluster, with its control plane.
///
/// It runs from [`start`](GaslightMqContainerConfiguration::start) until the test [stops](Self::stop) it.
/// It runs the [image](GaslightMqContainerConfiguration::image) of GaslightMQ, and the test holds it once the process
/// said that it is ready and the control plane says that its environment runs.
/// The container publishes every listener on a free port of the machine, so tests run side by side, and the proxy
/// names its brokers to its clients under the ports they are published on.
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
    /// A proxy in front of the real cluster whose bootstrap server is `cluster`, as the container reaches it, with
    /// every default.
    ///
    /// [`start`](GaslightMqContainerConfiguration::start) starts it, and the test changes what it wants to first.
    pub fn proxy_in_front_of(cluster: ServerAddress) -> GaslightMqContainerConfiguration {
        GaslightMqContainerConfiguration::proxy_in_front_of(cluster)
    }

    /// The address that a client of the test bootstraps from: the bootstrap listener of the proxy, where the
    /// container publishes it.
    ///
    /// The proxy names its brokers to the client under the ports that they are published on.
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

    /// Where the container publishes the control plane, for another client: the UI in a browser, or one with a
    /// configuration of its own.
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
    /// Every published port is free once the call returns.
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

/// Where a container publishes its ports, as the test reaches them.
struct Published {
    /// The host of the machine that runs the containers, as the test reaches it: `localhost`, for one.
    host: String,
    /// The port of the machine for each port of the container.
    ports: BTreeMap<u16, u16>,
}

impl Published {
    /// The address under which the test reaches `port` of the container.
    async fn address_of(&self, port: u16) -> Result<SocketAddr, String> {
        let published = self.ports.get(&port).copied().ok_or_else(|| unpublished(port))?;
        let mut addresses = tokio::net::lookup_host((self.host.as_str(), published))
            .await
            .map_err(|error| format!("the host {} of the containers does not resolve: {error}", self.host))?;
        addresses
            .next()
            .ok_or_else(|| format!("the host {} of the containers names no address", self.host))
    }
}

/// The text of a port of the container that was not published.
fn unpublished(port: u16) -> String {
    format!("the container runtime did not say where port {port} of the container is published")
}

impl GaslightMqContainerConfiguration {
    /// Starts GaslightMQ in a container next to the test, and returns once it is ready.
    ///
    /// The container joins the [network](Self::network) that the test names, and publishes the bootstrap listener,
    /// a listener for each of the [brokers](Self::brokers) and the control plane on free ports of the machine.
    /// The files of the workspace are copied into the container before the process starts.
    /// Once the container runs, the process learns where its brokers are published, and names them to its clients
    /// there. It is ready once it said so, and its control plane says that its environment runs.
    /// The image is pulled first when the machine does not hold it.
    ///
    /// # Errors
    ///
    /// - [`GaslightMqContainerStartError::InvalidImage`] when the environment names no image.
    /// - [`GaslightMqContainerStartError::WorkspaceUnreadable`] when the workspace of the test cannot be read.
    /// - [`GaslightMqContainerStartError::ImageMissing`] when the machine does not hold the image, and cannot pull it.
    /// - [`GaslightMqContainerStartError::ContainerNotStarted`] when no container starts.
    /// - [`GaslightMqContainerStartError::NotReady`] when the process in the container does not get ready within the
    ///   start timeout.
    pub async fn start(self) -> Result<GaslightMqContainer, GaslightMqContainerStartError> {
        let image = match &self.image {
            Some(image) => image.clone(),
            None => image_of_the_environment()?,
        };
        let mut request = GenericImage::new(image.repository(), image.tag())
            .with_cmd(self.arguments())
            .with_open_stdin(true)
            .with_user(OWNER_OF_THE_WORKSPACE)
            .with_label(LABEL, "");
        for port in self.published_ports() {
            request = request.with_mapped_port(ANY_HOST_PORT, port.tcp());
        }
        if let Some(network) = &self.network {
            request = request.with_network(network);
        }
        if let Some(directory) = &self.workspace {
            require_directory(directory).await?;
            request = request.with_copy_to(WORKSPACE_IN_THE_CONTAINER, directory.clone());
        }
        let container = request.start().await.map_err(|error| self.not_started(&image, error))?;

        match self.ready(&container).await {
            Ok(ready) => Ok(GaslightMqContainer {
                container,
                control: ready.control,
                environment: self.environment,
                bootstrap: ready.bootstrap,
                control_plane: ready.control_plane,
                run: ready.run,
                stop_timeout: self.stop_timeout,
            }),
            Err(error) => {
                let _ = container.rm().await;
                Err(error)
            }
        }
    }

    /// Hand the process in `container` the advertised listeners of its brokers, wait until it said that it is ready,
    /// and until its control plane says that its environment runs.
    async fn ready(&self, container: &ContainerAsync<GenericImage>) -> Result<Ready, GaslightMqContainerStartError> {
        let mut printed = Printed::keeping(self.log_lines_in_errors);
        let not_ready = |reason: String, printed: Printed| async move {
            GaslightMqContainerStartError::NotReady {
                reason,
                log: printed.with_errors_of(container).await,
            }
        };
        let published = match published(container, &self.published_ports()).await {
            Ok(published) => published,
            Err(reason) => return Err(not_ready(reason, printed).await),
        };
        let advertised = advertised_listeners(self, &published.host, |port| published.ports.get(&port).copied())
            .map_err(unpublished);
        let handed = match advertised {
            Ok(advertised) => hand_over(container.id(), &advertised).await,
            Err(reason) => Err(reason),
        };
        if let Err(reason) = handed {
            return Err(not_ready(reason, printed).await);
        }

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
        let run = match (followed, announcement.run()) {
            (Ok(true), Some(run)) => run,
            (Ok(true), None) => {
                let reason = "the process said that it is ready, and did not say its run".to_owned();
                return Err(not_ready(reason, printed).await);
            }
            (Ok(false), _) => {
                return Err(not_ready("the process ended before it was ready".to_owned(), printed).await);
            }
            (Err(_), _) => {
                let reason = format!(
                    "the process did not say that it is ready within {} ms",
                    self.start_timeout.as_millis()
                );
                return Err(not_ready(reason, printed).await);
            }
        };
        let addresses = match (
            published.address_of(BOOTSTRAP_PORT).await,
            published.address_of(CONTROL_PLANE_PORT).await,
        ) {
            (Ok(bootstrap), Ok(control_plane)) => (bootstrap, control_plane),
            (Err(reason), _) | (_, Err(reason)) => return Err(not_ready(reason, printed).await),
        };
        let (bootstrap, control_plane) = addresses;

        let mut configuration = ControlClientConfiguration::new(control_plane.into());
        configuration.surface = RequestSurface::Test;
        let control = ControlClient::new(configuration);
        let reason = match control.send(CheckHealth {}).await {
            Ok(Ok(_)) => {
                return Ok(Ready {
                    control,
                    bootstrap,
                    control_plane,
                    run,
                });
            }
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
            other => GaslightMqContainerStartError::ContainerNotStarted {
                reason: other.to_string(),
            },
        }
    }
}

/// What a process that is ready hands the test.
struct Ready {
    control: ControlClient,
    bootstrap: SocketAddr,
    control_plane: SocketAddr,
    run: RunId,
}

/// Where `container` publishes each of `ports`.
async fn published(container: &ContainerAsync<GenericImage>, ports: &[u16]) -> Result<Published, String> {
    let host = container
        .get_host()
        .await
        .map_err(|error| format!("the container runtime did not say which host its containers are on: {error}"))?;
    let mut published = BTreeMap::new();
    for port in ports {
        let outside = container
            .get_host_port_ipv4(port.tcp())
            .await
            .map_err(|_| unpublished(*port))?;
        published.insert(*port, outside);
    }
    Ok(Published {
        host: host.to_string(),
        ports: published,
    })
}

/// Write `advertised` as a line to the input of the container `id`, which the process reads before it starts.
async fn hand_over(id: &str, advertised: &str) -> Result<(), String> {
    let undelivered = |reason: String| format!("the process did not get the advertised listeners: {reason}");
    let docker = Docker::connect_with_defaults().map_err(|error| undelivered(error.to_string()))?;
    let options = AttachContainerOptions {
        stdin: true,
        stream: true,
        ..AttachContainerOptions::default()
    };
    let mut attached = docker
        .attach_container(id, Some(options))
        .await
        .map_err(|error| undelivered(error.to_string()))?;
    let line = format!("{advertised}\n");
    attached
        .input
        .write_all(line.as_bytes())
        .await
        .map_err(|error| undelivered(error.to_string()))?;
    attached
        .input
        .flush()
        .await
        .map_err(|error| undelivered(error.to_string()))
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
