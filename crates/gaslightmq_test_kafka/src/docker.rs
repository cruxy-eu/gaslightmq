//! The `docker` command, and the one container that a cluster runs in.
//!
//! # One container, however many brokers
//!
//! Every broker of a cluster is a process of its own in the same container.
//! A cluster of three brokers then starts as fast as one container starts, and needs no Docker network:
//! the brokers reach each other on `localhost`, and Docker publishes one port per broker for the clients.
//!
//! # The container reads its configuration from its input
//!
//! A broker advertises the address that clients reach it at, and that is a port Docker picks when the container starts.
//! The container therefore starts with a script that waits, see `entrypoint.sh`.
//! The cluster reads the ports, writes the `server.properties` of every broker to the input of the container,
//! and the script then sets up and starts the brokers.
//!
//! # The container ends with its input
//!
//! The script ends when the input of the container closes, and the container ends with it.
//! The input is a pipe that the test's process holds.
//! It closes when the cluster is dropped, and also when the process ends in any other way: a panic, a kill, a crash.
//! No container outlives its test, also when the cluster lives in a `static` that nothing ever drops.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use tokio::sync::watch;

use crate::error::KafkaClusterError;
use crate::image::KafkaImage;

/// The command that talks to Docker.
const DOCKER: &str = "docker";

/// What the container runs: it reads the configuration of each broker from its input, and starts the brokers.
const ENTRYPOINT: &str = include_str!("entrypoint.sh");

/// The line the script writes once it reads its input.
const AWAITING_CONFIGURATION: &str = "gaslightmq: awaiting configuration";

/// The words a broker logs once it serves clients.
const BROKER_STARTED: &str = "Kafka Server started";

/// The line of the input that starts the properties of one broker, followed by the ID of the broker.
const BROKER: &str = "# broker ";

/// The line of the input after which the script starts the brokers.
const START: &str = "# start";

/// The ID every cluster formats its storage with. A cluster shares no storage, so one ID serves them all.
const CLUSTER_ID: &str = "5L6g3nShT-eMCtK--X86sw";

/// The label of every container this crate starts, so `docker ps --filter label=gaslightmq.test_kafka` finds them.
const LABEL: &str = "gaslightmq.test_kafka";

/// The containers this process started, so each one gets a name of its own.
static STARTED: AtomicU64 = AtomicU64::new(0);

/// How far the container is.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Progress {
    /// The script reads its input.
    pub awaiting_configuration: bool,
    /// How many brokers said that they serve clients.
    pub brokers_started: usize,
    /// The container ended.
    ended: bool,
}

/// Why a container did not get as far as someone waited for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stalled {
    /// The container ended first.
    Ended,
    /// The time ran out first.
    TimedOut,
}

/// What the container wrote, and how far it is.
struct Following {
    /// The last lines the container wrote.
    lines: Mutex<VecDeque<String>>,
    /// How many lines to keep.
    keep: usize,
    progress: watch::Sender<Progress>,
}

impl Following {
    fn take(&self, line: &str) {
        if line.contains(AWAITING_CONFIGURATION) {
            self.progress
                .send_modify(|progress| progress.awaiting_configuration = true);
        }
        if line.contains(BROKER_STARTED) {
            self.progress.send_modify(|progress| progress.brokers_started += 1);
        }
        let mut lines = self.lines.lock().unwrap_or_else(PoisonError::into_inner);
        if lines.len() >= self.keep {
            lines.pop_front();
        }
        if self.keep > 0 {
            lines.push_back(line.to_owned());
        }
    }
}

impl Following {
    /// The last lines the container wrote.
    fn log(&self) -> String {
        let lines = self.lines.lock().unwrap_or_else(PoisonError::into_inner);
        lines.iter().map(String::as_str).collect::<Vec<_>>().join("\n")
    }
}

/// Read `pipe` to its end on a thread of its own, so the container never waits for someone to take what it writes.
/// The container has ended once `ends_the_container` has reached its end.
fn follow(pipe: impl Read + Send + 'static, following: Arc<Following>, ends_the_container: bool) {
    thread::spawn(move || {
        let mut pipe = BufReader::new(pipe);
        let mut line = Vec::new();
        while pipe.read_until(b'\n', &mut line).is_ok_and(|read| read > 0) {
            following.take(String::from_utf8_lossy(&line).trim_end());
            line.clear();
        }
        if ends_the_container {
            following.progress.send_modify(|progress| progress.ended = true);
        }
    });
}

/// Run `docker` with `arguments`, and wait for it.
fn docker(arguments: &[&str]) -> Result<Output, KafkaClusterError> {
    Command::new(DOCKER)
        .args(arguments)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| not_there(&error))
}

/// The error of a machine on which the `docker` command does not run.
fn not_there(error: &io::Error) -> KafkaClusterError {
    KafkaClusterError::not_started(format!(
        "it needs Docker, and the `docker` command did not run: {error}"
    ))
}

/// Everything `docker` wrote, on one line each.
fn said(output: &Output) -> String {
    let text = [&output.stdout, &output.stderr].map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned());
    text.into_iter()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Check that the `docker` command reaches a Docker engine.
pub(crate) fn require_engine() -> Result<(), KafkaClusterError> {
    let version = docker(&["version", "--format", "{{.Server.Version}}"])?;
    if version.status.success() {
        Ok(())
    } else {
        Err(KafkaClusterError::not_started(format!(
            "it needs Docker, and the container runtime is not running: {}",
            said(&version)
        )))
    }
}

/// Pull `image`, unless Docker holds it already.
pub(crate) fn require_image(image: &KafkaImage) -> Result<(), KafkaClusterError> {
    let image_name = image.to_string();
    if docker(&["image", "inspect", "--format", "{{.Id}}", &image_name])?
        .status
        .success()
    {
        return Ok(());
    }
    let pulled = docker(&["pull", "--quiet", &image_name])?;
    if pulled.status.success() {
        Ok(())
    } else {
        Err(KafkaClusterError::not_started(format!(
            "the image {image} is not there and could not be pulled: {}",
            said(&pulled)
        )))
    }
}

/// One running container, which holds every broker of a cluster.
pub(crate) struct Container {
    name: String,
    /// The `docker run` that started the container, and stays attached to it.
    attached: Child,
    /// The input of the container. The container ends when this closes.
    input: Option<ChildStdin>,
    following: Arc<Following>,
}

impl Container {
    /// Start a container of `image` that waits for the configuration of its brokers.
    /// Docker publishes each of `ports` on a free port of the host.
    /// An error shows the last `log_lines` lines the container wrote.
    pub fn run(image: &KafkaImage, ports: &[u16], log_lines: usize) -> Result<Self, KafkaClusterError> {
        let number = STARTED.fetch_add(1, Ordering::Relaxed) + 1;
        let name = format!("gaslightmq-test-kafka-{}-{number}", std::process::id());
        let mut run = Command::new(DOCKER);
        run.args(["run", "--rm", "--interactive", "--name", &name, "--label", LABEL])
            .args(["--env", &format!("CLUSTER_ID={CLUSTER_ID}")]);
        for port in ports {
            run.args(["--publish", &format!("127.0.0.1::{port}")]);
        }
        run.args(["--entrypoint", "bash", &image.to_string(), "-c", ENTRYPOINT])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut attached = run.spawn().map_err(|error| not_there(&error))?;
        let following = Arc::new(Following {
            lines: Mutex::default(),
            keep: log_lines,
            progress: watch::Sender::new(Progress::default()),
        });
        if let Some(output) = attached.stdout.take() {
            follow(output, Arc::clone(&following), true);
        }
        if let Some(errors) = attached.stderr.take() {
            follow(errors, Arc::clone(&following), false);
        }
        Ok(Self {
            name,
            input: attached.stdin.take(),
            attached,
            following,
        })
    }

    /// Wait until the container is as far as `reached` says, for at most `within`.
    pub async fn reaches(&self, reached: impl Fn(&Progress) -> bool, within: Duration) -> Result<(), Stalled> {
        let mut progress = self.following.progress.subscribe();
        let waited = progress.wait_for(|progress| reached(progress) || progress.ended);
        match tokio::time::timeout(within, waited).await {
            Ok(Ok(progress)) if reached(&progress) => Ok(()),
            Ok(_) => Err(Stalled::Ended),
            Err(_) => Err(Stalled::TimedOut),
        }
    }

    /// The address on the host that Docker published each port of the container on.
    pub fn published_ports(&self) -> Result<BTreeMap<u16, SocketAddr>, KafkaClusterError> {
        let listed = docker(&["port", &self.name])?;
        let unknown = || {
            KafkaClusterError::not_started(format!(
                "the container runtime did not say where clients reach the brokers: {}",
                said(&listed)
            ))
        };
        if !listed.status.success() {
            return Err(unknown());
        }
        // One line per port: `19001/tcp -> 127.0.0.1:55001`.
        String::from_utf8_lossy(&listed.stdout)
            .lines()
            .map(|line| {
                let (inside, outside) = line.split_once(" -> ")?;
                let (port, _protocol) = inside.split_once('/')?;
                Some((port.parse().ok()?, outside.parse().ok()?))
            })
            .collect::<Option<_>>()
            .ok_or_else(unknown)
    }

    /// Hand the container the properties of each broker, by broker ID, and let it start them.
    pub fn configure(&mut self, properties: &BTreeMap<u8, String>) -> Result<(), KafkaClusterError> {
        let mut configuration = String::new();
        for (broker, lines) in properties {
            configuration.push_str(BROKER);
            configuration.push_str(&broker.to_string());
            configuration.push('\n');
            configuration.push_str(lines);
        }
        configuration.push_str(START);
        configuration.push('\n');
        let undelivered = |error: io::Error| KafkaClusterError::NotStarted {
            reason: format!("the brokers did not take their configuration: {error}"),
            log: self.following.log(),
        };
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| undelivered(io::ErrorKind::BrokenPipe.into()))?;
        input
            .write_all(configuration.as_bytes())
            .and_then(|()| input.flush())
            .map_err(undelivered)
    }

    /// The last lines the container wrote.
    pub fn log(&self) -> String {
        self.following.log()
    }
}

/// Closing the input ends the container.
/// Docker is told to remove it too, for the case that the script no longer reads its input.
impl Drop for Container {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = docker(&["rm", "--force", "--volumes", &self.name]);
        let _ = self.attached.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_script_and_the_cluster_agree_on_what_they_tell_each_other() {
        assert!(ENTRYPOINT.contains(&format!("echo '{AWAITING_CONFIGURATION}'")));
        assert!(ENTRYPOINT.contains(&format!("'{BROKER}'*)")));
        assert!(ENTRYPOINT.contains(&format!("'{START}') break")));
    }

    fn running(name: &str) -> bool {
        let listed = docker(&["ps", "--quiet", "--filter", &format!("name={name}")]).unwrap();
        !listed.stdout.is_empty()
    }

    #[tokio::test]
    async fn a_container_runs_until_it_is_let_go() {
        let image = KafkaImage::default();
        require_engine().unwrap_or_else(|error| panic!("{error}"));
        require_image(&image).unwrap_or_else(|error| panic!("{error}"));

        let container = Container::run(&image, &[19_001], 10).unwrap();
        container
            .reaches(|progress| progress.awaiting_configuration, Duration::from_secs(60))
            .await
            .unwrap();
        let name = container.name.clone();
        let published = container.published_ports().unwrap();

        assert!(running(&name));
        assert!(published[&19_001].ip().is_loopback());
        assert_ne!(published[&19_001].port(), 0);

        drop(container);

        assert!(!running(&name));
    }

    #[test]
    fn progress_follows_what_the_container_writes() {
        let following = Following {
            lines: Mutex::default(),
            keep: 2,
            progress: watch::Sender::new(Progress::default()),
        };

        following.take(AWAITING_CONFIGURATION);
        following.take("[KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)");
        following.take("[KafkaRaftServer nodeId=2] Kafka Server started (kafka.server.KafkaRaftServer)");

        let progress = *following.progress.borrow();
        assert!(progress.awaiting_configuration);
        assert_eq!(progress.brokers_started, 2);
        // Only the last lines are kept.
        assert_eq!(following.lines.lock().unwrap().len(), 2);
    }
}
