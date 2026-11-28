//! What `gmq` prints for whoever runs it: where a process listens once it runs, and what a reload changed.
//!
//! Each line says one thing, so a script or a test reads the line it wants by how it starts.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use gaslightmq_control_api::field::Endpoint;
use gaslightmq_control_api::{Health, Reloaded};
use gaslightmq_environment::{EnvironmentName, ServerAddress};
use gaslightmq_host::RunId;
use gaslightmq_workspace_fs::Access;

/// The files that a process holds its definitions and its recordings in, or memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Holding {
    /// The files of the workspace in `directory`, and the recordings in `recordings`.
    Files {
        directory: PathBuf,
        access: Access,
        recordings: PathBuf,
    },
    /// Memory: nothing is written to disk, a recording neither.
    Memory,
}

/// A process of `gmq proxy` that runs: its run, its workspace, where its proxy listens and where its control plane
/// does. It writes one line for each, and `ready` last.
///
/// ```text
/// run 4be0643f1d98573b97cdca98a65347dd
/// no workspace: every definition and every recording lives in memory
/// environment "proxy" in front of kafka-1.staging:9092
/// bootstrap on 0.0.0.0:9092
/// broker 1 on 0.0.0.0:19092
/// control plane and UI on http://127.0.0.1:7092
/// ready: Ctrl-C stops it
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Running {
    pub(crate) run: RunId,
    pub(crate) holding: Holding,
    pub(crate) environment: EnvironmentName,
    pub(crate) upstream: ServerAddress,
    /// Every endpoint of the proxy that accepts connections, with its address.
    pub(crate) endpoints: Vec<(Endpoint, SocketAddr)>,
    /// Where the control plane listens, or `None` for a process without one.
    pub(crate) control: Option<SocketAddr>,
}

impl fmt::Display for Running {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "run {}", self.run)?;
        match &self.holding {
            Holding::Files {
                directory,
                access,
                recordings,
            } => {
                let access = match access {
                    Access::ReadWrite => "each change written back",
                    Access::WriteOnSave => "written back on save",
                    Access::ReadOnly => "read-only",
                };
                writeln!(f, "workspace {}, {access}", directory.display())?;
                writeln!(f, "recordings in {}", recordings.display())?;
            }
            Holding::Memory => writeln!(f, "no workspace: every definition and every recording lives in memory")?,
        }
        writeln!(
            f,
            "environment {:?} in front of {}",
            self.environment.as_str(),
            self.upstream
        )?;
        for (endpoint, address) in &self.endpoints {
            writeln!(f, "{endpoint} on {address}")?;
        }
        match self.control {
            Some(address) => writeln!(f, "control plane and UI on http://{address}")?,
            None => writeln!(f, "no control plane")?,
        }
        write!(f, "ready: Ctrl-C stops it")
    }
}

/// What a reload took in: a line for each definition that it changed, and its revision last.
///
/// ```text
/// rule "order-acknowledgements-get-lost" changed
/// reloaded at revision 12
/// ```
pub(crate) struct ReloadReport<'a>(pub(crate) &'a Reloaded);

impl fmt::Display for ReloadReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Reloaded { revision, changes } = self.0;
        for change in changes {
            writeln!(f, "{change}")?;
        }
        if changes.is_empty() {
            write!(f, "nothing changed, at revision {revision}")
        } else {
            write!(f, "reloaded at revision {revision}")
        }
    }
}

/// The environments that a process was started with: a line for each, which says whether it runs.
///
/// ```text
/// environment "proxy" runs
/// environment "orders-sim" does not run
/// ```
pub(crate) struct HealthReport<'a>(pub(crate) &'a Health);

impl fmt::Display for HealthReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Health { environments } = self.0;
        if environments.is_empty() {
            return write!(f, "the process started with no environment");
        }
        for (position, environment) in environments.iter().enumerate() {
            let separator = if position == 0 { "" } else { "\n" };
            let runs = if environment.running { "runs" } else { "does not run" };
            write!(
                f,
                "{separator}environment {:?} {runs}",
                environment.environment.as_str()
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_control_api::kernel::BrokerId;
    use gaslightmq_control_api::{DefinitionChange, EnvironmentHealth, Revision};

    use super::*;

    #[test]
    fn a_running_proxy_says_where_it_listens_and_that_it_is_ready_last() {
        let running = Running {
            run: RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd),
            holding: Holding::Files {
                directory: "gaslight".into(),
                access: Access::ReadOnly,
                recordings: "gaslight/recordings".into(),
            },
            environment: "proxy".parse().unwrap(),
            upstream: "kafka-1.staging:9092".parse().unwrap(),
            endpoints: vec![
                (Endpoint::Bootstrap, "0.0.0.0:9092".parse().unwrap()),
                (Endpoint::Broker(BrokerId::from(1)), "0.0.0.0:19092".parse().unwrap()),
            ],
            control: Some("127.0.0.1:7092".parse().unwrap()),
        };

        assert_eq!(
            running.to_string(),
            "run 4be0643f1d98573b97cdca98a65347dd\n\
             workspace gaslight, read-only\n\
             recordings in gaslight/recordings\n\
             environment \"proxy\" in front of kafka-1.staging:9092\n\
             bootstrap on 0.0.0.0:9092\n\
             broker 1 on 0.0.0.0:19092\n\
             control plane and UI on http://127.0.0.1:7092\n\
             ready: Ctrl-C stops it"
        );
    }

    #[test]
    fn a_proxy_without_files_or_control_plane_says_so() {
        let running = Running {
            run: RunId::from_random_bits(1),
            holding: Holding::Memory,
            environment: "proxy".parse().unwrap(),
            upstream: "kafka:9092".parse().unwrap(),
            endpoints: Vec::new(),
            control: None,
        };

        let lines: Vec<String> = running.to_string().lines().map(str::to_owned).collect();
        assert_eq!(
            lines[1],
            "no workspace: every definition and every recording lives in memory"
        );
        assert_eq!(lines[3], "no control plane");
    }

    #[test]
    fn a_reload_names_each_change_and_its_revision() {
        let reloaded = Reloaded {
            revision: Revision::new(12),
            changes: vec![DefinitionChange::Updated(
                "order-acknowledgements-get-lost"
                    .parse::<gaslightmq_control_api::rule::RuleName>()
                    .unwrap()
                    .into(),
            )],
        };

        assert_eq!(
            ReloadReport(&reloaded).to_string(),
            "rule \"order-acknowledgements-get-lost\" changed\nreloaded at revision 12"
        );
    }

    #[test]
    fn a_health_check_says_of_each_environment_whether_it_runs() {
        let health = Health {
            environments: vec![
                EnvironmentHealth {
                    environment: "proxy".parse().unwrap(),
                    running: true,
                },
                EnvironmentHealth {
                    environment: "orders-sim".parse().unwrap(),
                    running: false,
                },
            ],
        };

        assert_eq!(
            HealthReport(&health).to_string(),
            "environment \"proxy\" runs\nenvironment \"orders-sim\" does not run"
        );
        assert_eq!(
            HealthReport(&Health::default()).to_string(),
            "the process started with no environment"
        );
    }

    #[test]
    fn a_reload_that_changed_nothing_says_so() {
        let reloaded = Reloaded {
            revision: Revision::new(3),
            changes: Vec::new(),
        };

        assert_eq!(ReloadReport(&reloaded).to_string(), "nothing changed, at revision 3");
    }
}
