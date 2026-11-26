//! The commands of `gmq`, with their flags and the environment variables that stand in for them.
//!
//! A flag wins over its variable, and the variable over the default.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use gaslightmq_control_client::ControlPlaneAddress;
use gaslightmq_control_server::ControlServerConfiguration;
use gaslightmq_environment::{
    BrokerListeners, Environment, EnvironmentName, HostName, Instances, ListenAddress, Listeners, ListenersError,
    ProxyDefinition, ServerAddress, Upstream,
};
use gaslightmq_workspace_fs::Access;

use crate::brokers::broker_listeners;

/// GaslightMQ: a Kafka proxy that lies to its clients on purpose.
#[derive(Debug, Parser)]
#[command(name = "gmq", version)]
pub(crate) struct Gmq {
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// What `gmq` does.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Run a proxy in front of a real Kafka cluster, with the control plane and the UI in front of it.
    ///
    /// The flags describe the environment of the proxy, without a file.
    /// Without `--workspace`, every definition lives in memory, and nothing is written to disk.
    Proxy(ProxyFlags),
    /// Ask a process that runs to read the files of its workspace again, and print what changed.
    ///
    /// It fails with every error when the files do not read, or do not fit each other, and the process then
    /// keeps what it holds.
    Reload(ReloadFlags),
}

/// The flags of `gmq proxy`.
#[derive(Debug, Args)]
pub(crate) struct ProxyFlags {
    /// The bootstrap server of the real cluster: a host and a port.
    #[arg(long, env = "GMQ_UPSTREAM", value_name = "HOST:PORT")]
    pub(crate) upstream: ServerAddress,
    /// The name of the environment of the proxy, which the control plane and the UI know it by.
    #[arg(long, env = "GMQ_NAME", value_name = "NAME", default_value = "proxy")]
    pub(crate) name: EnvironmentName,
    /// Where clients connect first: the bootstrap listener of the proxy.
    #[arg(long, env = "GMQ_LISTEN", value_name = "IP:PORT", default_value = "0.0.0.0:9092")]
    pub(crate) listen: ListenAddress,
    /// Where the brokers of the cluster listen in the proxy: a range of ports, one for each broker that the proxy
    /// discovers, or a pin for every broker, such as `1=0.0.0.0:19092,2=0.0.0.0:19093`. Port 0 takes a free port.
    #[arg(
        long,
        env = "GMQ_BROKERS",
        value_name = "IP:PORTS",
        default_value = "0.0.0.0:19092-19191",
        value_parser = broker_listeners
    )]
    pub(crate) brokers: BrokerListeners,
    /// The host that clients see in Metadata, under which they reach the brokers of the proxy.
    #[arg(long, env = "GMQ_ADVERTISE", value_name = "HOST", default_value = "localhost")]
    pub(crate) advertise: HostName,
    #[command(flatten)]
    pub(crate) workspace: WorkspaceFlags,
    #[command(flatten)]
    pub(crate) control: ControlPlaneFlags,
}

impl ProxyFlags {
    /// The environment that the flags describe: a proxy in front of the real cluster.
    ///
    /// # Errors
    ///
    /// [`ListenersError`] when the bootstrap listener takes a port of the brokers.
    pub(crate) fn environment(&self) -> Result<Environment, ListenersError> {
        let listeners = Listeners::new(self.listen, self.brokers.clone(), self.advertise.clone())?;
        Ok(Environment::new(
            self.name.clone(),
            Instances::ProxyInFrontOfCluster {
                proxy: ProxyDefinition::new(listeners),
                upstream: Upstream::new(self.upstream.clone()),
            },
        ))
    }
}

/// Which workspace a process holds, and whether it writes back to its files.
#[derive(Debug, Args)]
pub(crate) struct WorkspaceFlags {
    /// The directory of the workspace, whose files hold the definitions. Without it, every definition lives in
    /// memory.
    #[arg(long, env = "GMQ_WORKSPACE", value_name = "DIRECTORY")]
    pub(crate) workspace: Option<PathBuf>,
    /// Read the files of the workspace, and never write them: the UI keeps no rule, and every change of a
    /// definition fails. A reload still reads them again.
    #[arg(long, env = "GMQ_READ_ONLY", requires = "workspace")]
    pub(crate) read_only: bool,
    /// When an edit of the UI reaches the files: as it happens, or when someone saves every edit that waits, in one
    /// go. `write_back` of `gaslight.yaml`. Default: `each-change`.
    #[arg(
        long,
        env = "GMQ_WRITE_BACK",
        value_name = "WHEN",
        requires = "workspace",
        conflicts_with = "read_only"
    )]
    pub(crate) write_back: Option<WriteBack>,
}

/// When an edit of the UI reaches the files of the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum WriteBack {
    /// Every edit writes its file before it answers.
    EachChange,
    /// Every edit waits in memory, and one save writes them all.
    OnSave,
}

impl WorkspaceFlags {
    /// Whether the stores of the workspace write back to its files, and when.
    pub(crate) fn access(&self) -> Access {
        match (self.read_only, self.write_back) {
            (true, _) => Access::ReadOnly,
            (false, None | Some(WriteBack::EachChange)) => Access::ReadWrite,
            (false, Some(WriteBack::OnSave)) => Access::WriteOnSave,
        }
    }
}

/// Where the control plane of a process listens, and whether it has one.
#[derive(Debug, Args)]
pub(crate) struct ControlPlaneFlags {
    /// Where the control plane and the UI listen. Default: `127.0.0.1:7092`.
    #[arg(long, env = "GMQ_CONTROL_LISTEN", value_name = "IP:PORT")]
    pub(crate) control_listen: Option<SocketAddr>,
    /// Run without a control plane, and without the UI.
    #[arg(long, env = "GMQ_NO_CONTROL", conflicts_with = "control_listen")]
    pub(crate) no_control: bool,
}

impl ControlPlaneFlags {
    /// What the control server is configured with, or `None` for a process without one.
    pub(crate) fn configuration(&self) -> Option<ControlServerConfiguration> {
        if self.no_control {
            return None;
        }
        let mut configuration = ControlServerConfiguration::default();
        if let Some(listen) = self.control_listen {
            configuration.listen = listen;
        }
        Some(configuration)
    }
}

/// The control plane of a process that runs, which a command asks.
#[derive(Debug, Args)]
pub(crate) struct ControlFlags {
    /// The address of the control plane of the process. Default: `http://127.0.0.1:7092`, where a process listens
    /// unless it names another address.
    #[arg(long, env = "GMQ_CONTROL", value_name = "URL")]
    pub(crate) control: Option<ControlPlaneAddress>,
}

/// The flags of `gmq reload`.
#[derive(Debug, Args)]
pub(crate) struct ReloadFlags {
    /// Let go of the edits that wait for a save, and take what the files hold. Without it, a reload refuses while
    /// an edit waits.
    #[arg(long, env = "GMQ_DISCARD_UNSAVED")]
    pub(crate) discard_unsaved: bool,
    #[command(flatten)]
    pub(crate) control: ControlFlags,
}

impl ControlFlags {
    /// The address of the control plane: the one the flags name, or the one a process listens on by default.
    pub(crate) fn address(&self) -> ControlPlaneAddress {
        self.control
            .clone()
            .unwrap_or_else(|| ControlServerConfiguration::default().listen.into())
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_commands_hold_together() {
        Gmq::command().debug_assert();
    }

    #[test]
    fn a_proxy_needs_its_upstream_alone() {
        let gmq = Gmq::try_parse_from(["gmq", "proxy", "--upstream", "kafka:9092"]).unwrap();

        let Command::Proxy(flags) = gmq.command else {
            panic!("gmq proxy is the proxy");
        };
        let environment = flags.environment().unwrap();
        assert_eq!(environment.name().as_str(), "proxy");
        let listeners = environment.listeners();
        assert_eq!(listeners.bootstrap(), "0.0.0.0:9092".parse().unwrap());
        assert_eq!(listeners.brokers(), &"0.0.0.0:19092-19191".parse().unwrap());
        assert_eq!(listeners.advertised_host(), &"localhost".parse::<HostName>().unwrap());
        assert_eq!(flags.workspace.workspace, None);
        assert_eq!(flags.workspace.access(), Access::ReadWrite);
        assert_eq!(
            flags.control.configuration(),
            Some(ControlServerConfiguration::default())
        );
    }

    #[test]
    fn a_read_only_workspace_names_its_directory() {
        let without = Gmq::try_parse_from(["gmq", "proxy", "--upstream", "kafka:9092", "--read-only"]);
        assert!(without.is_err());

        let gmq = Gmq::try_parse_from([
            "gmq",
            "proxy",
            "--upstream",
            "kafka:9092",
            "--workspace",
            "gaslight",
            "--read-only",
        ])
        .unwrap();
        let Command::Proxy(flags) = gmq.command else {
            panic!("gmq proxy is the proxy");
        };
        assert_eq!(flags.workspace.access(), Access::ReadOnly);
    }

    #[test]
    fn a_workspace_writes_each_change_until_the_flags_say_on_save() {
        let proxy = ["gmq", "proxy", "--upstream", "kafka:9092", "--workspace", "gaslight"];
        let on_save = Gmq::try_parse_from(proxy.iter().chain(&["--write-back", "on-save"])).unwrap();
        let Command::Proxy(flags) = on_save.command else {
            panic!("gmq proxy is the proxy");
        };
        assert_eq!(flags.workspace.access(), Access::WriteOnSave);

        let each_change = Gmq::try_parse_from(proxy.iter().chain(&["--write-back", "each-change"])).unwrap();
        let Command::Proxy(flags) = each_change.command else {
            panic!("gmq proxy is the proxy");
        };
        assert_eq!(flags.workspace.access(), Access::ReadWrite);

        // A read-only workspace writes nothing, so nothing waits for a save either.
        let both = Gmq::try_parse_from(proxy.iter().chain(&["--read-only", "--write-back", "on-save"]));
        assert!(both.is_err());
    }

    #[test]
    fn a_process_without_a_control_plane_names_no_address_for_it() {
        let gmq = Gmq::try_parse_from(["gmq", "proxy", "--upstream", "kafka:9092", "--no-control"]).unwrap();
        let Command::Proxy(flags) = gmq.command else {
            panic!("gmq proxy is the proxy");
        };
        assert_eq!(flags.control.configuration(), None);

        let both = Gmq::try_parse_from([
            "gmq",
            "proxy",
            "--upstream",
            "kafka:9092",
            "--no-control",
            "--control-listen",
            "127.0.0.1:7093",
        ]);
        assert!(both.is_err());
    }

    #[test]
    fn a_reload_asks_the_control_plane_where_a_process_listens_by_default() {
        let gmq = Gmq::try_parse_from(["gmq", "reload"]).unwrap();

        let Command::Reload(flags) = gmq.command else {
            panic!("gmq reload is the reload");
        };
        assert!(!flags.discard_unsaved);
        assert_eq!(
            flags.control.address(),
            ControlServerConfiguration::default().listen.into()
        );
    }
}
