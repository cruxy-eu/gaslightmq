//! `gmq proxy`: a proxy in front of a real cluster, with the control plane and the UI in front of it.

use std::sync::Arc;

use gaslightmq_control_server::{ControlServer, ControlServerConfiguration};
use gaslightmq_definition_api::Store;
use gaslightmq_environment::Environment;
use gaslightmq_host::{EnvironmentConfiguration, Host, HostConfiguration, RunId};
use gaslightmq_lie_api::Lies;
use gaslightmq_workspace::{MemoryContext, Stores, WorkspaceContext};
use gaslightmq_workspace_fs::{FileContext, FileStores};

use crate::cli::ProxyFlags;
use crate::error::ProxyError;
use crate::report::{Holding, Running};
use crate::stop::StopSignals;

/// Runs the proxy that the flags describe, until a signal stops it.
///
/// It opens the files of the workspace when the flags name one, and keeps every definition in memory otherwise.
/// The proxy starts first, and the control plane in front of it once it accepts connections.
/// It prints where everything listens, and `ready` last.
///
/// # Errors
///
/// [`ProxyError`] when something did not start. Nothing runs then.
pub(crate) async fn proxy(flags: ProxyFlags) -> Result<(), ProxyError> {
    let environment = flags
        .environment()
        .map_err(|error| ProxyError::ListenersMeet { error })?;
    let control = flags.control.configuration();
    match flags.workspace.workspace.clone() {
        Some(directory) => {
            let access = flags.workspace.access();
            let stores =
                FileStores::open(directory.clone(), access)
                    .await
                    .map_err(|error| ProxyError::WorkspaceUnreadable {
                        directory: directory.clone(),
                        error,
                    })?;
            let holding = Holding::Files { directory, access };
            serve(FileContext::new(stores), holding, &environment, control).await
        }
        None => serve(MemoryContext::new(), Holding::Memory, &environment, control).await,
    }
}

/// Starts the environment over the stores of `context`, and the control plane in front of it, and stops both once a
/// signal comes.
async fn serve<C: WorkspaceContext + 'static>(
    context: C,
    holding: Holding,
    environment: &Environment,
    control: Option<ControlServerConfiguration>,
) -> Result<(), ProxyError> {
    // The environment of the flags is no definition of the workspace, so the workspace holds none of its name.
    let held = context.stores().environments().get(environment.name()).await;
    match held {
        Ok(None) => {}
        Ok(Some(_)) => {
            return Err(ProxyError::NameTaken {
                name: environment.name().clone(),
            });
        }
        Err(error) => {
            return Err(ProxyError::WorkspaceNotRead {
                reason: error.to_string(),
            });
        }
    }
    let run = random_run()?;
    let signals = StopSignals::listen()?;
    let host = Arc::new(Host::new(HostConfiguration::default(), context, lies(), run));
    let configuration =
        EnvironmentConfiguration::new(environment).expect("a host configures a proxy in front of a real cluster");
    host.start(configuration)
        .await
        .map_err(|error| ProxyError::ProxyNotStarted { error })?;
    let server = match control {
        Some(configuration) => match ControlServer::start(configuration, Arc::clone(&host)).await {
            Ok(server) => Some(server),
            Err(error) => {
                host.stop_all().await;
                return Err(ProxyError::ControlPlaneNotStarted { error });
            }
        },
        None => None,
    };

    let name = environment.name();
    let endpoints = host
        .endpoints(name)
        .expect("the environment runs")
        .into_iter()
        .filter_map(|endpoint| {
            let address = host.address_of(name, endpoint).expect("the environment runs");
            address.map(|address| (endpoint, address))
        })
        .collect();
    let upstream = environment
        .instances()
        .upstream()
        .expect("the flags describe a proxy in front of a real cluster")
        .bootstrap()
        .clone();
    let running = Running {
        run,
        holding,
        environment: name.clone(),
        upstream,
        endpoints,
        control: server.as_ref().map(ControlServer::address),
    };
    println!("{running}");

    signals.wait().await;
    // The control plane stops first, so nobody starts an environment that is about to stop.
    if let Some(server) = server {
        server.stop().await;
    }
    host.stop_all().await;
    Ok(())
}

/// A run ID from the randomness of the system.
fn random_run() -> Result<RunId, ProxyError> {
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| ProxyError::NoRandomness {
        reason: error.to_string(),
    })?;
    Ok(RunId::from_random_bits(u128::from_be_bytes(random)))
}

/// The lies of the build: those that every build tells, and one for each crate of a lie kind that the build has.
fn lies() -> Lies {
    let lies = Lies::of_every_build();
    #[cfg(feature = "refusal")]
    let lies = lies
        .with(gaslightmq_lie_refusal::Refusal)
        .expect("no build tells the refusal without its crate");
    lies
}
