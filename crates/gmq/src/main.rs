//! `gmq`, the binary of GaslightMQ.
//!
//! | Command | Does |
//! |---|---|
//! | `gmq proxy --upstream <bootstrap>` | Runs a proxy in front of a real cluster, with the control plane and the UI in front of it. |
//! | `gmq reload` | Asks a process that runs to read the files of its workspace again, and prints what changed. |
//! | `gmq health` | Asks a process that runs whether every environment that it was started with runs. The health check of a container. |
//!
//! `gmq --help` and `gmq <command> --help` name every flag, its environment variable and its default.
//!
//! # What it puts together
//!
//! `gmq` reads the flags and the variables once, when it starts.
//! It hands the host and the control server their configuration, and neither reads a flag or a variable of its own.
//! It picks the stores of the workspace: the files of the directory that `--workspace` names, or memory without it.
//! It makes the ID of the run from the randomness of the system, and hands it to the host.
//!
//! # Output
//!
//! What a command did goes to the standard output, one thing per line.
//! Why it failed goes to the standard error, and the command exits with a failure.
//! A workspace whose files do not read prints every file that does not, one per line.
//!
//! # Features
//!
//! | Feature | Adds |
//! |---|---|
//! | `refusal`, on by default | The refusal: a rule answers a call with an error code. |
//!
//! The lies that every build tells come without a feature: timing, silence and hang-up.

#![expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a command of the command line tells whoever runs it what it did, and why it failed"
)]

mod brokers;
mod cli;
mod error;
mod health;
mod proxy;
mod reload;
mod report;
mod stop;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Command, Gmq};
use crate::error::{HealthError, ProxyError};
use crate::report::{HealthReport, ReloadReport};

#[tokio::main]
async fn main() -> ExitCode {
    let gmq = Gmq::parse();
    match gmq.command {
        Command::Proxy(flags) => match proxy::proxy(flags).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(ProxyError::WorkspaceUnreadable { directory, error }) => {
                eprintln!("the files of the workspace in {} do not read:", directory.display());
                for file in &error.errors {
                    eprintln!("{file}");
                }
                ExitCode::FAILURE
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
        Command::Health(flags) => match health::health(&flags).await {
            Ok(health) => {
                println!("{}", HealthReport(&health));
                ExitCode::SUCCESS
            }
            Err(HealthError::Unhealthy { error }) => {
                println!("{}", HealthReport(&error.health));
                eprintln!("{error}");
                ExitCode::FAILURE
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
        Command::Reload(flags) => match reload::reload(&flags).await {
            Ok(reloaded) => {
                println!("{}", ReloadReport(&reloaded));
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
    }
}
