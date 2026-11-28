//! Names of networks of containers and of aliases on them that no other test on the machine has.
//!
//! A name holds the ID of the process, so it reaches the operating system, and stays out of the configuration.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::configuration::{ContainerNetwork, NetworkListener};

/// The names this process handed out for networks and aliases, so each one is new.
static NAMED: AtomicU64 = AtomicU64::new(0);

impl ContainerNetwork {
    /// A network whose name starts with `prefix`, and that no other test on the machine names while this process runs:
    /// `orders-test-4711-1`, with the ID of the process in the middle.
    pub fn unique(prefix: &str) -> Self {
        Self::named(unique_on_the_machine(prefix))
    }
}

impl NetworkListener {
    /// A listener under an alias that starts with `prefix` and that no other cluster on the machine has while this
    /// process runs, from `port` on: `kafka-4711-1`, with the ID of the process in the middle.
    pub fn unique(prefix: &str, port: u16) -> Self {
        Self::new(unique_on_the_machine(prefix), port)
    }
}

/// A name that starts with `prefix`, and that no other process on the machine hands out while this one runs.
fn unique_on_the_machine(prefix: &str) -> String {
    let process = std::process::id();
    format!("{prefix}-{process}-{}", NAMED.fetch_add(1, Ordering::Relaxed) + 1)
}
