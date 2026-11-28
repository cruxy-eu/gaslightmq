//! What `gmq proxy` prints once it runs, read line by line: its run first, and `ready` last.
//!
//! ```text
//! run 4be0643f1d98573b97cdca98a65347dd
//! workspace /workspace, each change written back
//! recordings in /workspace/recordings
//! environment "proxy" in front of kafka:19092
//! bootstrap on 0.0.0.0:9092
//! broker 1 on 0.0.0.0:19092
//! control plane and UI on http://0.0.0.0:7092
//! ready: Ctrl-C stops it
//! ```
//!
//! Where the process listens is a port of the container. The test reaches it where the container publishes it.

use gaslightmq_control_api::RunId;

/// The line that holds the run.
const RUN: &str = "run ";
/// The line that comes last, once every listener accepts connections.
const READY: &str = "ready";

/// What the process said about itself so far.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Announcement {
    run: Option<RunId>,
    ready: bool,
}

impl Announcement {
    /// Take one line that the process printed.
    pub fn take(&mut self, line: &str) {
        if let Some(run) = line.strip_prefix(RUN) {
            self.run = run.trim().parse().ok();
        } else if line.starts_with(READY) {
            self.ready = true;
        }
    }

    /// Whether the process said that it is ready.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// The run of the process, once it said it.
    pub fn run(&self) -> Option<RunId> {
        self.run
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_that_is_ready_said_its_run_before() {
        let mut announcement = Announcement::default();
        for line in [
            "run 4be0643f1d98573b97cdca98a65347dd",
            "no workspace: every definition and every recording lives in memory",
            "environment \"proxy\" in front of kafka:19092",
            "bootstrap on 0.0.0.0:9092",
            "control plane and UI on http://0.0.0.0:7092",
        ] {
            announcement.take(line);
        }
        assert!(!announcement.is_ready());

        announcement.take("ready: Ctrl-C stops it");

        assert!(announcement.is_ready());
        assert_eq!(
            announcement.run(),
            Some(RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd))
        );
    }

    #[test]
    fn a_process_that_said_nothing_has_no_run() {
        let mut announcement = Announcement::default();

        announcement.take("ready: Ctrl-C stops it");

        assert!(announcement.is_ready());
        assert_eq!(announcement.run(), None);
    }
}
