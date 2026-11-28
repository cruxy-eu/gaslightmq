//! What `gmq proxy` prints once it runs, read line by line: its run, where it listens, and `ready` last.
//!
//! ```text
//! run 4be0643f1d98573b97cdca98a65347dd
//! workspace /workspace, each change written back
//! recordings in /workspace/recordings
//! environment "proxy" in front of 127.0.0.1:50123
//! bootstrap on 127.0.0.1:50124
//! broker 1 on 127.0.0.1:50125
//! control plane and UI on http://127.0.0.1:50126
//! ready: Ctrl-C stops it
//! ```

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use gaslightmq_control_api::RunId;

/// The line that holds the run.
const RUN: &str = "run ";
/// The line that holds where the bootstrap listener listens.
const BOOTSTRAP: &str = "bootstrap on ";
/// The line that holds where the control plane listens.
const CONTROL_PLANE: &str = "control plane and UI on http://";
/// The line that comes last, once every listener accepts connections.
const READY: &str = "ready";

/// What the process said about itself so far.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Announcement {
    run: Option<RunId>,
    bootstrap: Option<SocketAddr>,
    control_plane: Option<SocketAddr>,
    ready: bool,
}

/// Where a process that said it is ready runs, as the test reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Announced {
    pub run: RunId,
    pub bootstrap: SocketAddr,
    pub control_plane: SocketAddr,
}

impl Announcement {
    /// Take one line that the process printed.
    pub fn take(&mut self, line: &str) {
        if let Some(run) = line.strip_prefix(RUN) {
            self.run = run.trim().parse().ok();
        } else if let Some(address) = line.strip_prefix(BOOTSTRAP) {
            self.bootstrap = address.trim().parse().ok().map(reachable);
        } else if let Some(address) = line.strip_prefix(CONTROL_PLANE) {
            self.control_plane = address.trim().parse().ok().map(reachable);
        } else if line.starts_with(READY) {
            self.ready = true;
        }
    }

    /// Whether the process said that it is ready.
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Where the process runs, or what it did not say.
    ///
    /// # Errors
    ///
    /// The text that names what the process did not say before it said that it is ready.
    pub fn announced(&self) -> Result<Announced, String> {
        let missing = |what: &str| format!("the process said that it is ready, and did not say {what}");
        Ok(Announced {
            run: self.run.ok_or_else(|| missing("its run"))?,
            bootstrap: self.bootstrap.ok_or_else(|| missing("where its proxy listens"))?,
            control_plane: self
                .control_plane
                .ok_or_else(|| missing("where its control plane listens"))?,
        })
    }
}

/// The address under which a client of the test reaches a listener on `address`:
/// a listener on every address of the machine is reached on its loopback.
fn reachable(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, address.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_that_is_ready_said_where_it_listens_before() {
        let mut announcement = Announcement::default();
        for line in [
            "run 4be0643f1d98573b97cdca98a65347dd",
            "no workspace: every definition and every recording lives in memory",
            "environment \"proxy\" in front of 127.0.0.1:50123",
            "bootstrap on 127.0.0.1:50124",
            "broker 1 on 127.0.0.1:50125",
        ] {
            announcement.take(line);
        }
        assert!(!announcement.is_ready());

        announcement.take("control plane and UI on http://127.0.0.1:50126");
        announcement.take("ready: Ctrl-C stops it");

        assert!(announcement.is_ready());
        assert_eq!(
            announcement.announced(),
            Ok(Announced {
                run: RunId::from_random_bits(0x4be0_643f_1d98_573b_97cd_ca98_a653_47dd),
                bootstrap: "127.0.0.1:50124".parse().unwrap(),
                control_plane: "127.0.0.1:50126".parse().unwrap(),
            })
        );
    }

    #[test]
    fn a_listener_on_every_address_is_reached_on_the_loopback() {
        let mut announcement = Announcement::default();

        announcement.take("run 4be0643f1d98573b97cdca98a65347dd");
        announcement.take("bootstrap on 0.0.0.0:50124");
        announcement.take("control plane and UI on http://[::]:50126");
        announcement.take("ready: Ctrl-C stops it");

        let announced = announcement.announced().unwrap();
        assert_eq!(announced.bootstrap, "127.0.0.1:50124".parse().unwrap());
        assert_eq!(announced.control_plane, "[::1]:50126".parse().unwrap());
    }

    #[test]
    fn a_process_without_a_control_plane_is_not_reached() {
        let mut announcement = Announcement::default();

        announcement.take("run 4be0643f1d98573b97cdca98a65347dd");
        announcement.take("bootstrap on 127.0.0.1:50124");
        announcement.take("no control plane");
        announcement.take("ready: Ctrl-C stops it");

        assert_eq!(
            announcement.announced(),
            Err("the process said that it is ready, and did not say where its control plane listens".to_owned())
        );
    }
}
