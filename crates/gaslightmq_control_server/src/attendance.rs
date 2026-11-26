//! Who is there at the control plane: the presences that the server holds, on the clock that their grace period
//! runs on, in front of the host that ends their holds.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_clock_waitable::WaitableClock;
use gaslightmq_control_api::clock::Instant;
use gaslightmq_control_api::surface::PresenceId;
use gaslightmq_host::Host;
use gaslightmq_workspace::WorkspaceContext;

use crate::PingInterval;
use crate::presence::{Liveness, Presences};

/// The presences of the clients of the live stream, and the clock that their grace period runs on.
///
/// The server opens a presence in the host for a client that is new, and ends it there once the client stayed away
/// for the grace period, or the server stops. The host ends the holds of a presence that ended.
/// Every stream of a server shares one attendance.
pub(crate) struct Attendance<K> {
    presences: Arc<Mutex<Presences>>,
    /// How often the server pings the client of a stream, to learn whether it is still there.
    ping_interval: PingInterval,
    clock: Arc<K>,
}

impl<K> Clone for Attendance<K> {
    fn clone(&self) -> Self {
        Self {
            presences: Arc::clone(&self.presences),
            ping_interval: self.ping_interval,
            clock: Arc::clone(&self.clock),
        }
    }
}

/// Why a client of the live stream got no presence.
pub(crate) enum NoPresence {
    /// The server stops, and opens no presence any more.
    Stopping,
    /// The system gave no randomness for the ID of a presence. `reason` is what it said.
    NoRandomness { reason: String },
}

impl<K: WaitableClock> Attendance<K> {
    /// The attendance of a server that holds `presences` and pings every client at `ping_interval`.
    /// The grace period and the pings run on `clock`.
    pub(crate) fn new(presences: Presences, ping_interval: PingInterval, clock: K) -> Self {
        Self {
            presences: Arc::new(Mutex::new(presences)),
            ping_interval,
            clock: Arc::new(clock),
        }
    }

    /// The clock of the server.
    pub(crate) fn clock(&self) -> &K {
        &self.clock
    }

    /// The liveness of a client whose stream opens now.
    pub(crate) fn liveness(&self) -> Liveness {
        Liveness::new(self.ping_interval.span(), self.clock.now())
    }

    /// The presence of a client whose stream opened.
    ///
    /// It is the presence that the client comes back as, when the server still holds that one.
    /// Otherwise the client is new: its presence has an ID from the randomness of the system, and the host opens it.
    ///
    /// # Errors
    ///
    /// [`NoPresence`] when the server stops, or the system gave no randomness.
    pub(crate) fn arrive<C: WorkspaceContext>(
        &self,
        host: &Host<C>,
        comes_back_as: Option<PresenceId>,
    ) -> Result<PresenceId, NoPresence> {
        let mut presences = self.presences();
        if let Some(presence) = comes_back_as
            && presences.resumed(presence, self.clock.now())
        {
            return Ok(presence);
        }
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|error| NoPresence::NoRandomness {
            reason: error.to_string(),
        })?;
        let presence = PresenceId::from_random_bits(u128::from_be_bytes(random));
        if !presences.opened(presence) {
            return Err(NoPresence::Stopping);
        }
        // The presences stay taken until the host opened this one, so a server that stops in this moment ends it.
        host.open_presence(presence);
        Ok(presence)
    }

    /// A stream of `presence` ended at `left_at`. Returns the instant at which the presence ends unless its client
    /// comes back, or `None` when there is nothing to wait for: another stream of it is open, or the server stops.
    pub(crate) fn leave(&self, presence: PresenceId, left_at: Instant) -> Option<Instant> {
        self.presences().left(presence, left_at)
    }

    /// Waits until `ends_at`, and ends `presence` in the host unless its client came back in the meantime.
    pub(crate) async fn end_unless_back<C: WorkspaceContext>(
        &self,
        host: &Host<C>,
        presence: PresenceId,
        ends_at: Instant,
    ) {
        self.clock.until(ends_at).await;
        let ended = self.presences().ended(presence, self.clock.now());
        if ended {
            host.end_presence(presence);
        }
    }

    /// The server stops: every presence ends in the host at once, and none opens any more.
    pub(crate) fn end_all<C: WorkspaceContext>(&self, host: &Host<C>) {
        let ended = self.presences().end_all();
        for presence in ended {
            host.end_presence(presence);
        }
    }

    /// The presences. A task that panicked while it held them left nothing half done: every change is one step.
    fn presences(&self) -> MutexGuard<'_, Presences> {
        self.presences.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
