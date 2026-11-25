//! The sessions that are open, as whoever watches a proxy sees them, and what happens to them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use gaslightmq_clock::Instant;
use gaslightmq_field::{ClientAddress, ClientSoftware, ConnectionNumber, Endpoint};
use gaslightmq_kernel::ClientId;
use gaslightmq_session::{ClosedBy, Connection};
use tokio::sync::Notify;

/// A session that is open: its connection, when it opened, and who its client is as far as it is known.
///
/// [`Engine::sessions`](crate::Engine::sessions) lists them, and [`SessionEvent::Opened`] hands one over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenSession {
    connection: Connection,
    opened: Instant,
    identified: bool,
    client: Option<ClientId>,
    software: Option<ClientSoftware>,
}

impl OpenSession {
    /// The number of the connection, which names the session within its engine.
    pub const fn connection(&self) -> ConnectionNumber {
        self.connection.number()
    }

    /// The address the client connects from.
    pub const fn address(&self) -> ClientAddress {
        self.connection.address()
    }

    /// The endpoint the client connected to.
    pub const fn endpoint(&self) -> Endpoint {
        self.connection.endpoint()
    }

    /// When the connection opened, on the clock of the engine.
    pub const fn opened(&self) -> Instant {
        self.opened
    }

    /// Whether the first request of the session arrived, which says who its client is.
    pub const fn is_identified(&self) -> bool {
        self.identified
    }

    /// The `client.id` of the first request, when it carried one.
    pub const fn client(&self) -> Option<&ClientId> {
        self.client.as_ref()
    }

    /// The library of the client and its version, once an `ApiVersions` request named them.
    pub const fn software(&self) -> Option<&ClientSoftware> {
        self.software.as_ref()
    }
}

/// What happened to a session, as an [`EngineEvent::Session`](crate::EngineEvent::Session) hands it over.
///
/// The events of one session come in this order: it opens, it is identified, its software is named, it closes.
/// A session can close before it is identified, and the software can come with the identification:
/// [`Identified`](Self::Identified) then holds it, and no [`SoftwareNamed`](Self::SoftwareNamed) follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    /// A client connected.
    Opened(OpenSession),
    /// The first request of the session arrived. `client` is its `client.id`, and `software` the library of the
    /// client when that request named it.
    Identified {
        connection: ConnectionNumber,
        client: Option<ClientId>,
        software: Option<ClientSoftware>,
    },
    /// An `ApiVersions` request after the first request named the library of the client.
    SoftwareNamed {
        connection: ConnectionNumber,
        software: ClientSoftware,
    },
    /// The session ended, and `by` says who ended it.
    Closed { connection: ConnectionNumber, by: ClosedBy },
}

/// The sessions of one engine that are open, each with the way to ask it to close.
#[derive(Debug, Default)]
pub(crate) struct OpenSessions {
    open: Mutex<BTreeMap<ConnectionNumber, Entry>>,
}

#[derive(Debug)]
struct Entry {
    session: OpenSession,
    close: Arc<Notify>,
}

impl OpenSessions {
    /// A session opened on `connection` at `opened`. The notification it returns asks the session to close.
    pub fn open(&self, connection: Connection, opened: Instant) -> (OpenSession, Arc<Notify>) {
        let session = OpenSession {
            connection,
            opened,
            identified: false,
            client: None,
            software: None,
        };
        let close = Arc::new(Notify::new());
        self.lock().insert(
            connection.number(),
            Entry {
                session: session.clone(),
                close: Arc::clone(&close),
            },
        );
        (session, close)
    }

    /// The session on `connection` is identified, with what its first request said.
    pub fn identified(&self, connection: ConnectionNumber, client: Option<ClientId>, software: Option<ClientSoftware>) {
        if let Some(entry) = self.lock().get_mut(&connection) {
            entry.session.identified = true;
            entry.session.client = client;
            entry.session.software = software;
        }
    }

    /// The library of the client of the session on `connection` is known.
    pub fn software_named(&self, connection: ConnectionNumber, software: ClientSoftware) {
        if let Some(entry) = self.lock().get_mut(&connection) {
            entry.session.software = Some(software);
        }
    }

    /// The session on `connection` ended.
    pub fn closed(&self, connection: ConnectionNumber) {
        self.lock().remove(&connection);
    }

    /// Every open session, in the order of their connection numbers.
    pub fn list(&self) -> Vec<OpenSession> {
        self.lock().values().map(|entry| entry.session.clone()).collect()
    }

    /// Asks the session on `connection` to close. Returns whether a session is open there.
    pub fn close(&self, connection: ConnectionNumber) -> bool {
        match self.lock().get(&connection) {
            Some(entry) => {
                // The permit stays until the session takes it, even when the session is busy now.
                entry.close.notify_one();
                true
            }
            None => false,
        }
    }

    /// The map is whole whenever the lock is free, also after a panic of the task that held it:
    /// every change is one insert, one remove, or one change of a session.
    fn lock(&self) -> MutexGuard<'_, BTreeMap<ConnectionNumber, Entry>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
