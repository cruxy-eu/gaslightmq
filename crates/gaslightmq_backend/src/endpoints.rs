//! The endpoints clients connect to.

use std::collections::BTreeMap;

use gaslightmq_field::Endpoint;

/// The endpoints that clients connect to: the ones that accept connections now, and how often each one opened.
///
/// A backend keeps them, and reports them to the engine, which keeps one listener open for each.
/// A cluster has the `bootstrap` endpoint and one endpoint per broker.
///
/// | The broker of an endpoint | Its endpoint |
/// |---|---|
/// | Serves traffic. | Accepts connections. |
/// | Is unresponsive. | Accepts connections. The broker never answers. |
/// | Goes down. | [Closes](Self::close), until the broker is back. |
/// | Comes back, or joins the cluster. | [Opens](Self::open). |
///
/// An endpoint that closes and opens again is not the endpoint it was: every connection to it ended in between.
/// The endpoints therefore count how often each one opened.
/// Two values are equal when the same endpoints accept connections and none of them opened in between.
/// An engine that compares what it knows with what its backend reports misses no change,
/// however much happened between the two:
///
/// | The engine learns | From | And then |
/// |---|---|---|
/// | Which endpoints closed, also the ones that are open again. | [`closed_since`](Self::closed_since) | Closes the listener of each, and every session on it. |
/// | Which endpoints opened, also the ones that were open before. | [`opened_since`](Self::opened_since) | Opens a listener for each. |
///
/// The endpoints name no address.
/// Where each endpoint listens is a matter of the environment.
///
/// ```
/// use gaslightmq_backend::prelude::*;
/// use gaslightmq_field::prelude::*;
/// use gaslightmq_kernel::prelude::*;
///
/// let broker = |id: u16| Endpoint::Broker(BrokerId::from(id));
/// let mut cluster: Endpoints = [Endpoint::Bootstrap, broker(1), broker(2)].into_iter().collect();
/// let known = cluster.clone();
///
/// // Broker 2 goes down.
/// cluster.close(broker(2));
/// assert_eq!(cluster.iter().collect::<Vec<_>>(), [Endpoint::Bootstrap, broker(1)]);
///
/// // Broker 2 comes back before the engine looked. The same endpoints accept connections as before.
/// cluster.open(broker(2));
/// assert_eq!(cluster.iter().collect::<Vec<_>>(), known.iter().collect::<Vec<_>>());
///
/// // The engine still learns that the sessions on broker 2 ended, and that its listener opens anew.
/// assert_ne!(cluster, known);
/// assert_eq!(cluster.closed_since(&known).collect::<Vec<_>>(), [broker(2)]);
/// assert_eq!(cluster.opened_since(&known).collect::<Vec<_>>(), [broker(2)]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Endpoints {
    /// Every endpoint that ever opened, also the ones that are closed now.
    /// A closed one stays, so it opens under a new number.
    doors: BTreeMap<Endpoint, Door>,
}

/// One endpoint: whether it accepts connections, and how often it opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Door {
    /// Which opening this is, or was: the first, the second, and so on.
    opening: u64,
    open: bool,
}

impl Endpoints {
    /// No endpoint at all.
    ///
    /// It is what an engine knows before it asked its backend, so the first
    /// [`endpoints`](crate::Backend::endpoints) it asks for are those of the cluster as it is.
    pub const fn none() -> Self {
        Self { doors: BTreeMap::new() }
    }

    /// The same endpoints, with `endpoint` open.
    #[must_use]
    pub fn with(mut self, endpoint: Endpoint) -> Self {
        self.open(endpoint);
        self
    }

    /// `endpoint` accepts connections from now on.
    ///
    /// An endpoint that was closed opens anew, and [`closed_since`](Self::closed_since) keeps saying that it closed.
    /// Opening an endpoint that is open changes nothing.
    pub fn open(&mut self, endpoint: Endpoint) {
        let door = self.doors.entry(endpoint).or_insert(Door {
            opening: 0,
            open: false,
        });
        if !door.open {
            door.opening = door.opening.saturating_add(1);
            door.open = true;
        }
    }

    /// `endpoint` accepts no connections from now on, and every connection to it ended.
    ///
    /// Closing an endpoint that is not open changes nothing.
    pub fn close(&mut self, endpoint: Endpoint) {
        if let Some(door) = self.doors.get_mut(&endpoint) {
            door.open = false;
        }
    }

    /// Whether `endpoint` accepts connections.
    pub fn contains(&self, endpoint: Endpoint) -> bool {
        self.opening(endpoint).is_some()
    }

    /// The endpoints that accept connections: `bootstrap` first, then the brokers in the order of their IDs.
    pub fn iter(&self) -> impl Iterator<Item = Endpoint> + '_ {
        let open = self.doors.iter().filter(|(_, door)| door.open);
        open.map(|(endpoint, _)| *endpoint)
    }

    /// How many endpoints accept connections.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Whether no endpoint accepts connections.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    /// The endpoints that accepted connections in `known`, and that closed since then.
    ///
    /// An endpoint that closed and opened again is among them: every connection to it ended in between.
    /// The engine closes the listener of each of them, and every session on it.
    pub fn closed_since<'a>(&'a self, known: &'a Endpoints) -> impl Iterator<Item = Endpoint> + 'a {
        known
            .iter()
            .filter(|endpoint| self.opening(*endpoint) != known.opening(*endpoint))
    }

    /// The endpoints that accept connections now, and that opened since `known`.
    ///
    /// An endpoint that closed and opened again is among them.
    /// The engine opens a listener for each of them, once it closed what [`closed_since`](Self::closed_since) names.
    pub fn opened_since<'a>(&'a self, known: &'a Endpoints) -> impl Iterator<Item = Endpoint> + 'a {
        self.iter()
            .filter(|endpoint| self.opening(*endpoint) != known.opening(*endpoint))
    }

    /// Which opening of `endpoint` accepts connections, or `None` when the endpoint is closed.
    fn opening(&self, endpoint: Endpoint) -> Option<u64> {
        let door = self.doors.get(&endpoint).filter(|door| door.open);
        door.map(|door| door.opening)
    }
}

/// The endpoints that open, in a cluster where none opened before.
impl FromIterator<Endpoint> for Endpoints {
    fn from_iter<I: IntoIterator<Item = Endpoint>>(endpoints: I) -> Self {
        let mut opened = Self::none();
        opened.extend(endpoints);
        opened
    }
}

/// Opens every one of the endpoints.
impl Extend<Endpoint> for Endpoints {
    fn extend<I: IntoIterator<Item = Endpoint>>(&mut self, endpoints: I) {
        for endpoint in endpoints {
            self.open(endpoint);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(endpoints: impl Iterator<Item = Endpoint>) -> Vec<Endpoint> {
        endpoints.collect()
    }

    #[test]
    fn an_engine_that_asked_nothing_yet_knows_no_endpoint() {
        let none = Endpoints::none();

        assert!(none.is_empty());
        assert_eq!(none.len(), 0);
        assert_eq!(none, Endpoints::default());
        assert!(!none.contains(Endpoint::Bootstrap));
    }

    #[test]
    fn an_endpoint_opens_once_until_it_closes() {
        let mut endpoints = Endpoints::none().with(Endpoint::Bootstrap).with(Endpoint::Bootstrap);
        endpoints.extend([Endpoint::Bootstrap]);
        endpoints.open(Endpoint::Bootstrap);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints, [Endpoint::Bootstrap].into_iter().collect());
        assert_eq!(list(endpoints.iter()), [Endpoint::Bootstrap]);

        endpoints.close(Endpoint::Bootstrap);
        endpoints.close(Endpoint::Bootstrap);
        assert!(endpoints.is_empty());
        assert!(!endpoints.contains(Endpoint::Bootstrap));
    }

    #[test]
    fn an_engine_learns_what_opened_and_what_closed_since_it_looked() {
        let none = Endpoints::none();
        let known = Endpoints::none().with(Endpoint::Bootstrap);

        // Everything is new to an engine that knows nothing.
        assert_eq!(list(known.opened_since(&none)), [Endpoint::Bootstrap]);
        assert_eq!(list(known.closed_since(&none)), []);

        // Nothing happened since the engine looked.
        assert_eq!(list(known.opened_since(&known)), []);
        assert_eq!(list(known.closed_since(&known)), []);

        let mut closed = known.clone();
        closed.close(Endpoint::Bootstrap);
        assert_ne!(closed, known);
        assert_eq!(list(closed.closed_since(&known)), [Endpoint::Bootstrap]);
        assert_eq!(list(closed.opened_since(&known)), []);
    }

    #[test]
    fn an_endpoint_that_closed_and_opened_again_is_not_the_endpoint_it_was() {
        let known = Endpoints::none().with(Endpoint::Bootstrap);
        let mut bounced = known.clone();
        bounced.close(Endpoint::Bootstrap);
        bounced.open(Endpoint::Bootstrap);

        // The same endpoints accept connections, and the engine still sees that one of them closed in between.
        assert_eq!(list(bounced.iter()), list(known.iter()));
        assert_ne!(bounced, known);
        assert_eq!(list(bounced.closed_since(&known)), [Endpoint::Bootstrap]);
        assert_eq!(list(bounced.opened_since(&known)), [Endpoint::Bootstrap]);

        // Once the engine knows, nothing is left to do.
        assert_eq!(list(bounced.closed_since(&bounced.clone())), []);
    }
}
