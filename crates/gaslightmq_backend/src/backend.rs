//! The one trait through which the engine reaches a backend.

use std::sync::Arc;

use gaslightmq_field::ConnectionNumber;
use gaslightmq_incident::{ExpandedIncident, Incident, IncidentSupport};

use crate::{BackendAnswer, BackendRequest, Endpoints, IncidentRejection};

/// What answers a call behind a proxy: a real cluster, or a broker in the same process.
///
/// The engine reaches its backend through this trait and through nothing else.
/// The sessions of an engine share one backend, so a backend is `Send` and `Sync`, and so is every future it returns.
///
/// | A backend | Through |
/// |---|---|
/// | Answers a request with the truth. | [`call`](Self::call) |
/// | Lets go of a connection that ended. | [`close`](Self::close) |
/// | Reports the endpoints clients connect to. | [`endpoints`](Self::endpoints) |
/// | Declares which incidents it applies, and which it can only emulate. | [`incident_support`](Self::incident_support) |
/// | Applies or emulates an incident. | [`apply`](Self::apply) |
///
/// A backend never lies.
/// Whatever it answers is the truth: rules match on it, and explanations compare the delivered response with it.
///
/// The trait is a signature, and it has no logic.
/// It names no runtime: a backend waits however it likes, and the engine drives the futures on its own.
/// An implementation can write each of its async methods as an `async fn`.
pub trait Backend: Send + Sync {
    /// Takes one request, and answers it.
    ///
    /// The answer does not have to come at once.
    /// A Fetch waits for records, and a `JoinGroup` waits for the other members of its group.
    /// Until the future ends, the call is in flight in its session.
    ///
    /// - **Order.**
    ///   A backend takes the requests of one connection in the order in which the engine starts their calls.
    ///   A call starts when its future is polled for the first time.
    ///   The engine therefore polls the calls of one connection for the first time in the order in which
    ///   the pipeline forwarded them, and it does not wait for one answer before it starts the next call.
    ///   Answers come in any order.
    /// - **No answer.**
    ///   A backend may never answer: a broker that is unresponsive accepts requests and stays silent.
    ///   Such a call ends when the engine drops it.
    /// - **Dropping.**
    ///   The engine drops the future of a call that it no longer waits for, such as a call of a session that closed.
    ///   The backend then owes no answer.
    ///   The cluster may still have taken the request.
    /// - **Failure.**
    ///   A backend that gives up on a call answers [`BackendAnswer::Failed`].
    ///   A backend that cannot read what the cluster sent answers [`BackendAnswer::NotAResponse`].
    ///   It never makes up a response.
    fn call(&self, request: BackendRequest) -> impl Future<Output = BackendAnswer> + Send;

    /// Lets go of what the backend holds for a connection of a client that ended.
    ///
    /// The engine says so once for every connection that it handed a request of, whoever closed the connection.
    /// A backend with a connection of its own to the cluster for each connection of a client closes that one,
    /// so the cluster sees the client leave.
    /// No request of that connection follows.
    /// The engine drops the calls of the connection that are still in flight.
    fn close(&self, connection: ConnectionNumber) -> impl Future<Output = ()> + Send;

    /// Waits until the endpoints differ from `known`, and returns them.
    ///
    /// The engine starts with [`Endpoints::none`], so the first answer comes as soon as the backend knows its cluster.
    /// It then asks again with the answer it got.
    ///
    /// The endpoints differ once an endpoint opened or closed: a broker went down or came back,
    /// or a broker joined or left the cluster.
    /// They also differ once an endpoint closed and opened again, because [`Endpoints`] count how often each one opened.
    /// The engine therefore misses no change, however late it asks:
    /// a broker that went down and came back between two answers shows in the second one.
    ///
    /// The engine closes the listener of every endpoint that [closed](Endpoints::closed_since) since `known`,
    /// together with every session on it, and records that the backend closed those sessions.
    /// Then it opens a listener for every endpoint that [opened](Endpoints::opened_since).
    fn endpoints(&self, known: &Endpoints) -> impl Future<Output = Endpoints> + Send;

    /// Which kinds of incidents the backend applies, and which it can only emulate.
    ///
    /// The support of a backend never changes.
    /// The engine checks the requirements of a scenario against it when the scenario loads,
    /// so a scenario is rejected then, and not halfway through a run.
    fn incident_support(&self) -> IncidentSupport;

    /// Applies or emulates an incident, and returns it with the consequences it had.
    ///
    /// The backend [expands](Incident::expand) the incident with the topology of its own cluster,
    /// and applies the incident and its consequences in one step.
    /// The expansion is the same rule for every backend, so two backends with the same topology see the same list.
    /// The engine records what comes back in its journal.
    ///
    /// The incident is in force once the future ends: every call that the backend takes from then on sees it.
    /// An endpoint that the incident closes or opens shows in [`endpoints`](Self::endpoints).
    ///
    /// - **Starting.**
    ///   Like a call, an incident starts when its future is polled for the first time.
    ///   A future that the engine drops before that changes nothing.
    /// - **Dropping.**
    ///   An incident that started is not taken back.
    ///   When the engine drops its future, the incident still happens as a whole or not at all,
    ///   and the engine does not learn which.
    ///   The engine therefore keeps the future of an incident until it ends, also when the session that fired it closes.
    ///
    /// # Errors
    ///
    /// [`IncidentRejection`] when the backend can neither apply nor emulate the incident or one of its consequences,
    /// or when the incident cannot happen in the cluster as it is.
    /// A rejected incident changed nothing.
    fn apply(&self, incident: Incident) -> impl Future<Output = Result<ExpandedIncident, IncidentRejection>> + Send;
}

/// A reference to a backend is a backend, so code that drives one can borrow it.
impl<B: Backend> Backend for &B {
    fn call(&self, request: BackendRequest) -> impl Future<Output = BackendAnswer> + Send {
        (**self).call(request)
    }

    fn close(&self, connection: ConnectionNumber) -> impl Future<Output = ()> + Send {
        (**self).close(connection)
    }

    fn endpoints(&self, known: &Endpoints) -> impl Future<Output = Endpoints> + Send {
        (**self).endpoints(known)
    }

    fn incident_support(&self) -> IncidentSupport {
        (**self).incident_support()
    }

    fn apply(&self, incident: Incident) -> impl Future<Output = Result<ExpandedIncident, IncidentRejection>> + Send {
        (**self).apply(incident)
    }
}

/// A shared backend is a backend, so every session task can hold the one backend of its engine.
impl<B: Backend> Backend for Arc<B> {
    fn call(&self, request: BackendRequest) -> impl Future<Output = BackendAnswer> + Send {
        (**self).call(request)
    }

    fn close(&self, connection: ConnectionNumber) -> impl Future<Output = ()> + Send {
        (**self).close(connection)
    }

    fn endpoints(&self, known: &Endpoints) -> impl Future<Output = Endpoints> + Send {
        (**self).endpoints(known)
    }

    fn incident_support(&self) -> IncidentSupport {
        (**self).incident_support()
    }

    fn apply(&self, incident: Incident) -> impl Future<Output = Result<ExpandedIncident, IncidentRejection>> + Send {
        (**self).apply(incident)
    }
}
