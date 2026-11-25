//! The runtime of one proxy.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use gaslightmq_backend::Backend;
use gaslightmq_clock::Clock;
use gaslightmq_clock_waitable::{WaitableClock, WallClock};
use gaslightmq_field::Endpoint;
use gaslightmq_lie_api::Lies;
use gaslightmq_pipeline::{KnownTopics, SharedBySessions};
use gaslightmq_session::{CallIds, ClientContexts};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::endpoints::{Listening, OpenEndpoints};
use crate::events::EventBus;
use crate::session::Sessions;
use crate::{ArmedRules, EngineConfiguration, EngineStartError, EveryEvent, LiveEvents};

/// The runtime of one proxy: its sessions, its armed rules, its clock and its endpoints.
///
/// An engine stands between the clients of a proxy and its [backend](Backend).
/// It opens a listener for every endpoint that the backend reports, and runs every connection as a session:
/// one task that owns the transport and drives the pipeline.
///
/// | Part | Does |
/// |---|---|
/// | The sessions | Wait for frames of the client, answers of the backend, timers and new snapshots, report each to their pipeline, and perform what it returns. They count their calls together, so a call ID names one call of the proxy. |
/// | The [armed rules](Self::rules) | Arm and disarm rules, and publish a new snapshot to every session. |
/// | The events | Hand over the record of every call, and every incident that a rule fired: to whoever [watches](Self::live_events), and to whoever [keeps every one](Self::every_event). |
/// | The [clock](WaitableClock) | Tells every session the time, and ends every wait: a delay, a lifetime, the time a client context stays. |
/// | The endpoints | Follow the backend: an endpoint that closes ends its sessions, and one that opens gets a listener, whose address the backend hears. |
///
/// # Stopping
///
/// An engine runs from [`start`](Self::start) until someone [stops](Self::stop) it or lets go of it.
/// Stopping closes every endpoint, which ends every session.
/// An engine that someone lets go of stops the same way, a moment later, and nobody waits for it.
pub struct Engine<B, C = WallClock> {
    sessions: Arc<Sessions<B, C>>,
    listening: watch::Receiver<Listening>,
    /// The engine runs while this lives. Nothing is ever sent through it.
    running: watch::Sender<()>,
    /// The tasks of the engine itself, which end once it stops. The sessions are tasks of their listeners.
    tasks: Vec<JoinHandle<()>>,
}

impl<B: Backend + 'static, C: WaitableClock> Engine<B, C> {
    /// Starts the proxy in front of `backend`: asks it for its endpoints, opens a listener for each,
    /// and tells it where each one listens.
    ///
    /// The sessions tell the time on `clock`, and tell the `lies` of this build.
    /// Several engines of one process can share one set of lies.
    /// The engine runs its tasks on the runtime that the caller runs on.
    ///
    /// The call waits until the backend knows its endpoints, for as long as that takes.
    /// The proxy accepts connections from the moment the call returns.
    ///
    /// # Errors
    ///
    /// - [`EngineStartError::EndpointNotOpened`] when an endpoint has no listener.
    ///   Its error says why: the address did not take one, such as an address that another socket holds,
    ///   or the backend rejected the listener.
    /// - [`EngineStartError::BrokersWithoutListener`] when the listeners of the configuration have no address for
    ///   a broker endpoint.
    ///
    /// The error names the first endpoint that did not open, and the log names every other one.
    /// No endpoint stays open then.
    /// The backend keeps the addresses that it heard before the start failed: the contract has no word for a
    /// listener that is gone. It sends no client there for long, because an engine that starts again in front of it
    /// says where its new listeners are.
    pub async fn start(
        configuration: EngineConfiguration,
        backend: B,
        clock: C,
        lies: impl Into<Arc<Lies>>,
    ) -> Result<Self, EngineStartError> {
        let clock = Arc::new(clock);
        let shared = SharedBySessions::new(
            ClientContexts::new(configuration.client_context_linger.duration()),
            CallIds::new(),
            KnownTopics::new(),
            lies.into(),
        );
        let of_the_rules: Arc<dyn Clock + Send + Sync> = Arc::<C>::clone(&clock);
        let rules = ArmedRules::new(configuration.seed, of_the_rules);
        let events = EventBus::new(configuration.event_backlog);
        let (sessions, lingered) = Sessions::new(backend, clock, shared, rules, events);
        let sessions = Arc::new(sessions);

        let (mut endpoints, listening) = OpenEndpoints::none(Arc::clone(&sessions), configuration);
        let mut unopened = endpoints.follow_the_backend().await.into_iter();
        if let Some(first) = unopened.next() {
            // The error names one thing that stood in the way. The log names the others, so none of them is lost.
            for other in unopened {
                other.log();
            }
            endpoints.close().await;
            return Err(first.into());
        }

        let (running, stopping) = watch::channel(());
        let stopping = Stopping(stopping);
        let ending_lifetimes = {
            let (sessions, mut stopping) = (Arc::clone(&sessions), stopping.clone());
            async move {
                tokio::select! {
                    () = stopping.stopped() => {}
                    () = sessions.rules.clone().end_lifetimes(&*sessions.clock) => {}
                }
            }
        };
        let expiring_contexts = {
            let (sessions, mut stopping) = (Arc::clone(&sessions), stopping.clone());
            async move {
                tokio::select! {
                    () = stopping.stopped() => {}
                    () = sessions.expire_contexts(lingered) => {}
                }
            }
        };
        let tasks = vec![
            tokio::spawn(endpoints.run(stopping)),
            tokio::spawn(ending_lifetimes),
            tokio::spawn(expiring_contexts),
        ];
        Ok(Self {
            sessions,
            listening,
            running,
            tasks,
        })
    }

    /// The address that `endpoint` accepts connections on, or `None` while the endpoint is not open.
    ///
    /// It names the port the system picked, when the listeners of the configuration asked for port 0.
    pub fn address_of(&self, endpoint: Endpoint) -> Option<SocketAddr> {
        self.listening.borrow().get(&endpoint).copied()
    }

    /// The endpoints that accept connections at this moment: `bootstrap` first, then the brokers in the order of
    /// their IDs.
    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.listening.borrow().keys().copied().collect()
    }

    /// The armed rules of the proxy: the handle arms and disarms rules.
    pub fn rules(&self) -> &ArmedRules {
        &self.sessions.rules
    }

    /// A listener of what the sessions hand over from now on, as it happens: the records of their calls, and the
    /// incidents that their rules fire.
    ///
    /// It is for whoever watches, such as a traffic view. No session waits for it.
    /// The engine keeps a [backlog](EngineConfiguration::event_backlog) of events for it,
    /// and a listener that falls further behind misses the oldest ones, and learns how many.
    /// Whoever must not miss one asks for [every event](Self::every_event).
    pub fn live_events(&self) -> LiveEvents {
        self.sessions.events.listen_live()
    }

    /// A listener that misses nothing of what the sessions hand over from now on, as a recorder needs it.
    ///
    /// No session waits for it, and the engine drops no event for it.
    /// The price is memory: every event that the listener did not take yet stays in the engine, without a limit.
    /// A listener that is slow makes the engine hold more, and one that takes no event makes it hold them all,
    /// until the process has no memory left. Whoever asks for one takes its events, or lets go of it.
    pub fn every_event(&self) -> EveryEvent {
        self.sessions.events.listen_to_every_event()
    }

    /// What the sessions of the proxy share: the client contexts, the topics they know by ID, and the lies of the build.
    pub fn shared_by_sessions(&self) -> &SharedBySessions {
        &self.sessions.shared
    }

    /// The backend of the proxy.
    pub fn backend(&self) -> &B {
        &self.sessions.backend
    }

    /// The clock of the engine.
    pub fn clock(&self) -> &C {
        &self.sessions.clock
    }

    /// Stops the proxy, and waits until every session has ended, for as long as a stop may take.
    ///
    /// Every endpoint closes, so the addresses refuse connections and are free for a new engine once the call returns.
    /// Every session closes its connection, and its client reads what was sent before.
    /// The pipeline has no word for a proxy that stops, so a call that is in flight then gets no record.
    ///
    /// Listeners that follow a change of the backend when the call comes finish that first,
    /// so no listener is let go of halfway, and no address stays taken behind the call.
    ///
    /// The call takes no longer than the [`stop_timeout`](EngineConfiguration::stop_timeout).
    /// A client that keeps its side of a connection open holds a session up for its
    /// [`close_timeout`](EngineConfiguration::close_timeout), and a backend can hold one up for as long as it
    /// likes: it never says whether it takes a listener, or never lets go of a connection.
    /// After the stop timeout the engine lets go of what is left, and the call returns.
    /// An endpoint that was still open then closes a moment later, and nobody waits for the sessions on it.
    pub async fn stop(self) {
        let Self { running, tasks, .. } = self;
        drop(running);
        for task in tasks {
            // A task ends by itself once the engine stops. It has no result to hand over.
            let _ = task.await;
        }
    }
}

/// Writes where the endpoints listen, and what is armed.
impl<B, C> fmt::Debug for Engine<B, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("listening", &*self.listening.borrow())
            .field("rules", &self.sessions.rules)
            .field("shared_by_sessions", &self.sessions.shared)
            .finish_non_exhaustive()
    }
}

/// Tells every task of an engine when the engine stops.
#[derive(Debug, Clone)]
pub(crate) struct Stopping(watch::Receiver<()>);

impl Stopping {
    /// Waits until the engine stopped. Returns at once when it already has.
    ///
    /// Giving up the wait loses nothing.
    pub async fn stopped(&mut self) {
        // The channel carries no value. It ends when the engine stops or someone lets go of it.
        while self.0.changed().await.is_ok() {}
    }
}
