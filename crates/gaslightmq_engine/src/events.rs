//! What leaves the sessions of an engine: the records of their calls, and the incidents their rules fired.

use std::sync::{Arc, Mutex, PoisonError};

use gaslightmq_call::Call;
use gaslightmq_incident::Incident;
use gaslightmq_journal::JournalEntry;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::{EventBacklog, LiveEventsError, SessionEvent};

/// Something that a session of an engine handed over.
///
/// It is the one outlet of an engine.
/// Whoever keeps what happened listens here: the traffic view, the index and a recording take the calls,
/// the scenario runner takes the calls and the incidents, and whoever shows the sessions takes what happens to them.
/// The entries of the [journal](crate::Journal) leave here too, so a recording keeps them between the calls.
///
/// The events of one session come in the order in which they happened: it opens, its first request says who its
/// client is, its calls leave with what their rules fired before their records, and it closes.
/// The entries of the journal come in the order of their IDs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EngineEvent {
    /// A call ended, or a silence decided that it never gets an answer. This is its record.
    CallRecorded(Arc<Call>),
    /// A rule fired an incident on a call.
    ///
    /// The engine hands it over, and applies nothing: whoever runs the scenario passes it to the backend.
    IncidentFired(Incident),
    /// A session opened, said who its client is, or closed.
    Session(SessionEvent),
    /// Something changed in the environment of the proxy, and this is its entry in the [journal](crate::Journal).
    Journaled(JournalEntry),
}

/// The events of an engine as they happen, for a listener that watches: a traffic view, a UI.
///
/// [`Engine::live_events`](crate::Engine::live_events) makes one.
/// It receives the events from the moment it was made, in the order in which the sessions handed them over.
///
/// No session waits for it.
/// The engine keeps a [backlog](crate::EngineConfiguration::event_backlog) of events for it, and drops what is older:
/// a listener that falls further behind misses the oldest events, and learns how many.
/// Whoever must not miss one listens to [every event](EveryEvent).
#[derive(Debug)]
pub struct LiveEvents {
    events: broadcast::Receiver<EngineEvent>,
}

impl LiveEvents {
    /// Waits for the next event.
    ///
    /// Giving up the wait loses nothing.
    ///
    /// # Errors
    ///
    /// - [`LiveEventsError::Missed`] when the listener fell behind. It says how many events are gone,
    ///   and the next call returns the oldest event that the engine still keeps.
    /// - [`LiveEventsError::EngineStopped`] when the engine stopped, and the listener received every event it kept.
    pub async fn next(&mut self) -> Result<EngineEvent, LiveEventsError> {
        match self.events.recv().await {
            Ok(event) => Ok(event),
            Err(RecvError::Lagged(events)) => Err(LiveEventsError::Missed { events }),
            Err(RecvError::Closed) => Err(LiveEventsError::EngineStopped),
        }
    }
}

/// Every event of an engine, for a listener that keeps what happened: a recorder.
///
/// [`Engine::every_event`](crate::Engine::every_event) makes one.
/// It receives the events from the moment it was made, in the order in which the sessions handed them over.
/// Every listener of an engine sees that same order.
///
/// It misses nothing, however slow it is, and no session waits for it.
/// The price is memory, and no time:
/// the events that the listener did not take yet stay in the engine, without a limit.
/// A listener that is slow makes the engine hold more, and one that stops taking events makes it hold them
/// until the process has no memory left.
/// Whoever makes one takes its events, or lets go of it.
#[derive(Debug)]
pub struct EveryEvent {
    events: mpsc::UnboundedReceiver<EngineEvent>,
}

impl EveryEvent {
    /// Waits for the next event, or returns `None` once the engine stopped and the listener took every event.
    ///
    /// Giving up the wait loses nothing.
    pub async fn next(&mut self) -> Option<EngineEvent> {
        self.events.recv().await
    }
}

/// Where the sessions of an engine hand their events over, and where the listeners get them.
#[derive(Debug)]
pub(crate) struct EventBus {
    /// The listeners that miss nothing, each with the events it did not take yet.
    ///
    /// A session holds the lock while it hands one event to every listener, so all of them see one order.
    /// Handing over never waits for a listener: the lock is free again after a few moves in memory.
    every_event: Mutex<Vec<mpsc::UnboundedSender<EngineEvent>>>,
    live: broadcast::Sender<EngineEvent>,
}

impl EventBus {
    /// A bus that keeps `backlog` events for a live listener that falls behind.
    pub fn new(backlog: EventBacklog) -> Self {
        let (live, _) = broadcast::channel(backlog.events());
        Self {
            every_event: Mutex::default(),
            live,
        }
    }

    /// Hands `event` to every listener. It never waits for one.
    pub fn publish(&self, event: EngineEvent) {
        // The list is whole whenever the lock is free, also after a panic of the task that held it.
        let mut every_event = self.every_event.lock().unwrap_or_else(PoisonError::into_inner);
        // A listener that someone let go of takes no event, and leaves the list.
        every_event.retain(|listener| listener.send(event.clone()).is_ok());
        // An event that no live listener waits for is gone, which is what live means.
        let _ = self.live.send(event);
    }

    /// A listener of what happens now: it receives the events from now on, and misses the oldest when it falls
    /// further behind than the backlog.
    pub fn listen_live(&self) -> LiveEvents {
        LiveEvents {
            events: self.live.subscribe(),
        }
    }

    /// A listener that misses nothing: it receives every event from now on, however far it falls behind.
    pub fn listen_to_every_event(&self) -> EveryEvent {
        let (listener, events) = mpsc::unbounded_channel();
        let mut every_event = self.every_event.lock().unwrap_or_else(PoisonError::into_inner);
        every_event.push(listener);
        EveryEvent { events }
    }
}

#[cfg(test)]
mod tests {
    use gaslightmq_incident::Incident;

    use super::*;

    fn rebalance(group: &str) -> EngineEvent {
        EngineEvent::IncidentFired(Incident::Rebalance {
            group: group.parse().unwrap(),
        })
    }

    fn group_of(event: &EngineEvent) -> String {
        match event {
            EngineEvent::IncidentFired(Incident::Rebalance { group }) => group.to_string(),
            other => panic!("{other:?} is no rebalance"),
        }
    }

    #[tokio::test]
    async fn every_listener_receives_the_events_from_the_moment_it_listens() {
        let bus = EventBus::new(EventBacklog::default());
        bus.publish(rebalance("before"));
        let mut live = bus.listen_live();
        let mut keeping = bus.listen_to_every_event();

        bus.publish(rebalance("payments"));
        bus.publish(rebalance("analytics"));

        assert_eq!(group_of(&live.next().await.unwrap()), "payments");
        assert_eq!(group_of(&live.next().await.unwrap()), "analytics");
        assert_eq!(group_of(&keeping.next().await.unwrap()), "payments");
        assert_eq!(group_of(&keeping.next().await.unwrap()), "analytics");
    }

    #[tokio::test]
    async fn a_live_listener_that_falls_behind_learns_how_many_events_it_missed() {
        let bus = EventBus::new(EventBacklog::new(2).unwrap());
        let mut listener = bus.listen_live();

        for group in ["a", "b", "c", "d", "e"] {
            bus.publish(rebalance(group));
        }

        assert_eq!(
            listener.next().await.unwrap_err(),
            LiveEventsError::Missed { events: 3 }
        );
        assert_eq!(group_of(&listener.next().await.unwrap()), "d");
        assert_eq!(group_of(&listener.next().await.unwrap()), "e");
    }

    #[tokio::test]
    async fn a_listener_of_every_event_misses_nothing_however_far_it_falls_behind() {
        let bus = EventBus::new(EventBacklog::new(2).unwrap());
        let mut live = bus.listen_live();
        let mut keeping = bus.listen_to_every_event();
        let groups: Vec<String> = (1..=500).map(|number| format!("group-{number}")).collect();

        // Nobody takes an event while the sessions hand over two hundred and fifty times the backlog.
        for group in &groups {
            bus.publish(rebalance(group));
        }

        assert_eq!(live.next().await.unwrap_err(), LiveEventsError::Missed { events: 498 });
        for group in &groups {
            assert_eq!(group_of(&keeping.next().await.unwrap()), *group);
        }
    }

    #[tokio::test]
    async fn a_listener_that_someone_let_go_of_holds_nothing_back() {
        let bus = EventBus::new(EventBacklog::default());
        let gone = bus.listen_to_every_event();
        let mut staying = bus.listen_to_every_event();
        drop(gone);

        bus.publish(rebalance("payments"));

        assert_eq!(bus.every_event.lock().unwrap().len(), 1);
        assert_eq!(group_of(&staying.next().await.unwrap()), "payments");
    }

    #[tokio::test]
    async fn a_listener_of_an_engine_that_stopped_gets_what_was_kept_and_then_the_end() {
        let bus = EventBus::new(EventBacklog::default());
        let mut live = bus.listen_live();
        let mut keeping = bus.listen_to_every_event();
        bus.publish(rebalance("payments"));

        drop(bus);

        assert_eq!(group_of(&live.next().await.unwrap()), "payments");
        assert_eq!(live.next().await.unwrap_err(), LiveEventsError::EngineStopped);
        assert_eq!(group_of(&keeping.next().await.unwrap()), "payments");
        assert!(keeping.next().await.is_none());
    }
}
