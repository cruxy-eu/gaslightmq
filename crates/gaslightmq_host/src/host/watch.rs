//! The task that watches one environment: it counts its calls, and tells the changes of its sessions and its rules.

use std::sync::{Arc, Mutex};

use gaslightmq_engine::{ArmedRules, EngineEvent, EveryEvent, Journal, RuleChanges};
use gaslightmq_environment::EnvironmentName;
use tokio::sync::broadcast;

use crate::calls::{Watched, lock};
use crate::revisions::{self, RuleRevisions};
use crate::{HostChange, journaling};

/// Watches the environment `environment` until its proxy stopped and every event of it is taken.
///
/// | It takes | And |
/// |---|---|
/// | The record of a call | Counts it in `traffic`, which hands it to the followers of the calls its selectors match. |
/// | What happened to a session | Tells it as a [`HostChange::Session`]. |
/// | An entry of the journal | Tells it as a [`HostChange::Journaled`]. |
/// | A change of the armed rules | Counts every rule that disarmed without anyone counting it, so it takes its number in `revisions`, records it in `journal` with the surface `lifetime`, and tells a [`HostChange::RuleChanged`] for it. |
///
/// The host counts, records and tells what someone arms or disarms through it.
/// What is left for the task is what nobody asked the host for: a rule whose lifetime ended or whose budget is spent.
///
/// The task follows the armed rules `armed` from the moment of this call, before the environment takes its first
/// change, so it misses none of them.
pub(super) fn watch(
    environment: EnvironmentName,
    mut events: EveryEvent,
    armed: ArmedRules,
    journal: Journal,
    revisions: Arc<Mutex<RuleRevisions>>,
    traffic: Arc<Mutex<Watched>>,
    changes: broadcast::Sender<HostChange>,
) -> impl Future<Output = ()> + Send + 'static {
    let mut rules = Some(armed.changes());
    async move {
        let tell = |change: HostChange| {
            // A change that nobody follows is gone.
            let _ = changes.send(change);
        };
        loop {
            tokio::select! {
                event = events.next() => match event {
                    Some(EngineEvent::CallRecorded(call)) => lock(&traffic).record(&call),
                    Some(EngineEvent::Session(event)) => tell(HostChange::Session {
                        environment: environment.clone(),
                        event,
                    }),
                    Some(EngineEvent::Journaled(entry)) => tell(HostChange::Journaled {
                        environment: environment.clone(),
                        entry,
                    }),
                    // Whoever runs a scenario applies an incident, and no scenario runs yet.
                    Some(_) => {}
                    // The proxy stopped, and every event is taken: the followers of its calls get no more.
                    None => {
                        lock(&traffic).end();
                        return;
                    }
                },
                armed_or_disarmed = next_change(&mut rules) => match armed_or_disarmed {
                    Some(()) => {
                        // The armed rules are read under the lock, so a change that the host is counting is
                        // counted once, by the host.
                        let uncounted = {
                            let mut revisions = revisions::lock(&revisions);
                            journaling::catch_up(&mut revisions, &armed.snapshot(), &journal)
                        };
                        for rule in uncounted {
                            tell(HostChange::RuleChanged {
                                environment: environment.clone(),
                                rule,
                            });
                        }
                    }
                    // The armed rules are gone with the proxy, and the events tell the end.
                    None => rules = None,
                },
            }
        }
    }
}

/// Waits until the armed rules changed, or returns `None` once they are gone. Waits forever once they are gone.
async fn next_change(rules: &mut Option<RuleChanges>) -> Option<()> {
    match rules {
        Some(rules) => rules.next().await.map(|_| ()),
        None => std::future::pending().await,
    }
}
