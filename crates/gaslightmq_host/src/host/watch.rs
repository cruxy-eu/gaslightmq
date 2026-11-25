//! The task that watches one environment: it counts its calls, and tells the changes of its sessions and its rules.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use gaslightmq_engine::{EngineEvent, EveryEvent, RuleChanges};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_rule::{RuleName, Snapshot};
use gaslightmq_traffic::Traffic;
use tokio::sync::broadcast;

use crate::HostChange;

/// Watches the environment `environment` until its proxy stopped and every event of it is taken.
///
/// | It takes | And |
/// |---|---|
/// | The record of a call | Counts it in `traffic`. |
/// | What happened to a session | Tells it as a [`HostChange::Session`]. |
/// | A snapshot of the armed rules | Tells a [`HostChange::RuleChanged`] for every rule that armed or disarmed since the one before. |
///
/// A rule can arm and disarm between two snapshots that the task takes, and then it tells nothing:
/// the rule is in the state it was, and every change is a state.
pub(super) async fn watch(
    environment: EnvironmentName,
    mut events: EveryEvent,
    rules: RuleChanges,
    traffic: Arc<Mutex<Traffic>>,
    changes: broadcast::Sender<HostChange>,
) {
    let tell = |change: HostChange| {
        // A change that nobody follows is gone.
        let _ = changes.send(change);
    };
    let mut rules = Some(rules);
    let mut armed: BTreeSet<RuleName> = BTreeSet::new();
    loop {
        tokio::select! {
            event = events.next() => match event {
                Some(EngineEvent::CallRecorded(call)) => {
                    traffic.lock().unwrap_or_else(PoisonError::into_inner).record(call);
                }
                Some(EngineEvent::Session(event)) => tell(HostChange::Session {
                    environment: environment.clone(),
                    event,
                }),
                // Whoever runs a scenario applies an incident, and no scenario runs yet.
                Some(_) => {}
                // The proxy stopped, and every event is taken.
                None => return,
            },
            snapshot = next_snapshot(&mut rules) => match snapshot {
                Some(snapshot) => {
                    let now = names(&snapshot);
                    for rule in armed.symmetric_difference(&now) {
                        tell(HostChange::RuleChanged {
                            environment: environment.clone(),
                            rule: rule.clone(),
                        });
                    }
                    armed = now;
                }
                // The armed rules are gone with the proxy, and the events tell the end.
                None => rules = None,
            },
        }
    }
}

/// The next snapshot of the armed rules, or `None` once they are gone. Waits forever once they are gone.
async fn next_snapshot(rules: &mut Option<RuleChanges>) -> Option<Arc<Snapshot>> {
    match rules {
        Some(rules) => rules.next().await,
        None => std::future::pending().await,
    }
}

/// The names of the armed rules of a snapshot.
fn names(snapshot: &Snapshot) -> BTreeSet<RuleName> {
    snapshot.iter().map(|rule| rule.name().clone()).collect()
}
