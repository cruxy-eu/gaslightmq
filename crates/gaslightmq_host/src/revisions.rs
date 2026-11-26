//! The revision of every rule in one environment: the number of its last change there.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use gaslightmq_rule::{RuleName, Snapshot};
use gaslightmq_workspace::{Revision, RevisionCounter};

/// The last change of every rule in one environment, each with the number it took.
///
/// A rule changes in an environment when the environment starts, when the rule arms or disarms,
/// and when a live rule is made or changes its definition.
/// Every such change takes the next number of the counter of the process, whoever makes it.
///
/// A person arms and disarms through the host, which [counts](Self::count) the change as it makes it.
/// A lifetime that ends and a budget that is spent disarm a rule inside the proxy, and nobody tells:
/// the host [catches up](Self::catch_up) with the armed rules whenever it looks, and counts what it finds.
/// Whoever reads or changes a rule holds the lock from the look at the armed rules until the change is counted,
/// so a revision that the host answered with is the revision of the rule until something else changes it.
#[derive(Debug)]
pub(crate) struct RuleRevisions {
    counter: RevisionCounter,
    /// The number that the start of the environment took, which is the last change of every rule that did not
    /// change there since.
    started: Revision,
    /// The number of the last change of each rule that changed since the environment started.
    changed: BTreeMap<RuleName, Revision>,
    /// The rules that were armed at the last change that was counted.
    armed: BTreeSet<RuleName>,
}

impl RuleRevisions {
    /// The rules of an environment that starts now: the start takes the next number of `counter`,
    /// and nothing is armed.
    pub(crate) fn started(counter: RevisionCounter) -> Self {
        let started = counter.next_revision();
        Self {
            counter,
            started,
            changed: BTreeMap::new(),
            armed: BTreeSet::new(),
        }
    }

    /// The number of the last change of the rule in this environment: of its state, and of its definition when it
    /// is a live rule. A rule of the workspace also has the revision of its definition, and its revision in the
    /// environment is the later of the two.
    pub(crate) fn of(&self, rule: &RuleName) -> Revision {
        self.changed.get(rule).copied().unwrap_or(self.started)
    }

    /// Counts every rule that armed or disarmed since the last look without anyone counting it:
    /// each takes the next number. Returns their names, for whoever tells the followers of the host.
    pub(crate) fn catch_up(&mut self, armed_now: &Snapshot) -> Vec<RuleName> {
        let now: BTreeSet<RuleName> = armed_now.iter().map(|rule| rule.name().clone()).collect();
        let uncounted: Vec<RuleName> = self.armed.symmetric_difference(&now).cloned().collect();
        for rule in &uncounted {
            self.changed.insert(rule.clone(), self.counter.next_revision());
        }
        self.armed = now;
        uncounted
    }

    /// Counts one change of a rule that the host just made: it takes the next number.
    /// `armed_now` holds the armed rules after the change.
    pub(crate) fn count(&mut self, rule: &RuleName, armed_now: &Snapshot) -> Revision {
        let revision = self.counter.next_revision();
        self.changed.insert(rule.clone(), revision);
        if armed_now.get(rule).is_some() {
            self.armed.insert(rule.clone());
        } else {
            self.armed.remove(rule);
        }
        revision
    }

    /// Counts the end of a live rule that left the environment: the delete takes the next number,
    /// and nothing has that revision afterwards.
    pub(crate) fn count_removed(&mut self, rule: &RuleName) -> Revision {
        self.changed.remove(rule);
        self.armed.remove(rule);
        self.counter.next_revision()
    }
}

/// The revisions of the rules of an environment. They are whole whenever the lock is free, also after a panic of the
/// task that held it: every change is one number taken and kept.
pub(crate) fn lock(revisions: &Mutex<RuleRevisions>) -> MutexGuard<'_, RuleRevisions> {
    revisions.lock().unwrap_or_else(PoisonError::into_inner)
}
