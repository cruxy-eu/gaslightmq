//! The calls the live traffic shows: a window of the newest ones.

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use gaslightmq_control_api::call::{Call, CallId};
use gaslightmq_control_api::field::ConnectionNumber;

/// The newest calls of the environment that the UI shows, as many as the window holds.
///
/// A call that arrives while someone reads the traffic waits, so the rows stand still,
/// and it joins the window when the traffic goes on. A window that is full lets its oldest call go.
#[derive(Debug, Clone, PartialEq)]
pub struct Traffic {
    /// The calls shown, the newest first.
    calls: VecDeque<Call>,
    /// The calls that arrived while the traffic stood still, the newest first.
    waiting: VecDeque<Call>,
    paused: bool,
    window: NonZeroUsize,
    /// How many calls arrived since the traffic was cleared, whether the window still holds them or not.
    received: u64,
}

impl Traffic {
    /// An empty window that holds `window` calls.
    pub fn new(window: NonZeroUsize) -> Self {
        Self {
            calls: VecDeque::new(),
            waiting: VecDeque::new(),
            paused: false,
            window,
            received: 0,
        }
    }

    /// The calls shown, the newest first.
    pub fn calls(&self) -> impl ExactSizeIterator<Item = &Call> {
        self.calls.iter()
    }

    /// The call with that ID, while the window holds it.
    pub fn call(&self, id: CallId) -> Option<&Call> {
        self.calls.iter().find(|call| call.id() == id)
    }

    /// How many calls arrived since the traffic was cleared.
    pub fn received(&self) -> u64 {
        self.received
    }

    /// How many calls the window shows of the connection.
    pub fn calls_of(&self, connection: ConnectionNumber) -> usize {
        self.calls.iter().filter(|call| call.connection() == connection).count()
    }

    /// Whether the rows stand still.
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// How many calls wait for the traffic to go on.
    pub fn waiting(&self) -> usize {
        self.waiting.len()
    }

    /// A call arrived.
    pub fn arrived(&mut self, call: Call) {
        self.received = self.received.saturating_add(1);
        let into = if self.paused {
            &mut self.waiting
        } else {
            &mut self.calls
        };
        into.push_front(call);
        into.truncate(self.window.get());
    }

    /// The rows stand still, and new calls wait.
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// The calls that waited join the window, and new calls show as they come.
    pub fn resume(&mut self) {
        self.paused = false;
        while let Some(call) = self.waiting.pop_back() {
            self.calls.push_front(call);
        }
        self.calls.truncate(self.window.get());
    }

    /// Forgets every call: the traffic shows other calls from here on.
    pub fn clear(&mut self) {
        self.calls.clear();
        self.waiting.clear();
        self.received = 0;
    }
}
