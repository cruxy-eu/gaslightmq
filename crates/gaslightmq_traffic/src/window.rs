//! The live window: the newest calls of an engine.

use std::collections::VecDeque;
use std::sync::Arc;

use gaslightmq_call::Call;
use gaslightmq_selector::EffectiveSelector;

use crate::LiveWindowSize;

/// The newest calls of an engine, in the order in which they were recorded.
///
/// A window keeps as many calls as its [size](LiveWindowSize) says.
/// A call that arrives at a full window pushes the oldest one out.
/// It is what someone sees who starts to watch: the calls that ended just before they looked.
///
/// [`Traffic::live_window`](crate::Traffic::live_window) gives the window,
/// and [`Traffic::record`](crate::Traffic::record) is the only way into it.
///
/// A record is shared and never copied: the window hands out the record that it was given.
#[derive(Debug, Clone)]
pub struct LiveWindow {
    size: LiveWindowSize,
    calls: VecDeque<Arc<Call>>,
}

impl LiveWindow {
    /// A window of that size, without a call.
    pub(crate) const fn new(size: LiveWindowSize) -> Self {
        Self {
            size,
            calls: VecDeque::new(),
        }
    }

    /// Keeps `call` as the newest one, and lets go of the oldest when the window is full.
    pub(crate) fn push(&mut self, call: Arc<Call>) {
        if self.calls.len() >= self.size.calls() {
            self.calls.pop_front();
        }
        self.calls.push_back(call);
    }

    /// How many calls the window keeps at most.
    pub const fn size(&self) -> LiveWindowSize {
        self.size
    }

    /// How many calls the window keeps now.
    pub fn len(&self) -> usize {
        self.calls.len()
    }

    /// Whether the window keeps no call yet.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// The calls of the window, from the oldest to the newest.
    ///
    /// Read from the back, they come newest first, which is the order of a stream on a screen.
    pub fn calls(&self) -> impl DoubleEndedIterator<Item = &Arc<Call>> + ExactSizeIterator {
        self.calls.iter()
    }

    /// The calls of the window that `selector` matches, from the oldest to the newest.
    ///
    /// It is what a new [subscription](crate::Traffic::subscribe) missed,
    /// and what a selector that nobody saved yet finds in the traffic of the last moments.
    pub fn matching<'a>(&'a self, selector: &'a EffectiveSelector) -> impl DoubleEndedIterator<Item = &'a Arc<Call>> {
        self.calls
            .iter()
            .filter(|call| selector.evaluate(call.as_ref()).is_matched())
    }
}
