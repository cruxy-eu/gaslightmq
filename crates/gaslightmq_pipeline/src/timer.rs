//! Which wait of a session a timer ends.

use std::fmt;

use gaslightmq_call::CallId;
use gaslightmq_lie::Stage;

/// Which wait a timer ends: the call that waits, and the stage it waits at.
///
/// A delay before forwarding holds the request of a call back, and a delay after the response holds its answer back.
/// The pipeline asks for a timer with [`Output::SetTimer`](crate::Output::SetTimer),
/// and the engine reports it with [`Input::TimerFired`](crate::Input::TimerFired) under the same ID.
/// A call waits at most once per stage, so the two name every timer of a session.
///
/// The engine only keeps the ID and hands it back.
/// A timer that fires for a call that is gone, or that waits no more, changes nothing.
///
/// ```
/// use gaslightmq_call::prelude::*;
/// use gaslightmq_lie::prelude::*;
/// use gaslightmq_pipeline::prelude::*;
///
/// let call: CallId = "#7.3".parse()?;
/// let timer = TimerId::new(call, Stage::AfterResponse);
///
/// assert_eq!(timer.call(), call);
/// assert_eq!(timer.to_string(), "#7.3 after the response");
/// # Ok::<(), CallIdError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId {
    call: CallId,
    stage: Stage,
}

impl TimerId {
    /// The timer of the wait of `call` at `stage`.
    pub const fn new(call: CallId, stage: Stage) -> Self {
        Self { call, stage }
    }

    /// The call that waits.
    pub const fn call(&self) -> CallId {
        self.call
    }

    /// The stage the call waits at.
    pub const fn stage(&self) -> Stage {
        self.stage
    }
}

/// Writes the call and the stage: `#7.3 before forwarding`.
impl fmt::Display for TimerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.call, self.stage.phrase())
    }
}
