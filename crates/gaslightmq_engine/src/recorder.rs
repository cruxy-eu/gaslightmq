//! The recorder of one engine: the recording that runs, the queue in front of it, and the thread that writes it.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use gaslightmq_call::{Call, CallId};
use gaslightmq_clock::Clock;
use gaslightmq_environment::RecordingMode;
use gaslightmq_journal::{Change, ChangeSource, JournalEntry, RecordedName, RecordingStopCause};
use gaslightmq_pipeline as pipeline;
use gaslightmq_recording::{
    ActiveScenario, CallFrames, Closer, Description, End, Ending, Entry, RecordedBackend, RecordedSessionEvent,
    RecordingId, SessionChange, StartError, UtcInstant,
};
use gaslightmq_session::{CallIds, ClosedBy};
use tokio::sync::{mpsc, oneshot};

use crate::events::EventBus;
use crate::open_sessions::OpenSessions;
use crate::recordings::Writer;
use crate::{
    ArmedRules, EngineEvent, Journal, OpenSession, RecorderConfiguration, Recordings, SessionEvent,
    StartRecordingError, StopRecordingError,
};

mod queue;

pub(crate) use queue::Queue;
pub(crate) use queue::Waiting;
use queue::{QueueBounds, Queued};

/// What whoever starts a recording says about it. The engine adds the rest of its [description](Description).
///
/// ```
/// use gaslightmq_engine::prelude::*;
/// use gaslightmq_environment::RecordingMode;
/// use gaslightmq_journal::prelude::*;
/// use gaslightmq_recording::prelude::*;
///
/// let start = RecordingStart {
///     environment: "orders-sim".parse().expect("an environment name"),
///     started: "2026-11-21T14:02:11.527Z".parse().expect("an instant"),
///     mode: RecordingMode::Full,
///     backend: RecordedBackend::Upstream,
///     from: ChangeSurface::Test.into(),
/// };
/// assert_eq!(start.recording().to_string(), "orders-sim.2026-11-21T14-02-11.527Z");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingStart {
    /// The name of the environment of the engine.
    pub environment: RecordedName,
    /// The instant the recording starts, in UTC and to the millisecond: whoever starts it reads the wall clock.
    pub started: UtcInstant,
    /// How much of a call the recording keeps.
    pub mode: RecordingMode,
    /// What stands behind the proxy.
    pub backend: RecordedBackend,
    /// Where the start comes from. A recording that stops by itself, because the recorder fell behind or could not
    /// write, records that in the journal with the surface of its start, as the stop that never came would have had.
    pub from: ChangeSource,
}

impl RecordingStart {
    /// The ID of the recording that this start begins.
    pub fn recording(&self) -> RecordingId {
        RecordingId::new(self.environment.clone(), self.started)
    }
}

/// The recorder of an engine: it starts and stops the recordings of its environment, one at a time.
///
/// [`Engine::recorder`](crate::Engine::recorder) hands it out. The type is a handle, and a clone leads to the same
/// recorder. A handle keeps no engine running: once the engine stopped, a recording does not start.
///
/// The recording that runs takes the events of the sessions, the entries of the journal, and every call whose
/// request arrives while it runs, with its frames. A thread of its own writes it, behind a queue that no session
/// waits for. See [recordings](crate#recordings).
#[derive(Clone)]
pub struct Recorder {
    state: Arc<RecorderState>,
    /// The events of the engine, which end when the engine lets go of them. A handle does not hold them open.
    events: Weak<EventBus>,
    journal: Journal,
    clock: Arc<dyn Clock + Send + Sync>,
    call_ids: CallIds,
    rules: ArmedRules,
    open: Arc<OpenSessions>,
}

/// What every handle of a recorder shares: whether a recording runs, and the way to it.
pub(crate) struct RecorderState {
    recordings: Recordings,
    configuration: RecorderConfiguration,
    /// Whether a recording runs. A session reads it before every input it hands to its pipeline.
    runs: AtomicBool,
    running: Mutex<Option<Running>>,
    /// Held by a start or a stop, from the check that a recording runs until it runs or stopped.
    changing: tokio::sync::Mutex<()>,
}

/// The recording that runs.
struct Running {
    recording: RecordingId,
    queue: Arc<Queue>,
    /// The word of the recorder once it wrote the end, or what stood in the way.
    done: oneshot::Receiver<Result<End, String>>,
}

/// What a recorder works with, in the engine it records.
pub(crate) struct EngineParts {
    pub events: Weak<EventBus>,
    pub journal: Journal,
    pub clock: Arc<dyn Clock + Send + Sync>,
    pub call_ids: CallIds,
    pub rules: ArmedRules,
    pub open: Arc<OpenSessions>,
}

impl Recorder {
    /// The recorder of the engine that `engine` is a part of, which writes into `recordings`.
    pub(crate) fn new(recordings: Recordings, configuration: RecorderConfiguration, engine: EngineParts) -> Self {
        let EngineParts {
            events,
            journal,
            clock,
            call_ids,
            rules,
            open,
        } = engine;
        let state = Arc::new(RecorderState {
            recordings,
            configuration,
            runs: AtomicBool::new(false),
            running: Mutex::new(None),
            changing: tokio::sync::Mutex::new(()),
        });
        Self {
            state,
            events,
            journal,
            clock,
            call_ids,
            rules,
            open,
        }
    }

    /// Starts a recording of the environment of the engine, and returns its ID.
    ///
    /// The recording begins with an `opened` event for every session that is open, with what it learned by then,
    /// and with the entries of the journal since the last one that its description names. From then on it takes the
    /// events of the sessions, the entries of the journal, and every call whose request arrives from now on, with its
    /// frames. The recorder adds to `start` what the engine knows itself: the instant on its clock, the rules that
    /// are armed, the last entry of its journal, and the version of GaslightMQ.
    ///
    /// The recorder writes on a thread of its own, behind a queue of what it did not write yet, and no session
    /// waits for it. The queue holds as many entries as
    /// [`queue_entries`](crate::RecorderConfiguration::queue_entries) says, and frames of no more bytes than
    /// [`queue_size`](crate::RecorderConfiguration::queue_size). When it is full, the recording stops by itself:
    /// the journal records why, which is the last entry of the recording, and the recording ends.
    /// A recorder that cannot write stops the recording too, and the journal says so.
    /// Either way the events hand over an [`EngineEvent::RecordingStopped`].
    ///
    /// # Errors
    ///
    /// - [`StartRecordingError::AlreadyRecording`] when a recording runs already.
    /// - [`StartRecordingError::StorageFailed`] when the storage has a recording with its ID or did not keep it, the
    ///   recorder did not start, or the engine stopped.
    pub async fn start(&self, start: RecordingStart) -> Result<RecordingId, StartRecordingError> {
        let state = &self.state;
        let _changing = state.changing.lock().await;
        if let Some(recording) = self.recording() {
            return Err(StartRecordingError::AlreadyRecording { recording });
        }
        let armed = self.rules.snapshot();
        let scenario = self.rules.scenario().map(|position| ActiveScenario {
            scenario: recorded(position.scenario.as_str()),
            step: position.step.map(|step| recorded(step.as_str())),
        });
        let RecordingStart {
            environment,
            started,
            mode,
            backend,
            from,
        } = start;
        let journal_after = self.journal.last();
        let description = Description {
            environment,
            started,
            clock: self.clock.now(),
            mode,
            backend,
            written_by: env!("CARGO_PKG_VERSION").to_owned(),
            armed: armed.iter().map(|rule| recorded(rule.name().as_str())).collect(),
            scenario,
            journal_after,
        };
        let recording = description.id();
        let writer = state.recordings.start(description).await.map_err(|error| match error {
            StartError::AlreadyThere => StartRecordingError::StorageFailed {
                recording: recording.clone(),
                reason: error.to_string(),
            },
            StartError::NotStarted { reason } => StartRecordingError::StorageFailed {
                recording: recording.clone(),
                reason,
            },
        })?;

        let Some(events) = self.events.upgrade() else {
            return Err(StartRecordingError::StorageFailed {
                recording,
                reason: "the engine stopped".to_owned(),
            });
        };
        let bounds = QueueBounds {
            entries: state.configuration.queue_entries,
            frame_bytes: state.configuration.queue_size,
        };
        let (queue, taken) = Queue::new(bounds);
        // The recording begins here: the sessions that are open, and the journal entries since its description,
        // come first, and nothing the engine hands over from now on passes it by.
        // A session that opens or closes meanwhile may be among the open sessions and hand its event over as well:
        // the recorder keeps the first event that opens a session, and nothing of a session that it never opened.
        let first_call = self.journal.holding(|recent| {
            events.attach(Arc::clone(&queue), Arc::clone(&self.clock), || {
                for event in opened(&self.open.list()) {
                    queue.offer(Waiting::Session(event));
                }
                let since = match journal_after {
                    Some(last) => recent.after(last).cloned().collect::<Vec<JournalEntry>>(),
                    None => recent.entries().cloned().collect(),
                };
                for entry in since {
                    queue.offer(Waiting::Journal(entry));
                }
                let first = NonZeroU64::new(self.call_ids.assigned().saturating_add(1));
                state.runs.store(true, Ordering::SeqCst);
                first.map_or(CallId::FIRST, CallId::from)
            })
        });

        let writing = Writing {
            recording: recording.clone(),
            mode,
            first_call,
            from,
            clock: Arc::clone(&self.clock),
            journal: self.journal.clone(),
            events: Weak::clone(&self.events),
            recorder: Arc::downgrade(state),
            recordings: state.recordings.clone(),
            queue: Arc::clone(&queue),
            configuration: state.configuration,
        };
        // The recording runs before its thread does, so a recorder that stops it at once finds it.
        let (finished, done) = oneshot::channel();
        state.recordings.runs(recording.clone(), Arc::clone(&queue));
        *state.running() = Some(Running {
            recording: recording.clone(),
            queue: Arc::clone(&queue),
            done,
        });
        if let Err(reason) = writing.spawn(writer, taken, finished) {
            events.detach(&queue);
            state.stopped_by_itself(&recording);
            state.recordings.ended(&recording);
            return Err(StartRecordingError::StorageFailed { recording, reason });
        }
        events.publish(EngineEvent::RecordingStarted(recording.clone()));
        Ok(recording)
    }

    /// Stops the recording that runs, and waits until its recorder wrote what waited for it, and its end.
    ///
    /// The recording holds the calls that ended before the stop, and none that ends after it.
    ///
    /// # Errors
    ///
    /// - [`StopRecordingError::NotRecording`] when no recording runs.
    /// - [`StopRecordingError::CutShort`] when the recorder could not write: the recording stopped all the same,
    ///   and it holds what its last commit covers.
    pub async fn stop(&self) -> Result<End, StopRecordingError> {
        self.state.stop(Ending::Stopped, &self.events).await
    }

    /// The recording that runs, if one does.
    pub fn recording(&self) -> Option<RecordingId> {
        self.state.running().as_ref().map(|running| running.recording.clone())
    }

    /// The storage of the recordings, to list and open them.
    pub fn recordings(&self) -> &Recordings {
        &self.state.recordings
    }

    /// The sessions of the engine that are open.
    pub(crate) const fn open_sessions(&self) -> &Arc<OpenSessions> {
        &self.open
    }

    /// Whether a recording runs at this moment, as a session tells its pipeline.
    pub(crate) fn runs(&self) -> pipeline::Recording {
        if self.state.runs.load(Ordering::SeqCst) {
            pipeline::Recording::Running
        } else {
            pipeline::Recording::Idle
        }
    }

    /// The engine stops: the recording that runs ends with it, once every session has ended.
    pub(crate) async fn environment_stopped(&self) {
        // A recorder that could not write said so in the journal.
        let _ = self.state.stop(Ending::EnvironmentStopped, &self.events).await;
    }
}

/// Writes the recording that runs.
impl fmt::Debug for Recorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recorder")
            .field("recording", &self.recording())
            .finish_non_exhaustive()
    }
}

impl RecorderState {
    /// Stops the recording that runs in `ending`, and waits until its recorder wrote what the queue holds and its end.
    async fn stop(&self, ending: Ending, events: &Weak<EventBus>) -> Result<End, StopRecordingError> {
        let _changing = self.changing.lock().await;
        let Some(running) = self.running().take() else {
            return Err(StopRecordingError::NotRecording);
        };
        let Running { recording, queue, done } = running;
        if let Some(events) = events.upgrade() {
            events.detach(&queue);
        }
        self.runs.store(false, Ordering::SeqCst);
        queue.end(ending);
        match done.await {
            Ok(Ok(end)) => Ok(end),
            Ok(Err(reason)) => Err(StopRecordingError::CutShort { recording, reason }),
            Err(_) => Err(StopRecordingError::CutShort {
                recording,
                reason: "the recorder ended before it wrote the end".to_owned(),
            }),
        }
    }

    /// The recorder of `recording` stopped it by itself: the engine records nothing any more.
    fn stopped_by_itself(&self, recording: &RecordingId) {
        let mut running = self.running();
        if running.as_ref().is_some_and(|running| running.recording == *recording) {
            *running = None;
            self.runs.store(false, Ordering::SeqCst);
        }
    }

    /// The recording that runs. It is whole whenever the lock is free, also after a panic of the task that held it.
    fn running(&self) -> MutexGuard<'_, Option<Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// An engine that someone lets go of ends its recording as an engine that stops: the recorder writes what waits,
/// and the end.
impl Drop for RecorderState {
    fn drop(&mut self) {
        if let Some(running) = self.running().take() {
            running.queue.end(Ending::EnvironmentStopped);
        }
    }
}

/// The name that a rule had, as a recording keeps it.
fn recorded(name: &str) -> RecordedName {
    match RecordedName::new(name) {
        Ok(name) => name,
        // The name of every rule follows the one rule for names, and so does a recorded name.
        Err(error) => unreachable!("the name of a rule is a name that a recording keeps: {error}"),
    }
}

/// The event of a session as a recording keeps it, at `at`.
///
/// An identification that named the software of the client is two events of a recording.
pub(crate) fn recorded_events(event: &SessionEvent, at: gaslightmq_clock::Instant) -> Vec<RecordedSessionEvent> {
    let recorded = |connection, change| RecordedSessionEvent { connection, at, change };
    match event {
        SessionEvent::Opened(session) => opened(std::slice::from_ref(session)),
        SessionEvent::Identified {
            connection,
            client,
            software,
        } => {
            let identified = SessionChange::Identified {
                client_id: client.clone(),
            };
            let named = software
                .clone()
                .map(|software| recorded(*connection, SessionChange::SoftwareNamed { software }));
            std::iter::once(recorded(*connection, identified))
                .chain(named)
                .collect()
        }
        SessionEvent::SoftwareNamed { connection, software } => {
            let software = software.clone();
            vec![recorded(*connection, SessionChange::SoftwareNamed { software })]
        }
        SessionEvent::Closed { connection, by } => {
            let by = match by {
                ClosedBy::Client => Closer::Client,
                ClosedBy::Backend => Closer::Backend,
                ClosedBy::Lie => Closer::Lie,
                ClosedBy::Proxy => Closer::Proxy,
            };
            vec![recorded(*connection, SessionChange::Closed { by })]
        }
    }
}

/// The `opened` events of `sessions`, each at the instant its session opened, with what it learned by then.
fn opened(sessions: &[OpenSession]) -> Vec<RecordedSessionEvent> {
    let opened = |session: &OpenSession| RecordedSessionEvent {
        connection: session.connection(),
        at: session.opened(),
        change: SessionChange::Opened {
            address: session.address(),
            endpoint: session.endpoint(),
            client_id: session.client().cloned(),
            software: session.software().cloned(),
        },
    };
    sessions.iter().map(opened).collect()
}

/// The frames of a call as a recording takes them.
fn frames_of(frames: pipeline::KeptFrames) -> CallFrames {
    let pipeline::KeptFrames {
        request,
        upstream,
        truth,
        delivered,
    } = frames;
    let frames = CallFrames::new(request);
    let frames = upstream.into_iter().fold(frames, CallFrames::with_upstream);
    let frames = truth.into_iter().fold(frames, CallFrames::with_truth);
    delivered.into_iter().fold(frames, CallFrames::with_delivered)
}

/// What the thread of one recording writes with.
struct Writing {
    recording: RecordingId,
    mode: RecordingMode,
    /// The first call that the recording holds: a call with a lower ID arrived before it started.
    first_call: CallId,
    from: ChangeSource,
    clock: Arc<dyn Clock + Send + Sync>,
    journal: Journal,
    /// The events of the engine, which end when the engine lets go of them. The recorder does not hold them open.
    events: Weak<EventBus>,
    recorder: Weak<RecorderState>,
    recordings: Recordings,
    queue: Arc<Queue>,
    configuration: RecorderConfiguration,
}

/// What a recording holds so far.
#[derive(Debug, Default)]
struct Written {
    entries: u64,
    calls: u64,
    /// The sessions that the recording holds, by their connection.
    sessions: BTreeSet<gaslightmq_field::ConnectionNumber>,
    /// The bytes of the entries and frames since the last commit.
    uncommitted: u64,
    /// When the next commit is due, on the time of the system, once something waits for one.
    due: Option<tokio::time::Instant>,
}

/// Why a recorder stopped writing before the end.
struct Failed {
    reason: String,
}

impl Writing {
    /// Starts the thread of the recorder, which writes what it takes from `taken` with `writer`.
    ///
    /// The thread tells `finished` how the recording ended, or what stood in the way.
    fn spawn(
        self,
        writer: Box<dyn Writer>,
        taken: mpsc::UnboundedReceiver<Queued>,
        finished: oneshot::Sender<Result<End, String>>,
    ) -> Result<(), String> {
        let name = format!("recorder {}", self.recording);
        let thread = std::thread::Builder::new().name(name).spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build();
            let outcome = match runtime {
                Ok(runtime) => runtime.block_on(self.run(writer, taken)),
                Err(error) => {
                    let reason = format!("the recorder has no runtime: {error}");
                    self.could_not_write(&reason);
                    Err(reason)
                }
            };
            // Nobody waits for a recording that stopped by itself.
            let _ = finished.send(outcome);
        });
        match thread {
            Ok(_) => Ok(()),
            Err(error) => Err(format!("the recorder has no thread: {error}")),
        }
    }

    /// Takes what waits in the queue, and writes it, until the end of the recording.
    async fn run(self, mut writer: Box<dyn Writer>, mut taken: mpsc::UnboundedReceiver<Queued>) -> Result<End, String> {
        let mut written = Written::default();
        let mut falling_behind = false;
        loop {
            let due = written.due;
            let commit_is_due = async move {
                match due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let step = tokio::select! {
                biased;
                () = self.queue.fell_behind(), if !falling_behind => {
                    falling_behind = true;
                    self.fell_behind();
                    Ok(None)
                }
                queued = taken.recv() => match queued {
                    Some(queued) => {
                        self.queue.took(&queued);
                        self.take(queued.waiting, &mut *writer, &mut written).await
                    }
                    // Nobody hands anything over any more, and no end came: the engine is gone.
                    None => Ok(Some(Ending::EnvironmentStopped)),
                },
                () = commit_is_due => self.commit(&mut *writer, &mut written).await.map(|()| None),
            };
            match step {
                Ok(None) => {}
                Ok(Some(ending)) => return self.finish(writer, ending, &written).await,
                Err(Failed { reason }) => {
                    self.could_not_write(&reason);
                    return Err(reason);
                }
            }
        }
    }

    /// Writes one thing that the recorder took. Returns how the recording ends, when this is its end.
    async fn take(
        &self,
        waiting: Waiting,
        writer: &mut dyn Writer,
        written: &mut Written,
    ) -> Result<Option<Ending>, Failed> {
        let entry = match waiting {
            Waiting::Session(event) => {
                let known = match event.change {
                    SessionChange::Opened { .. } => written.sessions.insert(event.connection),
                    SessionChange::Closed { .. } => written.sessions.remove(&event.connection),
                    SessionChange::Identified { .. } | SessionChange::SoftwareNamed { .. } => {
                        written.sessions.contains(&event.connection)
                    }
                };
                // A session that the recording opened once, or never, has nothing more to say in it.
                if !known {
                    return Ok(None);
                }
                Entry::session(&event)
            }
            Waiting::Call(call, frames) => {
                if call.id() < self.first_call || !written.sessions.contains(&call.connection()) {
                    return Ok(None);
                }
                written.calls += 1;
                Entry::call(Call::clone(&call), frames_of(frames), self.mode).map_err(|error| Failed {
                    reason: error.to_string(),
                })?
            }
            Waiting::Journal(entry) => Entry::journal(&entry),
            Waiting::Commit(done) => {
                self.commit(writer, written).await?;
                // Whoever asked may have given up waiting.
                let _ = done.send(());
                return Ok(None);
            }
            Waiting::End(ending) => return Ok(Some(ending)),
        };
        let size = (entry.size() + entry.frames_size()) as u64;
        writer.append(entry).await.map_err(|error| Failed {
            reason: error.to_string(),
        })?;
        written.entries += 1;
        written.uncommitted = written.uncommitted.saturating_add(size);
        if written.uncommitted >= self.configuration.commit_size {
            self.commit(writer, written).await?;
        } else if written.due.is_none() {
            let interval = self.configuration.commit_interval.duration();
            written.due = Some(tokio::time::Instant::now() + interval);
        }
        Ok(None)
    }

    /// Commits what the recorder wrote since the last commit.
    async fn commit(&self, writer: &mut dyn Writer, written: &mut Written) -> Result<(), Failed> {
        written.due = None;
        if written.uncommitted == 0 {
            return Ok(());
        }
        writer.commit().await.map_err(|error| Failed {
            reason: error.to_string(),
        })?;
        written.uncommitted = 0;
        Ok(())
    }

    /// Commits, and ends the recording in `ending`.
    async fn finish(self, writer: Box<dyn Writer>, ending: Ending, written: &Written) -> Result<End, String> {
        let end = End {
            ending,
            at: self.clock.now(),
            entries: written.entries,
            calls: written.calls,
        };
        if let Err(error) = writer.finish(end).await {
            let reason = error.to_string();
            self.could_not_write(&reason);
            return Err(reason);
        }
        let why = (ending == Ending::FellBehind).then_some(RecordingStopCause::FellBehind);
        self.ended(why);
        Ok(end)
    }

    /// The queue was full: the recording stops, and the journal entry that says why is its last entry.
    fn fell_behind(&self) {
        let change = Change::RecordingStopped {
            why: RecordingStopCause::FellBehind,
        };
        self.journal
            .record_then(change, self.from, || self.stop_taking(Ending::FellBehind));
    }

    /// The recorder could not write: the recording stops, cut short at its last commit.
    /// The journal of the environment says why, and the recording does not.
    fn could_not_write(&self, reason: &str) {
        tracing::warn!(recording = %self.recording, reason, "the recorder could not write, and the recording stops");
        let change = Change::RecordingStopped {
            why: RecordingStopCause::CouldNotWrite,
        };
        self.journal
            .record_then(change, self.from, || self.stop_taking(Ending::Stopped));
        self.ended(Some(RecordingStopCause::CouldNotWrite));
    }

    /// The engine hands nothing over to the recording any more, and the queue ends in `ending`.
    fn stop_taking(&self, ending: Ending) {
        if let Some(events) = self.events.upgrade() {
            events.detach(&self.queue);
        }
        if let Some(recorder) = self.recorder.upgrade() {
            recorder.stopped_by_itself(&self.recording);
        }
        self.queue.end(ending);
    }

    /// The recording ended: nobody writes it any more, and whoever follows the engine learns of it.
    fn ended(&self, why: Option<RecordingStopCause>) {
        self.recordings.ended(&self.recording);
        if let Some(events) = self.events.upgrade() {
            let recording = self.recording.clone();
            events.publish(EngineEvent::RecordingStopped { recording, why });
        }
    }
}
