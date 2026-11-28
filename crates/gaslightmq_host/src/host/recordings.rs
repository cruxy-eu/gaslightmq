//! The recordings of the environments: starting and stopping one, listing them, and opening one.

use gaslightmq_clock::UtcInstant;
use gaslightmq_engine::{
    ListRecordingsError, OpenRecordingError, RecordingStart, StartRecordingError, StopRecordingError, StoredRecording,
};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::ChangeSource;
use gaslightmq_recording::{End, RecordedBackend, RecordingId};
use gaslightmq_workspace::WorkspaceContext;

use super::Host;
use crate::{NotRunningError, RecordingStartError, RecordingStopError, journaling};

impl<C: WorkspaceContext> Host<C> {
    /// Starts a recording of an environment, and returns its ID.
    ///
    /// The ID is the environment and the instant of the [`UtcClock`](gaslightmq_clock::UtcClock) of the host, to the millisecond: the wall clock
    /// of the system, unless someone hands it [another](Self::with_utc_clock). A recording that would start at the
    /// millisecond of the one before it in the environment, or before it, starts at the millisecond after it.
    /// The recording keeps as much of a call as the [configuration](crate::EnvironmentConfiguration::recording) of
    /// the environment says. It runs until someone [stops](Self::stop_recording) it, its environment stops, or it
    /// stops by itself: its recorder fell behind, or could not write. The journal of the environment records the
    /// last of these with the surface of `from`, and nothing else of a start or a stop.
    ///
    /// Whoever follows the host learns of the start, and of the end.
    ///
    /// # Errors
    ///
    /// - [`RecordingStartError::NotRunning`] when no environment of that name runs.
    /// - [`RecordingStartError::AlreadyRecording`] when the environment records already.
    /// - [`RecordingStartError::StorageFailed`] when the storage did not keep the recording, or its recorder did not
    ///   start.
    pub async fn start_recording(
        &self,
        environment: &EnvironmentName,
        from: impl Into<ChangeSource>,
    ) -> Result<RecordingId, RecordingStartError> {
        let not_started = |reason: String| RecordingStartError::StorageFailed {
            environment: environment.clone(),
            reason,
        };
        let now = self.utc.utc_now();
        let (recorder, start) = {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(RecordingStartError::NotRunning {
                    environment: environment.clone(),
                });
            };
            // No two recordings of an environment share their instant: a start in the millisecond of the one before it,
            // or before that one, takes the millisecond after it.
            let after_the_last = running.last_recording.map(|last| last.unix_millis().saturating_add(1));
            let started = match after_the_last {
                Some(after) if after > now.unix_millis() => {
                    UtcInstant::new(after).map_err(|error| not_started(error.to_string()))?
                }
                _ => now,
            };
            running.last_recording = Some(started);
            let start = RecordingStart {
                environment: journaling::environment(environment),
                started,
                mode: running.recording,
                backend: RecordedBackend::Upstream,
                from: from.into(),
            };
            (running.engine.recorder().clone(), start)
        };
        recorder.start(start).await.map_err(|error| match error {
            StartRecordingError::AlreadyRecording { recording } => RecordingStartError::AlreadyRecording {
                environment: environment.clone(),
                recording,
            },
            StartRecordingError::StorageFailed { reason, .. } => not_started(reason),
        })
    }

    /// Stops the recording of an environment, and waits until its recorder wrote what waited for it, and its end.
    ///
    /// # Errors
    ///
    /// - [`RecordingStopError::NotRunning`] when no environment of that name runs.
    /// - [`RecordingStopError::NotRecording`] when the environment records nothing.
    /// - [`RecordingStopError::CutShort`] when the recorder could not write: the recording stopped all the same,
    ///   and it holds what its last commit covers.
    pub async fn stop_recording(&self, environment: &EnvironmentName) -> Result<End, RecordingStopError> {
        let recorder = self
            .with_running(environment, |running| running.engine.recorder().clone())
            .map_err(|NotRunningError { name }| RecordingStopError::NotRunning { environment: name })?;
        recorder.stop().await.map_err(|error| match error {
            StopRecordingError::NotRecording => RecordingStopError::NotRecording {
                environment: environment.clone(),
            },
            StopRecordingError::CutShort { recording, reason } => RecordingStopError::CutShort { recording, reason },
        })
    }

    /// The recording that an environment writes, if it writes one.
    ///
    /// # Errors
    ///
    /// [`NotRunningError`] when no environment of that name runs.
    pub fn recording(&self, environment: &EnvironmentName) -> Result<Option<RecordingId>, NotRunningError> {
        self.with_running(environment, |running| running.engine.recorder().recording())
    }

    /// The ID of every recording of the storage, of every environment, in their order: by the environment, and then
    /// by the instant it started. A recording that runs is among them.
    ///
    /// # Errors
    ///
    /// [`ListRecordingsError::StorageUnavailable`] when the storage did not answer.
    pub async fn recordings(&self) -> Result<Vec<RecordingId>, ListRecordingsError> {
        self.recordings.list().await
    }

    /// Opens a recording, and says what it is and how far it goes.
    ///
    /// A recording that an environment writes is committed first, so it holds everything that its environment
    /// handed over until the call.
    ///
    /// # Errors
    ///
    /// The [`OpenRecordingError`] of the storage: no recording has the ID, this build cannot read it, or the storage
    /// did not answer.
    pub async fn open_recording(&self, recording: &RecordingId) -> Result<StoredRecording, OpenRecordingError> {
        self.recordings.open(recording).await
    }
}
