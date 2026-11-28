//! The recordings of the environments: starting and stopping one, listing them, and opening one.

use std::time::{SystemTime, UNIX_EPOCH};

use gaslightmq_engine::{
    ListRecordingsError, OpenRecordingError, RecorderStartError, RecorderStopError, RecordingStart, StoredRecording,
};
use gaslightmq_environment::EnvironmentName;
use gaslightmq_journal::ChangeSource;
use gaslightmq_recording::{End, RecordedBackend, RecordingId, UtcInstant};
use gaslightmq_workspace::WorkspaceContext;

use super::Host;
use crate::{NotRunningError, StartRecordingError, StopRecordingError, journaling};

impl<C: WorkspaceContext> Host<C> {
    /// Starts a recording of an environment, and returns its ID.
    ///
    /// The ID is the environment and the instant on the wall clock of the system, to the millisecond. A recording
    /// that would start at the millisecond of the one before it in the environment starts at the next one.
    /// The recording keeps as much of a call as the [configuration](crate::EnvironmentConfiguration::recording) of
    /// the environment says. It runs until someone [stops](Self::stop_recording) it, its environment stops, or it
    /// stops by itself: its recorder fell behind, or could not write. The journal of the environment records the
    /// last of these with the surface of `from`, and nothing else of a start or a stop.
    ///
    /// Whoever follows the host learns of the start, and of the end.
    ///
    /// # Errors
    ///
    /// - [`StartRecordingError::NotRunning`] when no environment of that name runs.
    /// - [`StartRecordingError::Recording`] when the environment records already.
    /// - [`StartRecordingError::NotStarted`] when the storage did not keep the recording, or its recorder did not
    ///   start.
    pub async fn start_recording(
        &self,
        environment: &EnvironmentName,
        from: impl Into<ChangeSource>,
    ) -> Result<RecordingId, StartRecordingError> {
        let not_started = |reason: String| StartRecordingError::NotStarted {
            environment: environment.clone(),
            reason,
        };
        let since_1970 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let now = u64::try_from(since_1970.as_millis()).unwrap_or(u64::MAX);
        let (recorder, start) = {
            let mut environments = self.environments();
            let Some(running) = environments.get_mut(environment) else {
                return Err(StartRecordingError::NotRunning {
                    environment: environment.clone(),
                });
            };
            let after_the_last = running
                .last_recording
                .map_or(0, |last| last.unix_millis().saturating_add(1));
            let started = UtcInstant::new(now.max(after_the_last)).map_err(|error| not_started(error.to_string()))?;
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
            RecorderStartError::Recording { recording } => StartRecordingError::Recording {
                environment: environment.clone(),
                recording,
            },
            RecorderStartError::IdTaken { .. } | RecorderStartError::NotStarted { .. } => {
                not_started(error.to_string())
            }
        })
    }

    /// Stops the recording of an environment, and waits until its recorder wrote what waited for it, and its end.
    ///
    /// # Errors
    ///
    /// - [`StopRecordingError::NotRunning`] when no environment of that name runs.
    /// - [`StopRecordingError::NotRecording`] when the environment records nothing.
    /// - [`StopRecordingError::NotFinished`] when the recorder could not write: the recording stopped all the same,
    ///   and it holds what its last commit covers.
    pub async fn stop_recording(&self, environment: &EnvironmentName) -> Result<End, StopRecordingError> {
        let recorder = self
            .with_running(environment, |running| running.engine.recorder().clone())
            .map_err(|NotRunningError { name }| StopRecordingError::NotRunning { environment: name })?;
        recorder.stop().await.map_err(|error| match error {
            RecorderStopError::NotRecording => StopRecordingError::NotRecording {
                environment: environment.clone(),
            },
            RecorderStopError::NotFinished { recording, reason } => {
                StopRecordingError::NotFinished { recording, reason }
            }
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
    /// [`ListRecordingsError::NotListed`] when the storage did not answer.
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
